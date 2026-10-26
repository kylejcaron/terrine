# terrine

Terrine builds typed analytical datasets from semantic definitions, with
point-in-time features, post-anchor outcomes and explicit observation coverage.
It includes independent synthetic worlds whose oracle describes expected dataset rows.

## Install for use

Requires CPython 3.13 or 3.14. For a released distribution, install the local
DuckDB backend; add only the dataframe integrations you use:

```sh
uv pip install 'terrine[duckdb]'
uv pip install 'terrine[duckdb,pandas]'
uv pip install 'terrine[duckdb,polars]'
```

## Contribute from a checkout

Install [uv](https://docs.astral.sh/uv/) and sync the development environment:

```sh
uv sync --group dev --group docs --all-extras
make check
```

These `uv sync` commands install the current checkout, not a released package.
Tests run in three tiers. The ordinary suite (`tests/`) is the fast unit tier
that `make check` runs; run a focused selection with
`make test TESTS="tests/test_windows.py"`. `uv run pytest -q integration` runs
the integration tier, and `make test-scale` runs the scheduled scale tier,
which ordinary runs deselect. The `lint`, `typecheck`, and `fmt` targets run
Ruff/Vulture/pydoclint/Tach, ty, and Ruff formatting respectively, over the
package, `tests/` and `integration/`. `make complexity` reports complexity and
is informational, not part of the gate. `make check` runs static checks and the
unit tier.

## Build a study dataset

This complete example creates two users, including one with no events, and
follows **Model → Design → Frame → Dataset**. It uses a temporary, file-backed
DuckDB database; nothing is uploaded.

<!-- --8<-- [start:first-dataset] -->
```python
from pathlib import Path
from tempfile import TemporaryDirectory

import ibis
import terrine as tr

with TemporaryDirectory(prefix="terrine-first-") as directory:
    root = Path(directory)
    con = ibis.duckdb.connect(str(root / "analytics.duckdb"))
    try:
        con.raw_sql("CREATE TABLE users(user_id VARCHAR)")
        con.raw_sql("INSERT INTO users VALUES ('a'), ('b')")
        con.raw_sql("CREATE TABLE events(user_id VARCHAR, occurred_at TIMESTAMP)")
        con.raw_sql(
            "INSERT INTO events VALUES "
            "('a', '2025-01-01 12:00:00'), "
            "('a', '2025-01-02 00:00:00'), "
            "('a', '2025-01-02 12:00:00')"
        )
        catalog = root / "catalog.yaml"
        catalog.write_text(
            """schema_version: 1
day_boundary: UTC
dialect: duckdb
sources:
  sessions:
    relation: {kind: table, name: events}
    timestamp: occurred_at
    entities: {user: user_id}
    facts: [session]
    properties: []
    dimensions: []
facts:
  session: {source: sessions, column: null}
properties: {}
dimensions: {}
measures:
  session_count: {fact: session, aggregation: count}
metrics:
  sessions: {kind: mean, entity: user, measure: session_count}
""",
            encoding="utf-8",
        )
        model = tr.Model.from_catalog(
            catalog,
            con=con,
            units={"user": tr.UnitSource("users", "user_id")},
            readiness={"sessions": tr.Coverage("2025-01-01", "2025-01-03")},
        )
        design = tr.Design(
            "user",
            tr.FixedDate("2025-01-02"),
            features={"sessions_before": tr.agg("sessions", window=("-1d", "0s"))},
            outcomes={"sessions_after": tr.agg("sessions", window=("0s", "1d"))},
        )
        frame = design.build(model)  # Compile; do not read source rows.
        try:
            frame.materialize(tr.Budget(max_rows=1, strict_scan=False))
        except tr.TerrineError as error:
            assert error.code == "terrine.budget.rows_exceeded"
            print(f"Safe refusal: {error.code}; raise the explicit row limit to continue")
        else:
            raise AssertionError("the one-row output cap should refuse two rows")
        budget = tr.Budget(max_rows=10, max_output_bytes=1_000_000, strict_scan=False)
        result = frame.materialize(budget)
        # Optional durable lifecycle: publish and reopen the same bounded result.
        dataset = frame.stage("first_dataset", budget=budget)
        reopened = tr.Dataset.from_manifest(dataset.manifest, con=con)
        columns = ["unit", "sessions_before", "sessions_after"]
        rows = sorted(
            (
                row
                for batch in reopened.to_arrow_batches(batch_size=2)
                for row in batch.select(columns).to_pylist()
            ),
            key=lambda row: row["unit"],
        )
        assert rows == [
            {"unit": "a", "sessions_before": 1, "sessions_after": 2},
            {"unit": "b", "sessions_before": 0, "sessions_after": 0},
        ]
        assert sorted(
            result.table.select(columns).to_pylist(), key=lambda row: row["unit"]
        ) == rows
        print(rows)
    finally:
        con.disconnect()
```
<!-- --8<-- [end:first-dataset] -->

The event at exactly midnight belongs to the outcome, not the feature.
`materialize` collects this tiny Frame with typed metadata. Staging is a separate,
optional durable lifecycle: `stage` publishes a pinned generation and
`Dataset.from_manifest` reopens it. Keep the database and retained snapshots for
a persistent dataset instead of using a temporary directory. See the
[first-dataset guide](docs/guides/first-dataset.md) for the next steps and the
[catalog guide](docs/guides/temporal-engine.md) for source/SCD2 setup.

`Coverage` asserts source completeness; it is not inferred from the latest event.
Windows are half-open. An unavailable feature/outcome retains a numeric null and
its exact reason; an observed zero is different. Unknown eligibility prevents
export rather than silently dropping a row. Only sources selected by the design
need readiness declarations, even when the original catalog also defines labels.

`frame.materialize(budget)` returns a `MaterializedFrame` with roles, column
metadata, eligibility totals and execution provenance. `to_pandas` uses nullable
Arrow-backed types; `to_polars` and `to_pandas` require their respective extras.
Arrow exports carry the same metadata. `frame.expr` is an explicitly unsafe Ibis
escape hatch: executing it directly bypasses validation, budgets and provenance.

See [library hand-offs](docs/guides/library-handoffs.md) for preflight-backed
DoubleML, DoWhy and same-connection duckreg workflows, plus lifelines and diff-diff
dataset conversions. Downstream libraries are optional; Terrine makes no causal
estimates or identification claims.

## Handling a real output refusal

The first-dataset example deliberately tries a one-row output limit against two
roster users, catches `terrine.budget.rows_exceeded`, then recovers with an
explicit larger row and byte budget. Limits abort rather than return truncated
success. They do **not** bound source scans, query work, backend memory or
retained inputs. Native scan costs are unknown; strict `max_scan_bytes` therefore
refuses before integrity reads. `tr.preview(design, model, bounded_count=1_000)`
reads capped candidate diagnostics, not example rows; capped results do not claim
complete cohort totals. Its integrity checks can still scan upstream data.

## Restricted dataset tools

`terrine.agent` exposes owner-configured dataset discovery, typed Design and
training/scoring definition edits, metadata-only cost preview, immutable execution
receipts, pinned staging/refresh, separately authorized Parquet exports and
bounded redacted previews. `AgentPolicy` independently controls preparation,
writes, exports, row preview, run reads and cancellation; inspection never grants
these operations. Runtime status and bounded events resolve the saved owner
invocation before access. Responses omit raw SQL, source snapshots and connection
details, and all calls share the session request cap.

Run the local restricted training workflow with
`uv run python examples/dataset_agent_tools.py`. It prints safe aggregate metadata
only. See [dataset agent tools](docs/guides/dataset-agent-tools.md) for exact
permissions, unknown scan-cost behavior, privacy boundaries and refusal routes.
Optional MCP transport remains owner-gated and is not installed or exposed by
this Python interface.

## Durable materialization runtime

See the [materialization runtime guide](docs/guides/materialization-runtime.md)
for saved-plan CLI commands, owner IPC, consistency guarantees, and correction workflow.

The runtime accepts an immutable `RefreshPlan` and delegates task execution and
publication to an owner-configured `Executor`. The executor owns physical source
reads and publication; request keys are durable and scoped to an explicit
`DestinationRef`. See `examples/materialization_runtime.py` for the DuckDB
execution path. A `RunReceipt` pins the published generation for consumer reads:

```python
from terrine.runtime import open_published_generation, run_plan

receipt = run_plan(
    plan,
    con=state_connection,
    namespace="owner",
    request_key="orders-correction-2026-06-17",
    destination=destination,
    executor=executor,
)
published = open_published_generation(receipt, con=state_connection, namespace="owner")
result = published.execute()
```

Do not infer correction intent from source freshness: declare a correction,
construct and save a new plan, and use a distinct request key. Query access is
bound to the receipt's exact generation even after a newer generation becomes
current. The durable DuckDB owner serializes publication and cancellation;
separate processes must use owner IPC while that owner holds the database.
Cancellation is a durable request, not proof that backend work stopped.

## Optional orchestration triggers

See the [orchestrator guide](docs/guides/orchestrators.md) for owner IPC,
partition and retry behavior, and the local DuckDB dbt lifecycle. Runnable
examples: [Airflow](examples/orchestrators/airflow_materialization.py),
[Dagster](examples/orchestrators/dagster_materialization.py), and
[dbt Core after-build](examples/orchestrators/dbt_core_after_build.py).

### Owned inputs for repeated builds

Ordinary native builds use one private transaction without taking over a caller
transaction. For a stable, runtime-owned copy of the complete input frontier:

```python
from terrine.execution.inputs import publish_native_inputs

with publish_native_inputs(model.catalog, con=con, units=model.units) as stored:
    binding = model.catalog.verify_immutable_inputs(
        stored.con, units=model.units, relations=stored.relations
    )
    pinned = tr.Model.from_catalog(
        model.catalog,
        con=stored.con,
        units=model.units,
        readiness=model.readiness.sources,
        event_ids=model.event_ids,
        input_binding=binding,
    )
    result = design.build(pinned).materialize(budget)
```

Keep the publication open for new builds. An execution already holding a lease
can finish after owner closure; later builds refuse. The managed artifact is
read-only and cleaned up after the last lease. A stored SQL root substitutes its
whole result, not a fresh query against mutable upstream tables.

Ordinary builds require a dedicated native connection with no active caller
transaction or unresolved TEMP inputs. Explicit publication can capture admitted
owner-local TEMP roots. Ambiguous partial names have an exact route:
declare fully qualified source SQL such as `SELECT * FROM "analytics"."main"."events"`.
Owner-staged named tables are the route for unproven functions or data-dependent
schema inference. Explicit single-root file-reader capture is available through
publication; arbitrary side-effecting queries are not admitted.

A Model pins its admitted relation definitions and schemas. Rebind after a schema
or view-definition change; ordinary row updates are captured at safe execution.

Concurrent exports from one Model coordinate shared-origin metadata and setup;
their private snapshot queries remain independent. Do not use the origin
connection externally during setup or capture. SQL retains the caller's timezone,
while exported anchor timestamps preserve their instants in UTC Arrow columns.

These are local execution identities, not portable snapshot/replay certificates.
Ordinary source generations and snapshot IDs remain unknown; managed copies have
issued generations, while replay equivalence is still `unverified`.

## Window contract

Window endpoints represent elapsed durations, not calendar dates.

## Build directly from PostgreSQL, BigQuery or Snowflake

With `terrine[postgres]`, `terrine[bigquery]` or `terrine[snowflake]`, a Model
can bind an Ibis PostgreSQL, BigQuery or Snowflake connection and
`Frame.materialize()` reads the source tables in one consistent snapshot (a
Time Travel instant on Snowflake), without copying them out first. Counts, exact
integer/decimal sums, as-of attributes and the panel and time-to-event shapes
produce the same rows as DuckDB; averages, ratios and float windows match it
within a tight tolerance. Staging and publishing Dataset generations stay on
DuckDB. [`examples/postgres/dataset.py`](examples/postgres/dataset.py),
[`examples/bigquery/dataset.py`](examples/bigquery/dataset.py) and
[`examples/snowflake/dataset.py`](examples/snowflake/dataset.py) are complete
examples, and [backend compatibility](docs/guides/backend-compatibility.md#postgresql)
lists exactly what is supported on each.

## Guides

- [All guides and references](docs/guides/index.md)
- [Capability matrix](docs/guides/capabilities.md)
- [Cost and data residency](docs/guides/cost-and-residency.md)
- [Inherent limits](docs/guides/limitations.md)
- [Refusal-code reference](docs/reference/refusals.md)

## License

Terrine is licensed under the [Apache License 2.0](LICENSE); see also
[NOTICE](NOTICE). Contributions require signing the Contributor License
Agreement; `CONTRIBUTING.md` explains how.
