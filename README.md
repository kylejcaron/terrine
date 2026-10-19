# terrine

Terrine builds typed analytical datasets from semantic definitions, with
point-in-time features, post-anchor outcomes and explicit observation coverage.
It includes independent synthetic worlds whose oracle describes expected dataset rows.

## Install and develop

Requires Python 3.13 or 3.14 and [uv](https://docs.astral.sh/uv/).

```sh
uv sync --group dev --extra duckdb --extra pandas
make check
```

The ordinary suite excludes tests marked `slow`. Run a
focused selection with `make test TESTS="tests/test_windows.py"`. The `lint`,
`typecheck`, and `fmt` targets run Ruff/Vulture/pydoclint/Tach, ty, and Ruff
formatting respectively. `make complexity` reports complexity and is
informational, not part of the gate. `make check` runs static checks and the
ordinary test suite.

The explicit expensive tier uses `make test-slow TESTS=...`. Keep the
ordinary feedback loop under 10 seconds on a warm local environment as an
initial development budget, not a per-test assertion. The foundation selection
measured 0.94 seconds (21 cases) on Python 3.13/macOS; investigate sustained
regressions and revisit the budget from measurements as capabilities grow.

## Build a study dataset

Install the DuckDB extra. The [native catalog example](docs/guides/temporal-engine.md)
defines a `sessions` source and metric. Connect it to an existing table with
`occurred_at`, `user_id` and a stable `session_id`:

```python
import ibis
import terrine as tr

con = ibis.duckdb.connect("analytics.duckdb", read_only=True)
model = tr.Model.from_catalog(
    "catalog.yaml",
    con=con,
    readiness={"sessions": tr.Coverage("2026-06-01", "2026-07-31")},
    event_ids={"sessions": "session_id"},
)
design = tr.Design(
    "user",
    tr.EventAnchor("session"),
    features={"sessions_before": tr.agg("sessions", window=("-7d", "0s"))},
    outcomes={"sessions_after": tr.agg("sessions", window=("0s", "7d"))},
)

frame = design.build(model)          # Compile; do not read source rows.
report = tr.preview(design, model)   # Metadata only; unknown costs stay unknown.
budget = tr.Budget(max_rows=100_000, max_output_bytes=128 * 1024**2)
arrow = frame.to_pyarrow(budget=budget)
```

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
DoubleML, DoWhy and same-connection duckreg recipes, plus lifelines and diff-diff
dataset conversions. Downstream libraries are optional; Terrine makes no causal
estimates or identification claims.

Output row/byte limits abort instead of returning truncated success. They do
**not** bound source scans, query work or peak memory. Native scan costs are
unknown; a strict `max_scan_bytes` therefore refuses before integrity reads.
`tr.preview(design, model, bounded_count=1_000)` reads capped candidate diagnostics,
not example rows; capped results do not claim complete cohort totals. Its
integrity checks can still scan upstream data.

## Restricted dataset tools

`terrine.agent` exposes owner-configured dataset discovery, typed Design and
training/scoring recipe edits, metadata-only cost preview, immutable execution
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

## Current pure contracts

```python
from terrine import TerrineError, Window

window = Window("-7d", "0s")  # normalized elapsed seconds, half-open bounds
try:
    raise TerrineError(
        "Unsupported operation",
        code="terrine.unsupported",
        context={},
    )
except TerrineError as error:
    print(error.code)
```

Window endpoints represent elapsed durations, not calendar dates.

## Guides

- [Synthetic dataset worlds and reference rows](docs/guides/synthetic-worlds.md)
- [Shared dataset contract with increment](docs/guides/shared-dataset-contract.md)
- [Bounded state panels](docs/guides/state-panels.md)
