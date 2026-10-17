# terrine

Terrine builds typed analytical datasets from semantic definitions, with
point-in-time features, post-anchor outcomes and explicit observation coverage.
It also includes independent synthetic worlds and baseline-treatment ATE estimation.

## Install and develop

Requires Python 3.13 or 3.14 and [uv](https://docs.astral.sh/uv/).

```sh
uv sync --group dev --extra duckdb --extra pandas
make check
```

The ordinary suite excludes tests marked `slow` or `parameter_recovery`. Run a
focused selection with `make test TESTS="tests/test_windows.py"`. The `lint`,
`typecheck`, and `fmt` targets run Ruff/Vulture/pydoclint/Tach, ty, and Ruff
formatting respectively. `make complexity` reports complexity and is
informational, not part of the gate. `make check` runs static checks and the
ordinary test suite.

Explicit expensive tiers use `make test-slow TESTS=...` and
`make test-calibration TESTS=...`. Keep the ordinary feedback loop under
10 seconds on a warm local environment as an initial development budget,
not a per-test assertion. The foundation selection measured 0.94 seconds
(21 cases) on Python 3.13/macOS; investigate sustained regressions and
revisit the budget from measurements as capabilities grow.

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

Output row/byte limits abort instead of returning truncated success. They do
**not** bound source scans, query work or peak memory. Native scan costs are
unknown; a strict `max_scan_bytes` therefore refuses before integrity reads.
`tr.preview(design, model, bounded_count=1_000)` reads capped candidate diagnostics,
not example rows; capped results do not claim complete cohort totals. Its
integrity checks can still scan upstream data.

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

Use a dedicated native connection with no active caller transaction, TEMP sources
or unsupported resolution state. Ambiguous partial names have an exact route:
declare fully qualified source SQL such as `SELECT * FROM "analytics"."main"."events"`.
Owner-staged named tables are the route for unproven functions or data-dependent
schema inference. Explicit single-root file-reader capture is available through
publication; arbitrary side-effecting queries are not admitted.

A Model pins its admitted relation definitions and schemas. Rebind after a schema
or view-definition change; ordinary row updates are captured at safe execution.

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

- [Synthetic worlds and independent truth](docs/guides/synthetic-worlds.md)
- [Native baseline ATE estimation](docs/guides/baseline-estimation.md)
