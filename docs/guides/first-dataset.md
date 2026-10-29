# Your first dataset

Requires CPython 3.13 or 3.14 and the `duckdb,pandas` extras for this example.
For an installed distribution:

```sh
uv pip install 'terrine[duckdb,pandas]'
```

No sibling project, warehouse account or semantic adapter is required. The
complete executable tutorial lives in the
[README](../../README.md#example-a-customer-spend-forecasting-table). To run it from a source checkout:

```sh
uv sync --extra duckdb --extra pandas
uv run python examples/first_dataset.py
```

--8<-- "README.md:first-dataset"
## Four objects

1. **Model** binds event tables with `Model.from_tables`, an authoritative unit
   roster and explicit completeness. Name the metrics you will reference
   (`tr.count()`, `tr.sum("amount")`), declare `labels=` for columns you filter
   or look up, and `history=` for attribute tables. Use `Model.from_catalog` for
   SQL relations, multi-unit sources and explicit catalog names.
2. **Design** describes anchors, features, outcomes and eligibility. Start with a
   constructor in `tr.recipes`; it returns an ordinary Design, not a wrapper.
   Names become output columns, with temporal validation on the same expressions.
3. **Frame** is a lazy compiled dataset. `Design.build(model)` does not materialize
   source rows. `frame.materialize()` collects a result with roles, column
   completeness, null reasons and provenance; an explicit budget bounds output.
4. **Dataset** pins a published generation. `frame.stage(name, budget=...)` uses a
   file-backed local DuckDB owner and retained inputs. `Dataset.from_manifest`
   reopens the exact generation; bounded Arrow batches do not consult a moving
   current-generation pointer.

Configure sources, roster and completeness once, then reuse the Model for each
Design. `Model.from_tables` requires `complete=` on every declared `Events`
descriptor, even if a Design will not use it. With `Model.from_catalog`, only
sources selected by the Design need readiness declarations; unused Catalog
sources, including unused label sources, do not.

The tutorial has three customers; `carol` has no orders. A complete empty count
is zero, and every roster customer gets a row. An order at exactly time zero
would be excluded from `[-30d,0s)` and included in `[0s,30d)`. Targets whose
window extends past the declared completeness end are null with reason
`incomplete_coverage`. The compact `to_pandas()` view keeps each value immediately followed
by `<name>__complete` and `<name>__reason`, while omitting observation bounds,
resolved-time bookkeeping and anchor provenance. Pass
`include_observation_metadata=True` to keep those columns. Arrow is always
lossless; filtering and sorting keep observation evidence aligned with rows.
Pandas uses nullable Arrow-backed types; pandas and Polars need their respective
extras. See [library interoperability](common-stack-interop.md).

`frame.expr` is an explicitly unsafe Ibis escape hatch: executing it directly
bypasses validation, budgets and provenance. Unknown eligibility prevents export
rather than silently dropping a row.

## Handle an output refusal

For these next snippets, run them after the tutorial in the same session. A
one-row output cap cannot return six customer snapshots. Catch its stable
refusal code, then choose a sufficient explicit row and byte budget:

```python
frame = design.build(model)
try:
    frame.materialize(tr.Budget(max_rows=1, strict_scan=False))
except tr.TerrineError as error:
    assert error.code == "terrine.budget.rows_exceeded"
else:
    raise AssertionError("the one-row output cap must refuse two roster users")
budget = tr.Budget(max_rows=10, max_output_bytes=1_000_000, strict_scan=False)
result = frame.materialize(budget)
```

Limits abort rather than return truncated success. They do **not** bound source
scans, query work, backend memory or retained inputs. Native scan costs are
unknown; a strict `max_scan_bytes` request refuses before integrity reads.
`tr.preview(design, model, bounded_count=1_000)` reads capped candidate
diagnostics, not example rows; capped results do not claim complete cohort
totals, and integrity checks can still scan upstream data. See
[cost and data residency](cost-and-residency.md).

## Keep a pinned dataset

Staging is a separate, optional durable lifecycle that needs a file-backed DuckDB
database. Bind the same tables there and stage the same Design:

```python
from tempfile import TemporaryDirectory

with TemporaryDirectory(prefix="terrine-first-") as directory:
    local = ibis.duckdb.connect(f"{directory}/shop.duckdb")
    local.create_table("customers", customers)
    local.create_table("orders", orders)
    local_model = tr.Model.from_tables(
        local,
        units={"customer": tr.UnitSource("customers", "customer_id")},
        events={"orders": tr.Events(
            "orders", time="ordered_at", key="customer_id",
            metrics={"orders": tr.count(), "spend": tr.sum("amount")},
            complete=("2025-01-01", "2025-03-20"),
        )},
    )
    dataset = design.build(local_model).stage("spend_forecast", budget=budget)
    reopened = tr.Dataset.from_manifest(dataset.manifest, con=local)
    rows = [
        row
        for batch in reopened.to_arrow_batches(batch_size=2)
        for row in batch.select(["unit", "orders_30d"]).to_pylist()
    ]
    local.disconnect()
assert sorted(rows, key=lambda row: (row["unit"], row["orders_30d"])) == [
    {"unit": "alice", "orders_30d": 1}, {"unit": "alice", "orders_30d": 2},
    {"unit": "bob", "orders_30d": 0}, {"unit": "bob", "orders_30d": 1},
    {"unit": "carol", "orders_30d": 0}, {"unit": "carol", "orders_30d": 0},
]
```

The reopened Dataset reads the pinned generation, not a moving current pointer.
Temporary files disappear when the tutorial ends; keep a persistent database,
retained inputs and manifests for a reusable dataset. See
[training/scoring and staging](ml-staging.md) and the
[durable runtime](materialization-runtime.md) for saved definitions and refresh.

## Choose the next step

- Configure [native source relations and coverage](temporal-engine.md), then
  [SCD2 attributes](temporal-attributes.md). Completeness is a source declaration,
  never the maximum observed event time.
- Inspect metadata-only costs with `preview`; see [cost and data residency](cost-and-residency.md)
  before collecting rows. `strict_scan=False` explicitly permits unknown scans;
  output row/byte caps do not bound upstream query work.
- Make [training or scoring views](ml-staging.md) and export a pinned generation
  to local Parquet. Preserve the manifest and Arrow metadata with the data.
- Use [panels](sequential-panels.md), [state pairs](state-panels.md),
  [trajectories/risk sets](trajectories-survival.md) or
  [bounded sequences](temporal-engine.md#bounded-event-sequences-and-event-time-origins)
  only when the row shape matches your question.
- Use [library hand-offs](library-handoffs.md) for downstream modeling. Terrine
  makes no treatment-effect estimate or causal identification claim.
