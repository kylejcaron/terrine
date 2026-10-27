# Your first dataset

Requires CPython 3.13 or 3.14 and the `duckdb,pandas` extras for this example.
For an installed distribution:

```sh
uv pip install 'terrine[duckdb,pandas]'
```

No sibling project, warehouse account or semantic adapter is required. The
complete executable tutorial lives in the
[README](../../README.md#build-a-study-dataset). To run it from a source checkout:

```sh
uv sync --extra duckdb --extra pandas
uv run python examples/first_dataset.py
```

--8<-- "README.md:first-dataset"
## Four objects

1. **Model** binds event tables with `Model.from_tables`, an authoritative unit
   roster and explicit completeness. Use `Model.from_catalog` for richer source
   semantics, including labels, filtered measures and as-of attributes.
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
Design. Only sources selected by the Design need readiness declarations, even
when the Catalog also defines unused label sources.

The tutorial uses two roster users, one without events. A complete empty count
is zero. The event at exactly time zero is excluded from `[-1d,0s)` and included
in `[0s,1d)`. The compact `to_pandas()` view keeps each value immediately followed
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

For these next snippets, keep the tutorial's connection open: put them inside
its `try` block, before `finally`. A one-row output cap cannot return the two
roster users. Catch its stable refusal code, then choose a sufficient explicit
row and byte budget:

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

Staging is a separate, optional durable lifecycle on file-backed DuckDB. In the
same open tutorial connection:

```python
dataset = frame.stage("first_dataset", budget=budget)
reopened = tr.Dataset.from_manifest(dataset.manifest, con=con)
rows = [
    row
    for batch in reopened.to_arrow_batches(batch_size=2)
    for row in batch.select(["unit", "before", "after"]).to_pylist()
]
assert sorted(rows, key=lambda row: row["unit"]) == [
    {"unit": "a", "before": 1, "after": 2},
    {"unit": "b", "before": 0, "after": 0},
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
