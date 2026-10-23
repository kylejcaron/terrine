# Your first dataset

Requires CPython 3.13 or 3.14 and the `duckdb` extra. For an installed
distribution:

```sh
uv pip install 'terrine[duckdb]'
```

No sibling project, warehouse account or semantic adapter is required. The
complete executable tutorial lives in the
[README](../../README.md#build-a-study-dataset). To run it from a source checkout:

```sh
uv sync --extra duckdb
uv run python examples/first_dataset.py
```

--8<-- "README.md:first-dataset"
## Four objects

1. **Model** binds a native Catalog to existing tables. It owns the unit roster,
   source coverage and optional event identity configuration, not a copy of your
   analytical rows. `Model.from_catalog` admits the declared schemas and SQL.
2. **Design** describes anchors, features, outcomes and eligibility. Names in the
   Design become output columns. Feature windows end before the outcome window;
   predicates and temporal validation use the same expression definitions.
3. **Frame** is a lazy compiled dataset. `Design.build(model)` does not materialize
   source rows. `frame.materialize(budget)` collects a bounded result with roles,
   column completeness, null reasons and provenance.
4. **Dataset** pins a published generation. `frame.stage(name, budget=...)` uses a
   file-backed local DuckDB owner and retained inputs. `Dataset.from_manifest`
   reopens the exact generation; bounded Arrow batches do not consult a moving
   current-generation pointer.

Configure the Catalog, unit roster and source readiness once when creating the
Model, then reuse that Model for each Design. Start with
`frame.materialize(budget)` to collect an ordinary bounded result. Staging is
optional and separate: use `frame.stage(name, budget=budget)` only when you need
a durable pinned generation, manifest/reopen or later streaming/export lifecycle.

The tutorial uses two roster users, one without events. A complete empty window
is zero. The event at exactly time zero is excluded from `[-1d,0s)` and included
in `[0s,1d)`. It asserts both ordinary materialization and reopened Dataset rows.
Temporary files are removed when the example ends; use a persistent database and
retain its snapshots/manifests for a real reusable dataset.

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
