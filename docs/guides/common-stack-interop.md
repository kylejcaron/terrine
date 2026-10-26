# Common-stack interoperability

Terrine constructs datasets in the caller's local connection. Arrow is the typed
handoff; pandas and Polars are optional bounded views, not portable Terrine
metadata containers. No trainer, hosted service, or relationship to increment is
required.

Install from a released distribution with only the integrations used:

```sh
uv pip install 'terrine[duckdb]'
uv pip install 'terrine[duckdb,pandas]'
uv pip install 'terrine[duckdb,polars]'
```

For development from a source checkout, use editable installs instead:

```sh
uv pip install -e '.[duckdb]'
uv pip install -e '.[duckdb,pandas]'
uv pip install -e '.[duckdb,polars]'
```

Core/backend metadata imports do not load optional dataframe or warehouse drivers.
Requesting an unavailable dataframe extra raises
`terrine.convert.missing_dependency` before executing the Frame, with the extra's
install route. PostgreSQL execution needs `terrine[postgres]`, BigQuery
execution `terrine[bigquery]` and Snowflake execution `terrine[snowflake]`. See
[backend compatibility](backend-compatibility.md).

## Bounded Frames

```python
from terrine import Budget
from terrine.data import MaterializedFrame

budget = Budget(max_rows=100_000, max_output_bytes=64_000_000)
arrow = frame.to_pyarrow(budget=budget)
materialized = MaterializedFrame.from_arrow(arrow)
pandas_frame = frame.to_pandas(budget=budget)
polars_frame = frame.to_polars(budget=budget)
```

Omitting `budget` is intentionally unbounded output, not a safe default. Output
row/byte limits do not cap source scans, backend memory or retained inputs; they
bound only the collected output. Choose a limit appropriate to the expected
result size and reuse it for each Frame materialization or export.

Each conversion executes independently under the same integrity checks and
budget; retain one Arrow table when sharing an already materialized result.
`to_pandas` uses nullable Arrow-backed dtypes, not floating-point coercion of
nullable integers or decimals. `to_polars` uses its Arrow conversion. Decimal
precision/scale, UTC timestamp instants, exact values and null locations survive
the exercised paths, including empty and all-null typed columns. Polars may use
large-string Arrow offsets on readback; this does not change string values or
imply physical Arrow schema identity. Neither dataframe carries a promise to
round-trip Terrine schema metadata.

Each output value `name` is followed by `name__complete` (Boolean) and
`name__reason` (string, or a list of exact reasons for LifeTable aggregates).
This includes boolean features/outcomes, treatment and eligibility. Output
names ending in `__complete` or `__reason` are reserved.

Default Frame conversions and `MaterializedFrame.to_arrow()` keep observation
bounds, anchor provenance and resolved-time bookkeeping out of visible columns.
Pass `include_observation_metadata=True` to expose them. Full materializations,
staged generations, manifests and JSON fixtures retain the observation columns.
Arrow stores the hidden columns as a lossless IPC payload alongside roles,
design/model identities, temporal cutoffs, completeness/null-reason references
and numeric policies under `terrine.materialized_frame.v1`.
`MaterializedFrame.from_arrow()` restores the original columns and typed metadata.
Restore the materialization before filtering, sorting or otherwise editing a
compact Arrow table; the embedded observation evidence belongs to its original
rows and order. Unrelated schema metadata is retained. Neither pandas nor Polars
promises to retain Terrine schema metadata. Do not zero-fill unavailable values
or discard their reason columns when preparing downstream data.

Output row/byte limits do not bound upstream scan work or peak query memory.
Strict scan budgets refuse when no verified scan bound exists. Non-strict scan
policy must be chosen explicitly when a scan-byte limit is supplied.

## Pinned Datasets and Parquet

Use a file-backed local DuckDB connection for staging:

```python
dataset = frame.stage("training_dataset", budget=budget)
for batch in dataset.to_arrow_batches(batch_size=65_536):
    print(batch.num_rows)
export = dataset.export_parquet(destination="exports/training", budget=budget)
```

The export's parent directory must already exist. Dataset batch reads do not
collect the complete Dataset in Python. Local Parquet export streams those
batches through PyArrow and keeps schema metadata in every shard, including an
empty export's schema-only shard. Native warehouse unload is not required or
claimed. Output export limits apply to the exported generation, not to original
source scan work.

Retain the Dataset manifest and export manifest alongside Parquet shards. The
Dataset manifest records the pinned output generation, column roles/metadata,
source snapshot receipt and completeness. The export manifest identifies that
same generation and records shard checksums, row counts and physical types.
Arrow's Frame provenance identifies its inputs/design; it is not a substitute for
the Dataset manifest's output-generation identity. A refresh returns another
pinned Dataset; an earlier reference remains attached to its earlier generation.

Consumers that strip Arrow metadata must keep the manifests separately. Loading
Parquet into a consumer dataframe does not recreate Terrine semantics by column
name. Terrine writes only caller-owned local files and does not write to cloud
object stores. To use the output from S3, GCS or Azure, copy the Parquet shards
together with both manifests to the bucket; the copy carries no atomic
publication guarantee of its own.

## Semantic inputs

A native `Catalog` binds explicit relations and definitions through
`Model.from_catalog`. Optional `Model.from_increment` remains the documented
versioned adapter in [increment compatibility](increment-compatibility.md).
Arbitrary YAML, MetricFlow and LookML do not establish a lossless mapping and are
not accepted as guessed metric definitions. This is a semantic-mapping boundary,
not evidence of runtime backend support.
The [semantic-layer matrix](semantic-layer-interop.md) separates verified native
and versioned increment contracts from unsupported or owner-gated mappings.

Named dbt relation binding remains owner-gated. This work does not add a dbt
manifest reader, execute macros, infer metrics from columns, or claim a reviewed
dbt relation-to-Catalog workflow. Existing orchestrator workflows remain unchanged.

Run `uv run python examples/common_stack_interop.py` for a self-contained local
Model → Design → Frame → Dataset → Arrow/Parquet workflow and optional dataframe
conversions.
