# Cost and data residency

## What leaves the database

- `Model.from_catalog`, `Design.validate` and ordinary `preview` inspect metadata,
  not example rows. Metadata/schema resolution can contact the caller's database.
- Frame materialization executes integrity checks and selected queries in an
  owned snapshot session. `materialize`, `to_pyarrow`, `to_pandas` and `to_polars`
  explicitly bring the selected output into the caller's Python process.
- Dataset staging retains input snapshots and bounded Arrow task artifacts in
  the owner's local filesystem, then publishes immutable tables in the owner's
  file-backed DuckDB database. Configure retention and filesystem permissions;
  these artifacts contain customer data, not just metadata.
- `Dataset.to_arrow_batches` reads the pinned generation into bounded client
  batches. Local Parquet export writes only to the explicit owner-selected path.
  It does not make an arbitrary object-storage URI a supported destination.
- Approximate quantile sidecars stream matched event values to the caller's
  DataSketches process; they are not warehouse-only aggregates. Same-connection
  duckreg is the documented aggregate-only downstream route.
- Terrine supplies no hosted row service. Optional telemetry exports only its
  documented redacted lifecycle attributes through an owner-supplied exporter;
  that exporter and its destination are the owner's responsibility. Agent row
  previews and export are separately authorized, never implied by inspection.

Catalogs, designs and owner error context can contain source names or source SQL.
Keep them within the owner's trust boundary. Agent envelopes deliberately omit
raw SQL, source snapshots and connection details; that is not permission to log
arbitrary backend exceptions or Dataset rows.

## Four different bounds

| Control | Bounds | Does not certify |
| --- | --- | --- |
| `Budget.max_rows` | Successful output rows | Source scans, join work, backend memory |
| `Budget.max_output_bytes` | Output Arrow bytes at the enforced boundary | Compressed Parquet size or peak query memory |
| `Budget.max_scan_bytes` with strict policy | Independently verified scan bounds/enforceable caps for every read phase | A guessed estimate derived from output size |
| Bounded Arrow batches/shards | Transfer/writer buffering | Backend query memory or total disk space |

Native DuckDB scan cost is unknown. A strict scan-byte request refuses with
`terrine.budget.scan_unknown` before integrity/source reads; use an independently
verified owner cost provider or explicitly choose non-strict scan policy after
reviewing the upstream work. Non-strict policy does not turn an unknown cost
into zero. Reducing an output limit is not a route to a strict scan guarantee.
No output limit returns truncated success.

`preview(..., bounded_count=N)` is an opt-in capped diagnostic read, not a
metadata-only preview. A cap does not establish complete cohort totals, and
integrity checks can still scan upstream data. The runtime records estimates,
authorization and observed completed-artifact costs separately; absent values
remain unknown. See [runtime cost evidence](materialization-runtime.md#cost-evidence-and-scan-budgets).

## Recovery and measured evidence

The [runtime guide](materialization-runtime.md) explains destination-scoped
request replay, retained inputs, fencing and exact-generation reads. Executed
local recovery cases live in
[publication recovery](../../integration/warehouse_execution/test_duckdb_runtime_recovery.py)
and [fresh-process dataset refresh](../../integration/warehouse_execution/test_ml_staging_duckdb.py).
The [benchmark guide](../../benchmarks/README.md) links bounded local compiler
measurements and the [saved local record](../../benchmarks/results/duckdb-scale-v2-small.json).
These are measured workloads, not a promise of billion-row performance. Remote
warehouse runs require owner authorization, credentials and separate measured
backend evidence; none is inferred from SQL compilation.
