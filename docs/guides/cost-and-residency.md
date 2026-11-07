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
| `Budget.max_output_bytes` | Cumulative Arrow bytes emitted by the selected output, checked before writing each export batch | Compressed Parquet size or peak query memory |
| `Budget.max_scan_bytes` with strict policy | Independently verified scan bounds/enforceable caps for every read phase | A guessed estimate derived from output size |
| Bounded Arrow batches/shards | Transfer/writer buffering | Backend query memory or total disk space |

Export manifests retain physical Parquet `byte_count` for storage and integrity;
that value does not satisfy an Arrow-output budget. A capped export replay
re-reads the pinned Arrow stream to enforce the same byte limit before returning
the verified export. An uncapped replay does not add this read.


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
and [fresh-process dataset refresh](../../integration/warehouse_execution/test_ml_staging_exports.py).
The [benchmark guide](../../benchmarks/README.md) links bounded local compiler
measurements and the [saved local record](../../benchmarks/results/duckdb-scale-v2-small.json).
These are measured workloads, not a promise of billion-row performance. Remote
warehouse runs require owner authorization, credentials and separate measured
backend evidence; none is inferred from SQL compilation.

## Generated SQL budget and query-time targets

The scheduled scale tier ([cost budget](../../integration/test_cost_budget.py))
builds one ordinary Design per recipe over the synthetic workload at 10,000
units and 300,000 events, and a hand-written query of the same semantics
([reference queries](../../benchmarks/workloads.py)). On that workload every
window lies inside declared coverage, so completeness flags are constant; the
check asserts that each recipe returns the hand-written query's values on
DuckDB and on PostgreSQL 16. SQL size is `len(ibis.to_sql(frame.expr))`, the
formatted statement; the statement sent for execution is more compact.

| Checked budget | Ceiling |
| --- | --- |
| Recipe SQL | 30,000 characters per feature (declared features and outcomes, plus one for the measurement the recipe's shape adds) |
| README design SQL | 20,000 characters |
| 5 metrics by 10 windows, BigQuery dialect | 1,000,000 characters (BigQuery accepts 1,024k) |

A size above its ceiling is open work: the check records it as an expected
failure that becomes a failure once the recipe meets its ceiling.

Query time is a tracked target, not a guarantee or a test gate: a recipe should
evaluate within 3 times (DuckDB) or 10 times (PostgreSQL 16) the time of its
hand-written query. `uv run python -m benchmarks.run_cost` prints both times and
their ratio per recipe; add `--postgres-url postgres://...` to measure a
PostgreSQL database too, in a schema it creates and drops. Each time is the
fastest of seven evaluations of the statement into a temporary table on the
server; Python-side compilation and validation are measured separately by the
[overhead benchmark](../../benchmarks/README.md#fixed-compiler-overhead).

Measured on 2026-11-07 (Apple M3 Pro; DuckDB 1.5.6; PostgreSQL 16.15):

| Recipe | Features | SQL characters | DuckDB time ratio (target 3) | PostgreSQL time ratio (target 10) |
| --- | ---: | ---: | ---: | ---: |
| `time_series` | 3 | 21,555 | 2.6 | 5.8 |
| `trial_panel` | 3 | 17,922 | 4.0 | 4.6 |
| `event_panel` | 2 | 14,589 | 6.4 | does not run |
| `ltv` | 1 | 79,098 | 6.9 | 7.2 |
| `survival` | 3 | 65,663 | 6.3 | 21.6 |
| `life_table` | 1 | 49,345 | 3.7–4.7 | 16.3 |
| `state_panel` | 1 | 27,336 | 4.5 | 41.4 |
| `event_sequence` | 3 | 21,685 | 4.2 | 4.7 |

The README design measures 19,663 characters. The 5-metric by 10-window design
measures 1,050,208 formatted characters in the BigQuery dialect, about 22,600
per metric window. On PostgreSQL the `event_panel` statement fails to
compile: the portable lookup casts a NULL struct anchor identifier.
