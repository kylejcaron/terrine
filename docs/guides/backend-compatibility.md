# Backend compatibility

Terrine distinguishes verified execution from SQL compilation. Native local Ibis
DuckDB is the reference execution provider, and an Ibis PostgreSQL backend
(`terrine[postgres]`, Ibis 12 or newer) materializes Frames directly from
PostgreSQL tables. An unrecognized connection—including a raw driver connection
or one that only declares `name = "duckdb"`—refuses before source schema access
with `terrine.backend.capability_unavailable`. Raw DuckDB connections remain
valid for native runtime protocols; they are not Ibis relation/execution providers.

Execution and publication share an immutable capability record without making
Model or the compiler depend on runtime. The provider preserves the caller-owned
connection, admits read-only SQL in its execution dialect, and verifies physical
relation identity. Each materialization opens an owned private session with a
transaction view and closes it after success, error or cancellation. The caller's
connection remains open. A transaction view is **not** a durable replay snapshot.

Runtime DuckDB publication retains its fenced, transactional manifest switch;
Ibis SQL compilation or DDL support alone does not establish publication safety.

Snowflake and BigQuery execution are unverified. No paid warehouse, credentials or
remote resources are accessed by the local workflow. Named dbt relation binding is
owner-gated; an existing dbt recipe is not evidence of a reviewed relation-to-Catalog
bridge.

## PostgreSQL

Each materialization opens a private connection with the caller's connection
parameters and search path, sets `TimeZone` to UTC and reads in one
`REPEATABLE READ READ ONLY` transaction, so every query sees one consistent
snapshot. The caller's connection, and any transaction open on it, is never used
or changed. Schemas of SQL relations are read without creating database objects.
A query that calls a name also defined as a user function outside the system
catalogs refuses, as does a view whose stored definition depends on such a
function.

Supported: counts and exact integer/decimal sums over windows, as-of attributes,
fixed, scheduled and event anchors, time-to-event shapes, cohort trajectories
with their total and mean summaries, and observed state panels, with rows
identical to the DuckDB reference
([differential probes][postgres]). Timestamps are exact at PostgreSQL's native
microsecond precision. Decimal columns need a declared `numeric(p, s)` with
`p <= 38`; unconstrained `numeric` refuses with
`terrine.semantic.decimal_scale_required` and NaN/infinite values with
`terrine.backend.nonfinite_decimal`. Sums that do not fit the output type refuse
with `terrine.temporal.exact_overflow`.

Averages, ratios and float-valued windows match DuckDB within a tight relative
tolerance rather than bit for bit. PostgreSQL keeps each window quantity exact in
`NUMERIC` and rounds to binary64 once: a float fact enters as the decimal value
of its shortest round-trip text (`REAL` is widened first), so history outside the
window cancels exactly and a window sum's error is at most 2^-53 times the sum of
its absolute values, plus the final rounding; a quotient is divided to at least
16 significant digits before rounding (relative error below 1e-15). These columns
carry `numeric_policy=None` and no absolute-error column; DuckDB's certified
`checked_float64_v1` results are unchanged. A non-finite value at or before the
window end, or a result beyond binary64 range, refuses with
`terrine.temporal.float64_precision` (`nonfinite_input` / `bound_overflow`).

Refused on PostgreSQL, with a route: retained input capture, Dataset staging,
publication and reopening (`terrine.backend.capability_unavailable`; these live
in the DuckDB runtime).

## Operation evidence

Capabilities distinguish verified (`True`), unsupported (`False`), and unverified
(`None`). A missing capability refuses only its own operation with structured
`backend`, `operation`, `status`, and `route` context.

- Query admission requires a verified dialect; transaction-scoped execution and
  Arrow materialization have independent gates.
- Source timestamp and declared decimal fields require exact-type evidence;
  roster-only anchor compilation also requires exact timestamp semantics.
  Existing precision and overflow checks still apply within the verified domain.
- Retained input capture requires `retained_snapshot`; unknown retained snapshot
  support does not block an ordinary Frame materialization.
- Generation publication requires both `table_publication` and
  `atomic_manifest_publish`, never merely writable SQL tables.
  Dataset staging checks these and retained capture before opening destination
  state or reading sources. Losing publication evidence does not block existing
  verified reads.
- Dataset Arrow reads and local Parquet export require `arrow_batches` before
  reading the generation or creating destination files. Native unload is not
  required: local export uses bounded Arrow batches and PyArrow.
- Strict scan budgets, including local Parquet export, still require an actual
  verified scan bound. An output row or byte limit does not certify source scan
  work. Explicit non-strict policy permits execution with the existing warning.
- DuckDB has no native replay-snapshot claim (`snapshot=False`). Owner-retained
  immutable capture and its receipt are a separate verified protocol.
- Native query cancellation, source change-feed evidence, scan estimates/limits
  and native Parquet unload are not inferred from the driver API. Cooperative
  runtime cancellation and closing owned sessions do not establish native query
  cancellation support.

`terrine.execution.backend.backend_capabilities(con)` inspects evidence without
admitting the connection or reading source schema. Snowflake, BigQuery and
unregistered backend connections return unverified operation values; attempting
execution still refuses.

## Python and dependency support

The package supports CPython 3.13 and 3.14. Native dataset execution requires
`terrine[duckdb]`; base Terrine has no mandatory backend, dataframe, orchestration
or modeling library. The local engine is pinned to DuckDB **1.5.6**, matching
the exact arithmetic capability check, rather than advertising unverified engine
versions. Ibis starts at 11 and SQLGlot at 29: Ibis 10 constrains SQLGlot below
the version needed for native `BIGNUM` types.

The [distribution acceptance command][distribution] builds a wheel from the
sdist, compares package contents, installs it into a fresh environment outside
the checkout, checks that optional distributions are absent, and imports every
public facade with optional imports blocked. It then installs the DuckDB extra
and executes the exact README tutorial, including retained staging and reopening.
The installed package location is checked, so a source checkout cannot mask
missing wheel files. The sdist retains guides, examples, typed package data, the
Apache-2.0 `LICENSE` and `NOTICE`, and the direct-dependency notice inventory;
wheels retain the same license files and declare `License-Expression:
Apache-2.0`.

CI runs this command for both Python versions with highest and lowest direct
dependency resolution. Lowest resolution uses `uv pip compile --resolution
lowest-direct --only-binary :all:`: the floor is the lowest compatible **wheel**
combination for that interpreter/platform, not an assurance that older dependency
sdists can be built on new Python versions. Source-building third-party native
libraries is not the supported installation path. Transitive dependencies are
resolved normally; the job does not claim a lowest-transitive combination.

Run either resolution locally after building (replace `3.13` with `3.14` for the
other interpreter):

```sh
uv build
uv run python -m tests.integration.distribution_smoke \
  --wheel dist/*.whl --sdist dist/*.tar.gz --python 3.13 \
  --resolution lowest-direct
```

Measured on macOS arm64, with the same expected rows in each completed wheel
tutorial (`a`: one feature/two outcomes; `b`: zero/zero):

| Python | Resolution | Ibis | DuckDB | Pydantic | PyYAML | SQLGlot | PyArrow | NumPy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 3.13.5 | Highest | 12.0.0 | 1.5.6 | 2.14.1 | 6.0.3 | 29.0.1 | 23.0.1 | 2.5.4 |
| 3.13.5 | Lowest direct wheels | 11.0.0 | 1.5.6 | 2.8.1 | 6.0.2 | 29.0.0 | 18.0.0 | 2.1.0 |
| 3.14.5 | Highest | 12.0.0 | 1.5.6 | 2.14.1 | 6.0.3 | 29.0.1 | 23.0.1 | 2.5.4 |
| 3.14.5 | Lowest direct wheels | 11.0.0 | 1.5.6 | 2.12.0 | 6.0.3 | 29.0.0 | 22.0.0 | 2.3.2 |

The 3.13 floor also passed 186 spec, metadata, error, window and column-compilation
tests with `python -m pytest -q -n 4 tests/spec tests/test_metadata.py
tests/test_errors.py tests/test_windows.py tests/compile/test_columns.py`.
The wheel matrix is narrower than the full operation and optional-adapter suites;
do not infer warehouse or downstream-library support from the tutorial.

### Optional extras

Each extra is separately declared; none is loaded by base/facade imports.
The locked Python 3.13 checks install all extras and run the integration suites.
The Python 3.14 native wheel checks do **not** establish that all optional
orchestration or modeling libraries support that interpreter.

| Extra | Dependency contract / evidence |
| --- | --- |
| `duckdb` | DuckDB 1.5.6 and Ibis DuckDB backend; wheel matrix above |
| `pandas`, `polars` | Common-stack round trips in the operation matrix below |
| `airflow` | Airflow 3.3.2 / standard provider 1.20.1; [adapter tests](../../tests/adapters/test_airflow.py) |
| `dagster` | Dagster 1.13.26; [adapter tests](../../tests/adapters/test_dagster.py) |
| `dbt` | Core 1.10.13 / dbt-duckdb 1.9.6; [adapter tests](../../tests/adapters/test_dbt_core_recipe.py) |
| `increment` | 0.1.0a2; [independent optional compatibility contract][increment] |
| `sketches` | DataSketches 5.2 or later below 6; [sketch support/refusals][sketches] |
| `doubleml`, `duckreg`, `lifelines`, `diff-diff` | Pinned downstream versions in `pyproject.toml`; [real library hand-offs][libraries] |
| `dowhy` | 0.14 on Python **below 3.14 only**; dependency marker omits it on 3.14, not a promise of adapter availability |

[Third-party notices](../../THIRD_PARTY_NOTICES.txt) inventory every direct base
and optional dependency. Upstream binaries and their transitive dependencies
retain their own complete platform-specific license/notice artifacts.

## Operation regression environment

The operation evidence below was run with Python **3.13.5**, Ibis **12.0.0**,
DuckDB driver **1.5.6** / embedded engine **v1.5.6**, and PyArrow **23.0.1**.
These are measured versions, not a claim for every release in a dependency range.
Python 3.14 distribution/tutorial checks above do not replace these operation tests.

| Operation | Local DuckDB versions above | PostgreSQL | Snowflake | BigQuery | Route or limitation |
| --- | --- | --- | --- | --- | --- |
| Read-only dialect admission, relation identity | [Verified][provider] | [Verified][postgres] | Unverified | Unverified | Use native local DuckDB or PostgreSQL; an Ibis dialect/name is not execution evidence |
| Accepted UTC timestamps, exact decimals, strict t0 and SCD2 boundaries | [Verified][acceptance] | [Verified][postgres]; microsecond timestamps, declared `numeric(p, s)` | Unverified | Unverified | Existing timestamp precision/domain and decimal overflow refusals remain active |
| Private transaction view and owned-session cleanup | [Verified][provider] | [Verified][postgres]; consistent `REPEATABLE READ` snapshot | Unverified | Unverified | Does not promise a durable source snapshot or native query cancellation |
| Retained immutable inputs and pinned generation reads | [Verified][acceptance] | Unverified | Unverified | Unverified | Use the owner-retained snapshot receipt; native source revisions remain unknown |
| Table publication and atomic fenced manifest switch | [Verified][publication] | Unverified | Unverified | Unverified | Materialize a bounded Frame if publication is unavailable |
| Bounded Arrow batch reads | [Verified][acceptance] | [Verified][postgres] | Unverified | Unverified | Requires independent Arrow-batch evidence |
| Local Parquet streaming and metadata readback | [Verified][handoffs] | Unverified | Unverified | Unverified | Bounded Arrow/PyArrow path; caller-owned local destination only |
| Output row/byte limits | [Verified][exports] | Unverified | Unverified | Unverified | Do not certify source scan work or peak query memory |
| Native durable/replay snapshot | Unsupported | Unverified | Unverified | Unverified | Use retained immutable capture, not a transaction-view claim |
| Native change-feed kinds | Unverified; no kinds declared | Unverified | Unverified | Unverified | Supply separately verified change evidence; do not infer revisions |
| Native scan estimate/limit | Unverified | Unverified | Unverified | Unverified | Requested strict scan-byte budgets refuse with `terrine.budget.scan_unknown`; explicit non-strict policy warns |
| Native query cancellation | Unverified | Unverified | Unverified | Unverified | Cooperative runtime cancellation remains distinct |
| Native Parquet unload | Unverified | Unverified | Unverified | Unverified | Use verified local Arrow streaming instead |

Snowflake/BigQuery remain owner-gated; no warehouse versions, auth integrations or
paid-resource support are claimed. PostgreSQL evidence was measured on
PostgreSQL 16. Unknown backends refuse with an actionable local route before
schema or source access. The [operation-gate tests][gates] separately prove that
missing publication, retained capture or Arrow evidence does not disable unrelated
work.

| Common-stack component | Measured version | Exercised contract |
| --- | --- | --- |
| PyArrow | 23.0.1 | [Typed values, reserved semantic metadata, batches and Parquet][handoffs] |
| pandas | 2.3.3 | [Arrow-backed nullable dtypes, exact decimal/timestamp values and nulls][handoffs] |
| Polars | 1.44.2 | [Exact logical values and nulls through Arrow; string offset width may differ][handoffs] |
| increment | 0.1.0a2 | [Documented optional dataset-definition projection][increment] |
| dbt Core / dbt-duckdb | 1.10.13 / 1.9.6 | Installed; existing orchestration regression only. New reviewed relation binding remains owner-gated and unimplemented |

Consumer dataframes are not portable Terrine metadata containers. Preserve the
Arrow metadata and Dataset/export manifests as described in
[common-stack interoperability](common-stack-interop.md). Native Catalog works
without increment; unsupported semantic mappings are listed separately in the
[semantic-layer matrix](semantic-layer-interop.md).

[provider]: ../../tests/execution/test_backend_provider.py
[acceptance]: ../../integration/warehouse_execution/test_backend_capabilities.py
[publication]: ../../integration/warehouse_execution/test_duckdb_runtime_recovery.py
[handoffs]: ../../integration/warehouse_execution/test_ml_staging_duckdb.py
[exports]: ../../integration/warehouse_execution/test_ml_staging_duckdb.py
[gates]: ../../tests/runtime/test_backend_capabilities.py
[increment]: ../../tests/compat/test_increment_semantics.py

[distribution]: ../../tests/integration/distribution_smoke.py
[sketches]: ../../tests/temporal/test_sketches.py
[libraries]: library-handoffs.md
[postgres]: ../../integration/warehouse_execution/test_postgres_windows.py
