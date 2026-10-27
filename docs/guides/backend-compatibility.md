# Backend compatibility

Terrine distinguishes verified execution from SQL compilation. Native local Ibis
DuckDB is the reference execution provider. An Ibis PostgreSQL backend
(`terrine[postgres]`, Ibis 12 or newer), an Ibis BigQuery backend
(`terrine[bigquery]`) and an Ibis Snowflake backend (`terrine[snowflake]`)
materialize Frames directly from their tables.

Other genuine Ibis backends refuse by default, before source schema access.
Explicitly pass `allow_unverified=True` to `Model.from_catalog`,
`Model.from_tables` or `Model.from_increment` to admit read-only operations.
Admission emits the filterable `terrine.UnverifiedBackendWarning`; permission
belongs to that Model, not the connection. A second Model still needs consent.
Raw driver connections and name-only objects remain refused, as do rejected
registered identities such as a PostgreSQL-wire-compatible server. Raw DuckDB
connections remain valid for native runtime protocols, not Ibis execution.

Execution and publication share an immutable capability record without making
Model or the compiler depend on runtime. Verified providers preserve the
caller-owned connection, admit read-only SQL in its real execution dialect,
and verify physical relation identity. Each verified materialization reads
through one consistent view (a private transaction, or on Snowflake a Time
Travel instant) and releases it after success, error or cancellation.
The caller's connection remains open. A transaction view is **not** a durable
replay snapshot.

An opted-in unverified provider instead borrows the caller's connection.
Every statement reads independently: schema checks, validation and output
queries do not share a coherent view. Terrine does not directly begin or end
transactions, change session settings, clone or disconnect that connection.
However, Ibis schema inspection on an unverified backend may end an open
transaction on the caller's connection. Use a connection with no pending writes
or a dedicated read connection; transaction preservation is not guaranteed.
Owned Arrow readers are released on exhaustion, error, early close or context
closure.

The actual backend name (for example `sqlite`), expression policy (`generic`)
and SQL dialect (`sqlite`) are distinct. Keep the real SQL dialect in
`Catalog.dialect`, never `generic`. Only required read primitives may be
attempted without verified evidence; unavailable primitives raise
`terrine.backend.capability_unavailable` with their operation. Decimal queries
first execute scalar exact-arithmetic witnesses within each execution context;
a count-only read does not need decimal admission. Generic timestamp and
arithmetic expression coverage remains operation-specific, not a promise that
every Ibis backend can materialize every Frame.

Unverified reads never enable retained input capture, staging, publication,
published Dataset reopening, native Parquet unload or transaction snapshots.
Strict scan budgets still refuse unknown source costs. Suppressing the warning
does not grant these capabilities or establish consistency.

Runtime DuckDB publication retains its fenced, transactional manifest switch;
Ibis SQL compilation or DDL support alone does not establish publication safety.

Verified engine identities, capability values, feature boundaries and lazy
implementation paths have one immutable registry in
`terrine.backends.registry`. Each `Engine` record names its identity admission
callback, expression policy and execution provider. Its `SessionStrategy`
describes the existing consistent-read lifetime: `TRANSACTION` for DuckDB and
PostgreSQL, `SESSION_TRANSACTION` for BigQuery, and `TIME_TRAVEL` for Snowflake.
These records do not replace the engine-specific session implementations.
Expression overrides live in one `terrine.backends.engines.<name>` module per
engine. Adding an engine requires a registry entry, its dialect overrides and
verified admission evidence, not another supported-engine list in a consumer.
Capability inspection is metadata-only and never queries a server; PostgreSQL
identity therefore remains unverified until admission. Admission checks exact
Ibis backend identity before exposing source metadata. Unsupported-operation
refusals offer only registered, verified engines implementing that operation;
compilation alone does not verify another engine or build.

The local workflow accesses no paid warehouse, credentials or remote resources;
BigQuery and Snowflake are reached only when a Model binds an Ibis connection to
them. Named dbt relation binding is owner-gated; an existing dbt workflow is not
evidence of a reviewed relation-to-Catalog bridge.

Runnable table-to-dataset examples:
[`PostgreSQL`](../../examples/postgres/dataset.py),
[`BigQuery`](../../examples/bigquery/dataset.py), and
[`Snowflake`](../../examples/snowflake/dataset.py).
Install the corresponding `postgres`, `bigquery`, or `snowflake` extra and bind
the Model directly to its Ibis connection; no preliminary copy to DuckDB is
needed for Frame materialization. Dataset staging and publication remain
DuckDB-only.

## PostgreSQL

Admission uses only the built-in `SELECT pg_catalog.version()` before source
metadata access and admits the PostgreSQL 16 major version measured by the
compatibility suite. PostgreSQL versions not yet measured are unverified;
PostgreSQL-wire-compatible servers such as Redshift, Greenplum and CockroachDB
are unsupported and refuse with `terrine.backend.capability_unavailable`. The
qualified built-in cannot be shadowed through `search_path`; no writes or
session settings are issued. The PostgreSQL engine identity callback makes this
check at admission, after checking the Ibis floor, because
relation discovery and binding may access source schema before materialization
opens its private connection. Materialization reads remain on that private
owned connection described below.

Each materialization opens a private connection with the caller's connection
parameters and search path, sets `TimeZone` to UTC and reads in one
`REPEATABLE READ READ ONLY` transaction, so every query sees one consistent
snapshot. The caller's connection, and any transaction open on it, is never used
or changed. Schemas of SQL relations are read without creating database objects.
A query that calls a name also defined as a user function outside the system
catalogs refuses, as does a view whose stored definition depends on such a
function. Built-in functions inside a view are evaluated on each query.

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

Averages, ratios and float-valued windows match DuckDB within a tight tolerance
rather than bit for bit. On every backend a float fact enters exact state as
the decimal value of its shortest round-trip text (`REAL` is widened first;
Snowflake uses an 18-significant-digit text, see below), which lies within
half an ulp of the stored float, so history outside the window cancels exactly
and a window sum is that decimal sum rounded to binary64 once. DuckDB holds
the text as a `BIGNUM` coefficient at a per-unit scale and
rounds through its correctly rounded string-to-double conversion; PostgreSQL
keeps each window quantity exact in `NUMERIC` and rounds to binary64 once.
Averages and ratios divide before that rounding: PostgreSQL to at least 16
significant digits (relative error below 1e-15 of its exact inputs), DuckDB
within `|r − p/q| ≤ 6·2⁻⁵³·|p/q| + 2⁻¹⁰⁷⁵` of the exact quotient. The parity
contract stays relative to the window's absolute sum, not to the result, since
cancellation defeats a purely relative bound. These columns carry
`numeric_policy=None` and no error column on every backend. A non-finite value
at or before the end of a non-empty window, or a result beyond binary64 range,
refuses with
`terrine.temporal.float64_precision` (`nonfinite_input` / `bound_overflow`); an
empty window is zero, as on DuckDB.

Refused on PostgreSQL, with a route: retained input capture, Dataset staging,
publication and reopening (`terrine.backend.capability_unavailable`; these live
in the DuckDB runtime).

## BigQuery

Each materialization opens a private session on a new client built from the
caller's credentials and project and runs `BEGIN TRANSACTION` there, so every
query sees one snapshot of the tables it reads. The caller's client, and any
session or job configuration it holds, is never used. Tables, table snapshots,
views and admitted SQL relations bind with an identity fingerprint (type,
creation time, schema and view text), so a replaced table or changed view
refuses after binding. A view that calls a dataset-qualified routine (a
persistent UDF or remote function) refuses with
`terrine.execution.snapshot_unavailable`; built-in functions inside a view are
evaluated on each query; external tables are not admitted.

Supported, matching the DuckDB reference ([live probes][bigquery]): everything
listed for PostgreSQL above, with the same tolerance contract for averages,
ratios and float windows. Exact state is `BIGNUMERIC`, which holds magnitudes
below about 5.7e38 with 38 fractional digits: an exact sum beyond it refuses with
`terrine.temporal.exact_overflow`, a float fact beyond it or non-finite refuses
with `terrine.temporal.float64_precision` (`bound_overflow` / `nonfinite_input`),
and a float fact's round-trip text is rounded to 38 fractional digits, which
perturbs the exact window sum by at most 5e-39 per value. Quotients divide to
38 fractional digits before the single rounding to binary64. Timestamps are
microsecond-exact from year 1 to 9999; `DATETIME` columns denote UTC. Ibis reads
every `NUMERIC` column as `decimal(38, 9)`, so decimal outputs carry scale 9 even
when the column declares `NUMERIC(12, 2)`; values are exact. `BIGNUMERIC` source
columns refuse with `terrine.semantic.decimal_scale_required`. Queries are billed
to the connection's project.

Refused on BigQuery, with a route: retained input capture, Dataset staging,
publication and reopening (`terrine.backend.capability_unavailable`).

## Snowflake

Snowflake isolates per statement, so a transaction gives no multi-statement
snapshot. Each materialization instead reads `CURRENT_TIMESTAMP()` once on the
caller's connection and pins every table and view reference in every query it
runs with Time Travel (`AT (TIMESTAMP => ...)`), which Snowflake applies to the
tables a view reads. Only reads are issued on the connection: no `USE`,
`ALTER SESSION`, transaction or temporary object ([live probes][snowflake-admission]
check the session's query history). Every table reached, directly, through a
view or through admitted SQL, must be a permanent or transient table with
`DATA_RETENTION_TIME_IN_DAYS >= 1`; a zero-retention table, an external or
dynamic table, a materialized view, a secure view (its definition is hidden)
or a view that calls a user-defined function (by the name written in its
definition, or schema-qualified; only routines the reading role can see in
`INFORMATION_SCHEMA.FUNCTIONS` are detected) refuses with
`terrine.execution.snapshot_unavailable`, as does a relation without `CREATED`
or `LAST_DDL` metadata (predating it, or newly replicated; read from a copy
made with `CREATE TABLE ... AS SELECT` or `CLONE`). Built-in functions inside
a view, `CURRENT_TIMESTAMP()` or `CONVERT_TIMEZONE` included, are evaluated
on each query. Snowflake evaluates row access and masking policies for the
reading role at query time, so results reflect that role's view of the data,
and a policy or mapping-table change during a run is not pinned by Time
Travel. The session's `TIMEZONE` must be `UTC`
(Ibis sets it at connect); any other value refuses with the same code and the
`ALTER SESSION` route.

Relation names follow Snowflake's identifier rules: an unquoted name such as
`events` resolves to `EVENTS`, a quoted `"events"` is exact, and
`database.schema.name` qualifies; the same rules resolve the names inside an
admitted SQL relation, so `RECENT` names a `recent` common table expression.
Every physical reference, inside admitted SQL too, is resolved against the
connection's database and schema at binding and rewritten fully qualified, so
a later `USE SCHEMA` on the caller's connection cannot redirect a pinned read;
a new materialization after such a change re-resolves the names and refuses
when they now denote other objects. A reference in admitted SQL that already
carries its own `AT`/`BEFORE` clause naming a timestamp or statement keeps it
(a relative `OFFSET` refuses), and admitted SQL calling a user
routine by name refuses as a view does. Columns are referenced exactly as the
connection's schema reports them. Every admitted relation binds with its
`CREATED` and `LAST_DDL` instants, which must not postdate the snapshot, and
the session re-reads them once each statement has completed and before any of
its rows, batched or not, reach the caller: Time Travel follows the object
rather than the name, so a table swapped (`ALTER TABLE ... SWAP WITH`), renamed
or replaced into place during a run, or a view redefined during it, refuses
with
`terrine.execution.invalid_input_binding` instead of answering from the other
object's history. A `NUMBER(p, 0)` column (Snowflake's integer types are
`NUMBER(38, 0)`) resolves as `decimal(p, 0)` when `p` exceeds the 18 digits
every `int64` holds, so a unit key or integer property beyond `int64` reaches
the output with its digits (as an Arrow decimal) instead of failing the export;
`dtype: int` properties accept such scale-zero decimal columns. Sums of these
columns stay exact in `NUMBER(38, 0)` and property filters compare against the
column's 38-digit range, so a 20-digit value is correctly excluded by a
21-digit threshold and a threshold beyond 38 digits settles without a literal.

Supported, matching the DuckDB reference ([parity probes][snowflake]): everything
listed for PostgreSQL above, with the same tolerance contract for averages,
ratios and float windows. Exact state is `NUMBER(38, 0)`; counts and decimal
sums are exact, and a sum or difference beyond 38 digits refuses with
`terrine.temporal.exact_overflow`. A float fact follows the DuckDB design with
one measured difference: Snowflake's row-wise `VARCHAR`, `VARIANT` and JSON
renderings of a double keep at most 15 or 16 significant digits (lossy), so
each float enters exact state as its `DECFLOAT` value, the binary value
correctly rounded to 18 significant digits (always round-tripping, within
`0.5 · 10⁻¹⁷` relative), rather than its shortest round-trip text. That text's
digits become an integer coefficient at a per-unit scale, so history outside
the window cancels exactly, and the window sum rounds once through
`TRY_TO_DOUBLE`, measured correctly rounded on 143 halfway, subnormal and
near-halfway cases. Each value's shortest text and its 18-digit rounding both
lie within half an ulp of it (a normal value's within `2⁻⁵³ · |x|`, a
subnormal's within `2⁻¹⁰⁷⁵`), so with `n` subnormal inputs the exact window
sums on the two backends differ by at most `1.05 · 2⁻⁵³ · Σ|x| + n · 2⁻¹⁰⁷⁴`
and, after each side's rounding, the float windows by at most
`2⁻⁵¹ · Σ|x| + (n + 1) · 2⁻¹⁰⁷⁴`: equal or adjacent doubles for a window of
normal values that does not cancel, several subnormal ulps apart otherwise
(200 values of `5e-324` sum to `1e-321` from the shortest texts and to
`9.88e-322` from the 18-digit ones), not always bit for bit. Quotients divide
19-digit normalized significands in
`NUMBER(38, 0)` within the same `|r − p/q| ≤ 6·2⁻⁵³·|p/q| + 2⁻¹⁰⁷⁵` bound. A
coefficient needing more than 38 digits (a value's 18 digits padded to its
unit's scale, the most fractional digits any finite value's text in that unit
needs: `0.1` needs 18, `1e-20` needs 38) refuses with
`terrine.temporal.float64_precision` (`bound_overflow`), as does a sum or
quotient beyond binary64's range, for which `TRY_TO_DOUBLE` returns NULL; a
non-finite value at or before the end of a non-empty window refuses with
`nonfinite_input`.

Timestamps are exact at their declared scale, nanoseconds by default, from year
1 to 9999 (`TIMESTAMP_TZ`, `TIMESTAMP_LTZ` and `TIMESTAMP_NTZ`; naive values
denote UTC). A scale other than 6 or 9 promotes losslessly for time-to-event
and first-event lookups: `TIMESTAMP_NTZ(3)` and the other scales up to 6 to
the microsecond axis, 7 and 8 to nanoseconds. Event-anchor identities render
the instant with an explicit full-precision format rather than the session's
`TIMESTAMP_NTZ_OUTPUT_FORMAT` (three fractional digits by default), so events
a microsecond apart anchor separately. Arrow carries nanosecond timestamps only
between 1677-09-21 and 2262-04-11: a nanosecond output column holding an
instant outside that range refuses with `terrine.execution.arrow_timestamp_range`
(declare the column `TIMESTAMP_TZ(6)` or cast it in a SQL relation).
Snowflake's native `ASOF JOIN` serves as-of lookups.

Refused on Snowflake, with a route: retained input capture, Dataset staging,
publication and reopening (`terrine.backend.capability_unavailable`).
Warehouse time is billed to the connection's warehouse.

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

`terrine.execution.backend.backend_capabilities(con)` inspects static evidence
without querying source metadata or changing connection state. PostgreSQL is
reported as unverified by this inspection because server identity requires a
query; `backend_provider()` performs the qualified version check before
admitting source access. Unregistered backend connections likewise return
unverified operation values.

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
public facade with optional imports blocked. It then installs the DuckDB and
pandas extras and executes the exact README tutorial to collect a compact
dataframe with completeness and reason columns. Retained staging and reopening
are exercised by the [first-dataset guide](first-dataset.md#keep-a-pinned-dataset)
in the [source integration acceptance](../../integration/warehouse_execution/test_ml_staging_examples.py).
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
| `dbt` | Core 1.10.13 / dbt-duckdb 1.9.6; [adapter tests](../../tests/adapters/test_dbt_core_after_build.py) |
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
| Read-only dialect admission, relation identity | [Verified][provider] | [Verified][postgres] | [Verified][snowflake-admission]; Snowflake identifier rules, identity re-verified after each read | [Verified][bigquery] | Use native local DuckDB, PostgreSQL, Snowflake or BigQuery; an Ibis dialect/name is not execution evidence |
| Accepted UTC timestamps, exact decimals, strict t0 and SCD2 boundaries | [Verified][acceptance] | [Verified][postgres]; microsecond timestamps, declared `numeric(p, s)` | [Verified][snowflake]; nanosecond timestamps (Arrow outputs within 1677-2262), `NUMBER(p, s)` | [Verified][bigquery]; microsecond timestamps, `NUMERIC` read as `decimal(38, 9)` | Existing timestamp precision/domain and decimal overflow refusals remain active |
| Private transaction view and owned-session cleanup | [Verified][provider] | [Verified][postgres]; consistent `REPEATABLE READ` snapshot | [Verified][snowflake-admission]; Time Travel instant on the caller's connection, reads only | [Verified][bigquery]; session transaction snapshot | Does not promise a durable source snapshot or native query cancellation |
| Retained immutable inputs and pinned generation reads | [Verified][acceptance] | Unverified | Unverified | Unverified | Use the owner-retained snapshot receipt; native source revisions remain unknown |
| Table publication and atomic fenced manifest switch | [Verified][publication] | Unverified | Unverified | Unverified | Materialize a bounded Frame if publication is unavailable |
| Bounded Arrow batch reads | [Verified][acceptance] | [Verified][postgres] | [Verified][snowflake-admission] | [Verified][bigquery] | Requires independent Arrow-batch evidence |
| Local Parquet streaming and metadata readback | [Verified][handoffs] | Unverified | Unverified | Unverified | Bounded Arrow/PyArrow path; caller-owned local destination only |
| Output row/byte limits | [Verified][exports] | Unverified | Unverified | Unverified | Do not certify source scan work or peak query memory |
| Native durable/replay snapshot | Unsupported | Unverified | Unverified | Unverified | Use retained immutable capture, not a transaction-view claim |
| Native change-feed kinds | Unverified; no kinds declared | Unverified | Unverified | Unverified | Supply separately verified change evidence; do not infer revisions |
| Native scan estimate/limit | Unverified | Unverified | Unverified | Unverified | Requested strict scan-byte budgets refuse with `terrine.budget.scan_unknown`; explicit non-strict policy warns |
| Native query cancellation | Unverified | Unverified | Unverified | Unverified | Cooperative runtime cancellation remains distinct |
| Native Parquet unload | Unverified | Unverified | Unverified | Unverified | Use verified local Arrow streaming instead |

PostgreSQL evidence was measured on PostgreSQL 16, BigQuery evidence on the US
multi-region with google-cloud-bigquery 3.46, and Snowflake evidence on an
X-Small warehouse with snowflake-connector-python 5.1 through the
`warehouse-backends` workflow (key-pair service user; no local credentials).
Unknown backends refuse with an actionable local route before schema or source
access. The [operation-gate tests][gates] separately prove that missing
publication, retained capture or Arrow evidence does not disable unrelated work.

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
[bigquery]: ../../integration/warehouse_execution/test_bigquery_parity.py
[snowflake]: ../../integration/warehouse_execution/test_snowflake_parity.py
[snowflake-admission]: ../../integration/warehouse_execution/test_snowflake_admission.py
