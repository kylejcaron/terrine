# Backend compatibility

Terrine distinguishes verified execution from SQL compilation. Native local Ibis
DuckDB is the reference execution provider. An Ibis PostgreSQL backend
(`terrine[postgres]`, Ibis 12 or newer), an Ibis BigQuery backend
(`terrine[bigquery]`) and an Ibis Snowflake backend (`terrine[snowflake]`)
materialize Frames directly from their tables.

## Backend verification status

| Backend | Verification status |
| --- | --- |
| DuckDB | Verified local reference backend |
| PostgreSQL | Verified on PostgreSQL 16 |
| Snowflake | Verified at 0.1.0.dev2 |
| BigQuery | Verified at 0.1.0.dev2 |

The support matrix and dependency extras are summarized before the operation
evidence below. The live probes and measured limits are detailed in each
backend's section.

### Optional extras

Each extra is separately declared; none is loaded by base/facade imports.
The locked Python 3.12 checks install all extras and run the integration suites.
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

Backends fall into three tiers:

- **Verified engines**: DuckDB, PostgreSQL 16, BigQuery and Snowflake. Each has
  registered identity admission, a native expression policy with the measured
  numerical contracts in its section below, and one consistent view per
  materialization. Nothing in the unverified tier changes these guarantees:
  their native conversions, refusals and session lifetimes are unaffected by
  the portable policy.
- **Opted-in unverified backends**: any other genuine Ibis backend, admitted
  for read-only operations only after `allow_unverified=True`, executed under
  the portable expression policy with coded, operation-specific refusals. See
  [unverified Ibis backends](#unverified-ibis-backends).
- **Refused**: raw driver connections, objects that merely claim an engine
  name, and registered identities that fail admission, such as an unmeasured
  PostgreSQL major or a PostgreSQL-wire-compatible server. `allow_unverified`
  never admits them. Raw DuckDB connections remain valid for native runtime
  protocols, not Ibis execution. Effects stay refused on unverified backends:
  retained input capture, staging, publication, published Dataset reopening
  and native Parquet unload refuse with `terrine.backend.capability_unavailable`
  (`status="unsupported"`) before any write.

Execution and publication share an immutable capability record without making
Model or the compiler depend on runtime. Verified providers preserve the
caller-owned connection, admit read-only SQL in its real execution dialect,
and verify physical relation identity. Each verified materialization reads
through one consistent view (a private transaction, or on Snowflake a Time
Travel instant) and releases it after success, error or cancellation.
The caller's connection remains open. A transaction view is **not** a durable
replay snapshot.

An opted-in unverified backend instead borrows the caller's connection and
reads each statement independently, as described in its section below.

Runtime DuckDB publication retains its fenced, transactional manifest switch;
Ibis SQL compilation or DDL support alone does not establish publication safety.

Verified engine identities, capability values, feature boundaries and lazy
implementation paths have one immutable registry in
`terrine.backends.registry`. Each `Engine` record names its identity admission
callback, expression policy and execution provider. Its `SessionStrategy`
describes the existing consistent-read lifetime: `TRANSACTION` for DuckDB and
PostgreSQL, `SESSION_TRANSACTION` for BigQuery, and `TIME_TRAVEL` for Snowflake.
These records do not replace the engine-specific session implementations.
The base `terrine.backends.dialect.Dialect` is the complete portable
expression policy; each verified engine overrides it in one
`terrine.backends.engines.<name>` module, and the separate `GENERIC` record
(status `unverified`, strategy `NO_SNAPSHOT`, provider
`terrine.execution._generic`) selects the unmodified base policy. Adding an
engine requires a registry entry, its dialect overrides and verified admission
evidence, not another supported-engine list in a consumer.
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
DuckDB-only. [`unverified_backend.py`](../../examples/unverified_backend.py)
opts in to the Ibis SQLite backend, shows the default refusal, the coded
refusal of an operation SQLite cannot represent, a real read and its stamp.

## Unverified Ibis backends

Any genuine `ibis.backends.BaseBackend` instance outside the registry refuses
by default, before source schema access, with
`terrine.backend.capability_unavailable` and context `backend` (the actual
backend name, for example `sqlite`), `operation="query"`,
`status="unverified"` and a route naming `allow_unverified=True` and the
verified engines. Pass `allow_unverified=True` to `Model.from_catalog`,
`Model.from_tables` or `Model.from_increment` to admit read-only operations for
that Model only; a second Model on the same connection needs its own consent,
and the choice must be a Boolean (`"true"` refuses with
`terrine.model.invalid_binding`). Admission emits
`terrine.UnverifiedBackendWarning`, a `UserWarning` subclass, once at the
caller's line; individual statements do not emit it again. Filter it with
`warnings.filterwarnings("ignore", category=UnverifiedBackendWarning)` when the
terms are accepted. Filtering changes neither the capability gates nor the
result's `engine_status="unverified"`.

### Statement-scoped reads on a borrowed connection

The opted-in provider borrows the caller's connection. Every statement reads
independently: schema checks, admission witnesses and output queries do not
share a coherent view, and a change to a source between two statements of one
materialization is visible to the later one. Terrine does not directly begin
or end transactions, change session settings, create temporary objects, clone
or disconnect that connection. Ibis schema inspection on an unverified backend
may nevertheless end an open transaction on it. Use a connection with no
pending writes or a dedicated read connection; transaction preservation is not
guaranteed. The execution snapshot records `snapshot_id=None`, the actual
backend name, unknown source generations and
`replay_equivalence="unverified"`. Owned Arrow readers are released on
exhaustion, error, early close or context closure, and a read failure during
batch consumption is translated to the same coded refusal as one at
statement start.

### Engine name, expression policy and SQL dialect

Three names stay distinct on an admitted Model. The engine name is the actual
backend (`sqlite`). The expression policy is `generic`, the portable base
policy, visible as `BoundSource.dialect`. The SQL dialect is the backend's real
compiler dialect (`sqlite`), which `Catalog.dialect` must declare; declaring
`generic` refuses with `terrine.semantic.dialect_mismatch`. A `SqlQuery`
relation is admitted by the same read-only parse as on verified engines, in
that real dialect. Verified engines then check each compiled study statement
for functions their own catalogs or routine rules do not own; an unverified
backend has no such rule, so every compiled statement is instead re-parsed
with the same read-only rules and built-in allowlist as admitted SQL, failing
closed: a statement that lowers to a dialect function without an admitted
read-only contract refuses with `terrine.sql.invalid_read_query` rather than
executing. Every such verification step, on every engine, runs on a fresh
thread so that its recursion depth is independent of the caller's stack; a
statement too deeply nested to verify refuses with the same code and
`reason="too_deep_to_verify"`, which is distinct from being shown unsafe.

### Operation admission before source data

Only required read primitives are attempted without verified evidence:
`BackendCapabilities.require` admits `None` for `dialect_admission`,
`exact_timestamps`, `exact_decimals` and `arrow_batches` after generic
admission, never a `False` capability. Unavailable primitives refuse with
`terrine.backend.capability_unavailable`, the actual backend, the owning
operation and the original backend exception as cause; `TerrineError`,
cancellation and process-control exceptions pass through unchanged. Two
witnesses run lazily, once per execution context, and only when a relation
needs them. Materialization and the bounded preview submit every relation
they will run (output, audit and each integrity check) for admission as soon
as the study is compiled, so an unsupported primitive anywhere in it refuses
before the first source row is read, not after the roster checks have
scanned; an independent statement on an
execution context is admitted when it is issued:

- A query carrying decimal state (study counts, sums, averages and ratios all
  do) first compiles and executes scalar known answers for scale-9 low digits
  at a large offset (`9007199254740993.000000001`), cancellation, the domain
  edges `±(10**29 - 10**-9)`, signed truncating quotients, both `int64`
  endpoints and a 38-digit coefficient. It also checks float quantization:
  `±6e-10` must become `±1e-9`, and the exactly representable tie `±1/1024`
  must become `±0.000976563`. A backend that truncates or rounds ties to even,
  whose answers otherwise differ, or whose decimals are stored by affinity
  (SQLite), refuses `operation="exact_state"`; a query without decimal state,
  such as a roster read, never runs this witness.
- A query carrying an aware-timestamp delta first reconstructs four known UTC
  wall clocks (2025-07-01, 2025-01-15, 1969-12-31 and 1850-06-01) through the
  portable conversion. Ibis lowers that delta through session wall clocks on
  PostgreSQL-family backends, so a session whose zone is not UTC reconstructs
  at least one of them wrongly and refuses `operation="utc_wall_clock"` instead
  of answering by the session offset; the registered PostgreSQL engine is
  unaffected because it keeps its native conversions on a UTC private
  connection. DuckDB's lowering is instant-exact and admits.

Nanosecond timestamps refuse the portable UTC conversions (`utc_wall_clock`,
`utc_instant`) while the expression is built, because Ibis lowers the integer
intervals that rebuild the instant at microsecond resolution at best;
microsecond and unscaled values convert exactly, and signed sub-second parts
keep nanosecond support.

### Shared numerical contract

Float measures have two arithmetic modes, chosen per metric declaration
(`tr.sum("amount")` is native; `tr.sum("amount", exact=True)` or a Catalog
measure without `exact: false` is exact). Counts, integer measures, declared
decimals, keys and timestamps are always exact. The mode is part of the
measure's identity: switching it rebuilds retained execution outputs, and every
metric-valued output column records it in `ColumnMeta.arithmetic`
(`"native"` when any contributing measure is native, else `"exact"`).
Float32 widens to Float64 first. NULL stays NULL. There is no bitwise or
order-independence promise in either mode. Row predicates compare original
values; output thresholds compare published results.

#### Selected-input aggregation

Ordinary and retained compilation use the same local `aggregate_windows`
reducer. Additive outputs aggregate selected source-window contributions,
from raw rows or eligible noncumulative day summaries, rather than lifetime
prefix differences. Selection, availability and numerical consumption are
per output: a wider count, another unit or an unavailable output does not
activate an otherwise unconsumed sum. Unused historical buckets and gaps do
not enter that sum's arithmetic.

Exact inputs retain the backend's coefficient domains and final output
narrowing. Exact floating inputs are selection-masked and quantized individually
before reduction; discarded nonzero terms retain `below_resolution`. Wide
integers that Ibis describes as decimals convert losslessly to the exact
coefficient domain before nullable selection, avoiding an unintended narrowing
to the declared decimal alias. Fixed-domain accumulation guards apply to each
consumed primitive/window, not the union of all source access.

An explicitly identified exact-type overflow retains
`terrine.temporal.exact_overflow`; native or ambiguous engine arithmetic errors
preserve their original cause rather than being relabeled as exact overflow.
This policy does not imply live-warehouse conformance evidence.

#### Native float arithmetic (default)

Selected contributions are summed in binary64 by the backend. Means divide by
their actual denominator; ratios convert each operand once and divide once.
Normal order dependence, rounding and underflow follow backend behavior. There
is no lifetime-history certificate, bitwise cross-engine promise or universal
relative-error bound for ill-conditioned sums and ratios. A small recent
window no longer loses its contributions through subtraction of large
excluded totals. Raw and day reductions preserve selected inputs, not every
exceptional-arithmetic outcome under different physical summation orders.

Selected nonfinite inputs refuse `terrine.temporal.float64_precision` with
`reason="nonfinite_input"`. Where the backend returns a nonfinite sum or
quotient, a cheap result check refuses with `reason="bound_overflow"`.
Where arithmetic itself raises, its backend execution cause is preserved;
there is no promise that every engine overflow or underflow becomes that
package refusal. For example, PostgreSQL can raise while summing two selected
`1e308` values or dividing a subnormal sum. Do not precompute magnitude
certificates merely to avoid these permitted backend errors.

Nonzero contributions below $5\cdot10^{-10}$ are retained in native mode,
subject to ordinary binary64 rounding and underflow. Unlike exact mode,
native arithmetic does not intentionally quantize them away.

#### Specialized cumulative and cohort arithmetic

The source-window change does not relax the separate safeguards for cumulative
trajectories or cohort reductions. PostgreSQL and portable policies (inherited
by BigQuery and Snowflake) still withhold unsafe inputs before those operations.
Their `bound_overflow` refusals can cover finite results when:

- cumulative or cohort magnitude totals reach about half the binary64 range;
- cohort mean normalization falls below $2^{-1020}$ with a divisor of
  magnitude at least one;
- rescaling falls below $2^{-1020}$ with a factor below one, or exceeds half
  the range with a factor at least one.

The existing quotient/product guards and missingness rules remain in force.
DuckDB may publish the corresponding finite results. An aggregate cannot be
made safe by wrapping its result in CASE; required guards withhold its inputs.

#### Exact float arithmetic (`exact=True`)

Every finite input is quantized once to a `DECIMAL(38, 9)`-scale value by the
engine's float-to-decimal conversion (nearest, ties away from zero, with
binary64 conversion error of about $10^{-16}$ relative), so a value within
that error of a halfway point may round either way; only the stated bound is
promised. The exact binary64 magnitude must be below $10^{29}$: the literal
`1e29` is admitted because its exact value is just below that boundary; the
next float up refuses `terrine.temporal.float64_precision` with
`reason="out_of_range"`. Non-finite inputs use `reason="nonfinite_input"`;
a published result beyond float64 uses `reason="bound_overflow"`.

The quantized value joins the declared-decimal coefficient path at scale 9:
DuckDB `BIGNUM`, PostgreSQL `NUMERIC`, BigQuery `BIGNUMERIC`, Snowflake
`NUMBER(38, 0)`, and portable `DECIMAL(38, 0)`. Selected coefficients combine
exactly within the backend's supported domain, so excluded history cannot
cancel a recent window. The bound depends on the window $W$ alone:

$$
|\hat S - S| \le e_S = 10^{-9}\,n_W + 10^{-12}\sum_{i\in W}|x_i|.
$$

A nonzero input smaller than $5\cdot10^{-10}$ quantizes to zero and leaves
the sum. The window still publishes the sum of its retained contributions,
`__complete` is unchanged, and `__reason` carries `below_resolution` whenever
at least one nonzero contribution of that window was discarded (a window of
exact zeros, or an all-null one, keeps its usual reason). Means and ratios
derived from such a sum carry the same note, as do cumulative trajectory
values and complete cohort summaries built from noted periods; a quantized
denominator that is exactly zero is still `zero_denominator` with a NULL
ratio. Declare decimal measures or rescale (for example into micro-units)
when that scale matters.

The portable policy caps the integral intermediate at $\pm(10^{29}-1)$
before its scale-9 cast, so a short float-to-decimal conversion that rounds
an admitted boundary value up to $10^{29}$ cannot overflow that cast;
the resulting value is still subject to the same window-sum bound.

#### Means and ratios

An exact-derived mean divides its sum's bound by its actual denominator,
including calendar-day means. Means and ratios convert each exact operand at
its own scale once, then divide once in float64. A native operand enters as it
is, without a general propagated-error certificate. A denominator that is
exactly zero returns NULL with `zero_denominator`. For operands with stated
bounds, when $|\hat D| > e_D$:

$$
|\hat r-N/D| \le
\frac{|\hat N|e_D+|\hat D|e_N}{|\hat D|(|\hat D|-e_D)}
+2^{-51}|\hat r|.
$$

For $0 < |\hat D| \le e_D$ the ratio is returned with no stated accuracy.

#### When to choose exact

- Quantized, order-independent source sums are useful when selected
  contributions have widely different magnitudes or cancellation, provided
  the fixed scale preserves the resolution the study needs.
- Exact sums agree across engines and summation orders within their stated
  quantization/conversion bound, except for permitted conversion ties. Native
  sums make no such order-independence promise.
- Nonzero contributions below $5\cdot10^{-10}$ can vanish in exact mode;
  native mode retains them subject to backend rounding and underflow. Declare
  decimals or rescale when that resolution must survive exact quantization.
- Exact mode costs coefficient arithmetic and requires a backend with the
  necessary exact domain. An engine without it refuses
  `terrine.temporal.exact_unsupported`; bounded coefficient engines can refuse
  `terrine.temporal.exact_overflow` even when native arithmetic would return a
  finite value. Choose from the required result contract, not a universal
  cost or accuracy ranking.

Signed integer quotients still truncate toward zero, including both `int64`
endpoints, and are NULL beyond `int64` or for a zero divisor.
- Bounded exact accumulations are admitted only when a binary64 bound on the
  selected magnitudes proves the exact sum fits the coefficient type. With
  `n` contributions, the computed bound `B` lies within
  `e = (1 + u)(1 + γₙ₋₁) − 1` of the true magnitude sum
  (`γₖ = k · u / (1 − k · u)`); admission requires
  `B < limit · (1 − e)`. Otherwise inputs are withheld before addition and
  consumers refuse `terrine.temporal.exact_overflow`, including exact-float
  coefficients. This can conservatively refuse opposite-sign selected inputs
  even when their signed sum would fit; excluded history is not part of that
  bound. DuckDB's `BIGNUM` coefficient accumulation is unbounded. Final output
  domains and the separate cumulative/cohort safeguards still apply.
- Timestamp comparisons use backend-native order or exact signed timestamp
  parts as appropriate. Fixed-offset days preserve the original timestamp
  precision; unused source timestamps are masked before calendar
  normalization. As-of selectors retain their union/window fallback, while
  additive source windows use local membership reductions. NULL-ignoring
  extrema and portable integer sequences remain backend-policy operations.

### Provenance stamp

Every materialized, staged or exported result records its engine stamp in
`Provenance`: `engine_name` is the actual backend (for example `sqlite`),
`dialect` the admitted expression policy (`generic` after opt-in, otherwise the
registered engine) and `engine_status` that policy's registered status. The
stamp survives Arrow, JSON, manifests and exports unchanged, and
`engine_name` always equals `execution_snapshot.backend`. Results saved before
the stamp existed carry none of the three fields and decode as their recorded
verified execution dialect, because no earlier writer had an unverified path; a
partial stamp, one contradicting the registered policy or the execution
snapshot's backend, or an absent stamp with an unregistered execution backend
refuses with `terrine.metadata.invalid_engine_stamp` (`reason` of `partial`,
`inconsistent` or `unknown_legacy_engine`, plus the present `fields`) and is
never upgraded to verified.

### What is and is not measured

The policy was measured on DuckDB 1.5.6 and PostgreSQL 16 by forcing it in
place of their native policies: the [seven engine known answers][conformance]
(whole-day schedules, NULL-ignoring extrema, float and scale-2 decimal
cancellation, non-UTC conversions, tied events, NULL observation censoring and
strict as-of cutoffs) pass unchanged with `--generic-dialect`, and the
[portable numerical and relational oracles][generic-oracles] check
fixed-scale entry and the coefficient domain, filtered wide source decimals,
signed quotients, window cancellation and order invariance, domain and
accumulation refusals, zero versus NULL versus non-finite flags, session-zone
independent UTC round trips, signed sub-second parts, daylight-saving deltas
and as-of request multiplicity against independent Python `Decimal`,
`Fraction` and integer oracles. Real unregistered-backend
reads are exercised on [Ibis SQLite][generic-reads]: roster reads, integer
counts through the execution context, SQL relation admission, borrowed-connection
lifetime, early reader release and the operation-specific `exact_state`
refusal. This is evidence for the policy on those connections, not a promise
that every Ibis backend materializes every Frame; coverage remains
operation-specific and refusals carry the operation that was unavailable.
The forced conformance run is part of the ordinary integration and
PostgreSQL checks; run the forced checks locally with
`uv run pytest integration/conformance integration/backends/test_generic.py --generic-dialect`
and, on a configured scratch server,
`uv run --extra postgres pytest integration/conformance integration/backends/test_generic.py --generic-dialect -m postgres`.

Unverified reads never enable retained input capture, staging, publication,
published Dataset reopening, native Parquet unload or transaction snapshots.
Strict scan budgets still refuse unknown source costs. Suppressing the warning
does not grant these capabilities or establish consistency.

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
fixed, scheduled and event anchors, event panels, time-to-event shapes, cohort
trajectories with their total and mean summaries, and observed state panels,
with rows identical to the DuckDB reference
([differential probes][postgres]). Timestamps are exact at PostgreSQL's native
microsecond precision. Decimal columns need a declared `numeric(p, s)` with
`p <= 38`; unconstrained `numeric` refuses with
`terrine.semantic.decimal_scale_required` and NaN/infinite values with
`terrine.backend.nonfinite_decimal`. Sums that do not fit the output type refuse
with `terrine.temporal.exact_overflow`.

Prefix and first-occurrence lookups use set-based union/window evaluation,
not correlated top-one subqueries. Equal-timestamp ordering retains strict
prefix cutoffs and the inclusive lower bound of a first-occurrence window;
duplicate requests keep their multiplicity. Lookup keys use the UTC wall
clock at native microsecond precision, including naive source timestamps.
Nullable anchor identities remain null-safe while admitting hash/merge joins.
Struct anchor identities, such as an event panel's `{event_id, period}`, are
anonymous records on PostgreSQL: a missing one stays NULL, distinct from a record
of NULL fields, and results holding them are read in binary, which carries each
field's type, so they publish as the same Arrow struct as the DuckDB reference.
Compatible integer unit keys use a lossless common timeline type, including
different roster and event key widths; output keys retain their original types.

Float windows, averages and ratios use the [shared numerical contract](#shared-numerical-contract),
not bitwise parity with DuckDB. Native float measures sum in `float8`, and
because `float8` raises on overflow and underflow the PostgreSQL policy
withholds sums, quotients and products before evaluating them (the finite
refusals listed in the contract). Exact float measures accumulate scale-9
coefficients in `NUMERIC`; each exact quotient operand converts at its own
scale before one float64 division. These columns carry
`numeric_policy=None`, their `arithmetic`, and no error column. A non-finite
or out-of-range input at or before the end of a non-empty window refuses; an
empty sum window remains zero.
PostgreSQL rounds in unconstrained `NUMERIC` at scale 9, so a conversion
that rounds an admitted boundary input up to $10^{29}$ cannot overflow an
intermediate fixed-precision cast.

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
ratios and float windows. Exact coefficient state is `BIGNUMERIC`, whose
accumulation limit remains that of declared decimals; an exact accumulation
beyond it refuses with `terrine.temporal.exact_overflow`. Exact float
measures quantize at scale 9 under the shared contract, native ones sum in
`FLOAT64` under the portable guards, and quotients divide converted float64
operands once. Timestamps are
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
`terrine.temporal.exact_overflow`, including float coefficients.
Exact float measures quantize at scale 9 and share this exact coefficient
state; native ones sum in `FLOAT` under the portable guards. Unsafe bounded
accumulations are withheld and refuse rather than publishing partial sums.
Means and ratios convert each operand once with its scale and divide once in
float64 under the shared error bounds.

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
admitting source access. An unregistered genuine Ibis backend reports the
generic record: snapshots, publication, retained capture, the runtime reader
and native unload `False`; read primitives, scan bounds and cancellation
`None`. Raw drivers report every value as `None`.

## Python and dependency support

The package requires Python 3.12 or later, with no upper version bound, and supports
macOS and Linux; the owner runtime and Parquet export refuse on Windows. CI
exercises CPython 3.12 and 3.14. Native dataset execution requires
`terrine[duckdb]`; base Terrine has no mandatory backend, dataframe, orchestration
or modeling library. The local engine supports DuckDB `>=1.5,<2`. Before the
first source read that uses exact arithmetic, execution checks three arithmetic
known answers once per execution context; count-only paths need no witness.
Ibis starts at 11 and SQLGlot at 29: Ibis 10 constrains SQLGlot below the version
needed for native `BIGNUM` types.

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

CI runs this command on Python 3.12, 3.13, and 3.14 with highest and lowest direct
dependency resolution. Lowest resolution uses `uv pip compile --resolution
lowest-direct --only-binary :all:`: the floor is the lowest compatible **wheel**
combination for that interpreter/platform, not an assurance that older dependency
sdists can be built on new Python versions. Source-building third-party native
libraries is not the supported installation path. Transitive dependencies are
resolved normally; the job does not claim a lowest-transitive combination.

Run either resolution locally after building; use `3.12`, `3.13`, or `3.14` to
match the interpreter in the distribution matrix:

```sh
uv build
uv run python -m tests.integration.distribution_smoke \
  --wheel dist/*.whl --sdist dist/*.tar.gz --python 3.12 \
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

Python 3.13.5 also passed 186 spec, metadata, error, window and column-compilation
tests with `python -m pytest -q -n 4 tests/spec tests/test_metadata.py
tests/test_errors.py tests/test_windows.py tests/compile/test_columns.py`.
The wheel matrix is narrower than the full operation and optional-adapter suites;
do not infer warehouse or downstream-library support from the tutorial.


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
| Scale-9 float quantization: signed sub-nanounit values, exact ties, magnitude bounds and admitted boundary | [Local probe](../../tests/backends/test_numeric.py) | [Live probe][float-quantization] on every push and weekly; unconstrained `NUMERIC` rounding | [Weekly live probe][float-quantization]; native `NUMBER` casts under the portable split-and-cap policy | [Weekly live probe][float-quantization]; native `NUMERIC` casts | Conversion error near halfway points is allowed only within the shared window-sum bound; the next binary64 above `1e29` refuses |
| Bounded float coefficient accumulation (`9e28 + 9e28`) | [Forced portable-policy probe][conformance] on every push; native `BIGNUM` is unbounded | [Forced portable-policy live probe][conformance] on every push and weekly; native `NUMERIC` admits this sum | [Weekly live probe][conformance]; `NUMBER(38, 0)` refuses before returning output | Native `BIGNUMERIC` admits this sum | Bounded policies refuse with `terrine.temporal.exact_overflow`; no materialized result is returned |
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
The float-quantization and bounded-overflow rows identify scheduled live coverage,
not a claim that the cloud probes ran locally. BigQuery and Snowflake probes run
in the weekly `warehouse-backends` workflow; a manual dispatch incurs warehouse
cost and requires the owner's approval.
Unregistered backends refuse with an actionable local route before schema or
source access unless a Model explicitly opts in; opted-in backends are not in
this table, since their operations are admitted per statement, not verified.
The [operation-gate tests][gates] separately prove that missing publication,
retained capture or Arrow evidence does not disable unrelated work.

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
[handoffs]: ../../integration/warehouse_execution/test_ml_staging_reads.py
[exports]: ../../integration/warehouse_execution/test_ml_staging_exports.py
[gates]: ../../tests/runtime/test_backend_capabilities.py
[increment]: ../../tests/compat/test_increment_semantics.py

[distribution]: ../../tests/integration/distribution_smoke.py
[sketches]: ../../tests/temporal/test_sketches.py
[libraries]: library-handoffs.md
[postgres]: ../../integration/warehouse_execution/test_postgres_windows.py
[bigquery]: ../../integration/warehouse_execution/test_bigquery_parity.py
[snowflake]: ../../integration/warehouse_execution/test_snowflake_parity.py
[snowflake-admission]: ../../integration/warehouse_execution/test_snowflake_admission.py
[conformance]: ../../integration/conformance/test_engines.py
[generic-oracles]: ../../integration/backends/test_generic.py
[generic-reads]: ../../integration/execution/test_generic_provider.py
[float-quantization]: ../../integration/backends/test_numeric.py
