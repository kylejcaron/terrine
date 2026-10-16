# terrine Temporal Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bind native terrine semantic catalogs (with an optional increment adapter) and declared completeness to a sparse, timestamp-correct temporal engine that produces type-preserving additive study features, checked Float64 results, mixed-readiness results, and strict SCD2 attribute lookups.

**Architecture:** `terrine.semantic.{models,io,bind,sql}` owns the minimal versioned Catalog, safe loader, normalized descriptors, source binding, and SQL admission; it has no required increment import. `Model.from_catalog(...)` is primary and `Model.from_increment(...)` is a lazy optional adapter. The temporal engine consumes the same immutable Bound* descriptors from either path and explicit source readiness. Sparse daily sufficient statistics serve day-aligned queries, while event-resolution prefix state and union/window as-of fallback serve arbitrary timestamp boundaries without anchor-by-event or unit-by-day fanout. Integer/decimal states remain exact; Float64 states carry certified error bounds. Calendar-day averages require aligned endpoints; nonadditive measures and unsupported transforms are explicit refusals, not approximations.

**Tech Stack:** Python >=3.12,<3.15; ibis-framework with DuckDB reference backend; sqlglot for read-only SQL admission; pydantic v2; PyYAML; pyarrow; pytest. Increment is an optional adapter extra only.

**Spec:** `docs/superpowers/specs/2026-10-16-terrine-handoff.md` and shared interfaces in `docs/superpowers/plans/2026-10-16-terrine-build.md`.

## Global Constraints

- Native terrine semantic models, loading, binding, SQL admission, and temporal execution form the core. `Model.from_catalog` works when importing `increment` is blocked; only `terrine.adapters.increment` may import it, and only when the optional adapter is invoked.
- `Model.from_catalog(path_or_catalog, *, con, readiness, units=None, event_ids=None)` is primary. `Model.from_increment(path, *, con, readiness, units=None, event_ids=None)` is a lazy optional compatibility facade supplied by Task 0.2 (`terrine[increment]`); it maps upstream definitions to the same normalized Bound* descriptors. No core semantic/temporal module imports increment.
- Task 4.2 adds optional `input_binding: VerifiedInputBinding | None = None` to both Model facades and explicit backend metadata verification, as specified in the Study API. Its runtime handles stay model-side; ExecutionSnapshot serializes only safe receipt IDs and source-generation identities. Task 1.1 does not implement this later execution capability or perform hidden snapshot queries.
- Day boundaries use `Catalog.day_boundary` fixed-offset grammar and local-day conversion; timestamp anchors remain UTC-aware instants and are never floored. Public Coverage stores dates only; model binding normalizes it once to timezone-aware UTC `BoundCoverage(start, end_exclusive)` using the Catalog boundary.
- Windows are half-open `[start,end)`; temporal prefix `P(x)` includes only events with timestamp `< x`; additive window `[a,b)` is `P(t0+b)-P(t0+a)`.
- Never construct anchor × event/window range joins or densify unit × day. Aggregate sparse source rows and perform point lookups; union-and-window as-of fallback may scale with prefix states plus lookup points, not source-event fanout.
- Coverage is explicit, per fact source, and represents full local days `[start midnight, midnight after inclusive through)` bound once to UTC with the Catalog's fixed offset; max observed timestamp MUST NOT imply completeness.
- SCD2 validity intervals are half-open; strict “before cutoff” requires `valid_from < cutoff AND (valid_to >= cutoff OR valid_to IS NULL)`; duplicate/overlapping validity histories are refusals. Event-time dimension enrichment for measure filters uses ordinary interval membership, not the strict feature-as-of rule.
- No dynamic SQL, arbitrary user SQL fragments, or undeclared dialect behavior; use versioned safe Catalog data, sqlglot admission against the execution dialect, and ibis expression construction.
- `sum`, event `count`, `avg_event`, `avg_calendar_day`, ratio, and conversion are near-term capabilities. Only `avg_calendar_day` requires positive whole local days with both endpoints aligned; other supported aggregations retain arbitrary event-resolution boundaries. Nonadditive capabilities and all declared winsorization transforms are explicit refusals, not approximated by prefixes.
- Static Catalog binding and Design validation/compilation are lazy and metadata-only; no hidden source snapshot, watermark, count, or integrity queries. Task 4.2 owns one pinned `ExecutionSnapshot` for exact integrity checks, eligibility, and final export; temporal execution binds all fact/dimension/roster relations to that same snapshot or refuses if unavailable. Never infer snapshot identity from a hash or separate mutable-table copies.

## Review Focus

1. **Schema drift and unsafe source SQL.** A bound descriptor reflects declared source/entity/fact/property/dimension/measure/metric fields; native Catalog schema and SQL admission reject unknown fields, unsupported dialect, non-read-only SQL and schema drift before execution.
2. **Equal timestamps and event-level cutoff.** Prefix subtraction excludes every event exactly at cutoff, including duplicate timestamps. Task 1.3 tests equal-time groups on both boundaries.
3. **Mixed completeness.** A ready numerator and incomplete denominator cannot yield a ratio; no source watermark stands in for another. Task 1.3 checks per-column observed-until and source intersection.
4. **Null meaning.** No events in a fully covered interval is observed zero for additive counts/sums; a nonempty all-null value series has null sum/averages; unknown/uncovered/censored intervals are null with reason. Tasks 1.2–1.3 test all states.
5. **Historical boundaries and precision.** A dimension version starting at t0 is not “before t0”; large integer/decimal prefixes subtract without float rounding. Float64 windows must certify both historical accumulation and endpoint subtraction, not merely the final subtraction. Tasks 1.2–1.3 pin these cases.
6. **Observation versus membership.** Unit entry/exit controls roster membership at t0 only; explicit observation bounds independently constrain measurements, and absent bounds rely only on the declared source-completeness assumption.
7. **Dimension-backed event filters.** Facts are enriched using each dimension's selected source entity key at event time before measure filters; no accidental join to the analysis unit key and no duplicate filter application.

---

## File structure and shared contracts

Create or own these files; use Task 0.1 shared `Coverage`, `Window`, and `ColumnMeta` types rather than redefining them. Keep boundaries: native semantic binding is the only source-descriptor bridge; temporal modules accept bound terrine descriptors and emit ibis expressions; pure result metadata lives in `terrine.data`.

- `terrine/semantic/models.py`: versioned native Catalog and normalized Bound* descriptor models.
- `terrine/semantic/io.py`: Python Catalog passthrough and safe, strict versioned YAML load/dump; no arbitrary callbacks.
- `terrine/semantic/bind.py`: source/schema resolution, descriptor binding, metric/measure validation, and `normalize_events`.
- `terrine/semantic/sql.py`: read-only SQL admission against connection dialect.
- `terrine/adapters/increment.py`: optional-only increment translation, owned by Task 0.2; not imported by temporal/core.
- `terrine/temporal/readiness.py`: `Readiness(sources: Mapping[str, BoundCoverage])`, per-source lookup.
- `terrine/temporal/events.py`: normalized event relation with timestamp, selected canonical unit key, retained physical entity columns, fact/property columns, and optional stable event ID.
- `terrine/temporal/daily.py`: sparse local-day sufficient statistics.
- `terrine/temporal/prefix.py`: sparse cumulative states by selected unit and metric/source; exact integer/decimal accumulators, checked Float64 certificates, and shared numerical refusal declarations/helpers.
- `terrine/temporal/lookup.py`: timestamp-prefix point lookup; native backend as-of and union/window fallback.
- `terrine/temporal/aggregate.py`: half-open window subtraction and kind-specific aggregations, readiness propagation, ratio/conversion.
- `terrine/temporal/attributes.py`: normalized SCD2 ranges, event-time dimension enrichment, and strict feature as-of lookup.
- `terrine/data.py`: Task 0.1 owns shared `Coverage`, `BoundCoverage`, `ColumnMeta` and frame metadata; retain its observation/window/role fields and add the two precision fields specified below through that owner. `Window` is imported from `terrine/windows.py`.
- `tests/temporal/`: native/adapter binding, safe SQL, sparse-state, lookup/aggregate, observation bounds, and SCD2/enrichment acceptance tests; fixtures include non-UTC boundary and duplicate-time cases.

Shared exact interfaces: Task 0.1 owns `Coverage(start: date, through: date)` and `BoundCoverage(start: UTC timestamp, end_exclusive: UTC timestamp)`. `Model.from_catalog` binds Coverage once using `Catalog.day_boundary`; `Readiness` stores bound intervals only. `UnitSource(table, key, entered_at=None, exited_at=None, observed_from=None, observed_until=None)` uses `observed_from`/`observed_until` as optional TIMESTAMP column names. The anchor compiler carries their values as nullable UTC `_observed_from`/`_observed_until`; an event anchor may receive them from an explicitly configured roster, otherwise has no unit bounds. `entered_at`/`exited_at` are timestamp column names constraining candidate membership at exact t0 as `[entered_at, exited_at)` only. Null observation bound means unbounded on that side within declared source completeness; absent observation columns assume continuous observation only within declared source Coverage. Each result's requested half-open window must start at or after `_observed_from` and end at or before `_observed_until`; otherwise it is incomplete. `Window` belongs to `terrine.windows`; it is signed elapsed seconds with `start < end`; `parse_duration("7d") == 604800`, `parse_duration("28d") == 2419200`, `parse_duration("1w") == 604800`.

Use Task 0.1's native `TerrineError(message, *, code, context)` and `RefusalSpec(code, template).error(**context)` for stable terrine-owned refusals; no increment error types are required by core.

```python
@dataclass(frozen=True)
class BoundFilter:
    property_name: str
    op: Literal["equals", "not_equals", "in", "not_in", "gt", "gte", "lt", "lte", "between"]
    values: tuple[JsonScalar, ...]

@dataclass(frozen=True)
class PropertyBinding:
    name: str
    column: str
    dtype: Literal["string", "int", "float", "bool", "date"]
    as_of: Literal["event_time", "pre_exposure", "static"]
    dimension_name: str | None

@dataclass(frozen=True)
class BoundValidity:
    valid_from: str | None
    valid_to: str | None
    changed_at: str | None

@dataclass(frozen=True)
class BoundDimension:
    name: str
    relation: RelationSpec
    source_entity: str  # logical entity on event source, may differ from analysis unit
    entity_column: str  # physical join-key column in dimension relation
    validity: BoundValidity | None
    properties: Mapping[str, PropertyBinding]

@dataclass(frozen=True)
class BoundMeasure:
    fact: str
    source_name: str
    aggregation: Literal["sum", "count", "avg_event", "avg_calendar_day", "min", "max", "count_distinct"]
    filters: tuple[BoundFilter, ...]
    window_days: int | None
    value_type: str | None

@dataclass(frozen=True)
class BoundSource:
    name: str
    relation: RelationSpec
    timestamp_column: str
    entities: Mapping[str, str]  # logical entity name -> physical source column
    facts: Mapping[str, str | None]
    properties: Mapping[str, PropertyBinding]
    dimensions: tuple[BoundDimension, ...]
    dialect: str
    day_boundary: str

@dataclass(frozen=True)
class BoundMetric:
    name: str
    entity: str  # logical Catalog entity name
    entity_columns: Mapping[str, str]  # source name -> selected physical unit column
    kind: Literal["mean", "conversion", "ratio"]
    measures: tuple[BoundMeasure, ...]  # ratio has numerator then denominator
    source_names: tuple[str, ...]
    transforms: tuple[TransformSpec, ...]  # serializable declaration retained even when unsupported and refused

@dataclass(frozen=True)
class TemporalResult:
    table: ibis.Table
    columns: Mapping[str, ColumnMeta]

def normalize_source(con: ibis.BaseBackend, catalog: Catalog, source_name: str) -> BoundSource
def bind_metric(catalog: Catalog, metric_name: str) -> BoundMetric
def normalize_events(source_table: ibis.Table, source: BoundSource, unit_column: str) -> ibis.Table
def enrich_events(events: ibis.Table, source: BoundSource, dimension_relations: Mapping[str, ibis.Table]) -> ibis.Table
def sparse_daily(events: ibis.Table, measure: BoundMeasure, boundary: str, unit_column: str) -> ibis.Table
def sparse_prefix(state: ibis.Table, measure: BoundMeasure, *, resolution: Literal["day", "event"], unit_column: str) -> ibis.Table
def lookup_prefix(points: ibis.Table, prefix: ibis.Table, *, unit_column: str, timestamp_column: str, strategy: Literal["native", "union_window"] = "native") -> ibis.Table
def aggregate_window(anchors: ibis.Table, metric: BoundMetric, measure_states: Mapping[str, ibis.Table], window: Window, readiness: Readiness, *, boundary: str) -> TemporalResult
def attribute_asof(points: ibis.Table, history: ibis.Table, dimension: BoundDimension, property_name: str, *, unit_column: str, cutoff_column: str, strict: bool = True) -> ibis.Table
```

Native `Catalog` has `schema_version: Literal[1]`, `day_boundary`, `dialect`, and mapping registries `sources`, `facts`, `properties`, `dimensions`, `measures`, `metrics`, keyed by canonical names (records do not duplicate names). Records are limited to: `Source(relation, timestamp, entities: Mapping[logical_entity, physical_source_column], facts: tuple[fact_name,...], properties: tuple[property_name,...], dimensions: tuple[dimension_name,...])`; `Fact(source, column: str | None)`; `Property(source, column, dtype, as_of, dimension: str | None)`; `Dimension(relation, source_entity: logical_source_entity_name, entity_column: physical_dimension_column, validity: Validity | None, properties: tuple[property_name,...])`; `Measure(fact, aggregation, filters=(), window_days=None)`; `Metric(kind, entity: logical_entity_name, measure | numerator/denominator, winsorization=None)`. Each source fact/property/dimension reference resolves to the top-level registry; each fact/property identifies its owning source. A dimension's `source_entity` identifies the event-side logical key, whose declared physical source column joins to its explicit physical `entity_column`; it may differ from the analysis entity. `relation` is a named table or admitted read-only SQL; runtime Ibis expressions may bind but are not serializable unless represented by a named relation. Filters are typed property comparisons with literal values; no callables, SQL fragments, or arbitrary expression language. Missing/unmatched dimension history and unavailable filter property values are unknown, not false; materialization refuses an unknown filter result by default (`terrine.filter.unknown`). `IsNull` may test a known observed null only. YAML loading uses safe constructors, rejects duplicate keys/unknown fields/schema versions, and round-trips the same Python models. Catalog and descriptor validation are metadata-only.

`RelationSpec` is a tagged union: `TableRef(kind="table", name: str)` or `SqlQuery(kind="sql", sql: str)`; Catalog supplies the declared dialect. Never guess whether an arbitrary string means SQL or a table. Runtime expression bindings are a separate nonportable input, not a third YAML constructor. Ratios may use different physical unit columns across sources: `BoundMetric.entity_columns[source_name]` resolves the same logical unit per source, and normalization gives both the same internal unit-column name before aggregation/joining.

Minimal native catalog for a table containing session events only (other measures/properties use the same registries):

```yaml
schema_version: 1
day_boundary: UTC
dialect: duckdb
sources:
  sessions:
    relation: {kind: table, name: session_events}
    timestamp: occurred_at
    entities: {user: user_id}
    facts: [session]
    properties: []
    dimensions: []
facts:
  session: {source: sessions, column: null}
properties: {}
dimensions: {}
measures:
  session_count: {fact: session, aggregation: count, filters: []}
metrics:
  sessions: {kind: mean, entity: user, measure: session_count}
```

Here `kind: mean` describes a unit-level metric whose downstream population summary is a mean; `aggregation: count` determines each unit's dataset value. It does not average events or overwrite the selected window. The native setup tutorial must explain this distinction and execute this example without any increment installation.
Use shared Coverage/ColumnMeta from `terrine.data` and Window from `terrine.windows`. Offsets are signed elapsed seconds; the fixed day boundary controls day bucketing and Coverage binding, not flooring t0. A source's `entities` map names logical entities to actual physical columns; metric `entity` selects one declared logical key. Other declared physical entity columns remain available for dimension joins only; no implicit composite key or SQL alias requirement. Every function receives relations explicitly; no global connection state. `TemporalResult` carries each output's value, observed-until, complete and reason. For every requested interval intersect the source's `BoundCoverage` with the optional unit observation interval `[_observed_from, _observed_until)`; null observation bounds are unbounded only within declared source completeness. Without declared unit observation columns, continuous observation within Coverage is an explicit assumption. Entry/exit dates are candidate-membership constraints at t0 only and never substitute for observation. A window crossing an observation boundary is incomplete and null with `incomplete_observation`, never a partial aggregate; exact half-open endpoint equality is complete. Incomplete source coverage has `incomplete_coverage`; both readiness and per-unit bounds remain independent across ratio inputs.

### Calendar-average domain

Let `s=t0+window.start` and `e=t0+window.end` be exact UTC instants. `avg_calendar_day` is `sum([s,e))/D`, where `D=(e-s)/86400` is a **positive integer** and both `s` and `e` are local midnight under the Catalog's fixed UTC offset. Test midnight by exact timestamp/day-boundary operations, including the fractional-second component, never floating modulo or rounding. This is a calendar-day average, not an elapsed-fractional-day rate. A whole-day-multiple duration alone is insufficient: an intraday t0, or nonaligned offsets from an aligned t0, can leave both endpoints intraday.

Task 1.3 owns one `RefusalSpec` declaration, `terrine.metric.unaligned_calendar_average`, in `temporal/aggregate.py`; Design validation imports it rather than declaring another code. Reject an impossible duration, or endpoints known to be unaligned from static anchor metadata, during validation/compilation without a query. For event anchors and other data-dependent instants, Task 4.2 checks alignment inside the pinned execution snapshot before evaluating that expression and before returning any rows. Check candidate anchors when the expression is needed for eligibility, and retained anchors when it is needed only for retained feature/outcome results. This is an execution refusal, not a null/reason state, row filter, implicit partial-day denominator, or permission to floor t0. Context contains metric/measure names, window offsets, day boundary, phase, and an invalid-anchor count when executed; never customer keys or timestamp samples.

The refusal explains the alternatives: use `sum` or `avg_event` for arbitrary instants, or explicitly choose genuinely aligned anchors and endpoints. Counts, conversion, sums, event averages, and ratios of otherwise supported measures keep event-resolution support; a ratio containing a calendar-day-average measure must satisfy that measure's domain. Complete empty aligned windows have calendar-day average zero; all-null and incomplete windows retain their existing missingness rules only within the supported domain.

### Default Float64 error contract

The following formulas define the implementation and its future acceptance criteria, not measured numerical evidence. No probes, tests, builds, or numerical passing results are claimed by this planning revision. Implementation records each numerical command and its actual output; all checks listed below remain future work.

**Domain and fixed policy.** `checked_float64_v1` measures error against exact real arithmetic on the **stored binary64 input values**, not against an unrecorded decimal value before ingestion. Float32 values may widen losslessly to Float64; do not narrow wider floats or route integer/decimal accumulators through Float64. Nulls keep the stated observation semantics. Selected nonnull float inputs must be finite. The backend must supply round-to-nearest binary64 arithmetic with gradual underflow, native addition-tree aggregates, exact checked counts, and outward scalar bound operations. Reordering and disjoint parallel merges are allowed; approximate aggregates, flush-to-zero, unchecked fast-math, and silently changed accumulator semantics are not.

Use ordinary native Float64 `SUM(value)` and `SUM(abs(value))`, grouped sparsely by unit plus local day or equal timestamp, then cumulatively added. This is the concrete default, not a promise that unspecified compensation repairs prefixes. Carry original nonnull leaf counts through every grouping/merge. Each resulting prefix must be representable as an addition tree whose leaves are the original selected nonnull values, each used once; the same is required separately for their absolute values. Adding exact zero identities does not increase the error budget. A backend must establish this capability and the bound primitives before advertising float support; a missing capability refuses at compilation with `terrine.temporal.float64_unsupported`. DuckDB is the required reference implementation, not an optional unsupported-float path.

**Outward arithmetic.** Write `RU`/`RD` for an expression evaluated with each elementary operation rounded outward, not a final adjustment after an already rounded formula. Implement these as Ibis scalar expressions using native binary64 operations followed by `nextafter(result,+infinity)` or `nextafter(result,-infinity)`, with exact representable constants and explicit finite/positive-denominator checks. DuckDB documents [`nextafter` and `isfinite`](https://duckdb.org/docs/current/sql/functions/numeric.html); bind native functions in Ibis, not a Python row UDF or SQL assembled from user text. Intermediate intervals use the usual sign-aware rules; clamp a downward bound on a known-nonnegative quantity at zero. An overflowing outward endpoint is uncertifiable, never rounded back to the largest finite value. This primitive and its backend conformance tests are part of Task 1.2.

**Prefix derivation.** For a prefix of `n` original nonnull values, let `S=sum(x_i)`, `A=sum(abs(x_i))` in exact real arithmetic; let `S_hat` and `A_hat` be the two native grouped/cumulative sums. Counts remain exact. Put `u=2**-53`, `eta=2**-1074`, `k=max(n-1,0)`, `gamma=k*u/(1-k*u)`, and `beta=k*eta/(1-k*u)`. The standard addition-tree model `fl(a+b)=(a+b)*(1+delta)+epsilon`, `abs(delta)<=u`, `abs(epsilon)<=eta`, gives

```text
abs(S_hat-S) <= gamma*A + beta
abs(A_hat-A) <= gamma*A + beta
A <= (A_hat+beta)/(1-gamma)
E_prefix = gamma*(A_hat+beta)/(1-gamma) + beta.
```

The `beta` term conservatively covers subnormal rounding. The tree has at most `n-1` potentially rounding additions on any leaf-to-root path and at most `n-1` internal additions in all; grouping into days/timestamps and merging disjoint partitions do not discard either error source. Compute upper bounds `g>=gamma`, `b>=beta` and then `A_upper=RU((A_hat+b)/(1-g))`, `E_prefix=RU(g*A_upper+b)` with the outward primitives, using a downward denominator wherever required. Refuse if `k*u>=1/4`, or if any required count, input, state, bound, or denominator is invalid/nonfinite; the count guard also makes the conversion of `k` to Float64 exact. Empty prefixes are the exact zero identity. Do not use the number of daily rows in place of `n`, `abs(S_hat)` in place of `A_hat`, or `SUM(abs(daily_sum))` in place of the cumulatively summed original absolute-value state.

Thus `A_hat` is **not assumed exact**: its own aggregation error is inverted by the `1-gamma` denominator. Keep `sum_abs_value`, original `nonnull_count`, and any invalid-state flag with each sparse prefix alongside existing sums/counts. Preserve these through native and union/window lookups. Historical group/prefix overflow must stay flagged even if a later operation or merge would mask it. Checks are for the states needed by the requested expressions, within the same snapshot as export.

**Window subtraction and acceptance.** At endpoints `s,e`, obtain `(S_s,E_s)` and `(S_e,E_e)` and compute `W_hat=fl(S_e-S_s)`. For any rounded elementary binary64 result `z_hat`, use the outward rounding-radius bound `R(z_hat)=RU((u*abs(z_hat)+eta)/(1-u))`. Since the true subtraction differs from the subtraction of stored endpoints by at most `E_s+E_e`,

```text
abs(W_hat-sum(x_i for s <= ts_i < e)) <= E_window
E_window = RU(E_s+E_e+R(W_hat)).
```

This deliberately does not cancel the unknown historical prefix errors when endpoints are close. Large offsets, mixed signs, or a tiny later window after a huge historical sum can therefore fail even when `W_hat` happens to look finite or plausible. Event counts, nonnull counts, and conversion use exact count subtraction, not floating cancellation.

Every nonnull Float64 result `v_hat` must have a finite nonnegative certified absolute error `E` and satisfy the fixed default

```text
E <= 2**-40 + 2**-30 * abs(v_hat).
```

Compare the upper error against a **downward-rounded** threshold so acceptance cannot result from rounding the threshold upward. This is an absolute floor plus a relative budget in the returned result's units; near zero the absolute floor governs. These are versioned engineering constants, not user math knobs or statistical uncertainty. A result within this contract is not labeled exact. Accepted results need not be bitwise identical across legal permutations/partitionings, and this policy does not promise that a borderline case's acceptance is order-independent; each returned result must independently certify the same fixed budget.

**Averages, ratios, and casts.** `avg_event` divides the certified window sum by the exact positive nonnull window count; aligned `avg_calendar_day` divides by the exact positive day count. For any Float64 division, including a ratio whose sufficient states were exact integers/decimals, first enclose each input as `[RD(v_hat-E), RU(v_hat+E)]`. A proven lossless conversion has zero conversion error; otherwise an integer/decimal-to-Float64 cast must receive a certified outward enclosure of the original exact value. Integer casts may use the binary64 rounding-radius bound; decimal casts require a backend primitive or exact representational comparison against the original declared precision/scale. If such a cast cannot be certified, retain a supported exact declared result type or refuse; never call it exact merely because its accumulator was exact, and never blanket-cast to decimal(38,9).

For numerator/denominator intervals `[L_n,U_n]`, `[L_d,U_d]`, require the denominator interval to exclude zero before division. Obtain the quotient enclosure by the outward minimum/maximum of the four endpoint quotients. For the actual returned quotient `q_hat=fl(n_hat/d_hat)`, use `E_q=RU(max(abs(q_hat-L_q),abs(U_q-q_hat)))` with outward differences; this includes division rounding and input/cast errors. Reapply the same fixed tolerance to the **final** average/ratio, as well as requiring valid constituent certificates. A known exact zero denominator retains `null/zero_denominator`; an uncertain denominator interval spanning zero is a numerical refusal, not a fabricated zero-denominator null. Exact integer/decimal sums/counts and representable exact results remain exact with their original scale; nonrepresentable exact arithmetic still widens losslessly or refuses. No unnecessary float conversion is introduced by this policy.

**Failure and execution.** `temporal/prefix.py` owns the single declarations `terrine.temporal.float64_unsupported` and `terrine.temporal.float64_precision`; `daily.py`, `aggregate.py`, and consumers import them. Precision-refusal context uses `failure: Literal["nonfinite_input","nonfinite_state","count_limit","bound_overflow","uncertifiable_cast","denominator_interval_zero","tolerance"]`, source/measure/output identifiers, and aggregate failed-count context only. Unsupported-backend context names the backend and missing required capability. Do not include customer rows, keys, or values, and keep the native immutable `TerrineError` context. Task 4.2 executes these checks under its pinned snapshot before returning any data; no hidden queries at binding/build/validation, partial export, dropping failed units, null-as-refusal, silent direct-range recomputation, or changed tolerance. Existing incomplete/all-null semantics are not numerical failures; only evaluated nonnull numeric results need a tolerance certificate.

The refusal routes users to correctly declared integer/decimal facts, a backend with the required checked operations, or a smaller **semantically sufficient** measurement history with explicit completeness. Reducing measurement-prefix history must still retain every requested window and must not redefine original first/nth event-anchor selection or omit required eligibility history. No anchor-by-event range join, dense calendar, per-anchor reread, or unrequested algorithm fallback is allowed.

**Metadata integration owned by Task 0.1.** Add exactly `numeric_policy: Literal["exact","checked_float64_v1"] | None = None` and `absolute_error_column: str | None = None` to shared `ColumnMeta`, retaining all its current fields. A checked Float64 aggregate/result must name its finite, nonnegative per-row absolute-error column; a null value has a null error, and an observed exact empty float sum may have error zero. `numeric_policy="exact"` has no error column and is valid only for exact output arithmetic, not rounded casts/division. `None` denotes no compiler precision claim, not an alias for exact or checked. The compiler reserves deterministic error-column names in the existing internal namespace, excludes them from user feature/outcome roles, and preserves them plus the two metadata fields in Arrow/materialized serialization. The policy version participates in compiled-plan/cache identity. No `BoundMeasure`, `BoundMetric`, function-signature, readiness, or snapshot-field change is needed; `BoundMetric.entity_columns[source_name]` continues selecting each source's physical analysis-unit key. Estimation consumers preserve/record this input precision metadata and retain separate cast-error checks; these compiler error bounds are not confidence intervals.

**Future numerical acceptance, not executed results.** Tasks 1.2–1.3 compare every accepted value/certificate against independent exact `Fraction.from_float` or `Decimal.from_float` arithmetic at sufficient precision, with `math.fsum` as an additional independent practical reference rather than a claimed exact oracle. Verify reference error `<= E` and `E` within the fixed budget, or the specific refusal before any output. Cover cancellation/offsets (`1e16`, a later `1.0`, then historical subtraction), opposite signs with near-zero net, neighboring representable floats, subnormal inputs/results, large equal-time groups, multiple days, legal row permutations and disjoint merged partitions retaining raw counts/absolute states, count-guard boundaries using constructed sufficient states, and near-overflow in signed sum, absolute sum, outward bounds, casts, and division. Deliberately understated/lost absolute-count state must fail structural/certificate tests, not silently pass. Assert no bitwise-equality requirement between differently ordered float sums; count/conversion and exact integer/decimal results remain identical.

Task 4.4's ordinary Float64 world remains an explicitly supported integration case: its default native revenue path uses one finite outcome posting per unit, possibly negative for refunds, and the `sum` metric at an intraday anchor. It must run through these same certificates, not switch to decimal, bypass checks, or refuse all floats. The future native-catalog recovery smoke must assert reference agreement, a valid precision metadata/error column, unchanged unit coverage, and no precision refusal for that ordinary fixture. Also exercise ordinary multiple-posting revenue histories as accepted cases so singleton support cannot substitute for implementing general Float64 aggregation.


### Task 1.1: Native Catalog binding, readiness, and event normalization

**Files:**
- Create: `terrine/semantic/models.py`
- Create: `terrine/semantic/io.py`
- Create: `terrine/semantic/bind.py`
- Create: `terrine/semantic/sql.py`
- Create: `terrine/model.py` (native Model; Task 0.2 later adds the lazy optional adapter facade)
- Create: `terrine/temporal/readiness.py`
- Test: `tests/temporal/test_semantic_binding.py`
- Test: `tests/temporal/test_readiness.py`

- Consumes native version-1 Catalog Python objects or safe YAML; no required increment API. Optional adapter input is translated by `terrine.adapters.increment` into the same Catalog/Bound* meaning, mapping each alternative source entity column to an identity logical name and preserving selected metric entity.
- Produces immutable BoundSource/BoundMetric and the normalization signatures above; this task owns Model and native binding. Task 0.1 owns only pure Coverage/BoundCoverage, metadata and TerrineError/RefusalSpec. `Model.from_catalog(path_or_catalog, *, con, readiness, units=None, event_ids=None)` binds descriptors, coverage, roster and event IDs. `Readiness.for_fact(source_name) -> BoundCoverage` refuses absent readiness.

- [ ] **Step 1: Write binding and readiness acceptance tests.** Native objects and YAML bind identically; reject duplicate keys, unsafe tags, unknown schema/fields and missing references. Selecting logical `user` from `{"user":"user_id","workspace":"workspace_id"}` produces `entity_columns["sessions"] == "user_id"`; the workspace column remains available only for dimension joins, not a composite unit. Add a ratio whose second source maps user to `customer_id`; both normalize to the same logical unit correctly. Occurrence-only facts have no value column. Preserve aggregation, filters, source identity and transform declarations; fixed/percentile winsorization compilation refuses, never silently drops. Coverage `2025-03-01` through `2025-03-31` at UTC−05 binds to `[2025-03-01T05:00Z,2025-04-01T05:00Z)`. Native loading/binding performs no data scans; data-dependent checks wait for execution. Full Design.build assertions belong to 4.2, not this prerequisite.
- [ ] **Step 2: Run focused tests to confirm failure.** Run `uv run pytest tests/temporal/test_semantic_binding.py tests/temporal/test_readiness.py -q`; expected failures identify missing native modules/APIs, not unrelated import or fixture errors.
- [ ] **Step 3: Implement native Catalog and deterministic descriptor binding.** Define schema version 1 records exactly as in shared contracts; Python models and safe YAML produce the same Catalog. Reject unknown fields, unsafe YAML tags, duplicate keys, unsupported schema versions, missing references, invalid type/field combinations, and arbitrary callbacks. Validate `Fact.source`, `Property.source/dimension`, selected logical entity, `Measure.fact`, typed filters, and metric's required measure shape. Support only declared `mean`, `conversion`, and `ratio` near-term kinds; unsupported metric kinds refuse with `terrine.metric.unsupported_temporal_kind`. Bind winsorization into `BoundMetric.transforms` without altering the declaration, then reject attempted design compilation with `terrine.metric.unsupported_transform` for any fixed or percentile winsorization. No broad estimator declarations or features. Bind readiness once with Catalog's fixed-offset day boundary; pure Coverage never owns timezone interpretation. `BoundCoverage` is `[UTC start, UTC end_exclusive)` and `Readiness` stores that interval. This task performs no hidden data snapshot, readiness, count, or integrity query.
- [ ] **Step 4: Implement SQL admission, schema resolution, and lazy event normalization.** Named table relations resolve through `con.table`; SQL relations must parse as exactly one read-only query in `semantic/sql.py` with sqlglot, rejecting DDL/DML, multiple statements, locking, `SELECT INTO`, unsupported constructs and declared/execution-dialect mismatch before query execution. No upstream loader import and no SQL rewriting. Runtime Ibis table expressions can bind in-memory but cannot serialize unless represented as a named source relation. Resolve declared columns and types from static Ibis/schema metadata; refuse missing columns/type mismatches without a hidden data scan. `normalize_events(source_table, source, unit_column)` projects selected physical unit key, exact timestamp, declared facts/properties and all other declared physical entity columns needed for dimensions, preserving event multiplicity. No data joins occur in source normalization. `window_days` conflict with a requested window is a coded refusal. `Property.as_of="pre_exposure"` without an explicit supported rewrite refuses. Event ID is terrine-side config; additive measures need none; operations requiring deterministic event tie order refuse if absent. No data query in Catalog load/build/validate.
- [ ] **Step 5: Verify, document, and commit.** Run `uv run pytest tests/temporal/test_semantic_binding.py tests/temporal/test_readiness.py -q`; run an in-memory DuckDB smoke with alternate `user_id`/`workspace_id` columns, confirming partitioning only by selected key while retaining the other key. Task 0.1 owns core import-boundary checks; Task 0.2 owns optional adapter descriptor mapping. Assert metadata load/build do not execute data queries. Update `docs/guides/temporal-engine.md` and `CHANGELOG.md`; run applicable focused Ruff checks. Commit task files with `feat: add native semantic catalog binding`.

### Task 1.2: Sparse sufficient states and cumulative prefixes

**Dependency:** Task 1.1 and Task 1.4 (dimension enrichment is needed for dimension-backed filter tests; source normalization in 1.1 remains join-free).

**Files:**
- Create: `terrine/temporal/daily.py`
- Create: `terrine/temporal/prefix.py`
- Test: `tests/temporal/test_sparse_state.py`

**Interfaces:**
- Consumes: `BoundSource`, `BoundMeasure`, normalized fact event relation from Task 1.1 and `Catalog.day_boundary`; every aggregation receives the one selected physical `unit_column`. Task 1.4 is a dependency for dimension-backed measure-filter acceptance; base fact normalization in 1.1 remains join-free.
- Produces: `sparse_daily(events, measure, boundary, unit_column)` with `(unit_column, local_day, event_count, nonnull_count, sum_value)` as applicable, plus original-value `sum_abs_value` and invalid-state tracking for Float64 measures. Sparse prefix retains observed state only and carries those original counts/absolute states plus certified bounds; no dense calendar. Other source entity keys remain available for dimension joins, not unit aggregation.

- [ ] **Step 1: Write sparse-state tests.** Assert three source events on two days produce exactly two daily rows, never one row per roster day; selected unit with no events contributes no sparse row; duplicate timestamps contribute full multiplicity; null measure values increment event_count but not nonnull_count or sum_value. Distinguish complete empty intervals (`count=0`, `sum=0`, `conversion=False`, `avg_calendar_day=0` when positive full aligned days exist, `avg_event=null/no_observations`) from nonempty all-null values (`count=event_count`, `conversion=True`, `sum` and averages `null/all_values_null`; retain both event_count and nonnull_count). Incomplete coverage overrides either result to `null/incomplete_coverage`. Assert null/missing filter properties remain unknown under the declared filter-missingness semantics and are not silently excluded.
- Add the future Float64 acceptance matrix from the default error contract to `test_sparse_state.py`: independently verify grouped/prefix sum and absolute-state bounds, outward scalar primitives, subnormals, neighboring floats, permutation/disjoint-merge behavior, exact raw counts across grouping, and nonfinite/count/overflow refusals. Include ordinary multi-posting accepted cases and assert the emitted metadata never calls Float64 accumulation exact.
- [ ] **Step 2: Run focused tests to confirm failure.** Run `uv run pytest tests/temporal/test_sparse_state.py -q`; expected failures identify absent sparse-state implementation.
- [ ] **Step 3: Implement daily grouping and sufficient state.** Convert timestamps to local day using Catalog's fixed offset, grouping only existing events by selected unit + day. Apply each semantic measure filter exactly once to an event relation already enriched where needed by Task 1.4; unknown filter values use the explicit source filter-missingness contract, never silently become false. Keep event and non-null counts distinct, and preserve a known-observed-null marker so it is not conflated with unavailable history. Preserve integer/decimal type and scale without blanket casts; exact integer overflow either widens losslessly under a declared result type or refuses, never wraps. Implement `checked_float64_v1` and its two module-owned refusals exactly as derived above, with raw absolute-value state and original nonnull counts through both grouping and cumulative merging. `count` is event count; `sum` sums nonnull values; `avg_event` carries sum + nonnull count; `avg_calendar_day` carries sums and later divides only within the aligned domain. Event-resolution state groups equal timestamps before cumulative accumulation. Native numeric checks execute only under the consumer's pinned snapshot, never during lazy binding/build.
- [ ] **Step 4: Verify precision, DuckDB, docs and commit.** Run `uv run pytest tests/temporal/test_sparse_state.py -q`; assert integer >2**53 and `Decimal("12345678901234567890.123400")` survive sum/prefix, then record actual results of the future Float64 reference/certificate matrix and native outward-operation conformance checks. Run in-memory DuckDB smoke proving sparse rows exist only for days with events and ordinary multiple-posting Float64 sums certify without fallback. Update `docs/guides/temporal-engine.md` and `CHANGELOG.md` with the fixed policy, exact-versus-checked distinction, refusal remedies, and metadata fields; run `uv run ruff check terrine/temporal/daily.py terrine/temporal/prefix.py tests/temporal`; commit task files with `feat: add sparse temporal sufficient state`.

### Task 1.3: Timestamp prefixes, window aggregates, and mixed readiness

**Files:**
- Create: `terrine/temporal/lookup.py`
- Create: `terrine/temporal/aggregate.py`
- Test: `tests/temporal/test_prefix_lookup.py`
- Test: `tests/temporal/test_window_aggregates.py`
- Consumes sparse states from Task 1.2, `BoundMetric` and `Readiness` from Task 1.1, anchors `(unit_column, anchor_id, t0, _observed_from?, _observed_until?)` with UTC-aware t0 and optional UTC observation bounds, and explicit measure-state relations by bound measure identity.
- Produces strict timestamp-prefix lookup `P(x)=sum(ts < x)` and `aggregate_window` over `[t0+a,t0+b)`. `TemporalResult` includes per-output value, observed-until, completeness, nullable reason, and precision metadata/error-column references as applicable.

- [ ] **Step 1: Write exact-boundary, empty/null, and readiness tests.** Events at t0−1µs, t0, t0+1µs and two at t0+1h: at each lookup x exclude all events exactly at x and include earlier ones; native and fallback yield identical counts. For `[t0-1h,t0)`, exclude t0; exclude an event at the right endpoint. For complete empty intervals assert count=0,sum=0,conversion=False,avg_calendar_day=0 only for positive whole aligned local days, avg_event=null/no_observations, and ratio with denominator zero=null/zero_denominator. For nonempty all-null values assert count includes events, conversion=True, and sum/averages=null/all_values_null; a ratio with an all-null numerator is null/all_values_null. Incomplete intervals override every empty/all-null/ratio result with null/incomplete_coverage or null/incomplete_observation. Numerator/denominator with different non-whole-day endpoints are tested via low-level `BoundCoverage` intervals (public Coverage remains whole local days and uses the same fixed boundary), yielding ratio incomplete at the minimum endpoint. Unit observation ending before interval end similarly nulls with incomplete_observation; exact interval-end equality is complete. Readiness fixture keys are fact-source names.
- Add calendar-domain assertions: subday duration statically refuses; a noon-to-noon multiday window refuses despite whole-day duration; an aligned t0 with nonaligned offsets refuses; positive aligned whole local days succeed at UTC and a nonzero fixed offset. Known fixed anchors refuse without data execution; unknown event anchors defer the same code until exact snapshot-backed pre-output checks. Assert unaligned empty/all-null/incomplete requests do not manufacture a supported calendar denominator, and no row filtering or first emitted batch precedes refusal. Sum/event-average/count retain the original intraday anchors and event-resolution results.
- Add Float64 subtraction/average/ratio assertions from the default contract. In a complete one-unit fixture, put an old posting of `1e16` before the window and a posting of `1.0` inside it: the default prefix certificate must raise `terrine.temporal.float64_precision` with `failure="tolerance"` before any output, rather than returning a plausible uncertified difference. Contrast ordinary singleton/multiple-posting histories that must certify and match independent references. Check final quotient error propagation, certified casts of exact integer/decimal states, exact decimal results retaining scale, nonrepresentable casts/division never labeled exact, known zero versus uncertain denominator, all-null/incomplete error-column nulls, and per-row precision metadata round trips. Run both lookup strategies without anchor-event joins, dense rows, or direct-range fallback.
- [ ] **Step 2: Run focused tests to confirm failure.** Run `uv run pytest tests/temporal/test_prefix_lookup.py tests/temporal/test_window_aggregates.py -q`.
- [ ] **Step 3: Implement dual-resolution point lookup without fanout.** Day-aligned windows use sparse daily prefixes. Arbitrary instants use event-resolution prefix states grouped by selected unit and timestamp, so equal-time events become one cumulative state. Native backend as-of is preferred when supported; fallback unions prefix states with lookup points and selects latest qualifying state via partitioned ordered window, without anchor×event join/range fanout. Carry all count, absolute-state, certificate, and invalid-state columns as a constant-width state through either strategy; numeric checking does not add per-anchor event scans. Native/fallback agree on typed zero for no prior state; event-resolution cost is surfaced. Never floor t0.
- [ ] **Step 4: Implement kind-aware results, missingness, and mixed completeness.** Enforce the calendar-average domain and its single refusal before evaluating unsupported requests. Sufficient state keeps `event_count` and `nonnull_count`; never COALESCE a missing nonnull sum to zero in a way that erases all-null values. For a complete empty interval, count=0, sum=0, conversion=False, avg_calendar_day=0 only for a positive count of fully aligned local days, avg_event=null with `no_observations`, and ratio with a known exact denominator zero=null with `zero_denominator`. For nonempty all-null values, count counts events and conversion is true, while sum/averages are null with `all_values_null`. Incomplete coverage or observation takes precedence and gives null with the applicable reason, regardless of event/value presence, within the aggregation's supported domain. Ratio readiness is the intersection of each numerator/denominator source and observation interval. Unknown filter values follow the catalog-declared missingness rule and cannot be silently treated as false; IsNull only tests a known observed null, not unavailable history. Unsupported nonadditive/report-only kinds (`min/max`, distinct, quantile, retention, total, active) refuse with `terrine.metric.unsupported_temporal_kind`; never subtract nonadditive state. Preserve integer/decimal arithmetic; overflow must widen losslessly or refuse. Propagate the default checked Float64 subtraction/cast/division certificates, enforce the final fixed budget, and emit the precision/error-column metadata before any consumer export. Exact counts/conversion need no floating prefix subtraction.
- [ ] **Step 5: Run focused verification, DuckDB smoke, docs and commit.** Run `uv run pytest tests/temporal/test_prefix_lookup.py tests/temporal/test_window_aggregates.py -q`; record actual boundary, alignment-refusal, certified-float, and lookup-strategy results. In-memory DuckDB smoke asserts t0 event excluded from pre-window and included in post-window, with the Float64 revenue slice still supported at intraday t0. Update temporal-engine guide/changelog with aligned calendar-average semantics, precision metadata, fixed numerical budget and refusal remedies; run focused `uv run ruff check`; commit task files with `feat: add checked timestamp window aggregates`.
### Task 1.4: Strict SCD2 attribute as-of

**Files:**
- Create: `terrine/temporal/attributes.py`
- Test: `tests/temporal/test_scd2_attributes.py`

**Interfaces:**
- Consumes `BoundDimension` (`source_entity` logical key and physical dimension `entity_column`), explicit dimension history and normalized event points; analysis unit may be a different declared entity.
- Produces `enrich_events(events, source, dimension_relations) -> ibis.Table`, applying each static or validity join at the event timestamp using the dimension's selected source entity key, preserving event identity and cardinality; and `attribute_asof(points, history, dimension, property_name, *, unit_column, cutoff_column, strict=True) -> ibis.Table` including value and found/unknown status and preserving point identity.

- [ ] **Step 1: Write dimension interval and event enrichment tests.** Range validity `[valid_from,valid_to)`: inside selects row, valid_to excluded; strict cutoff t0 excludes a version starting at t0 and selects prior only if `valid_from < t0 AND (valid_to >= t0 OR valid_to IS NULL)`. For changed_at, ordinary as-of uses `[changed_at,next_changed_at)`; strict cutoff equal to changed_at selects prior version. Create workspace events with multiple alternative entity columns and an account-tier dimension keyed by a different selected declared entity (e.g. `workspace_id -> account_id`); changing tier histories attach the tier valid at each event timestamp, even though the analysis unit is `user_id`. Assert one enriched row per event, no accidental join to analysis key, static dimensions enrich identically, and filters consume the enriched property once. Duplicate starts/changes, overlapping history, multiple matches, or missing selected join key refuse; a genuinely absent prior feature row is unknown/null, not an observed category.
- [ ] **Step 2: Run focused tests to confirm failure.** Run `uv run pytest tests/temporal/test_scd2_attributes.py -q`.
- [ ] **Step 3: Normalize and validate SCD2 history; enrich events.** Normalize native dimension validity (`valid_from`/`valid_to` or `changed_at`) to terrine-owned `BoundValidity`; changed-at intervals end at next change, final interval is open-ended. Validate duplicate/overlapping ranges per selected dimension entity before materialization. Static dimensions remain static; versioned properties use temporal history. `enrich_events(events, source, dimension_relations)` resolves each `BoundDimension.source_entity` through `source.entities` to the event-side physical key and joins dimension-side `entity_column`; the key may differ from the metric's selected analysis entity. For event-time properties use `[valid_from <= event_ts < valid_to)` (or equivalent changed-at interval), ordinary as-of semantics, not strict-before. Preserve each source event once and refuse key/type mismatch, missing required join key, overlap, or cardinality >1; distinguish unmatched row from matched null property. This is the shared event enrichment operation used by dimension-backed filters so filter logic is applied once. Ambiguous properties require named dimension selection. No implicit frame-t0 join to a different entity: later feature as-of requires explicit roster mapping or refuses.
- [ ] **Step 4: Implement strict temporal feature selection.** Ordinary event-time enrichment joins with `event_ts >= valid_from AND event_ts < valid_to`; strict left-limit features require `valid_from < cutoff` and `(valid_to >= cutoff OR valid_to IS NULL)`, choosing a unique row. Join using dimension-declared entity key and preserve point identity/cardinality. Distinguish no matching row from a matched row whose value is null via match flag; effective-time history does not imply knowledge-at-time, and recorded-at history remains future scope.
- [ ] **Step 5: Run focused verification, DuckDB smoke, docs and commit.** Run `uv run pytest tests/temporal/test_scd2_attributes.py -q`; verify exact t0 strict cutoff, ordinary event-time version change, workspace-to-account join with `user_id` analysis key, static dimension, missing/multiple matches and preserved event cardinality. Update temporal-engine guide/changelog; run focused checks; commit task files with `feat: add strict SCD2 and event-time dimension enrichment`.
## Consumer/API integration acceptance

Task 1.3's event-resolution lookup is a prerequisite to, not a substitute for, the public consumer API plan. The primary example uses native `Model.from_catalog("semantic.yaml", con=con, readiness={"sessions_fact_source": Coverage(...), "revenue_fact_source": Coverage(...)})`; these keys are actual fact-source names, not metric names. `Model.from_increment(...)` is an optional adapter example only. Define `Design(unit="account", anchor=EventAnchor("feature_enabled", which="first"), features={"sessions_pre": tr.agg("sessions", window=("-28d","0d")), "tier_before": tr.asof("plan_tier", before="0d")}, outcomes={"revenue_post": tr.agg("revenue", window=("0d","7d"))}, gap="0d")`. The event timestamp is exact UTC-aware t0. Pre-feature lookup excludes facts/attribute versions at t0; outcomes include t0 in `[0d,7d)`. A frame may require explicit roster mapping to bind a t0 dimension entity different from its analysis unit; without that mapping refuse rather than joining identifiers by coincidence. Fixed-offset Coverage `through` is inclusive local date, converted once to exclusive next-local-midnight UTC; explicit `BoundCoverage` tests can represent distinct source endpoints without changing public whole-day Coverage.

## Later temporal capabilities (not included in Tasks 1.1–1.4)

- Add range-based sparse computation for `min`/`max`; these are non-subtractable and must query sparse daily/event rows within bounded windows without anchor × event fanout.
- Add exact distinct or a named approximate sketch with explicit error guarantees; never use subtractive distinct prefixes.
- Add quantiles, retention semantics, event sequences and deterministic event-ID tie-breaking as separately scoped capabilities.
- Add survival/censoring estimators only with explicit observed-until/event/censoring roles and supported estimator model; incomplete outcomes remain null until then.
- Add recorded-at system-time axis before offering “known at t0” guarantees for retroactively corrected dimensions.
