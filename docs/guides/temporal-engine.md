# Native temporal catalogs

Terrine's native `Catalog` describes source relations and their meaning without
requiring the optional increment adapter. A catalog can be constructed from a
Python mapping with `Catalog.model_validate(...)` or loaded from safe YAML with
`load_catalog(path)`. Both paths use the same strict version-1 Pydantic models;
unknown fields, duplicate YAML keys, unsafe YAML tags, and unsupported schema
versions are rejected.

Schema failures from `load_catalog` raise `TerrineError` with code
`terrine.catalog.invalid_definition`. Inspect its immutable `context["issues"]`
for ordered locations, stable validation categories, and package-owned recovery
hints; the same actionable locations and corrections appear in the displayed
error. For example, `day_boundary: UTC+14:01` points to `day_boundary`: use
`UTC`, a signed `HH:MM` offset, or a `UTC`-prefixed fixed offset within ±14:00
(the endpoints are limited to exactly `14:00`). Correct the YAML and load it
again. Direct `Catalog.model_validate(...)` construction continues to use
Pydantic's `ValidationError`.

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

Here `kind: mean` describes the downstream population summary; `aggregation:
count` describes the per-unit session count. It does not average events or
change a requested feature window.

`day_boundary` accepts `UTC` or a fixed signed `HH:MM` offset, with an optional
`UTC` prefix (for example, `+05:30` or `UTC-05:00`). Coverage days are local to
that boundary and bind once to half-open UTC instants. Offsets are fixed, not
daylight-saving timezones.

Declared event, dimension-validity, and roster time columns must be scalar
timestamps; timestamp nullability does not change admission. A versioned
dimension declares either `valid_from` with an optional `valid_to`, or
`changed_at` alone. Empty or mixed time-axis declarations are rejected.

Bind a catalog and explicit source completeness without scanning event rows:

```python
from datetime import UTC, datetime

import ibis

from terrine import Coverage, Model
from terrine.semantic.io import load_catalog

con = ibis.duckdb.connect()
con.create_table(
    "session_events",
    ibis.memtable({
        "occurred_at": [datetime(2025, 3, 1, 12, tzinfo=UTC)],
        "user_id": ["u-1"],
    }),
)
model = Model.from_catalog(
    load_catalog("catalog.yaml"),
    con=con,
    readiness={"sessions": Coverage("2025-03-01", "2025-03-31")},
)
```

Coverage dates are inclusive local days and are bound once to UTC instants
using `day_boundary` (for example, a UTC−05:00 boundary binds March 1 midnight
to 05:00 UTC). `Readiness` stores only these bound half-open intervals. A
source's observed maximum timestamp never implies completeness.

Logical entities map to physical source keys. A metric selecting `user` uses
only that logical key as its canonical unit key; other entity columns remain
available for explicit dimension joins and are not combined into an implicit
composite identity. Ratio inputs may select different physical columns for the
same logical entity.

Relation declarations are tagged: `{kind: table, name: ...}` names a table and
`{kind: sql, sql: ...}` declares SQL. SQL must parse as one read-only query for
the connection dialect. Metadata binding resolves relation schemas and refuses
missing or mismatched declared columns before execution. It does not create a
runtime data-verification receipt or infer table integrity.

`avg_calendar_day` is defined only on a positive whole number of aligned local
calendar days. For known anchors, both exact endpoints must be local midnight;
unknown anchors require a runtime check. Use `sum` or `avg_event` for arbitrary
instants, or choose aligned anchors and endpoints. Unsupported temporal kinds
and declared transforms retain their meaning and are refused rather than
silently approximated.

Explicit fixed and training-fitted winsorization is available on dataset
Frames at a serialized post-aggregation stage; it does not change the source
metric or silently reinterpret adapter declarations. See
[frozen winsorization](ml-staging.md#frozen-winsorization) for train-only exact
thresholds, immutable artifacts, supported types, and scoring reuse.

## Quantile declarations

`quantile(metric, probability, window=...)` is a typed, canonical specification:
the probability is a `Decimal` or decimal string in `[0, 1]`, the window is
half-open, and interpolation is Hyndman--Fan type 7 (`linear_type7`). The
specification serializes through Design JSON/YAML and contributes to its
canonical hash.
The native DuckDB compiler executes exact type-7 order statistics over matched
half-open event windows. Integer and Decimal facts retain exact decimal
interpolation at a supported output scale; unsupported numeric types or
precision refuse with a stable error before query execution. Results exclude
null observations and preserve distinct reasons for no observations, all-null
windows, and incomplete coverage. Min, max, and distinct-count operations use
the same sparse range machinery; distinct values are counted once across the
full window rather than by adding daily counts.

Exact quantiles and approximate distinct are capability-gated to DuckDB until
other backend behavior is verified. `approx_distinct(metric, window=...)` uses
DuckDB's native HLL aggregate and records its algorithm, engine version and
relative-error description in the output column metadata. Exact distinct
remains the default when requested through `agg`; approximation is explicit.

`approx_quantile(metric, probability, window=..., sketch=SketchRequest(...))`
uses DataSketches t-digest on `cross_section` designs only. It is available with
the optional `terrine[sketches]` extra; otherwise the request refuses before
source queries and points to `uv add 'terrine[sketches]'` or exact `quantile`.
The compiler streams canonical `(unit, anchor, event time, value)` Arrow batches
to the client, updating t-digests in sorted anchor order. This transfers all
matched window values out of DuckDB and can cost more than in-database exact
work; t-digest accuracy is empirical and has no guaranteed rank bound. Both
approximate results record algorithm, implementation/version, parameters, and
error description in `ColumnMeta` and preserve that record through Frame Arrow
and JSON metadata serialization. Other shapes and runtime composition refuse
approximate quantiles; ordinary `Frame.materialize()` is the supported route.

### Bounded repeated windows

Additive panel metrics retain sparse prefix lookups, including long and sparse
windows. Exact min/max/distinct/quantile and approximate distinct use sparse
time-bucket equality joins followed by the exact half-open timestamp predicates.
Only occupied event buckets and at most two requests per anchor exist; there
is no unit-by-day spine. No nonadditive state is prefix-subtracted.

The compiler derives a conservative candidate multiplier before executing
queries. For window width `W`, schedule step `S` and `N` anchors per unit,
`K = min(N, 2 ceil(W/S))`. A fixed or first/nth event anchor has `N = 1`.
An EventPanel over a first/nth event uses its declared period count and step.
Re-anchoring may collapse those spacings, so it uses the whole anchor count.
For `A` input anchors and `E` selected events, request rows are at most `2A`,
bucketed event rows at most `E`, and equality-join candidates at most `K E`.
The exact time predicates can only remove candidates. These are relational
cardinality bounds, **not** source-scan, client-memory or execution-time bounds.
The immutable `CompiledStudy.range_plans` records the width, multiplier and
derivation for each named nonadditive output; no row count is guessed.

The supported candidate multiplier is at most 128. Unknown per-unit event-anchor
multiplicity (`EventAnchor(..., which="each")`) and larger declared overlaps
refuse before execution with `terrine.temporal.range_bound_unavailable`.
Use first/nth event anchors, fewer event periods/anchors, a coarser schedule or
narrower window, or an additive metric with sparse prefix lookups. Additive
requests do not inherit this nonadditive overlap limit. This also applies to
the raw values streamed by cross-section t-digest sidecars.

The temporal retention primitive can consume that same derived range plan.
It counts distinct identities of independently selected target-event
occurrences across the full horizon, then applies the exact integer threshold
bands; it never adds daily distinct counts. Finite horizons use their declared
duration as the block width. For an open horizon bounded by an explicit source
cutoff, bounded execution additionally requires a declared source completeness
start; the block width is the coverage span rounded upward to integral seconds.
Only anchors within that covered history contribute event matches. An unknown
open span or a mismatched plan refuses with the same range-bound code, rather
than silently reverting to the unbounded exact path.

Retention preserves zero for a complete horizon with no target occurrences,
null for incomplete observation, `horizon_unavailable` for a missing/invalid
origin, and `incomplete_coverage` when the origin precedes declared source
history. A null observation endpoint remains unavailable for finite follow-up;
for an explicitly complete open horizon it means no narrower observation
cutoff. Missing target labels preserve `incomplete_label_history`, not zero.

### Named retention outcomes

Use the ordinary immutable Design expression:

```python
from terrine import Design, FixedDate, retention

design = Design(
    "user",
    FixedDate("2025-03-01"),
    outcomes={
        "retention_band": retention(
            "purchase_occurrence",
            label_property="event_name",
            target_event="purchase",
            thresholds=(1, 3, 10),
            horizon="7d",  # or the explicit "open_ended"
        ),
    },
)
dataset = design.build(model).materialize()
```

The fact's column supplies independent occurrence IDs, scoped by analysis unit.
If the fact is a row marker without a column, declare `Model.event_ids` for its
source. Repeated evidence for one identity is counted once across the entire
horizon; unrelated labels and null fact identities do not count. The output is
the ordinal band `sum(count >= threshold)` over strictly increasing positive
Int64 thresholds, with exact integer comparison and no statistical approximation.

Finite follow-up is `[t0, t0 + duration)`. Open follow-up is `[t0, U)`, where `U`
is the source's declared exclusive completeness endpoint, not the final observed
event. Source history, source follow-up, and declared roster observation bounds
must cover the horizon. Finite follow-up requires a known roster observation end;
an absent end is not evidence of complete finite follow-up. These outcomes may
not be used as pre-anchor features or eligibility predicates. Their reference
and threshold declarations round-trip through JSON/YAML and saved recipes;
Arrow metadata retains exactness, completeness/reason, conservative support and
the resolved horizon endpoint. Sequential/calendar treatment checks include the
retention horizon, even when the occurrence and treatment sources differ.

Run `uv run python examples/retention_metrics.py` for a complete local example.

Catalog records and bound Models are immutable, including nested definition maps.
Create a new definition and bind a new Model when semantics change; an existing
model's hash cannot drift with a mutated catalog.

SQL admission traverses the whole statement, including CTEs. Writes, locks,
sequence operations and unreviewed or qualified function calls are refused.
Use a supported read-only built-in or an owner-reviewed named source relation.
This static gate does not replace least-privilege credentials or a trusted backend
catalog; it is not a sandbox for hostile database functions.

## Sparse sufficient state

The internal temporal builders consume normalized events and the existing
`BoundSource`; the source mapping is required explicitly:

```python
from terrine.temporal.daily import resolve_measure, sparse_daily
from terrine.temporal.prefix import sparse_prefix

typed_measure = resolve_measure(events, measure, source=source)
daily = sparse_daily(
    events, typed_measure, source.day_boundary, "user_id", source=source
)
prefix = sparse_prefix(daily, typed_measure, resolution="day")
```

`resolve_measure` replaces the immutable bound descriptor's `value_type` using
the actual relation schema. A compiler retains that replacement for downstream
arithmetic; this is not another user-authored catalog setting. No builder
executes a data query. Dimension filters consume the deterministic value/found
aliases produced by `enrich_events`, not guessed physical names. Measure
filters use event-time values; a property's `pre_exposure` annotation does not
create an exposure, assignment, or anchor-specific filter.

Only observed `(selected unit, local day)` groups exist. A roster member with
no source events has no sparse row. Duplicate timestamps keep full
multiplicity. Another declared entity key is not an additional grouping key.
State always names the selected key `_unit`, matching event normalization.
Physical keys such as `value_type` or `event_count` cannot overwrite state
metadata or merge distinct entities during accumulation.
`local_day` is a date under the fixed catalog offset; `_timestamp` is that
bucket's start as an aware UTC instant, independent of the database session
timezone. Naive source timestamps denote UTC. For day bucketing only, a
nanosecond instant is floored exactly to microseconds, correcting native
negative-residue truncation. Every fixed-offset midnight is microsecond-aligned,
so this preserves its day without out-of-range nanosecond threshold casts.
The original timestamp remains unchanged for event-resolution lookups.

A day prefix is valid only for aligned local-midnight endpoints. An
equal-timestamp event state uses the same internal `_sparse_group` filter and
arithmetic machinery, then `sparse_prefix(..., resolution="event", ...)`.
Prefix rows include their own group; a strict point lookup selects the last
group with `_timestamp < cutoff`. Arbitrary intraday cutoffs require event
states, not a daily approximation or a dense calendar.

### Counts, nulls, and unknown predicates

`event_count` counts selected events, including events whose fact value is
null; `nonnull_count` counts their nonnull fact values. For an occurrence fact
with no value column both counts are event counts. `count` sums events rather
than nonnull values. `observed_null_count` is a projection of
`event_count - nonnull_count`, not another independently accumulated count.
Daily all-null groups have null `sum_value`; prefix arithmetic normalizes this
to the zero identity without losing the two counts. Window aggregation
distinguishes complete empty intervals, nonempty all-null intervals,
and incomplete coverage using counts and explicit readiness.

Predicates use SQL three-valued conjunction: known false overrides unknown.
Known-false-only events need not create a group. An unknown event is **not**
silently excluded or turned into zero. These exact, subtractable counters
accompany each state:

- `unknown_filter_count`: events with an unknown full predicate;
- `unknown_filter_null_count`: such events with an observed-null filter property;
- `unknown_filter_missing_count`: such events with missing dimension history;
- `unknown_filter_literal_count`: such events with a comparison involving a null
  literal that itself evaluated unknown;
- `unknown_filter_nonfinite_count`: such events with an observed nonfinite
  numeric filter property.

The provenance counters are nonexclusive. They do not partition the total.
No-filter states use constant zero counters, without reductions over zeros.
A window consumer retains these fields and refuses unresolved membership with
`terrine.filter.unknown` before output. NaN and both infinities in a referenced
numeric property make its comparison unknown, not false and not an observed
SQL NULL. Known false still overrides unknown. Only an otherwise available
requested window is affected: incomplete coverage/observation keeps precedence,
and an invalid value outside the window or in an unreferenced property does
not block a result. These are predicate-domain checks, not Float64 precision
failures.

A null comparison literal is a **typed SQL NULL**, not `IS NULL`: `equals:
null` and `not_equals: null` evaluate unknown, including on observed null
properties. `in`/`not_in` retain SQL null propagation; a known membership match
overrides a null list member. There is no configurable missingness policy.
Numeric filter comparisons never narrow exact integer/decimal properties to
Float64: exact unscaled coefficients are compared with rational literal
thresholds. Integer literals mean exact integers; finite Python/JSON float
literals mean their stored binary64 values. On an exact numeric property, a
**quoted JSON-number spelling** means an exact decimal threshold. This is a
native literal contract, not a claim of compatibility with another system's
implicit coercions. Quote decimal prices and rates for every comparator:

| Operator | Example threshold(s) for an exact `price` property |
| --- | --- |
| `equals` | `"0.10"` |
| `not_equals` | `"0.10"` |
| `gt` | `"0.105"` |
| `gte` | `"0.10"` |
| `lt` | `"0.105"` |
| `lte` | `"0.10"` |
| `in` | `["0.10", "0.20"]` |
| `not_in` | `["0.10", "0.20"]` |
| `between` | `["0.10", "0.20"]` (inclusive) |

For example, an exact decimal price `0.10` equals `"0.1"`, but does **not**
equal float `0.1`; that binary64 value is slightly larger than exact
one-tenth. Consequently the price is less than float `0.1`, not greater than
or equal to it. Quoting thresholds matters for ordering as well as equality.
For a scale-two rate, `gt: "0.105"` selects `0.11` and larger, without
rounding the threshold or source. For an integer quantity, `gt: "1.5"`
selects `2` and larger.

The quoted grammar is exactly JSON number syntax: optional leading minus,
an integer with no extra leading zeros, optional fraction with digits after
the decimal point, and optional signed decimal exponent. Whitespace, leading
plus, underscores, `NaN`, and `Infinity` are refused. Numeric-looking strings
remain ordinary strings on string properties. No Decimal constructor,
tolerance setting, or new public input type is required.

Fractional thresholds use equivalent exact floor/ceiling predicates rather
than representability-at-scale refusals. Extremely large/small exponents
are handled symbolically against the physical coefficient domain; they do
not construct enormous integer powers and are not subject to an invented
global exponent cap. Float32 properties widen losslessly. Binary floating
properties accept typed float/int thresholds; an integer literal not exactly
representable in Float64 is refused instead of rounded. Boolean and date
filters remain typed.

For DuckDB's signed `HUGEINT`, threshold clipping covers its full signed
128-bit range even when Ibis reports the column as `Decimal(38, 0)`.

### Exact numeric state

On the supported native DuckDB 1.5.6 route, integer and decimal sums and counts
use internal `BIGNUM` coefficients. For decimal scale `s`, the coefficient is
the exact integer `z` in `value = z * 10**(-s)`. `value_type` and `value_scale`
retain the schema type and original scale; `numeric_policy` is `exact`.
Grouping, cumulative addition, and `exact_subtract` never multiply or divide
BIGNUM values and do not narrow prefixes to HUGEINT or decimal precision 38.
A large historical prefix is not a reason to reject a representable later
window.

Raw BIGNUM is **not** an Arrow numeric export. Diagnostic projections must
cast it to canonical strings. A final-output consumer must construct the
scale-preserving text, check conversion to its declared numeric representation,
and refuse a nonrepresentable required result before export. Checked-cast
nulls cannot masquerade as missing data. Native decimal precision above 38 is
not available on this route.

Float32 widens to Float64. A Float64 fact enters the same exact state: each
finite value stands for its shortest round-trip decimal text, the text DuckDB
prints for it and reads back as the same binary64, held as the `BIGNUM`
coefficient of that decimal at its unit's scale. The scale is derived from the
data in the same query, per unit, as the largest number of fractional digits
any of the unit's selected values needs, so one tiny value such as `5e-324`
widens only its own unit's coefficients. Every finite binary64 is held
exactly; no input is rounded on entry, and a long or large history is never a
reason to refuse. The float state carries `exact_scale` alongside the exact
counts, plus `float64_nonfinite_count` and `float64_unrepresentable_count` for
inputs the exact state cannot hold: NaN and infinities, on BigQuery values
beyond `BIGNUMERIC`'s range, and on Snowflake coefficients beyond
`NUMBER(38, 0)`'s 38 digits. Snowflake holds the same scaled coefficients as
DuckDB (its shortest round-trip text comes from a `VARIANT`); PostgreSQL and
BigQuery hold the shortest text unscaled in `NUMERIC`/`BIGNUMERIC`, and
PostgreSQL's `NUMERIC` admits NaN and infinities, which the output rounding
refuses instead.

Window aggregation exposes its data-dependent checks through
`TemporalResult.validate(pinned_executor)`. Checks and export must use the
**same snapshot**. Neither builder opens a transaction nor executes a hidden
probe. Refusals contain aggregate failed counts and identifiers only, never
customer rows, keys, or values. `terrine.temporal.float64_precision`
identifies `nonfinite_input` (a non-finite value at or before the end of a
non-empty window) or `bound_overflow` (a result beyond binary64's range).

## Exact timestamp windows

The internal `lookup_prefix` compiler supports strict native ASOF and
`union_window` strategies. Both preserve duplicate requests, return the complete
state strictly before the cutoff, and distinguish an unmatched state from a
matched state containing SQL NULL. Repeated lookups of one prefix use distinct
relation identities, so a second cutoff cannot reuse the first cutoff's state.
The fallback carries the complete timestamp-order marker, then rejoins the
original state once; it does not independently skip null fields or expand
anchors across events. NULL requests match no state, including nonnumeric payloads.

Native and fallback lookups share lossless structured ordering for mixed native
microsecond/nanosecond axes, signed pre-epoch fractions and supported infinities.
They do not narrow a nanosecond event or scale a wide microsecond cutoff into
overflow. Unsupported native coarse timestamp representations raise
`terrine.temporal.timestamp_order_unsupported`: normalize to a losslessly
representable native microsecond/nanosecond timestamp before binding.

`aggregate_window` consumes resolved measure-to-prefix mappings and returns a
lazy `TemporalResult` for a named feature or outcome. It retains original
anchors and records each output's window, role, observation interval,
completeness and nullable reason. Event prefixes preserve exact intraday t0;
day prefixes require both endpoints at the catalog's local midnight.
For `[t0-1h,t0)`, t0 is excluded; for `[t0,t0+1h)`, t0 is included and the
right endpoint is excluded.

Readiness intersects every metric source and the unit's optional observation
interval. Incomplete coverage takes precedence over incomplete observation,
then cell-level empty/all-null/zero-denominator meanings. Unavailable cells
retain numeric NULL and their exact reason.
Complete empty sums/counts are zero and conversion is false. Nonempty all-null
facts still count as events and conversion is true, but sums/averages are NULL.
Unknown required filters in an available window refuse before output.

Exact sums export as lossless decimal values with precision up to 38 and the
original scale; counts use a checked exact integer representation.
`terrine.temporal.exact_overflow` refuses a required unrepresentable result,
not a wide internal prefix whose later window fits. Conversion booleans have
no numeric precision policy.

A float window differences the exact prefix state, so history before the
window cancels exactly, and rounds the result to binary64 once through the
backend's correctly rounded decimal-text parser: a window sum is
`RN(Σ shortest decimal of each value)`, one rounding, so `0.1 + 0.2 − 0.3` is
`0.0`. An average or ratio of exact quantities `p / q` divides before that
rounding. DuckDB and Snowflake normalize each operand to a signed 19-digit
significand with a decimal exponent, divide those as wide integers (DuckDB's
128-bit `HUGEINT`, Snowflake's `NUMBER(38, 0)`; each truncation below
`10**-18` relative) and round the quotient once, so the published value `r`
satisfies `|r − p/q| ≤ 6·2⁻⁵³·|p/q| + 2⁻¹⁰⁷⁵` for normal and subnormal results
alike, for operands beyond binary64's range, and for a ratio of averages,
which divides the unrounded averages. PostgreSQL and BigQuery divide in
`NUMERIC`/`BIGNUMERIC` and round once. Float outputs carry
`numeric_policy=None` and no error column on every backend; eligibility
predicates and state-panel bins compare the rounded value. A known exact zero
denominator is NULL with `zero_denominator`. Cohort totals and means and
cumulative lifetime value reduce published binary64 values natively and refuse
`bound_overflow` when the result is not finite.

Run the focused fast boundary and window checks with
`uv run pytest tests/temporal/test_prefix_lookup.py tests/temporal/test_window_aggregates.py`.
Native quotient/calendar checks whose compilation cost exceeds the unit budget
run in the integration tier with `uv run pytest integration/temporal`.

## Bounded event sequences and event-time origins

`EventSequence` returns one header for each candidate anchor. The default
`positions` representation adds one child row per observed event; `arrays`
keeps one row per anchor and stores ordered event structs in an Arrow list.
Both preserve event identity, exact UTC timestamps, known-null property reasons,
and the distinction between complete-empty and unavailable intervals. Set a
positive `max_events` per anchor, then materialize with positive `Budget`
`max_rows` and `max_output_bytes` limits. The per-anchor cap is checked before
array aggregation; output limits are enforced before a materialized frame is
returned. They are not scan limits.

Events are ordered by `(timestamp, configured event ID)`. If the source has no
configured event ID, equal timestamps within a unit refuse rather than receive
an arbitrary order. `SessionSpec` partitions independently by anchor and starts
a new session only when the adjacent elapsed gap is strictly greater than its
configured gap. Funnels retain bounded attempts from each first-step event and
carry progress/status when a selected observation window ends before a
deadline.

`EventTime(fact, window)` selects the first matching fact occurrence in its
strict half-open window; `occurrence="nth"` selects the corresponding
one-based occurrence. Its optional paired `label_property` and `equals`
arguments refine selection. An expression can use that timestamp as its origin:
`agg("metric", window=("0s", "1h"), relative_to=selector)` or
`asof("property", before="0s", relative_to=selector)`. Runtime time bounds are
retained separately from the measured window. After the selector resolves to a
present origin, the relative result preserves the underlying value,
completeness, and null reason independently; a complete result can still carry a
reason such as `all_values_null`. An incomplete selector or missing origin
retains precedence over the result's reason. Static validation propagates every
selector's possible support and refuses feature or eligibility chains that may
cross the configured gap, without executing source queries or clipping the
window. Baseline estimators refuse outcomes with event-time origins until they
support that timing model.

Materialized provenance records the sequence schema and each expression's
selector references, conservative support intervals, and resolved time-column
references. `MaterializedFrame.to_arrow()` writes these typed values into
Arrow schema metadata, and `MaterializedFrame.from_arrow()` restores them with
the original nested event values.

Run `uv run python examples/event_sequences.py` for a local DuckDB example of
stable timestamp ties, an empty anchor interval, a known-null event property,
sessions, funnels, and both output representations.
