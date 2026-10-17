# Native temporal catalogs

Terrine's native `Catalog` describes source relations and their meaning without
requiring the optional increment adapter. A catalog can be constructed from a
Python mapping with `Catalog.model_validate(...)` or loaded from safe YAML with
`load_catalog(path)`. Both paths use the same strict version-1 Pydantic models;
unknown fields, duplicate YAML keys, unsafe YAML tags, and unsupported schema
versions are rejected.

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

### Exact and checked numeric state

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

Float32 widens to Float64. Float64 uses ordinary native `SUM(x)` and
`SUM(abs(original x))`, retaining original nonnull leaf counts across disjoint
group and prefix reductions. Constant-vector blocks may use `RN(m*x)`:
accepted multiplicities are exact, and each block consumes at most its
original `m-1` rounding-operation budget. It is never labeled exact. The backend
contract assumes stock DuckDB 1.5.6 native aggregates, round-to-nearest
binary64 arithmetic, and gradual underflow; other builds/backends must
establish the same capabilities rather than silently changing algorithms.
Per-leaf native conformance expressions check rounding-sensitive operations
and subnormal arithmetic in the execution thread. Finite conformance probes
do not prove arbitrary hidden changes to the floating-point environment.

For `k=max(n-1,0)`, `u=2**-53`, `eta=2**-1074`, the certificate constructs
outward upper bounds `g >= k*u/(1-k*u)` and `b >= k*eta/(1-k*u)`, then
`sum_abs_upper = RU((sum_abs_value+b)/(1-g))` and
`prefix_error = RU(g*sum_abs_upper+b)`. Each elementary operation is followed
by native `nextafter` when finite, with downward-rounded positive denominators.
An elementary overflow stays nonfinite; inward rounding cannot hide it.
Empty prefixes are the exact zero identity. `k*u >= 1/4`, nonfinite inputs or
states, and invalid/nonfinite bounds are failures. Both grouped and past
prefix failures remain flagged, even if a later tree could mask them.

Certificate construction projects intermediate bounds once in the lazy
relation, rather than recursively duplicating scalar expressions. This adds
no row callbacks, extra source queries, or dense calendar.

The internal Float64 fields are `sum_abs_value`, `sum_abs_upper`,
`prefix_error`, `float64_nonfinite_input`, `float64_nonfinite_state`,
`float64_count_limit`, `float64_bound_overflow`, and
`float64_conformance_failure`, alongside the exact counts.
`numeric_policy="checked_float64_v1"` describes these sufficient states, not
a completed final-window precision claim.

A prefix consumer can validate selected states with
`validate_float64(..., execute=pinned_executor, source=..., measure=...,
output=...)`. Window aggregation exposes its endpoint and final-result checks
through `TemporalResult.validate(pinned_executor)`. Checks and export must use
the **same snapshot**. Neither builder opens a transaction nor executes a hidden probe.
Refusals contain aggregate failed counts and identifiers only, never customer
rows, keys, or values. `terrine.temporal.float64_unsupported` identifies a
missing capability; `terrine.temporal.float64_precision` identifies one of
`nonfinite_input`, `nonfinite_state`, `count_limit`, `bound_overflow`,
`uncertifiable_cast`, `denominator_interval_zero`, or `tolerance`.

Window subtraction, casts and division need their own outward certificates.
Shared scalar primitives include the rounding radius
`RU((u*abs(value)+eta)/(1-u))` and a downward-rounded fixed final tolerance
`2**-40 + 2**-30 * abs(value)`. A final checked `ColumnMeta` must name a
finite, nonnegative absolute-error column using
`numeric_policy="checked_float64_v1"` and `absolute_error_column`; an exact
output has policy `exact` and no error column. These final-result obligations
are not replaced by accepting a prefix certificate.

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
retain numeric NULL and their exact reason; checked errors are NULL too.
Complete empty sums/counts are zero and conversion is false. Nonempty all-null
facts still count as events and conversion is true, but sums/averages are NULL.
Unknown required filters in an available window refuse before output.

Exact sums export as lossless decimal values with precision up to 38 and the
original scale; counts use a checked exact integer representation.
`terrine.temporal.exact_overflow` refuses a required unrepresentable result,
not a wide internal prefix whose later window fits. Conversion booleans have
no numeric precision policy or error column.

Float window subtraction retains both endpoint errors and adds its own outward
rounding radius. Averages and ratios enclose inputs and all four endpoint
quotients, then certify the returned quotient against the same fixed final
tolerance. Intermediate certificates must remain finite and conservative;
the output tolerance applies in the final result's units, not to unpublished
sums. Every required outward input or quotient endpoint must remain finite.
Exact fractional inputs use the pinned backend's correctly rounded canonical
decimal-text parser, not an uncertified direct numeric cast.
A known exact zero denominator is NULL with `zero_denominator`; an uncertain
interval spanning zero raises `terrine.temporal.float64_precision`. A large
old sum followed by a tiny window may refuse conservatively. Use genuinely
exact integer/decimal facts, a supported checked backend, or a smaller
semantically sufficient history; do not redefine first-event anchors or omit
required measurement history to evade a refusal.

Run the focused fast boundary/certificate checks with
`uv run pytest tests/temporal/test_prefix_lookup.py tests/temporal/test_window_aggregates.py`.
The explicit native quotient/calendar integration tier runs with
`uv run pytest tests/temporal/test_window_aggregates.py -m slow`.
