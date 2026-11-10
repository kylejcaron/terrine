# Native temporal catalogs

Terrine's native `Catalog` describes source relations and their meaning without
requiring the optional increment adapter. A catalog can be constructed from a
Python mapping with `Catalog.model_validate(...)` or loaded from safe YAML with
`load_catalog(path)`. YAML text uses the same strict version-3 schema gate;
unknown fields, duplicate YAML keys, unsafe YAML tags, and unsupported schema
versions are rejected. `load_catalog`, `load_catalog_text` and
`Model.from_catalog` also accept version 2 documents, which load with every
measure exact and keep their model hash; `Catalog.model_validate(...)` accepts
version 3 only.

Schema failures from `load_catalog` and `load_catalog_text` raise `TerrineError`
with code `terrine.catalog.invalid_definition`. Inspect its immutable
`context["issues"]` for ordered locations, stable validation categories, and
package-owned recovery hints; the same actionable locations and corrections
appear in the displayed error. For example, `day_boundary: UTC+14:01` points
to `day_boundary`: use `UTC`, a signed `HH:MM` offset, or a `UTC`-prefixed
fixed offset within ±14:00 (the endpoints are limited to exactly `14:00`).
Correct the YAML and load it again. Direct `Catalog.model_validate(...)`
construction continues to use Pydantic's `ValidationError`.

## Binding event tables

Use `Model.from_tables` for table-backed events with one unit key per table.
`UnitSource` is the authoritative roster, including units without matching
events; Terrine never derives that roster from event rows.

```python
import terrine as tr
from terrine import Events, History, Model, UnitSource

# con already contains customers(id), orders(customer_id, occurred_at, amount, status)
# and plans(customer_id, plan, starts, ends).
model = Model.from_tables(
    con,
    units={"customer": UnitSource("customers", "id")},
    events={
        "orders": Events(
            "orders",
            time="occurred_at",
            key="customer_id",
            labels=("status",),
            metrics={
                "orders": tr.count(),
                "refunds": tr.count(where={"status": "refunded"}),
                "refund_rate": tr.ratio("refunds", "orders"),
                "spend": tr.sum("amount"),
                "order_value": tr.mean("amount"),
            },
            complete=("2025-01-01", "2025-01-31"),
        )
    },
    history={
        "plan": History(
            "plans", key="customer_id", column="plan",
            valid_from="starts", valid_to="ends",
            complete=("2024-01-01", "2025-01-31"),
        )
    },
)
```

`table` is the only positional argument; `time` names the event timestamp and
`key` the unit key column. `unit` is inferred from a single `UnitSource` and
required when the model declares several (omitting it refuses with
`terrine.model.ambiguous_unit`, context `owner`, `choices`, `parameter`).
An `Events` with no `metrics` declares the event alone: anchors, `event_time`,
survival and sequences still work, and nothing is named for you.

Use `Events(where=...)` to split a mixed-kind table into event sources without
copying or prefiltering it. Each declaration has its own event membership:

```python
events = {
    kind: Events(
        activity,
        time="occurred_at",
        key="customer_id",
        where={"kind": kind},
        complete=("2025-01-01", "2025-01-31"),
    )
    for kind in ("signup", "purchase")
}
```

Source filters use the same operators and typed comparisons as metric filters,
but their keys name physical columns; those columns need not be in `labels=`.
They apply before metrics, event selectors, anchors, survival, sequences and
event-backed `asof` lookups. `Events` detaches the mapping and freezes collection
values, and source filters round-trip in the version-3 catalog. Changing a source
filter also changes its features' definition identities, so fitted transforms
cannot be reused with a different event population.

A source filter must determine membership for every raw row. An unknown combined
predicate (including a null or nonfinite numeric value) refuses with
`terrine.compile.anchor_integrity`, `check_key="unknown_event_filter_value"`.
Conjunction follows SQL three-valued logic: a row definitively excluded by one
term is allowed even if another term is unknown. Filter columns and literal
types are checked at model binding, without reading rows; mismatched values
refuse with `terrine.semantic.schema_mismatch`, `field="where"`.

Metrics are named by you and built with `tr.count()`, `tr.sum(column)`,
`tr.mean(column)` (per event; `per="day"` divides by the window's calendar
days), `tr.min`, `tr.max`, `tr.count_distinct(column)` and
`tr.ratio(numerator, denominator)` over two metrics of the same `Events`.
Metric names are unique across the model; a metric may share its event's name.
Consume them with ordinary `agg` expressions. `tr.sum`, `tr.mean`, `tr.min` and
`tr.max` allow native float64 arithmetic for float columns unless given
`exact=True`; counts, distinct counts and integer or decimal columns are always
exact (see [Native Catalog declarations](#native-catalog-declarations)).

`labels=` names event columns that become event-time properties with the same
name, usable in metric `where=`, in `event_time(..., label_property=, equals=)`, in
`asof(label, before=)` (the last value strictly before the cutoff, carried from
the event log) and in `Treatment(label)`. The dtype is read from the bound
column: string, integer (or scale-zero decimal), float (or decimal), boolean
and date; any other type, timestamps included, refuses with
`terrine.model.invalid_binding` (`reason="unsupported_column_type"`, context
`source`, `column`, `dtype`, `supported`).


`UnitSource(attributes=("channel", ...))` can declare static attributes directly
on the authoritative roster. Each name must identify a roster column and may
not collide with an event, property, dimension or metric name. These values are
available at every anchor, including anchors outside event-table completeness,
and `asof("channel")` reads the roster value strictly before t0 by default.
Roster attributes need no event-source readiness; roster observation bounds
still apply. The declarations are part of the unit binding, not the catalog, so
pass `units=model.units` when rebinding a catalog.

Metric `where=` is a mapping keyed by `<label>` or `<label>__<op>`; several keys are a
conjunction. A scalar value means equality and a list, tuple or set means
membership; `__ne`, `__in`, `__not_in`, `__gt`, `__gte`, `__lt`, `__lte` and
`__between` are the other operators. `__between` takes an ordered `(low, high)`
list or tuple, both ends included; a set, or two numbers with `low > high`, is
refused rather than matching nothing, while text bounds such as `("2", "10")`
are exact literals ordered by the label's type when the filter compiles.
`__` is reserved for that split: a label name containing it refuses at
`Events(...)` with `reason="reserved_name"`, and a key with an unknown
operator, a second `__`, the wrong number of values or a non-scalar refuses
at metric construction (or at model binding for source filters) with
`reason="invalid_filter"` and the `key`. A metric `where=`
label the `Events` does not declare, or a ratio operand that is not another
non-ratio metric of
the same `Events`, refuses at `from_tables` with
`terrine.semantic.missing_reference` (context `kind`, `name`, `owner`,
`available`). Comparing with `None` compiles a null literal whose comparison
is unknown for every row, so a build refuses with `terrine.filter.unknown`
rather than returning zeros.

`history=` binds one attribute per `History`: the mapping key names the
attribute, `column` its value column, and exactly one time axis is declared,
either `valid_from` (optionally with `valid_to`) or `changed_at` alone (a
`changed_at` row is effective until the next change); anything else refuses
with `reason="history_time_axis"`. A table with several attribute columns is
declared once per column. The attribute works in `asof` and `Treatment` as a
label does, with [effective-time semantics](temporal-attributes.md).

Every `Events.complete` and `History.complete` declares an inclusive pair of
complete local calendar days, interpreted in `day_boundary`. Missing
completeness refuses with `terrine.model.missing_event_completeness` and
context `source`; supply an authoritative pair, never an observed maximum
event timestamp. `event_ids` declares stable identity columns as with
`Model.from_catalog`. Use `dialect=` to declare a dialect explicitly; by
default it is the admitted connection's dialect. `day_boundary` defaults to
`UTC`.

Names are checked before any schema is read. Event names (the `events=` keys),
history names and labels share one namespace because `Initiation(name)` resolves
across events and labels; metric names are unique across every `Events`; and
each metric column reserves a value event named `<events name>_<column>`
(`orders_amount` above) against every event, label and history name. A clash
refuses with `terrine.model.invalid_binding` (`reason="name_collision"`,
context `kind` of `metric`, `label` or `event`, `name`, and the sorted
`owners`, such as `("events:orders.amount", "labels:orders")`).

`model.to_catalog()` writes the equivalent schema-version-3 catalog as YAML.
Pass that YAML directly to `Model.from_catalog(model.to_catalog(), con=con,
units=model.units, readiness=model.readiness, event_ids=model.event_ids)` to
bind the same model with an equal `model_hash`. A catalog contains only source,
event, property, dimension, measure and metric definitions; it excludes
`units`, `readiness` and `event_ids`, which are provided separately when
binding. Catalog YAML, `model_hash` and owner model references all derive from
one canonical catalog payload that omits fields equal to their defaults, so a
catalog that gains a defaulted field keeps its model hash, saved plans and agent
references.
`Model.from_catalog` also accepts a `Path` or a single-line string as a file
path. Choose it directly for SQL relations, multi-unit sources or explicit
catalog names; those are richer source declarations, not options inferred from
tables.

### Scheduled time series

Start with `recipes.time_series`: one row per roster unit and scheduled calendar
anchor. Required arguments are `every`, `start`, and `end`; `unit` is the model's
only unit unless named (see [unit inference](design-specs.md#unit-inference)). Dates may
be calendar dates or ISO date strings, never timestamps. The schedule defines
the anchors, without a separate period count: `t0` runs from `start` in steps
of `every` through the `end` day, which is included, so `start == end` is a
single anchor. `every` is an elapsed duration (`"7d"`, `"4w"`, integral
seconds) or a calendar step: `"1mo"`, `"3mo"` or `"1y"` (twelve months).
Calendar anchors are `start` plus `k` months, each clamped to its own month's
last day and never chained from the previous anchor, so `every="1mo"` from
`2025-01-31` through `2025-04-30` gives Jan 31, Feb 28, Mar 31 and Apr 30, and
`every="1y"` from `2024-02-29` gives Feb 28 in 2025 and 2026. Supported
options are `features`, `outcomes`, `eligibility`, `treatment`, `anchor_range`,
and `gap` (default `"0d"`); optional roles and filters default to `None`.

```python
from terrine import Budget, agg, recipes

outcomes = {
    "count": agg("orders", window=("0d", "7d")),
    "amount": agg("spend", window=("0d", "7d")),
    "mean": agg("order_value", window=("0d", "7d")),
}
design = recipes.time_series(
    every="7d", start="2025-01-01", end="2025-01-07", outcomes=outcomes,
)
result = design.build(model).materialize(
    budget=Budget(max_rows=1_000, max_output_bytes=1_000_000)
)
```

A customer in `customers` with no orders still has a row. In a complete window
its count and sum are `0`, and its mean is null with reason `no_observations`.
An incomplete window is null with its completeness reason, including for a
zero-activity customer; unknown is not zero.

A monthly series is the same call with a calendar step:

```python
monthly = recipes.time_series(
    every="1mo", start="2025-01-31", end="2025-03-31", outcomes=outcomes,
)
```

Its anchors are the local midnights of Jan 31, Feb 28 and Mar 31. Only a
`Schedule` accepts calendar units: windows, `asof(before=)`, `gap=`,
`EventAnchor(offset=)`, `ltv(period=)`, `life_table(bin=)` and
`event_sequence(horizon=)` are elapsed time and refuse `"1mo"` with
`terrine.window.invalid_duration` (context `field`, `value`); use `"30d"` or
`"4w"`. `state_panel(every="1mo")` refuses with
`terrine.state_panel.invalid_schedule` (`reason="calendar_months_unsupported"`,
`route="4w"`) because a state transition's period is a fixed width.

#### Under the hood

```python
from terrine import Design, Schedule

equivalent = Design(
    None, Schedule("7d", "2025-01-01", "2025-01-07"), outcomes=outcomes,
)
assert design == equivalent
```

The event `orders` also works as `start_event="orders"` in
[`recipes.ltv`](trajectories-survival.md#cumulative-ltv). Its required `metric`
must be an untransformed metric with exactly one SUM measure, such as
`spend` above; counts, means and ratios are not accepted by this shape. Run
`uv run python examples/table_first_dataset.py` for a complete local example
that creates the roster and orders, checks zero-activity and incomplete-window
results, and materializes cumulative event-origin values.

## Native Catalog declarations

```yaml
schema_version: 3
day_boundary: UTC
dialect: duckdb
sources:
  sessions:
    relation: {kind: table, name: session_events}
    time: occurred_at
    units: {user: user_id}
    events: [session]
    properties: []
    dimensions: []
events:
  session: {source: sessions, column: null}
properties: {}
dimensions: {}
measures:
  session_count: {event: session, aggregation: count, filters: []}
metrics:
  sessions: {kind: mean, unit: user, measure: session_count}
```

Here `kind: mean` describes the downstream population summary; `aggregation:
count` describes the per-unit session count. It does not average events or
change a requested feature window.

A `sum`, `avg_event`, `avg_calendar_day`, `min` or `max` measure over float
values allows native float64 arithmetic with `exact: false`; without the key
it is exact. Counts, distinct counts and integer or decimal event values always
aggregate exactly, so `exact: false` on them refuses with
`terrine.catalog.invalid_definition`. `Model.from_tables` writes `exact: false`
for float metrics declared without `exact=True`. The mode is part of the
measure's identity: switching it changes the model hash and rebuilds persisted
layers rather than reusing them.

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

Logical units map to physical source keys. A metric selecting `user` uses
only that logical key as its canonical unit key; other unit columns remain
available for explicit dimension joins and are not combined into an implicit
composite identity. Ratio inputs may select different physical columns for the
same logical unit.

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
half-open event windows. Integer and Decimal event values retain exact decimal
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

Import `quantile`, `approx_distinct` and `approx_quantile` from `terrine.spec`.
`approx_quantile(metric, probability, window=...)` uses DataSketches t-digest
on `cross_section` designs only, at the installed package version with fixed
compression `k=200`. No sketch settings or requested rank error are study inputs.
It is available with the optional `terrine[sketches]` extra; otherwise the
request refuses before source queries and points to `terrine[sketches]` or
exact `quantile`.
The compiler streams canonical `(unit, anchor, event time, value)` Arrow batches
to the client, updating t-digests in sorted anchor order. This transfers all
matched window values out of DuckDB and can cost more than in-database exact
work; t-digest accuracy is empirical and has no guaranteed rank bound. Both
approximate results record algorithm, implementation/version, parameters, and
error description in `ColumnMeta` and preserve that record through Frame Arrow
and JSON metadata serialization. Other shapes and runtime composition refuse
approximate quantiles; ordinary `Frame.materialize()` is the supported route.

### Bounded repeated windows

Additive panel metrics use local batched reductions, including long and sparse
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
multiplicity (`EventAnchor(..., occurrence="each")`) and larger declared overlaps
refuse before execution with `terrine.temporal.range_bound_unavailable`.
Use first/nth event anchors, fewer event periods/anchors, a coarser schedule or
narrower window, or an additive metric with local reduction. Additive
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
from terrine import Design, FixedDate
from terrine.spec import retention

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

The event's column supplies independent occurrence IDs, scoped by analysis unit.
If the event is a row marker without a column, declare `Model.event_ids` for its
source. Repeated evidence for one identity is counted once across the entire
horizon; unrelated labels and null event identities do not count. The output is
the ordinal band `sum(count >= threshold)` over strictly increasing positive
Int64 thresholds, with exact integer comparison and no statistical approximation.

Finite follow-up is `[t0, t0 + duration)`. Open follow-up is `[t0, U)`, where `U`
is the source's declared exclusive completeness endpoint, not the final observed
event. Source history, source follow-up, and declared roster observation bounds
must cover the horizon. Finite follow-up requires a known roster observation end;
an absent end is not evidence of complete finite follow-up. These outcomes may
not be used as pre-anchor features or eligibility predicates. Their reference
and threshold declarations round-trip through JSON/YAML and saved definitions;
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

## Local batched window reduction

The internal `terrine.temporal.aggregate.aggregate_windows` builder accepts
resolved `AggregateRequest` records over normalized event sources. Its `event`
and `day` forms share contribution selection and publication; neither reads
lifetime cumulative state. This boundary is internal, not a new user-authored
execution mode. Ordinary and retained study compilation collect additive leaves
from features, outcomes and eligibility before lowering a compatible group.

Each primitive selects its own half-open interval and available candidate
anchors before arithmetic. Compatible requests share source access without
sharing incompatible predicates, denominators or availability. Each source is
reduced at `(unit, anchor_id)` before independent sources are joined, so a ratio
cannot multiply raw event rows. Duplicate anchor instances with distinct
anchor IDs remain separate.

The collector resolves each logical event selector once against the invocation's
common candidates, including nested selected origins. Requests share a group only
at the same actual origin and anchor grain. Each group's relation and checks join
back once by null-safe `(unit, anchor_id)`; public names, roles, timing support and
per-output availability remain independent. Candidate validation precedes
eligibility filtering, even when the published relation is empty. Selector and
source memoization are local to one compiler invocation.

Bounded exact reduction keeps numerical guard intermediates private to each
source; joining three or more independent exact sources preserves the same
public values, domains and anchor grain.

Independent contexts, contribution primitives and output finalizers share
projection layers at the same named-column dependency depth. Arithmetic stays
behind its explicit column boundaries: selected-input masks still precede float
conversion, quantization and invalid-input checks. This avoids serially nesting
every output's intermediates without introducing per-output source joins or
inlining repeated numerical expressions. The five-metric, ten-window raw/day
DuckDB smoke compiles, validates and exports in both integer and native modes;
this is exercised evidence, not a performance guarantee or a new public mode.

Compatible outputs reuse context expressions within one aggregate build. Each
output's finalization branch projects only its own primitives, availability,
observed bounds and alignment check, while retaining every original anchor
field. Sibling output payloads do not inflate those branches. Execution-bound
recompilation still occurs under the materialization snapshot; expressions,
validation results and source data are not cached across calls.

Same-source support expressions can share observed bounds without sharing
availability, completeness, reason or check records. An unfiltered count may
reuse an existing unfiltered event-count payload only when the donor covers its
full membership identity; the count still applies its own final mask. Source
scope remains unique at `(unit, day)` before full anchor-instance fanout.

Event counts include selected null values; event means divide by the selected
nonnull count. For values `2, NULL, 3`, the count is `3`, sum is `5`, and event
mean is `2.5`. Complete empty sums/counts are zero, complete empty conversions
are false, and empty event means carry `no_observations`. Nonempty all-null
sums/means remain unavailable with `all_values_null`. Coverage and unit
observation bounds are output-specific; a ratio intersects both operands'
source availability. An available zero divisor retains `zero_denominator`.
Unknown filter candidates retain their provenance and refuse an available
output instead of disappearing through a filter.

The builder returns one multi-output `TemporalResult`. Its deferred validation
must execute before export under the same snapshot. Integer and declared-decimal
domains, per-input exact-float quantization and `below_resolution` notes remain
intact. Native selected sums and divisions follow the backend's ordinary binary64
arithmetic, without a lifetime-history error certificate.

The daily form first derives a per-unit start/end envelope from available,
aligned requests and joins one envelope row to each event. Timestamps outside
that envelope become typed NULL inputs to local-day conversion, so an unused
endpoint cannot overflow calendar normalization even if an optimizer moves
filters. The original timestamps and numerical/filter inputs remain unchanged.
It then derives exact required `(unit, bucket)` identities: the envelope alone
does not activate payloads in gaps or for unavailable requests. Each equivalent
primitive has its own consumption flag within
that shared scope: a bucket needed only by a count cannot enter a sum's float
conversion, finite checks, exact quantization or arithmetic. Duplicate anchors
and overlapping requests cannot multiply events within a primitive/bucket.
Only wholly contained fixed-offset local days are usable, including offsets
such as `UTC+05:30`; partial edge buckets are never reconstructed.
Source buckets use the precision-preserving local-day conversion, so an event one
nanosecond before a pre-epoch local midnight remains in the preceding day. For
aligned windows, bucket-start membership `start <= bucket < end` is equivalent
to full-bucket containment. Scope selection never adds a day to an unused source
bucket: an event on the backend's final supported day cannot overflow merely
while deciding whether a distant requested window consumes it.
DuckDB compares mixed naive/aware timestamps through exact timestamp parts,
without attaching a zone to wide naive source instants. This preserves raw
selection over valid finite endpoints even when the session is not UTC.
Microsecond parts use native epoch ticks rather than a calendar difference,
preserving equality and adjacent representable ticks near the finite endpoints.

The internal `compile.numeric.lower_aggregates` chooses raw input for a single
distinct fixed origin, relative windows, partial days or unproven alignment.
Repeated, statically aligned fixed/scheduled origins may share daily summaries.
Mixed representations preserve requested output order and metadata. Strategy
labels are `local_event/native_float`, `local_day/native_float` and
`local_event/exact` or `local_day/exact`; the exact label includes counts,
integer and declared-decimal domains. Selection reads declarations, not source
row counts, and uses no density threshold or persisted summary cache.

Supplied/generated anchor relations do not inherit an outer schedule's alignment
proof. Without declaration evidence about their actual clocks, they use raw
input, as do event-relative origins.

Exact floats quantize each input before grouping, not the daily sum: two
`4e-10` values retain zero with `below_resolution`, while native arithmetic
retains approximately `8e-10`. Daily grouping changes the order of native sums;
raw/day bit identity is not promised for ill-conditioned values. Exact bounded
accumulation guards both bucket formation and merging, carrying withheld flags
only to consuming outputs. Only existing lossless integer/declared-decimal
coefficient conversion precedes nullable selection; all guards and sums follow
the primitive's mask.

Daily scope has relational cost: deriving observed identities, reducing unique
consumption flags, joining back to events, grouping buckets and merging them.
An engine may share source scans with CTEs, but larger SQL/plans and compilation
overhead remain possible even on tiny inputs. This is a declaration-based
choice, not a guarantee that daily summaries are faster for every workload.

An ordinary public five-metric, ten-window native smoke materializes all 50
nonempty outputs and retains roster units without events at the default Python
recursion limit. Its inspected DuckDB plan scans the event source once; generated
SQL is still large, so this demonstrates shared lowering and correct execution,
not an end-to-end speedup. Retained staging and correction use the same compiler;
no-change refresh reuses only a verified current-revision generation.

The separate [archived matched literal-SQL comparison](https://github.com/kylejcaron/terrine/blob/771e9d55ba62bca5b6d512f9109e598a32a0e9cd/docs/research/2026-11-09-compact-generated-sql.md)
measures five unprofiled repeats at 10,000 units/300,000 events. Its wide query
median is 1.839× the literal query with identical typed modeling/quality output,
while the richer public call is 30.013×. README/scaled queries still exceed the
3× waypoint. These bounded measurements do not change declaration-based strategy
selection, exact numeric/timestamp semantics or source-validation order.

Binned state queries may compact the period relation only for one untransformed
sum yielding `DECIMAL(38,0)` under the active unbounded-coefficient policy, with
whole-day endpoint/count/host-clock proof. The original scheduled and endpoint
validation studies remain separate and unchanged. Native floats, bounded
policies, other metric shapes and declined schedules keep the original union;
there is no public switch or cached validation. The separately pinned state
comparison reduced SQL compilation but increased construction/public-call
medians, not an established query-runtime speedup.

Execution revision 2 identifies this compiler. A checksummed revision-1 plan
cannot execute or replay, and a no-change refresh from revision 1 recomputes.
Explicit historical reads preserve the old generation's values and metadata.
Exact declarations and specialized cumulative/cohort arithmetic safeguards are
unchanged.


## Shared contribution payload

The compiler normalizes each source with its explicit `BoundSource` unit and
event mapping, then calls `resolve_measure(events, measure, source=source)` to
retain the descriptor's actual schema type. The internal
`terrine.temporal.daily.contributions` builds selected per-event primitives;
`reduce_payload` reduces them by the requested keys. Raw windows reduce at full
`(unit, anchor_id)` identity. The daily form first reduces unique consumed
unit/day payloads, then combines only the buckets each window consumes.
Both forms feed the same `aggregate_windows` finalization.

These are internal builders, not required user inputs. Use the
[scheduled time-series workflow](#scheduled-time-series) to declare several
outputs together, or run
[`examples/table_first_dataset.py`](../../examples/table_first_dataset.py)
for a complete Model → Design → Frame example with tiny local tables.
No builder executes a data query. Dimension filters consume the deterministic
value/found aliases produced by `enrich_events`, not guessed physical names.
Measure filters use event-time values; a property's `pre_exposure` annotation
does not create an exposure, assignment, or anchor-specific filter.

The payload contains only needed primitives: event and nonnull counts, a native
sum or exact coefficient sum, applicable invalid-input/discarded-term counts,
and unknown-filter provenance. Bounded exact reduction also carries withheld
sum flags. Native sums carry no magnitude certificate. Arithmetic mode, type
and scale come from the resolved descriptors, not user-authored state rows.

Only consumed, observed unit/day pairs are summarized; there is no dense
unit-by-day spine. A roster member without source events still gets its original
anchor rows through finalization. Duplicate events and timestamps keep full
multiplicity. Another declared entity key is not an additional grouping key:
normalized `_unit` is the selected source key. Collision-safe payload names
preserve physical keys such as `event_count` without merging distinct units.
Grouping keys are projected after contributions are formed, so a physical
numeric event or filter column named `local_day` retains its original values.
The derived local day and its aware UTC start use the fixed catalog offset,
independently of the database session timezone; naive timestamps denote UTC.
Day bucketing preserves pre-epoch nanosecond membership without changing the
original timestamp used for exact event-window comparisons.

### Counts, nulls, and unknown predicates

`event_count` counts selected events, including events whose value is null;
`nonnull_count` counts their nonnull values when the operation needs them.
Counts and conversions consume event multiplicity without converting the value
column or constructing a sum. Null multiplicity can be derived from
`event_count - nonnull_count`; it is not another independently accumulated
field. All-null groups may have a null `sum_value`; finalization uses the counts
to distinguish complete empty intervals, nonempty all-null intervals and
incomplete coverage.

Predicates use SQL three-valued conjunction: known false overrides unknown.
Known-false events contribute no selected count or value. An unknown event is
**not** silently excluded or turned into zero. Filtered payloads retain:

- `unknown_filter_count`: events with an unknown full predicate;
- `unknown_filter_null_count`: such events with an observed-null filter property;
- `unknown_filter_missing_count`: such events with missing dimension history;
- `unknown_filter_literal_count`: such events with a comparison involving a null
  literal that itself evaluated unknown;
- `unknown_filter_nonfinite_count`: such events with an observed nonfinite
  numeric filter property.

The provenance counters are nonexclusive; they do not partition the total.
Unfiltered measures omit them because the predicate is literal `TRUE`.
Even a count or conversion retains applicable filter provenance and refuses
unresolved membership with `terrine.filter.unknown` before output. NaN and both
infinities in a referenced numeric property make its comparison unknown, not
false and not an observed SQL NULL. Known false still overrides unknown.
Only an otherwise available requested window is affected: incomplete
coverage/observation keeps precedence, and an invalid value outside the window
or in an unreferenced property does not block a result. These are
predicate-domain checks, not Float64 precision failures.

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

| Operator | `where=` key | Design predicate | Example threshold(s) for an exact `price` property |
| --- | --- | --- | --- |
| `equals` | `price` | `tr.eq` | `"0.10"` |
| `not_equals` | `price__ne` | `tr.ne` | `"0.10"` |
| `gt` | `price__gt` | `tr.gt` | `"0.105"` |
| `gte` | `price__gte` | `tr.gte` | `"0.10"` |
| `lt` | `price__lt` | `tr.lt` | `"0.105"` |
| `lte` | `price__lte` | `tr.lte` | `"0.10"` |
| `in` | `price` (a list) or `price__in` | `tr.isin` | `["0.10", "0.20"]` |
| `not_in` | `price__not_in` | `tr.not_(tr.isin(...))` | `["0.10", "0.20"]` |
| `between` | `price__between` | `tr.between` | `["0.10", "0.20"]` (inclusive) |

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

Design comparisons share these operators and their typed lowering with
`where=` filters and Catalog filters: each record's `op` is the Catalog
operator, and a Design comparison over a value compiles to the same predicate
as the matching filter over a column. A Design comparison over an incomplete
or null value is unknown (null) with that value's reason; a null literal gives
`null_comparison`. `all_of`, `any_of` and `not_` combine comparisons and
`is_null` with SQL three-valued AND, OR and NOT: a known false term settles
`all_of`, a known true term settles `any_of`, and `not_` of an unknown rule
stays unknown.

### Exact and native numeric reduction

On the supported native DuckDB 1.5.6 route, integer and decimal sums use internal
`BIGNUM` coefficients. For decimal scale `s`, the coefficient is the exact
integer `z` in `value = z * 10**(-s)`; the resolved measure retains the type and
scale. Selected coefficient reduction does not narrow intermediates to HUGEINT
or decimal precision 38. Excluded history is not part of the sum or its guards.
Other policies retain their existing exact coefficient domains and bounded
accumulation checks at bucket formation and window combination.

DuckDB counts take a cheaper route. Event, nonnull, unknown-filter and float
provenance terms are native `BIGINT` zeros or ones, accumulated in `HUGEINT`.
They are added over selected contributions, never recovered by subtracting
running totals. A published count narrows through checked conversion to
`int64`; an unrepresentable required count refuses
`terrine.temporal.exact_overflow`, never wraps. Other engines count in their
exact coefficient type. Retained execution stores anchors and final outputs;
these inline payloads are not an Arrow intermediate ABI.

Raw BIGNUM is **not** an Arrow numeric export. Diagnostic projections must cast
it to canonical strings. Finalization constructs scale-preserving text and
checks conversion to the declared numeric representation before export.
Checked-cast nulls cannot masquerade as missing data. Exact integer/decimal
sums publish lossless decimals with precision up to 38 and their original
scale; a required result outside that domain refuses
`terrine.temporal.exact_overflow`.

Float32 widens to Float64. A native float measure (`exact: false`, the
`Model.from_tables` default) sums selected finite contributions in backend
binary64. Selected nonfinite inputs are counted instead of added and refuse
`nonfinite_input`; returned nonfinite sums or quotients refuse `bound_overflow`.
If backend arithmetic raises, its execution cause is preserved rather than
misclassified as exact overflow. Ordinary rounding, order dependence and
underflow follow the backend. There is no lifetime-history or magnitude
certificate and no universal relative-error bound for ill-conditioned sums or
ratios. Nonzero contributions below `5e-10` are retained subject to normal
binary64 behavior, not intentionally quantized away.

For an exact float measure, each selected finite input is quantized once to a
`DECIMAL(38, 9)`-scale value by the engine's float-to-decimal conversion
(nearest, ties away from zero, with about $10^{-16}$ relative binary64
conversion error). Values within that error of a halfway point may round
either way. Thus `6e-10` becomes `1e-9`, and the exact tie `1/1024` becomes
`0.000976563`. Quantization precedes any daily grouping, so two `4e-10` inputs
both disappear rather than first summing to `8e-10` and rounding to `1e-9`.
The payload carries counts of nonfinite, out-of-range and discarded nonzero
inputs, including `float64_below_resolution_count`, without data-derived
scales. Coefficients accumulate in DuckDB `BIGNUM`, PostgreSQL `NUMERIC`,
BigQuery `BIGNUMERIC`, Snowflake `NUMBER(38, 0)` or the portable exact domain.
Bounded policies withhold unsafe selected coefficients before addition and
refuse `terrine.temporal.exact_overflow`; DuckDB's BIGNUM accumulation is
unbounded.

For original float inputs in window $W$, the exact sum satisfies
$|\hat S-S| \le e_S = 10^{-9}n_W+10^{-12}\sum_{i\in W}|x_i|$.
Event means divide by the actual nonnull count; calendar-day means divide by
the declared aligned day count. Means and ratios convert each exact operand
once at its own scale, then divide once in float64. A native operand selects
ordinary binary64 division; exact-only operands retain policy-aware conversion
and division. An already floating quotient is not an unrounded exact
intermediate. A zero denominator gives NULL with `zero_denominator`.
See the [means and ratios contract](backend-compatibility.md#means-and-ratios)
for propagated exact bounds and the case of a nonzero denominator within its
bound of zero, where no accuracy is stated.

Nonzero exact float inputs below $5\cdot10^{-10}$ disappear from the sum.
The selected window's discarded-term count sets `__reason` to
`below_resolution` beside the retained sum, with `__complete` unchanged.
Means, ratios, cumulative values and complete cohort summaries carry the note;
all-zero and all-null windows keep their usual reasons. Declare decimals (for
example `DECIMAL(18, 12)` for a `1e-12` measure), or rescale into micro-units
before binding when that resolution matters. Row predicates compare original
values; output thresholds compare the published result. Neither arithmetic
mode promises bitwise or order-independent floating results.

The distinct magnitude, quotient, product and underflow safeguards for
cumulative trajectories and cohort reductions are unchanged. On PostgreSQL and
portable policies they can withhold finite results with `bound_overflow`;
DuckDB may publish those results. This is not a guard on ordinary native
source-window sums or ratios. See the
[shared numerical contract](backend-compatibility.md#shared-numerical-contract)
for those specialized limits and backend details.

### Validation and publication

Window aggregation keeps its data-dependent checks as boolean flag columns
of its own relation (`TemporalResult.row_checks`: endpoint alignment,
unknown filters, exact overflow and the float precision flags, in that
order); a TRUE row fails the check and a NULL row passes. A result validated
on its own (`TemporalResult.validate(pinned_executor)`) counts every flag in
one aggregate. In a compiled study the flags ride the window's join into the
audit relation as `__terrine_check_*` columns, and the study's deferred
checks form one ordered schedule: the anchor checks, then each result's
checks in registration order (features, outcomes and eligibility, then the
shape checks), then unresolved eligibility, then the row and byte budgets.
Each maximal run of checks that reduce to one row (the audit's flag sums and
the one-row aggregate checks of nonadditive metrics, first occurrences and
shapes) is one gate statement, its parts cross-joined in schedule order; a
check that must run in place (an as-of attribute or history check, whose
join could duplicate candidate rows) is its own statement between runs, and
a window registered before such a check keeps its flags on its own relation
so its counts stay exact. Materialization runs every earlier run and
in-place check, then one output statement cross-joined with the final run's
gate row and the eligibility summary; the gate columns of the first batch
are decoded before any row leaves, and an empty output runs that aggregate
alone so a refusal on excluded candidates still fires. Composition for
staging and the bounded preview run the same schedule with the final run as
its own aggregate (the preview caps only its eligibility read), and a staged
output statement carries the summary alone. The first flagged check in
schedule order refuses with `failed_count` and its context before the
summary is read. Checks and export must use the **same snapshot**. Neither
builder opens a transaction nor executes a hidden probe. Normal Frame readers,
preview and materialization execute validation; directly executing raw
`Frame.expr` bypasses that boundary and does not establish validated output.
Refusals contain aggregate failed counts and identifiers only, never customer
rows, keys, or values. `terrine.temporal.float64_precision` identifies, in
`context["reason"]`, `nonfinite_input` for selected nonfinite values,
`out_of_range` for selected exact binary64 magnitudes at least $10^{29}$, or
`bound_overflow` for nonfinite returned sums/conversions/quotients. The literal
`1e29` is admitted because its exact value is just below $10^{29}$; the next
float up is refused. The separate cumulative/cohort guards can also refuse
finite values, as described above. Native or ambiguous backend arithmetic
failures are not `terrine.temporal.exact_overflow`.

## Exact timestamp windows

`aggregate_windows` consumes resolved metrics and normalized source mappings,
preserves original anchor instances, and returns one lazy multi-output
`TemporalResult`. Every output records its window, role, observation interval,
completeness and nullable reason. Raw event selection uses exact timestamp
comparisons over `[t0 + start, t0 + end)`: `[t0-1h,t0)` excludes t0, while
`[t0,t0+1h)` includes t0 and excludes its right endpoint. Daily input requires
both endpoints at the catalog's local midnight and consumes whole buckets only.

Backend timestamp helpers preserve supported native microsecond/nanosecond
ordering, signed pre-epoch fractions and mixed naive/aware comparisons without
narrowing nanosecond events or scaling wide microsecond bounds into overflow.
Naive timestamps denote UTC, independently of the session timezone.
Unsupported representations refuse
`terrine.temporal.timestamp_order_unsupported`; normalize to a losslessly
representable supported timestamp before binding. When executing an Ibis
expression directly, sort explicitly if row order matters.

Exact fallback comparisons stage and reuse a timestamp's rank and coordinate
fields. DuckDB may instead compare native shifted UTC microsecond timestamps
when a fixed/scheduled declaration proves that every origin and shifted bound
fits the supported host domain. The schedule's last actual origin supplies its
upper bound, not a sample used to prove calendar alignment. Naive/nanosecond
inputs and supplied, persisted or event-relative origins retain the exact
fallback; a declined optimization does not change the refusal contract.

Daily envelope masking can reuse guarded native keys or integer timestamp
parts. With conservative proof over every requested offset, a naive
microsecond source can compare against finite UTC-microsecond point bounds
normalized explicitly to naive UTC. Source clocks are never cast for that
optimization. Nanoseconds, unproven origins, unsupported policies and declined
host bounds retain exact integer ordering, including pre-epoch signed fractions
and wide finite endpoints independently of the caller's session timezone.

Attribute/as-of lookups and event-time selectors keep their own strict cutoff,
tie and missing-history rules; they are not cumulative numeric window lookups.
See [effective-time attributes](temporal-attributes.md) and
[event-time origins](#bounded-event-sequences-and-event-time-origins).
Min/max, distinct counts, quantiles and retention likewise keep their
[separate range algorithms](#bounded-repeated-windows).

Readiness intersects every metric source and the unit's optional observation
interval. Incomplete coverage takes precedence over incomplete observation,
then cell-level empty/all-null/zero-denominator meanings. Unavailable cells
retain numeric NULL and their exact reason. Complete empty sums/counts are zero
and conversion is false. A nonempty all-null window still counts events and
conversion is true, but sums/averages are NULL with `all_values_null`.
Unknown required filters in an available window refuse before output.

Float outputs carry `numeric_policy=None`, `arithmetic` set to `native` or
`exact`, and no error column; eligibility thresholds and state-panel bins
compare the published value. Conversion booleans have no numeric precision
policy.

Run the focused fast payload and window checks with
`uv run pytest tests/temporal/test_sparse_state.py tests/temporal/test_window_aggregates.py`.
Native quotient/calendar compositions whose compilation cost exceeds the unit
budget run in the integration tier with `uv run pytest integration/temporal`.

## Bounded event sequences and event-time origins

Start a bounded ordered dataset with `recipes.event_sequence`. Required
arguments are `event`, `horizon`, and positive `max_events`. It selects
the first event as anchor by default and includes matching events in
`[anchor, anchor + horizon)`. `occurrence` accepts `"first"`, `"each"`, or
`"nth"`; only `"nth"` takes a positive one-based `n`. Supported options are
`session_gap=None`, `features`, `outcomes`, `eligibility`, `anchor_range`, and
`gap="0d"`. It accepts no treatment or administrative cutoff.

```python
from terrine import recipes

sequence = recipes.event_sequence(
    event="activity", horizon="2h", max_events=4, session_gap="1h",
)
```

### Under the hood

```python
from terrine import Design, EventAnchor
from terrine.spec import EventSequence, SessionSpec
from terrine.windows import Window

equivalent = Design(
    None, EventAnchor("activity"),
    shape=EventSequence(
        "activity", Window(0, "2h"), max_events=4, sessions=SessionSpec("1h"),
    ),
)
assert sequence == equivalent
```

Elapsed duration inputs are strings with a unit or `Duration` values; only bare
`0` is accepted without one. Saved metadata keeps normalized integer seconds,
and design documents write canonical duration text such as `1h`.

Use the low-level shape when the selected events differ from the anchor event,
or when specifying properties, funnels, or the array representation.


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

`EventTime(event, window)` selects the first matching event occurrence in its
strict half-open window; `occurrence="last"` selects the last matching event,
and `occurrence="nth"` selects the corresponding one-based occurrence. Both
endpoints follow `(timestamp, configured event ID)` order when IDs are present.
Without configured IDs, tied timestamps are accepted because the selected value
is the same for every tie order. First/nth event anchors and event sequences
still refuse tied timestamps without configured IDs because their occurrence
order affects the result. Its optional paired `label_property` and `equals`
arguments refine selection. A complete window with no matching event returns
null with `complete=True`; do not fill this with zero, which would imply an
event occurred at the origin. An expression can use the selected timestamp as
its origin: `agg("metric", window=("0s", "1h"), relative_to=selector)` or
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

### Conditional events are an expression pattern

There is no conditional-event constructor. Use the low-level `EventAnchor` and
an `event_time` selector, then make an expression relative to that selected
timestamp. For example, a signup-anchored row can measure activity after the
first purchase-labeled event:

```python
from terrine import Design, EventAnchor, agg, event_time

purchase = event_time(
    "activity", window=("0s", "2h"), label_property="kind", equals="purchase",
)
conditional = Design(
    "user", EventAnchor("signup"),
    outcomes={"after_purchase": agg(
        "orders_count", window=("0s", "1h"), relative_to=purchase,
    )},
)
```

Here the Catalog must declare `activity`, `signup`, `kind`, and `orders_count`.
The selector refines event selection; it does not introduce another dataset
shape. A missing or incomplete origin keeps its null and reason.

Materialized provenance records the sequence schema and each expression's
selector references, conservative support intervals, and resolved time-column
references. `MaterializedFrame.to_pyarrow()` writes these typed values into
Arrow schema metadata, and `MaterializedFrame.from_arrow()` restores them with
the original nested event values.

Run `uv run python examples/event_sequences.py` for a local DuckDB example of
stable timestamp ties, an empty anchor interval, a known-null event property,
sessions, funnels, and both output representations.
