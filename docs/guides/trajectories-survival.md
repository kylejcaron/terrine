# Trajectories and time-to-event datasets

Start with `terrine.recipes` for cumulative LTV, survival records, and grouped
life tables. These return ordinary Designs; build and materialize them with an
explicit output budget. The low-level shapes remain available for other cohort
and trajectory choices. See [synthetic worlds](synthetic-worlds.md) for raw-source
workflows and run `uv run python examples/trajectories_survival.py` for all three
constructors with assertions on values and materialized roles.

## Cumulative LTV

`recipes.ltv` returns one row per unit/cohort origin and age. Required arguments
are `unit`, `start_event`, `metric`, `period`, and positive `periods`. `metric`
must be an untransformed native metric backed by exactly one SUM measure.
With `Model.from_tables`, that is a `tr.sum(column)` metric declared on the
`Events`; counts, means and ratios are not accepted by this shape.
Other aggregations, multi-measure metrics, and transforms refuse with
`terrine.design.invalid` (reason `trajectory_metric_must_be_additive`).
The first start event is the origin; each age represents a period of elapsed
time from it. For ages `a = 0, ..., periods - 1` and positive period width `p`,
let `m_j` be the metric value in `[t0 + j*p, t0 + (j+1)*p)`. The cumulative
value is `m_0 + ... + m_a`, the total in `[t0, t0 + (a+1)*p)`.
**Age 0 includes the origin event**, rather than starting
after it. An event declared by
[`Model.from_tables`](temporal-engine.md#binding-event-tables) can be the origin.

Supported options are `features`, `eligibility`, `treatment`, `anchor_range`,
`gap="0d"`, and `administrative_cutoff=None`. Other optional inputs default to
`None`. Arbitrary `outcomes` are not accepted: the trajectory defines the value
columns. The absolute cutoff bounds observation without changing source
readiness.

```python
from datetime import UTC, datetime
from terrine import recipes

cutoff = datetime(2025, 1, 22, tzinfo=UTC)
ltv = recipes.ltv(
    "user", start_event="purchase", metric="net_revenue", period="7d", periods=3,
    administrative_cutoff=cutoff,
)
```

### Under the hood

```python
from terrine import CohortSpec, Design, EventAnchor, Trajectory

equivalent_ltv = Design(
    "user", EventAnchor("purchase"),
    shape=CohortSpec("purchase", Trajectory("net_revenue", "7d", 3, cumulative=True)),
    administrative_cutoff=cutoff,
)
assert ltv == equivalent_ltv
```

A `Trajectory` also supports period-only values through the low-level API. A
`CohortTable`, returned by `frame.cohorts(grouping=..., value_column=...)`,
aggregates values by cohort age. Both preserve observation completeness, null
reasons, and cohort denominators. Exact decimal cumulative values retain their
declared precision and scale. If all contributing periods are present but the
cumulative total cannot fit that exact output type, materialization refuses with
`terrine.temporal.exact_overflow` rather than returning a null value. When an
earlier period is unavailable, its null and reason remain visible in cumulative
outputs instead of being reported as arithmetic overflow.
The value/support triples are `period_value, period_value__complete,
period_value__reason` and `cumulative_value, cumulative_value__complete,
cumulative_value__reason`. Cohort summaries use `value__complete` and
`value__reason`, with the same adjacency rule.


## Survival records

`recipes.survival` requires `unit`, `start_event`, and `end_event`. It returns one
per-unit start record with elapsed `duration`, terminal `event_time` or
`censor_time`, entry time, and completeness/reason metadata. Start occurrence
defaults to `"first"`; use `occurrence="nth", n=2` for the second start. `n` must
be positive and is omitted for `"first"`; `"each"` is not supported.

Supported options are `features`, `eligibility`, `treatment`, `anchor_range`,
`gap="0d"`, and `administrative_cutoff=None`. There is **no relative duration
horizon**: observation coverage or the optional absolute cutoff ends follow-up.
No arbitrary `outcomes` are accepted. `duration` is an Arrow duration measured
from `t0`; delayed entry does not change its origin. These are observed dataset
rows: Terrine does not fit a curve or infer a survival probability. Use a
downstream package such as lifelines when a model or curve is required.

```python
survival = recipes.survival(
    "user", start_event="purchase", end_event="churn",
    administrative_cutoff=cutoff,
)
```

### Under the hood

```python
from terrine import TimeToEvent

equivalent_survival = Design(
    "user", EventAnchor("purchase"), shape=TimeToEvent("churn"),
    administrative_cutoff=cutoff,
)
assert survival == equivalent_survival
```

Per-unit survival columns use `duration__complete`/`duration__reason`,
`event__complete`/`event__reason`, and `event_time__complete`/`event_time__reason`.
`censor_reason` remains a distinct value describing censoring, not a shared
completeness field.

Nanosecond axes retain nanoseconds in the Arrow duration. DuckDB's lazy
published-generation relation cannot express nanosecond intervals and refuses
that read with `terrine.runtime.lazy_duration_unsupported`; use
`open_published_frame` or `read_partition` to keep the exact Arrow values.

When a design declares a treatment, each retained survival row includes the
`treatment` value and its support metadata. The `treatment` role refers to that
output column; event and censoring rules are unchanged.

### Landmark survival

To begin follow-up after a baseline period, use an offset event anchor with the
low-level Design. The recipes keep their original start-event behavior.

```python
from terrine import Design, EventAnchor, TimeToEvent, agg

landmark_survival = Design(
    "user",
    EventAnchor("purchase", which="first", offset="1d"),
    features={"day_one_sessions": agg("sessions", window=("-1d", "0s"))},
    shape=TimeToEvent("churn"),
    administrative_cutoff=cutoff,
)
```

Here `t0 = purchase + 1d`, and the half-open feature window covers
`[purchase, purchase + 1d)`, excluding the landmark instant. `duration` is
measured from the landmark, not from purchase. A churn event strictly before
the landmark excludes that unit as prevalent; an event exactly at the
landmark remains eligible. This conditions the dataset on being at risk at
the landmark; it is not survival from purchase. Prevalent exclusions are not
currently reported as a separate count.

Offsets are non-negative elapsed durations, not calendar-day shifts.
`anchor_range` filters original purchase instants after first/nth selection,
before the offset is applied. Nonzero offsets participate in saved Design
hashes, anchor identity and provenance; zero-offset serialization is unchanged.
Use ordinary Frame materialization or a full rebuild: persisted-layer
substitution for shifted anchors refuses before source access with
`terrine.compile.runtime_shape_unsupported`.


## Grouped life tables

`recipes.life_table` requires `unit`, `start_event`, `end_event`, and `bin`. A
row is a descriptive risk-set summary for one declared feature group and
elapsed-time bin. Start selection has the same `occurrence="first"` / `"nth"`
and positive `n` rules as survival. Supported options are `features`,
`group_by=()`, `eligibility`, `anchor_range`, `gap="0d"`, and
`administrative_cutoff=None`. Follow-up uses coverage or the absolute cutoff,
not a relative duration horizon. Neither `treatment` nor `outcomes` is accepted.

Every `group_by` name must be a declared feature, and the feature names must
exactly match `group_by`. Extra features raise `ValueError` before building;
they never silently disappear. With no groups, omit features too.

Grouping features may be expressions or Boolean predicates. Their timing support
is the conservative interval from the earliest contributing start to the latest
contributing end across the group, not the elapsed-time bin identity. These
absolute bounds are retained when selecting grouping features for surrogate Arrow
export.

```python
from terrine import asof

features = {"region": asof("region", before="-1d")}
life = recipes.life_table(
    "user", start_event="purchase", end_event="churn", bin="7d",
    features=features, group_by=("region",), administrative_cutoff=cutoff,
)
```

### Under the hood

```python
from terrine import LifeTable

equivalent_life = Design(
    "user", EventAnchor("purchase"),
    shape=LifeTable(TimeToEvent("churn"), "7d", group_by=("region",)),
    features=features, administrative_cutoff=cutoff,
)
assert life == equivalent_life
```

Materialized grouping columns remain visible: this example has
`roles.features == ("region", "time_bin")`. The outcome roles are
`("at_risk", "events", "censored", "delayed_entries", "event_rate", "censor_rate")`,
and `roles.treatment is None`. Life tables summarize counts and plain rates;
they do not multiply interval survival probabilities, assume independent
censoring, or attach intervals.

Bins are half-open elapsed-time intervals from each row's analysis origin.
Delayed entry contributes to the risk set starting at its actual entry bin;
events and censoring are recorded in their terminal-time bin. Censor reasons
remain explicit counts in `censored_by_reason`; no incomplete record is
zero-filled or reclassified as an event.
Every risk-set value has its own adjacent support columns, such as
`events__complete` and `events__reason`. The latter is `list<string>` and retains
all distinct exact incompleteness reasons for the trajectory, not just one.


Arrow dataset export is available independently of any survival calculation:
`to_surrogate_arrow(frame, features=..., outcomes=...)` returns an
`ArrowSurrogate` with selected values, identity, null-reason support, and
portable provenance metadata. It does not imply any survival-times-value
product.
