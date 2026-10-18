# Trajectories and survival analysis

## Exact first eligible occurrence

The temporal `first_occurrence` primitive selects the earliest event in each
candidate's exact half-open `[lower, upper)` interval. It preserves every
candidate, including candidates with no eligible event, and returns a nullable
`first_timestamp`, an optional stable `first_event_id`, and a `no_event` flag.

The lookup uses a correlated ordered point lookup instead of materializing an
anchor-by-event join. When an event ID is supplied, its non-null uniqueness is
a deferred check on the returned `TemporalResult`; callers must run its checks
inside the same pinned execution snapshot before consuming the result. Equal
timestamps are ordered by that ID. Timestamp comparisons preserve supported
microsecond and nanosecond values and do not floor event times.

## LTV trajectories and cohort summaries

A `CohortSpec` pairs a first-occurrence event anchor with one additive metric and
a finite number of disjoint, half-open age windows. `Trajectory.cumulative=True`
adds a running value partitioned by unit, selected anchor identity, and cohort
time. A complete observed zero remains zero. A missing or incomplete period
nulls the cumulative value from that age onward and retains the first relevant
null reason; it is never filled with zero.

`Design.administrative_cutoff` is an absolute UTC instant independent of source
readiness. It can make a period incomplete without changing readiness metadata.
The cutoff is included in the design identity and materialized provenance.

`Frame.cohorts(...)` creates a recipe over the same LTV relation. Collection
compiles the grouping into the parent's verified execution snapshot rather than
running a separate source query. Cohorts are bucketed by fixed elapsed
durations from an explicit aware origin. `at_risk` is the unique first-entry
cohort size and stays fixed across ages; `observable` counts entrants with known
full-age measurement. Both denominators are null only for an age wholly beyond
the administrative cutoff. A partially immature cohort retains its risk set,
while its incomplete aggregate value is null. Group values support `mean` and
`total`, preserve observed zero, and remain null when any entrant's selected
value is unavailable.

Collection budgets abort the operation rather than truncating output. Cohort
Arrow metadata records the parent design hash and canonical grouping operation.

This LTV summary does not establish an identity between survival and value.

## Time-to-event and Kaplan–Meier

`TimeToEvent(event, exit=..., entry=..., delayed_entry=...)` builds one
analysis history per unit. Event occurrences are selected with the same exact
first-eligible point lookup used by temporal aggregation. The eligible interval
starts at the latest supported lower bound and ends at the earliest of event
source coverage, unit observation end, an optional exit, and the independent
administrative cutoff. These intervals are half-open: an event at `t0` is an
event at elapsed time zero; an event exactly at an exit or cutoff is censored.
Churn remains an observed event unless an explicit exit/censor rule is supplied.

An event already present before the supported start (including before declared
delayed entry) is prevalent and excluded rather than replaced by a later event.
Without delayed entry, a source-coverage or unit-observation gap after `t0`
produces `complete=False` with reason `incomplete_initial_history`; the
Kaplan–Meier estimator excludes those rows. Declaring delayed entry left
truncates at the exact entry timestamp, subject to source and unit bounds.

`terrine.estimate.survival.kaplan_meier` consumes a `MaterializedFrame` and
works from its Arrow values only. It accepts normalized `{seconds,
nanoseconds}` elapsed structs or absolute UTC timestamp columns; timestamp
durations and entries are converted relative to each row's `t0` using integer
Arrow timestamp counts, preserving nanoseconds. Tied failures use the risk set
immediately before that instant and are applied before tied censors leave the
subsequent risk set. Greenwood variance is reported alongside the
product-limit curve; it is unavailable when survival reaches zero.

Changing `Design.administrative_cutoff` changes follow-up and event selection
without changing declared readiness. No survival-times-value identity is
implied.

## Earlier-only backtests and surrogate Arrow

`backtest_survival(...)` accepts absolute aware UTC cutoffs in strictly
increasing order. For a cutoff `c`, it retains only units whose entry instant is
strictly before `c`, then censors every retained row at the earlier of its
observed end and `c`. An event exactly at the cutoff is censored; an event after
the cutoff is hidden from the fitter. Entry, event, censor and cutoff are
compared on the exact timestamp axis relative to each row's own `t0`, so
different anchor times do not share an incorrect global elapsed duration.

This v1 backtest is a measurement-time selection over retrospective
effective-time measurements. It labels fitted data
`retrospective_effective_time`; it does not establish what a warehouse had
recorded at an earlier date. In particular, a value effective before a
prediction cutoff but corrected later can appear in a historical refit. Such a
late correction is not silently treated as historically available:
`knowledge="recorded_at"` refuses because effective-time metadata and
`FeatureCutoff.knowledge` do not prove recorded-at history. Stronger historical
availability claims require explicit recorded-at source evidence, which this
workflow does not implement.

`to_surrogate_arrow(frame, features=..., outcomes=...)` returns an
`ArrowSurrogate` with a typed Arrow projection and a versioned schema metadata
envelope. The export retains unit identity, row `t0`, requested feature and
outcome columns, survival roles, and referenced completeness/reason,
observation-bound and numeric-certificate support columns. Its metadata carries
design/model hashes, source generations, execution snapshot, cutoff declarations
and cohort-operation provenance when present. `ArrowSurrogate.to_arrow()` and
`ArrowSurrogate.from_arrow()` round-trip the versioned metadata with the Arrow
schema. Decimal/null/time types stay in Arrow; metadata contains no row
payloads. There is no runtime import of increment and no survival-times-value
product is implied.
