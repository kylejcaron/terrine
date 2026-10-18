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
