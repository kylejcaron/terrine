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
