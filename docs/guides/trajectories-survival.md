# Trajectories and time-to-event datasets

A `Trajectory` builds period and cumulative metric datasets; a `CohortTable`
aggregates values by cohort age. Both preserve observation completeness, null
reasons, and cohort denominators. See the dataset constructors and examples in
[`synthetic-worlds.md`](synthetic-worlds.md) for raw-source workflows.
Exact decimal cumulative values retain their declared precision and scale. If
all contributing periods are present but the cumulative total cannot fit that
exact output type, materialization refuses with
`terrine.temporal.exact_overflow` rather than returning a null value. When an
earlier period is unavailable, its null and reason remain visible in cumulative
outputs instead of being reported as arithmetic overflow.

`TimeToEvent` builds one per-unit record containing elapsed `duration`, the
terminal `event_time` or `censor_time`, entry time, and completeness/reason
metadata. `duration` is an Arrow duration measured from `t0`; delayed entry does
not change its origin. These are observed dataset rows: Terrine does not fit a
curve or infer a survival probability. Use the records with a downstream
package such as lifelines when a model or curve is required.

Nanosecond axes retain nanoseconds in the Arrow duration. DuckDB's lazy
published-generation relation cannot express nanosecond intervals and refuses
that read with `terrine.runtime.lazy_duration_unsupported`; use
`open_published_frame` or `read_partition` to keep the exact Arrow values.

When a design declares a treatment, each retained survival row includes the
`treatment` value and its support metadata. The `treatment` role refers to that
output column; event and censoring rules are unchanged.

The `LifeTable` design shape bins those records into a descriptive risk-set
summary. Each group/time bin reports number at risk, events, delayed entries,
and censor counts by reason, along with plain rates. It does not multiply
interval survival probabilities, assume independent censoring, or attach
intervals; these are dataset summaries only.

```python
from terrine.spec import Design, EventAnchor, LifeTable, TimeToEvent

life = Design(
    "user",
    EventAnchor("signup", which="first"),
    shape=LifeTable(TimeToEvent(event="churn"), interval="7d"),
).build(model).materialize()
```

Bins are half-open elapsed-time intervals from each row's analysis origin.
Delayed entry contributes to the risk set starting at its actual entry bin;
events and censoring are recorded in their terminal-time bin. Censor reasons
remain explicit counts in `censored_by_reason`; no incomplete record is
zero-filled or reclassified as an event.

Arrow dataset export is available independently of any survival calculation:
`to_surrogate_arrow(frame, features=..., outcomes=...)` returns an
`ArrowSurrogate` with selected values, identity, null-reason support, and
portable provenance metadata. It does not imply any survival-times-value
product.
