# Trajectories and time-to-event datasets

A `Trajectory` builds period and cumulative metric datasets; a `CohortTable`
aggregates values by cohort age. Both preserve observation completeness, null
reasons, and cohort denominators. See the dataset constructors and examples in
[`synthetic-worlds.md`](synthetic-worlds.md) for raw-source workflows.

`TimeToEvent` builds one per-unit record containing event time, duration, entry
time, censor time, and completeness/reason metadata. These are observed dataset
rows: Terrine does not fit a curve or infer a survival probability. Use the
records with a downstream package such as lifelines when a model or curve is
required.

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
