# Sequential trials and event panels

Start with `terrine.recipes` for scheduled trial panels or event-relative panels.
Both return plain Designs, built through `design.build(model)` and
`frame.materialize(budget)`. Run `uv run python examples/sequential_trials.py`
for both constructors with calendar-date and relative-period assertions.

## Scheduled trial panels

`recipes.trial_panel` requires `unit`, `every`, `start`, `end`, and a
`treatment` history declaration (`Initiation` or `PersistentAdoption`). Its rows
are eligible units at scheduled trial time zero; schedule dates, not a redundant
period count, define the trials. Dates are calendar dates or ISO date strings,
never timestamps, and the `end` day is included. For example, the following
schedule has Jan 1, Jan 15, and Jan 29 anchors.

Supported options are `trial_weight=None`, `anticipation="0d"`,
`reference_period="-1d"`, `features`, `outcomes`, `eligibility`, `anchor_range`,
and `gap="0d"`. Other optional inputs default to `None`. A trial panel does not
accept `periods`, `period`, or an administrative cutoff.

```python
from datetime import UTC, datetime
from terrine import recipes
from terrine.spec import Initiation, from_json, to_json

treatment_history = Initiation("started", history_start=datetime(2024, 1, 1, tzinfo=UTC))
trials = recipes.trial_panel(
    "user", every="14d", start="2025-01-01", end="2025-01-29",
    treatment=treatment_history,
)
assert from_json(to_json(trials)) == trials
```

### Under the hood

```python
from terrine.spec import Design, Schedule, Sequential

base = Design("user", Schedule("14d", "2025-01-01", "2025-01-29"))
equivalent_trials = Sequential(base, treatment=treatment_history)
assert trials == equivalent_trials
```

`Sequential` is a public transform that lowers a base Design to the canonical
sequential shape. It does not infer exposure from an event anchor.
`CalendarPanel` accepts that canonical sequential design unchanged.
`Initiation` names the event or label whose first observation starts treatment,
the earliest complete history needed for classification, and the information
policy (`recorded_at` or `retrospective_effective_time`). The owning source is
the one that declares that name; a name declared as an event on one source and
a label on another refuses with `terrine.design.invalid`
(`reason="ambiguous_reference"`). `PersistentAdoption` is a separate declaration
and requires explicit absorbing semantics; it is not inferred from initiation.
A source maximum timestamp does not establish history completeness.

## Event panels

`recipes.event_panel` requires `unit`, `event`, an explicit tuple of relative
`periods`, and elapsed `period` width. A row is one selected parent event at one
declared relative period. `occurrence` defaults to `"first"` and accepts
`"each"` or `"nth"`; only `"nth"` takes a positive one-based `n`. Supported
options are `reference_period=-1`, `features`, `outcomes`, `eligibility`,
`anchor_range`, and `gap="0d"`. It accepts no treatment or administrative cutoff.

```python
from terrine import agg, recipes

outcomes = {"next_week": agg("orders", window=("1d", "8d"))}
event_rows = recipes.event_panel(
    "user", event="checkout", periods=(-2, -1, 0, 1), period="1d",
    outcomes=outcomes,
)
```

### Under the hood

```python
from terrine.spec import Design, EventAnchor, EventPanel

equivalent_events = EventPanel(
    Design("user", EventAnchor("checkout"), outcomes=outcomes),
    periods=(-2, -1, 0, 1), period_seconds=86_400, reference_period=-1,
)
assert event_rows == equivalent_events
```

`EventPanel` remains a public transform. The row evaluation origin is
`parent_t0 + period * period_seconds`; its typed `anchor_id` contains both the
parent event identity and relative period. Expression windows retain their
declared widths and are evaluated relative to each row origin, so an outcome
window does not have to equal one event period. Negative event periods are valid
without making expression-relative window starts negative.

Both shapes survive design serialization, Frame recompilation, and Arrow
metadata round trips. Event time zero is descriptive identity only; it does not
assert treatment or exposure.

## Treatment history and matched t0

Sequential treatment is classified at each schedule's exact `t0` from the declared source history. The source must declare complete coverage from `history_start` through the final trial boundary; a latest observed event timestamp is not a completeness guarantee. `Initiation` means that the named event or label has been observed by t0. `PersistentAdoption` is a distinct state selector with an explicit adopted value and absorbing-history declaration.

The sequential `anticipation` interval extends the control no-initiation risk
horizon and the corresponding treatment-source and roster-history coverage
checks beyond the outcome window. Readiness validation and runtime risk checks
use the same extended endpoint. With no outcome aggregate, the declared
schedule-end horizon remains the base and anticipation extends it rather than
replacing it. Its elapsed seconds are preserved in typed Arrow provenance so
downstream users can inspect the applied policy.

A matched-t0 policy belongs on the canonical `Design`, not on a transient compiler option:

```python
from terrine.spec import MatchedT0Sample, matched_t0

sampled = matched_t0(
    trials,
    model,
    sample=MatchedT0Sample(strata=("region",), controls_per_treated=2, seed=17),
)
frame = sampled.materialize()
```

For each t0 and declared stratum, all treated units are retained and eligible controls are sampled without replacement. The compiler refuses a stratum without enough eligible controls. Output roles identify the trial and inclusion-probability columns; the exact policy and probability survive materialization and Arrow metadata round trips. Inclusion probabilities are 1 for treated units and `n_sampled_controls / n_eligible_controls` for sampled controls. Recompilation uses the serialized sample policy and deterministic identity-based ranking, so input row order does not alter selected units. A sampled population must not be analyzed as an unweighted full risk set.

`TrialWeight` is separate from sampling probability and unit-level target weights. Explicit trial aggregation weights are normalized once and retained in panel provenance; omitting a target-weight column means unit weights, not an inferred sampling correction.

