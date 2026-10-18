# Sequential trials and event panels

Sequential trials use a `Schedule` as repeated trial time zero. `Sequential` lowers a base `Design` to the canonical sequential shape and requires a treatment-history declaration; it does not infer exposure from an event anchor. `CalendarPanel` accepts that canonical sequential design unchanged.

```python
from datetime import UTC, datetime

from terrine.spec import Design, Initiation, Schedule, Sequential, to_json, from_json

base = Design("user", Schedule("30d", "2025-01-01", "2025-07-01"))
treatment_history = Initiation(
    "assignments", "started", history_start=datetime(2024, 1, 1, tzinfo=UTC)
)
trials = Sequential(base, treatment=treatment_history)
assert from_json(to_json(trials)) == trials
```

`Initiation` describes treatment onset, its declared source/property, the earliest complete history needed for classification, and the information policy (`recorded_at` or `retrospective_effective_time`). `PersistentAdoption` is a separate declaration and requires explicit absorbing semantics; it is not inferred from initiation. A source maximum timestamp does not establish history completeness.

An `EventPanel` expands each selected parent event to exactly the declared integer-relative periods. Its row evaluation origin is `parent_t0 + period * period_seconds`; its typed `anchor_id` contains both the parent event identity and relative period. Expression windows retain their declared widths and are evaluated relative to each row origin, so an outcome window does not have to equal one event period. Negative event periods are valid without making expression-relative window starts negative.

```python
from terrine.spec import Design, EventAnchor, EventPanel, agg

event_rows = EventPanel(
    Design(
        "user",
        EventAnchor("checkout", "first"),
        outcomes={"next_week": agg("orders", window=("1d", "8d"))},
    ),
    periods=(-2, -1, 0, 1),
    period_seconds=86_400,
    reference_period=-1,
)
```

Build and materialize either design through the ordinary `Design.build(model)` and `Frame.materialize(budget)` workflow. The shape definition survives design serialization, Frame recompilation, and Arrow metadata round trips. Event time zero is descriptive identity only; it does not assert treatment or exposure.
