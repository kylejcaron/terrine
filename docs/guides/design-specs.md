# Data-only design specifications

A `Design` is an immutable description of a study. Construct expression trees and anchors without loading a model, opening a database connection, or importing Ibis:

```python
from terrine.spec import (
    AnchorRange, Design, EventAnchor, agg, asof, eq, gt, is_null,
    all_of, from_yaml, to_yaml,
)

spec = Design(
    unit="user",
    anchor=EventAnchor("feature_enabled", which="first"),
    anchor_range=AnchorRange(start="2025-03-01", end="2025-08-01"),
    features={"prior_sessions": agg("sessions", window=("-28d", "0d")),
              "plan": asof("plan_tier", before="0d")},
    outcomes={"later_sessions": agg("sessions", window=("0d", "28d"))},
    eligibility={"active": gt(agg("sessions", window=("-56d", "-28d")), 0),
                 "known_plan": is_null(asof("plan_tier", before="0d"), negate=True)},
)
text = to_yaml(spec)
loaded = from_yaml(text)
assert loaded == spec
print(spec.spec_hash)
```

`FixedDate`, `EventAnchor`, and `Schedule` are the supported anchor variants. Schedules use a positive elapsed `every` duration and an exclusive end date. `AnchorRange` is half-open, needs at least one bound, and accepts calendar dates or aware timestamps; timestamps are normalized to UTC, while dates remain tagged dates. A naive timestamp is refused. A date is a calendar-day bound, not a midnight instant: its eventual interpretation uses the model Catalog's declared day boundary.

Mixed date/timestamp bounds are ordered only after catalog binding resolves the calendar-day boundary.

`agg(metric, window=(start, end))` stores a half-open interval as signed integral seconds using the shared `s`/`m`/`h`/`d`/`w` duration parser. Thus equivalent elapsed forms have identical specs and hashes, e.g. `60m` and `1h`. `asof(attribute, before=...)` stores an exact signed-second strict cutoff, including zero. Predicate constructors `gt`, `eq`, `is_null`, and `all_of` form a separate typed predicate tree. Comparands are finite JSON scalars only; Python functions and other runtime objects are never serialized.

`to_yaml` and `to_json` return portable text. `from_yaml` and `from_json` accept text or bytes and only construct the known data types. They reject duplicate keys, unknown schema versions, unknown node tags, extra fields, unsafe YAML tags, and invalid node data. Canonical JSON recursively sorts mapping keys, uses compact UTF-8 JSON, and is the basis of `Design.spec_hash`; map insertion order does not affect a design hash.

Treatment is explicit and descriptive: `Treatment(attribute, at="0d", known_at_t0=False)` records a treatment attribute, its signed elapsed cutoff, and whether the design declares it known at time zero. An event anchor does not imply treatment assignment. Design construction and serialization are data-only; model validation, compilation, and frame execution belong to later APIs.
