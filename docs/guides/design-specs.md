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

`FixedDate("2025-03-01T12:34:56.123456Z")` preserves an intraday instant rather than reducing it to a calendar day. Portable instant definitions use exact microsecond precision: equivalent offsets and trailing-zero fractional precision canonicalize identically; nonzero submicroseconds are refused rather than truncated, including timestamp subclasses supplied by dataframe libraries. `Schedule` remains calendar-date-only.

`EventAnchor(fact, which="first", n=None, offset=0)` can shift the selected
event by a non-negative elapsed duration. For example,
`EventAnchor("purchase", offset="1d")` starts analysis one elapsed day after
purchase. First/nth selection and `anchor_range` still operate on original
event times. Nonzero offsets are serialized as integral seconds and change
anchor identity, provenance and `spec_hash`; zero is omitted, preserving
existing saved Designs and hashes. See the low-level
[landmark survival example](trajectories-survival.md#landmark-survival).

Mixed date/timestamp bounds are ordered only after catalog binding resolves the calendar-day boundary.

`agg(metric, window=(start, end))` stores a half-open interval as signed integral seconds using the shared `s`/`m`/`h`/`d`/`w` duration parser. Thus equivalent elapsed forms have identical specs and hashes, e.g. `60m` and `1h`. `asof(attribute, before=...)` stores an exact signed-second strict cutoff, including zero. Predicate constructors `gt`, `eq`, `is_null`, and `all_of` form typed true/false rules accepted in `features`, `outcomes`, and `eligibility`. Comparands are finite JSON scalars only; Python functions and other runtime objects are never serialized.

`to_yaml` and `to_json` return portable text. `from_yaml` and `from_json` accept text or bytes and only construct the known data types. They reject duplicate keys, unknown schema versions, unknown node tags, extra fields, unsafe YAML tags, and invalid node data. Canonical JSON recursively sorts mapping keys, uses compact UTF-8 JSON, and is the basis of `Design.spec_hash`; map insertion order does not affect a design hash.

Treatment is explicit and descriptive: `Treatment(attribute, at="0d", known_at_t0=False)` records a treatment attribute, its signed elapsed cutoff, and whether the design declares it known at time zero. An event anchor does not imply treatment assignment. Design construction and serialization are data-only; model validation, compilation, and frame execution belong to later APIs.

## Boolean features and outcomes

Use predicates directly for labels such as conversion and churn, without losing
the distinction between fully observed inactivity and unavailable follow-up:

```python
import terrine as tr

spec = tr.Design(
    "user",
    tr.FixedDate("2025-03-01"),
    features={
        "no_prior_order": tr.is_null(
            tr.event_time("order", window=("-28d", "0s"))
        ),
    },
    outcomes={
        "converted_7d": tr.gt(tr.agg("orders", window=("0s", "7d")), 0),
        "churned_30d": tr.eq(tr.agg("orders", window=("0s", "30d")), 0),
        "converted_then_inactive": tr.all_of(
            tr.gt(tr.agg("orders", window=("0s", "7d")), 0),
            tr.eq(tr.agg("orders", window=("7d", "30d")), 0),
        ),
    },
)
```

A leaf rule over an incomplete window is **null with its original reason**,
even if an event observed so far already meets the condition. It is never an
early `True`. A complete empty count is zero: `converted_7d` is `False` and
`churned_30d` is `True`. `is_null` over a complete event-time search with no
matching event is `True`; an incomplete search remains null.

`all_of` uses three-valued AND, exactly like eligibility: a complete `False`
term makes the conjunction `False` and complete, even alongside an unknown
term; `True` together with an incomplete term remains null with the incomplete
term's reason. Boolean columns retain feature/outcome roles, observation
bounds, completeness and reason metadata through Arrow and saved datasets.

Each operand obeys its role's usual temporal restrictions. A predicate's
temporal support spans the earliest operand start through the latest operand
end, including event-selector dependencies. Sequential follow-up and training
time splits therefore use the full declared support, not the first observed
event or a short-circuiting term. An unresolved event origin retains its
conservative declared support even when another conjunct already determines
`False`. Boolean outcomes can be selected directly with
`frame.training_set(label="converted_7d")`.

For plain as well as boolean outcomes, sequential follow-up includes every
event-selector search window, recursively through `relative_to`. A measurement
window ending before its selector does not shorten follow-up: the full search
must still be observed, including sustained-control initiation checks.

Approximate-quantile sidecars cannot be predicate operands in any role. These
requests refuse during validation and persisted-runtime admission with
`terrine.metric.approx_eligibility_unavailable`; use exact `quantile` or output
the approximate value without wrapping it in a predicate.
