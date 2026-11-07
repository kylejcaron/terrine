# Data-only design specifications

A `Design` is an immutable description of a study. Construct expression trees and anchors without loading a model, opening a database connection, or importing Ibis:

```python
from terrine.spec import (
    AnchorRange, Design, EventAnchor, agg, asof, eq, gt, is_null,
    all_of, from_yaml, to_yaml,
)

spec = Design(
    unit="user",
    anchor=EventAnchor("feature_enabled", occurrence="first"),
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

`FixedDate`, `EventAnchor`, and `Schedule` are the supported anchor variants. Schedules use a positive `every` step and calendar `start` and `end` dates; the `end` day is included, `start == end` is one anchor, and `start > end` refuses with `terrine.design.invalid` (`reason="schedule_end_before_start"`). The step is an elapsed duration (`"7d"`, `"4w"`, a signed integer with a unit, or a `Duration`; stored in `every` as a `Duration`) or a calendar step (`"1mo"`, `"3mo"`, `"1y"` = twelve months, or `Months(3)`; stored in `every` as `Months`). A calendar anchor is `start` advanced by `k` whole months and clamped to that month's last day, never chained from the previous anchor: `"1mo"` from `2025-01-31` is Jan 31, Feb 28, Mar 31, Apr 30. Calendar units are a `Schedule` feature only; every other elapsed duration must include a unit, except bare `0`.

`FixedDate("2025-03-01T12:34:56.123456Z")` preserves an intraday instant rather than reducing it to a calendar day. Portable instant definitions use exact microsecond precision: equivalent offsets and trailing-zero fractional precision canonicalize identically; nonzero submicroseconds are refused rather than truncated, including timestamp subclasses supplied by dataframe libraries. `Schedule` remains calendar-date-only.

`EventAnchor(event, occurrence="first", n=None, offset="0s")` can shift the selected
event by a non-negative elapsed duration. For example,
`EventAnchor("purchase", offset="1d")` starts analysis one elapsed day after
purchase. First/nth selection and `anchor_range` still operate on original
event times. Nonzero offsets are serialized as canonical duration text and change
anchor identity, provenance and `spec_hash`; zero is omitted, so equivalent
zero-offset Designs have the same hash. See the low-level
[landmark survival example](trajectories-survival.md#landmark-survival).

Mixed date/timestamp bounds are ordered only after catalog binding resolves the calendar-day boundary.

`agg(metric, window=(start, end))` stores a half-open interval of exact signed `Duration` endpoints using units `s`, `min`, `h`, `d` or `w`; minutes are spelled `min`, while `m` is refused with a hint to use `min` or `mo`. Thus equivalent elapsed forms have identical specs and hashes, e.g. `60min` and `1h`. `asof(attribute, before=...)` stores an exact signed `Duration` strict cutoff, including zero. Predicate constructors `gt`, `eq`, `is_null`, and `all_of` form typed true/false rules accepted in `features`, `outcomes`, and `eligibility`. Comparands are finite JSON scalars only; Python functions and other runtime objects are never serialized.

`to_yaml` and `to_json` return portable text with `schema_version: 4`, writing every duration in canonical units (`gap: 1w`, `every: 1mo`, `period: 1h30min`), so `"60min"` and `"1h"` save identically. Schema-4 design documents accept legacy minute text `m` as well as `min`; the same decoder reads embedded designs in definitions, and re-saving writes canonical `min`. `from_yaml` and `from_json` accept text or bytes and only construct the known data types. Text that does not parse refuses with `terrine.design.invalid` (`reason="malformed_document"`); a document from an earlier version refuses with the same code and `reason="unsupported_schema_version"`, with the supplied `schema_version` in context, and there is no migration: construct the design again and save it. Duplicate keys, unknown node tags, extra fields, unsafe YAML tags, and values of the wrong type (including a duration that is not text) refuse with the same code and `reason="malformed_document"`; a node whose value its constructor refuses keeps that constructor's refusal. Canonical JSON recursively sorts mapping keys, uses compact UTF-8 JSON, and is the basis of `Design.spec_hash`; map insertion order does not affect a design hash.
Dataset definitions also use version 4. Saved plans and dataset manifests keep
their version-1 outer envelopes, while their nested designs and definitions
must use version 4. Catalog documents remain version 2.

Documents omit every field equal to its default: `Design("user", FixedDate("2025-03-01"))` saves only `unit` and `anchor`, and `Treatment("assigned")` saves only its `attribute`. Reading supplies each omitted default, and a document that spells a default out still loads. An optional field added later within version 4 is omitted while it holds its default, so existing documents and `spec_hash` values keep their identity; a default never changes within a document version. Editing a design, for example with `dataclasses.replace`, gives it a new `spec_hash`.

Recipes and low-level shapes use the same names: `occurrence` on event anchors,
`end_event`, `entry_event` and `exit_event` on `TimeToEvent`, `bin` on `LifeTable`,
`periods` on `StatePanel`, `period` on event panels, and `anticipation` and
`reference_period` on sequential specifications. Elapsed durations are authored
as unit-bearing strings or `terrine.windows.Duration` values and stored as
`Duration` values (calendar steps as `Months`), so `dataclasses.replace` keeps
them unchanged; read `.seconds` for integer seconds and `str()` for canonical text:

```python
from dataclasses import replace

from terrine.windows import Duration

weekly = Design("user", EventAnchor("purchase", offset="7d"), gap="24h")
copy = replace(weekly, unit="account")
assert copy.gap == Duration(86_400) and str(copy.gap) == "1d"
assert str(copy.anchor.offset) == "1w"
assert "gap: 1d" in to_yaml(copy) and from_yaml(to_yaml(copy)) == copy
```

Equality predicates use
`eq(expr=..., value=...)` and expose the same `expr` field as other predicates.
`Coverage(start=..., end=...)` includes both calendar days, just like `Schedule`.

Treatment is explicit and descriptive: `Treatment(attribute, at="0d", known_at_t0=False)` records a treatment attribute, its signed elapsed cutoff, and whether the design declares it known at time zero. An event anchor does not imply treatment assignment. Design construction and serialization are data-only; model validation, compilation, and frame execution belong to later APIs.

## Unit inference

`unit` names the roster unit each row is about. `Design(None, ...)`, which is what every `recipes` constructor builds when its `unit` is omitted, means the model's only unit: the design is resolved wherever it meets a model (`validate`, `build`, `snapshot`, `preview`, the agent tools and staging). With exactly one `UnitSource` the design is read as if that name had been typed; with several it refuses with `terrine.model.ambiguous_unit` (context `owner="design"`, sorted `choices`, `parameter="unit"`); with none it refuses with `terrine.design.invalid` (`reason="no_unit_source"`). `to_yaml` and `to_json` write `unit: null` and read it back, and `spec_hash` of such a design hashes that document. The identity recorded by staging, saved definitions and scoring views is the resolved design's, so a typed and an inferred unit give the same staged `design_hash`, definition digest and scoring `view_hash`, and a winsorization fitted on one is admitted by the other.

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
