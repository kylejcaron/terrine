# Candidate anchor compilation

Use `design.build(model)` to compile a lazy Frame; materialization and staging run the required integrity checks through the owned execution path. Its candidate rows retain `unit`, `anchor_id`, exact `t0`, observation bounds and anchor provenance. Building does not execute source-data queries. Anchor integrity refusals expose stable `check_key` values such as `null_roster_key`, `duplicate_roster_key`, `null_event_id`, `duplicate_event_id`, `null_event_unit`, `null_event_timestamp`, `tied_event_timestamps` and `ambiguous_event_identity`. Correct the named source contract instead of silently deduplicating candidates.

`FixedDate` and `Schedule` use the explicitly declared `UnitSource` roster for the design's logical unit. Roster entry/exit bounds are membership bounds evaluated as `[entered_at, exited_at)` at each candidate `t0`. Observation bounds are copied independently; a missing bound remains null and roster exit is not used as censoring. Fixed-date calendar bounds resolve through the Catalog's declared day boundary. Schedules advance by elapsed seconds from start and exclude their end.

`EventAnchor` reads the named event's bound source and physical unit mapping. Event values are not used as event-presence predicates: a row is an event even when its event column is null. The source's `Model.event_ids` column is retained from raw relation rows for stable tie ordering and identity. Aware timestamp components of fallback or configured event identities are rendered after explicit UTC normalization, independent of the session timezone. `first` and `nth` are selected over full source history before `Design.anchor_range` filtering; `each` retains every event. A range is half-open; calendar-date endpoints resolve through Catalog day-boundary semantics and aware datetime endpoints remain exact UTC instants.

## Landmark event anchors

`EventAnchor("purchase", offset="1d")` sets `t0` to the selected purchase
instant plus one elapsed day. Offsets default to zero and accept non-negative
integral seconds or `s`/`m`/`h`/`d`/`w` durations. Negative offsets refuse at
construction with `terrine.design.invalid`. Selection and `anchor_range` use
the **original event time**, not the landmark; the shift happens afterward.
Roster membership and all feature/outcome windows use the resulting `t0`.

For survival from the day after purchase, with baseline features covering that
first day, use the low-level Design:

```python
from terrine import Design, EventAnchor, TimeToEvent, agg

landmark = Design(
    "user",
    EventAnchor("purchase", which="first", offset="1d"),
    features={"day_one_sessions": agg("sessions", window=("-1d", "0s"))},
    shape=TimeToEvent("churn"),
)
```

The feature window is `[purchase, purchase + 1d)`; follow-up starts at the
landmark. Units whose terminal event precedes the landmark are excluded as
prevalent. See [survival records](trajectories-survival.md#landmark-survival).

Nonzero offsets are retained in `anchor_id`, anchor provenance and serialized
Designs. Equivalent elapsed forms share a spec hash; zero is omitted from
serialization so existing saved Designs and hashes remain unchanged.
Persisted-layer runtime substitution for shifted event anchors refuses with
`terrine.compile.runtime_shape_unsupported` before source access; use ordinary
Frame materialization or a full rebuild instead.

## Source integrity


An event anchor must name an event declared by its bound source, not merely
present elsewhere in the Catalog; missing references use the same refusal
as `Design.validate`. Unrelated source columns cannot change generated
identities, including columns whose names differ only by case from a
compiler helper.

Range bounds outside a nanosecond column's representable domain still
include or exclude its events correctly. Only those comparisons use a
wider timestamp representation; ranking, precision validation, and the
published anchor retain the original instant.

If no roster is bound for an event anchor, observation bounds are null under the continuous-observation assumption within the declared source completeness. When an optional roster has no row for an event unit, that event remains a candidate with null observation bounds; matched rows contribute their independent bounds and entry/exit membership checks. If there is no configured stable event ID, equal-timestamp first/nth rows are rejected by explicit validation rather than given an arbitrary winner. Duplicate configured IDs and duplicate candidate `(unit, anchor_id)` identities are likewise rejected, not silently deduplicated.

Configured event IDs need only be unique within their fact source and analysis unit; different units may reuse the same ID. Missing rosters and event-source entity mappings use `terrine.design.incompatible_unit` with the same structured context as static validation.

Candidate compilation is internal to Design/Frame execution; it is not a separate public entry point. Schedule sequence expansion uses DuckDB's lazy `range` relation.

Source and roster timestamps must ultimately publish as UTC-aware values. DuckDB-backed naive timestamps are interpreted explicitly as UTC rather than through the connection session timezone. For native `timestamp[ns]` columns, `validate` checks the original relation under the caller's pinned executor: microsecond-aligned values are retained exactly, while an actual submicrosecond value raises `terrine.compile.anchor_timestamp_precision` with a request to normalize source precision. Roster entry, exit, and observation bounds receive the same guard before UTC conversion. This does not detect precision already discarded by an upstream ingestion path (for example, DuckDB coercing an aware Arrow nanosecond timestamp to microseconds before Catalog binding).
