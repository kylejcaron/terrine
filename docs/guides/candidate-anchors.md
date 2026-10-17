# Candidate anchor compilation

`terrine.compile.compile_anchors(design, model)` constructs a lazy `AnchorTable`. Its `.table` relation contains `unit`, `anchor_id`, exact `t0`, `_observed_from`, `_observed_until`, and `anchor_provenance`. Constructing it does not execute source-data queries. The caller must run `AnchorTable.validate(execute=...)` through its pinned executor before consuming candidate rows. The callback must return a one-row PyArrow table with the `invalid_count` column; missing, empty, malformed, or negative results fail closed. Integrity refusals expose stable `check_key` values such as `null_roster_key`, `duplicate_roster_key`, `null_event_id`, `duplicate_event_id`, `null_event_unit`, `null_event_timestamp`, `tied_event_timestamps`, and `ambiguous_event_identity`; timestamp precision refusals retain `column`.

`FixedDate` and `Schedule` use the explicitly declared `UnitSource` roster for the design's logical unit. Roster entry/exit bounds are membership bounds evaluated as `[entered_at, exited_at)` at each candidate `t0`. Observation bounds are copied independently; a missing bound remains null and roster exit is not used as censoring. Fixed-date calendar bounds resolve through the Catalog's declared day boundary. Schedules advance by elapsed seconds from start and exclude their end.

`EventAnchor` reads the declared fact's bound source and physical entity mapping. Fact values are not used as event-presence predicates: a row is an event even when its fact column is null. The source's `Model.event_ids` column is retained from raw relation rows for stable tie ordering and identity. Aware timestamp components of fallback or configured event identities are rendered after explicit UTC normalization, independent of the session timezone. `first` and `nth` are selected over full source history before `Design.anchor_range` filtering; `each` retains every event. A range is half-open; calendar-date endpoints resolve through Catalog day-boundary semantics and aware datetime endpoints remain exact UTC instants.

An event anchor must name a fact declared by its bound source, not merely
present elsewhere in the Catalog; missing references use the same refusal
as `Design.validate`. Unrelated source columns cannot change generated
identities, including columns whose names differ only by case from a
compiler helper.

Range bounds outside a nanosecond column's representable domain still
include or exclude its events correctly. Only those comparisons use a
wider timestamp representation; ranking, precision validation, and the
published anchor retain the original instant.

If no roster is bound for an event anchor, observation bounds are null under the continuous-observation assumption within the declared source completeness. When an optional roster has no row for an event unit, that event remains a candidate with null observation bounds; matched rows contribute their independent bounds and entry/exit membership checks. If there is no configured stable event ID, equal-timestamp first/nth rows are rejected by explicit validation rather than given an arbitrary winner. Duplicate configured IDs and duplicate candidate `(unit, anchor_id)` identities are likewise rejected, not silently deduplicated.

The compiler is intentionally concrete: it adds no root-level Terrine API, frame execution, eligibility/treatment logic, or generic registry. Schedule sequence expansion currently uses DuckDB's lazy `range` relation.

Source and roster timestamps must ultimately publish as UTC-aware values. DuckDB-backed naive timestamps are interpreted explicitly as UTC rather than through the connection session timezone. For native `timestamp[ns]` columns, `validate` checks the original relation under the caller's pinned executor: microsecond-aligned values are retained exactly, while an actual submicrosecond value raises `terrine.compile.anchor_timestamp_precision` with a request to normalize source precision. Roster entry, exit, and observation bounds receive the same guard before UTC conversion. This does not detect precision already discarded by an upstream ingestion path (for example, DuckDB coercing an aware Arrow nanosecond timestamp to microseconds before Catalog binding).
