"""Exact occurrence-count retention bands with explicit follow-up completeness."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.errors import RefusalSpec
from terrine.temporal._timestamps import compare_timestamps
from terrine.temporal.ranges import (
    _MAX_CANDIDATES,
    _RANGE_BOUND,
    RangePlan,
    blocked_range_join,
)
from terrine.windows import Window

_INVALID = RefusalSpec(
    "terrine.temporal.retention_invalid",
    "The retention request cannot be represented ({reason}).",
)


def retention_threshold_bands(
    events: ibis.Table,
    anchors: ibis.Table,
    *,
    unit: str,
    event_id: str,
    event_name: str,
    target_event: str,
    timestamp: str,
    anchor_id: str,
    anchor_timestamp: str,
    observed_until: str,
    count_thresholds: tuple[int, ...],
    finite_horizon_seconds: int | None = None,
    source_complete_through: datetime | None = None,
    source_complete_from: datetime | None = None,
    range_plan: RangePlan | None = None,
) -> ibis.Table:
    """Count distinct target-event occurrences after each anchor.

    Finite horizons require a non-null observation endpoint through the
    horizon end. Open-ended horizons require an explicit complete-through
    timestamp; a null ``observed_until`` means no narrower bound in that mode.
    Incomplete results carry a null count/band and an exact reason. A declared
    source start also requires the anchor to be within covered history.
    Bounded execution consumes the same range plan as nonadditive windows;
    an open horizon additionally needs a source start to bound its span.
    """
    if not isinstance(events, ibis.Table) or not isinstance(anchors, ibis.Table):
        raise TypeError("events and anchors must be Ibis tables")
    required_events = (unit, event_id, event_name, timestamp)
    required_anchors = (unit, anchor_id, anchor_timestamp, observed_until)
    if any(name not in events.columns for name in required_events):
        raise _INVALID.error(reason="missing_event_column")
    if any(name not in anchors.columns for name in required_anchors):
        raise _INVALID.error(reason="missing_anchor_column")
    if not target_event:
        raise _INVALID.error(reason="empty_target_event")
    if (
        not count_thresholds
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in count_thresholds
        )
        or tuple(sorted(set(count_thresholds))) != count_thresholds
    ):
        raise _INVALID.error(reason="invalid_count_thresholds")
    if finite_horizon_seconds is not None:
        if isinstance(finite_horizon_seconds, bool) or finite_horizon_seconds <= 0:
            raise _INVALID.error(reason="invalid_finite_horizon")
        if source_complete_through is not None:
            raise _INVALID.error(reason="cutoff_with_finite_horizon")
    elif source_complete_through is None or source_complete_through.tzinfo is None:
        raise _INVALID.error(reason="missing_open_horizon_cutoff")
    if source_complete_from is not None and (
        not isinstance(source_complete_from, datetime) or source_complete_from.tzinfo is None
    ):
        raise _INVALID.error(reason="invalid_source_start")
    bounded_width = finite_horizon_seconds
    if range_plan is not None and finite_horizon_seconds is None:
        if source_complete_from is None:
            raise _RANGE_BOUND.error(
                expression="retention",
                reason="the open horizon has no declared lower source bound",
                candidates_per_event=None,
                limit=_MAX_CANDIDATES,
                route="declare both source completeness endpoints or use a finite horizon",
            )
        assert source_complete_through is not None
        span = source_complete_through.astimezone(UTC) - source_complete_from.astimezone(UTC)
        bounded_width = span.days * 86_400 + span.seconds + bool(span.microseconds)
    if range_plan is not None and (
        bounded_width != range_plan.block_seconds
        or range_plan.max_candidates_per_event > _MAX_CANDIDATES
    ):
        raise _RANGE_BOUND.error(
            expression="retention",
            reason="the range plan does not bound the declared horizon",
            candidates_per_event=range_plan.max_candidates_per_event,
            limit=_MAX_CANDIDATES,
            route="derive a range plan for the declared horizon and anchor population",
        )

    for table, name in (
        (events, timestamp),
        (anchors, anchor_timestamp),
        (anchors, observed_until),
    ):
        dtype = table.schema()[name]
        if not isinstance(dtype, dt.Timestamp) or dtype.timezone not in (None, "UTC"):
            raise _INVALID.error(reason=f"unsupported_timestamp_{name}")

    anchor = anchors.select(
        _ret_unit=anchors[unit],
        _ret_anchor_id=anchors[anchor_id],
        _ret_anchor_time=anchors[anchor_timestamp],
        t0=anchors[anchor_timestamp],
        _ret_observed_until=anchors[observed_until],
    )
    source = (
        events.filter(
            ((events[event_name] == target_event) | events[event_name].isnull())
            & events[timestamp].notnull()
            & events[event_id].notnull()
        )
        .select(
            _ret_unit=events[unit],
            _ret_event_id=events[event_id],
            _ret_event_time=events[timestamp],
            _ret_target=events[event_name] == target_event,
        )
        .distinct()
    )
    if finite_horizon_seconds is None:
        end = cast(ir.TimestampValue, ibis.literal(source_complete_through))
    else:
        end = anchor._ret_anchor_time
    end_offset = 0 if finite_horizon_seconds is None else finite_horizon_seconds
    anchor_valid = anchor._ret_anchor_time.notnull() & compare_timestamps(
        anchor._ret_anchor_time, end, "lt", right_offset_seconds=end_offset
    )
    source_history = (
        compare_timestamps(
            anchor._ret_anchor_time,
            cast(ir.TimestampScalar, ibis.literal(source_complete_from)),
            "ge",
        )
        if source_complete_from is not None
        else cast(ir.BooleanValue, ibis.literal(True))
    )
    if finite_horizon_seconds is None:
        has_followup = anchor._ret_observed_until.isnull() | compare_timestamps(
            anchor._ret_observed_until, end, "ge"
        )
    else:
        has_followup = anchor._ret_observed_until.notnull() & compare_timestamps(
            anchor._ret_observed_until, end, "ge", right_offset_seconds=end_offset
        )
    reason = ibis.cases(
        (~anchor_valid, ibis.literal("horizon_unavailable")),
        (~source_history, ibis.literal("incomplete_coverage")),
        (~has_followup, ibis.literal("insufficient_followup")),
        else_=ibis.null().cast("string"),
    )
    complete = reason.isnull()
    if range_plan is None:
        matched = anchor.left_join(
            source,
            [
                anchor._ret_unit == source._ret_unit,
                compare_timestamps(source._ret_event_time, anchor._ret_anchor_time, "ge"),
                compare_timestamps(
                    source._ret_event_time, end, "lt", right_offset_seconds=end_offset
                ),
            ],
        )
    else:
        bounded_source = source
        if finite_horizon_seconds is None:
            bounded_source = source.filter(compare_timestamps(source._ret_event_time, end, "lt"))
        matched = blocked_range_join(
            anchor.filter(complete),
            bounded_source,
            plan=range_plan,
            window=Window(0, range_plan.block_seconds),
            unit_column="_ret_unit",
            event_unit="_ret_unit",
            event_timestamp="_ret_event_time",
        ).matched
    grouped = matched.group_by(matched._ret_unit, matched._ret_anchor_id).aggregate(
        _ret_count=matched._ret_event_id.nunique(where=matched._ret_target.fill_null(False)),
        _ret_unknown=(matched._ret_event_id.notnull() & matched._ret_target.isnull()).any(),
    )
    joined = anchor.left_join(
        grouped,
        [
            anchor._ret_unit == grouped._ret_unit,
            anchor._ret_anchor_id == grouped._ret_anchor_id,
        ],
    )
    reason = ibis.cases(
        (reason.notnull(), reason),
        (joined._ret_unknown.fill_null(False), "incomplete_label_history"),
        else_=ibis.null().cast("string"),
    )
    complete = reason.isnull()
    count = complete.ifelse(joined._ret_count.fill_null(0).cast("int64"), ibis.null().cast("int64"))
    band = ibis.literal(0).cast("int64")
    for index, threshold in enumerate(count_thresholds, start=1):
        band = ibis.ifelse(count >= ibis.literal(threshold), index, band)
    return joined.select(
        joined._ret_unit.name(unit),
        joined._ret_anchor_id.name(anchor_id),
        count.name("occurrence_count"),
        complete.name("complete"),
        complete.ifelse(band, ibis.null().cast("int64")).name("retention_band"),
        reason.name("reason"),
    )
