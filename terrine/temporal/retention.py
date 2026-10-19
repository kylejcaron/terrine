"""Exact occurrence-count retention bands with explicit follow-up completeness."""

from __future__ import annotations

from datetime import datetime
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.errors import RefusalSpec
from terrine.temporal._timestamps import compare_timestamps

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
) -> ibis.Table:
    """Count distinct target-event occurrences after each anchor.

    Finite horizons require coverage through the horizon end. Open-ended
    horizons require an explicit complete-through timestamp. Null
    ``observed_until`` means no narrower anchor-specific coverage bound.
    Incomplete results carry a null count/band and ``reason`` of
    ``insufficient_followup`` or ``horizon_unavailable``.
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
        _ret_observed_until=anchors[observed_until],
    )
    source = (
        events.filter(
            (events[event_name] == target_event)
            & events[timestamp].notnull()
            & events[event_id].notnull()
        )
        .select(
            _ret_unit=events[unit],
            _ret_event_id=events[event_id],
            _ret_event_time=events[timestamp],
        )
        .distinct()
    )
    if finite_horizon_seconds is None:
        end = cast(ir.TimestampValue, ibis.literal(source_complete_through))
    else:
        end = cast(
            ir.TimestampValue,
            anchor._ret_anchor_time + ibis.interval(seconds=finite_horizon_seconds),
        )
    anchor_valid = anchor._ret_anchor_time.notnull() & compare_timestamps(
        anchor._ret_anchor_time, end, "lt"
    )
    has_followup = anchor._ret_observed_until.isnull() | compare_timestamps(
        anchor._ret_observed_until, end, "ge"
    )
    reason = ibis.ifelse(
        ~anchor_valid,
        ibis.literal("horizon_unavailable"),
        ibis.ifelse(
            ~has_followup,
            ibis.literal("insufficient_followup"),
            ibis.null().cast("string"),
        ),
    )
    complete = reason.isnull()
    matched = anchor.left_join(
        source,
        [
            anchor._ret_unit == source._ret_unit,
            compare_timestamps(source._ret_event_time, anchor._ret_anchor_time, "ge"),
            compare_timestamps(source._ret_event_time, end, "lt"),
        ],
    )
    grouped = matched.group_by(matched._ret_unit, matched._ret_anchor_id).aggregate(
        _ret_count=matched._ret_event_id.nunique()
    )
    joined = anchor.join(
        grouped,
        [
            anchor._ret_unit == grouped._ret_unit,
            anchor._ret_anchor_id == grouped._ret_anchor_id,
        ],
    )
    count = complete.ifelse(joined._ret_count.cast("int64"), ibis.null().cast("int64"))
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
