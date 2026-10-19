"""Exact occurrence-count retention bands with explicit follow-up completeness."""

from __future__ import annotations

from datetime import datetime

import ibis
import ibis.expr.datatypes as dt

__all__ = ["retention_threshold_bands"]


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

    Finite horizons require the anchor's observation coverage to reach the
    horizon end. Open-ended horizons require an explicit complete-through
    timestamp and observation coverage through that cutoff. Incomplete results
    are null, never interpreted as zero occurrences.
    """
    if not isinstance(events, ibis.Table) or not isinstance(anchors, ibis.Table):
        raise TypeError("events and anchors must be Ibis tables")
    required_events = (unit, event_id, event_name, timestamp)
    required_anchors = (unit, anchor_id, anchor_timestamp, observed_until)
    if any(name not in events.columns for name in required_events):
        raise ValueError("retention event relation is missing a required column")
    if any(name not in anchors.columns for name in required_anchors):
        raise ValueError("retention anchor relation is missing a required column")
    if not target_event:
        raise ValueError("target_event must be nonempty")
    if (
        not count_thresholds
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in count_thresholds
        )
        or tuple(sorted(set(count_thresholds))) != count_thresholds
    ):
        raise ValueError("count_thresholds must be strictly increasing positive integers")
    if finite_horizon_seconds is not None:
        if isinstance(finite_horizon_seconds, bool) or finite_horizon_seconds <= 0:
            raise ValueError("finite_horizon_seconds must be positive")
        if source_complete_through is not None:
            raise ValueError("source_complete_through is only valid for open-ended horizons")
    elif source_complete_through is None or source_complete_through.tzinfo is None:
        raise ValueError("open-ended horizon requires a timezone-aware source cutoff")

    for table, name in (
        (events, timestamp),
        (anchors, anchor_timestamp),
        (anchors, observed_until),
    ):
        dtype = table.schema()[name]
        if not isinstance(dtype, dt.Timestamp) or dtype.timezone not in (None, "UTC"):
            raise ValueError(f"{name} must be a UTC timestamp")

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
        cutoff = ibis.literal(source_complete_through)
        end = cutoff
        complete = anchor._ret_observed_until >= cutoff
    else:
        end = anchor._ret_anchor_time + ibis.interval(seconds=finite_horizon_seconds)
        complete = anchor._ret_observed_until >= end
    matched = anchor.left_join(
        source,
        [
            anchor._ret_unit == source._ret_unit,
            source._ret_event_time >= anchor._ret_anchor_time,
            source._ret_event_time < end,
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
    # An open-horizon cutoff before the anchor is not a complete interval.
    complete = complete & (anchor._ret_anchor_time < end)
    count = complete.ifelse(joined._ret_count.cast("int64"), ibis.null().cast("int64"))
    band = ibis.literal(0, type=dt.int64)
    for threshold in count_thresholds:
        band = band + (count >= threshold).cast("int64")
    return joined.select(
        joined._ret_unit.name(unit),
        joined._ret_anchor_id.name(anchor_id),
        count.name("occurrence_count"),
        complete.name("complete"),
        complete.ifelse(band, ibis.null().cast("int64")).name("retention_band"),
    )
