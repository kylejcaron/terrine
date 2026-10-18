"""Per-anchor session assignment and adjacent-event transitions."""

from __future__ import annotations

import ibis

__all__ = ["sessionize"]


def sessionize(
    events: ibis.Table,
    *,
    unit_column: str,
    anchor_id_column: str,
    timestamp_column: str,
    event_id_column: str,
    gap_seconds: int,
    max_events: int,
) -> ibis.Table:
    """Annotate each bounded anchor event with session and predecessor fields."""
    if gap_seconds < 0 or max_events <= 0:
        raise ValueError("gap_seconds must be non-negative and max_events positive")
    required = (unit_column, anchor_id_column, timestamp_column, event_id_column)
    if any(name not in events.columns for name in required):
        raise ValueError("event relation is missing a session key")

    ordered = events.order_by(
        [
            events[unit_column],
            events[anchor_id_column],
            events[timestamp_column],
            events[event_id_column],
        ]
    )
    groups = [ordered[unit_column], ordered[anchor_id_column]]
    order = [ordered[timestamp_column], ordered[event_id_column]]
    prior = ordered.mutate(
        previous_event_id=ordered[event_id_column].lag().over(group_by=groups, order_by=order),
        previous_event_time=ordered[timestamp_column].lag().over(group_by=groups, order_by=order),
    )
    session_start_column = "_new_session"
    suffix = 0
    while session_start_column in ordered.columns:
        suffix += 1
        session_start_column = f"_new_session_{suffix}"
    starts = prior.mutate(
        **{
            session_start_column: (
                prior.previous_event_time.isnull()
                | (
                    (prior[timestamp_column] - prior.previous_event_time)
                    > ibis.interval(seconds=gap_seconds)
                )
            ).cast("int64")
        }
    )
    running = ibis.cumulative_window(group_by=groups, order_by=order)
    annotated = starts.mutate(
        session_index=starts[session_start_column].cumsum().over(running) - 1,
        transition_reason=ibis.ifelse(
            starts.previous_event_id.isnull(), "missing_predecessor", None
        ),
    )
    return annotated.drop(session_start_column)
