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
    label_column: str | None = None,
) -> ibis.Table:
    """Annotate each bounded anchor event with session and predecessor fields."""
    if gap_seconds < 0 or max_events <= 0:
        raise ValueError("gap_seconds must be non-negative and max_events positive")
    required = (unit_column, anchor_id_column, timestamp_column, event_id_column)
    if any(name not in events.columns for name in required):
        raise ValueError("event relation is missing a session key")
    if label_column is not None and label_column not in events.columns:
        raise ValueError("event relation is missing the transition label")

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
    predecessor_fields = {
        "previous_event_id": ordered[event_id_column].lag().over(group_by=groups, order_by=order),
        "previous_event_time": ordered[timestamp_column]
        .lag()
        .over(group_by=groups, order_by=order),
    }
    previous_label_column = None
    if label_column is not None:
        previous_label_column = "__terrine_previous_event_label"
        suffix = 0
        while previous_label_column in ordered.columns:
            suffix += 1
            previous_label_column = f"__terrine_previous_event_label_{suffix}"
        predecessor_fields[previous_label_column] = (
            ordered[label_column].lag().over(group_by=groups, order_by=order)
        )
    prior = ordered.mutate(**predecessor_fields)
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
    ).view()
    session_keys = [
        annotated[unit_column],
        annotated[anchor_id_column],
        annotated.session_index,
    ]
    summaries = annotated.group_by(session_keys).aggregate(
        session_event_count=annotated[event_id_column].count(),
        session_start_time=annotated[timestamp_column].min(),
        session_end_time=annotated[timestamp_column].max(),
    )
    collisions = {
        "session_event_count",
        "session_start_time",
        "session_end_time",
    }.intersection(annotated.columns) - {
        unit_column,
        anchor_id_column,
        timestamp_column,
        event_id_column,
    }
    left = annotated.drop(*collisions).view()
    right = summaries.view()
    enriched = left.left_join(
        right,
        [
            left[unit_column] == right[unit_column],
            left[anchor_id_column] == right[anchor_id_column],
            left.session_index == right.session_index,
        ],
    )
    transition = ibis.struct(
        {
            "from_event_id": left.previous_event_id,
            "to_event_id": left[event_id_column],
        }
    )
    transition_fields = {
        "transition": ibis.ifelse(
            left.previous_event_id.isnull(),
            ibis.literal(None, type=transition.type()),
            transition,
        )
    }
    if label_column is not None:
        assert previous_label_column is not None
        transition_fields["transition_from"] = left[previous_label_column]
        transition_fields["transition_to"] = left[label_column]
    enriched = enriched.mutate(**transition_fields)
    return enriched.drop(session_start_column).order_by(
        [
            enriched[unit_column],
            enriched[anchor_id_column],
            enriched[timestamp_column],
            enriched[event_id_column],
        ]
    )
