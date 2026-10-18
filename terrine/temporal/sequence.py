"""Sparse deterministic event selection relative to candidate anchors."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Literal

import ibis
import pyarrow as pa

from terrine.data import ColumnMeta, Roles
from terrine.errors import RefusalSpec
from terrine.temporal.sessionize import sessionize
from terrine.windows import Window

_MAX_EVENTS_EXCEEDED = RefusalSpec(
    "terrine.sequence.max_events_exceeded",
    "Sequence contains more than the configured max_events value {limit}.",
)
_INVALID_BOUNDS = RefusalSpec(
    "terrine.sequence.incomplete_time_bounds",
    "Event sequence completeness bounds are invalid.",
)


@dataclass(frozen=True, slots=True)
class SequenceResult:
    table: ibis.Table
    roles: Roles
    columns: Mapping[str, ColumnMeta]
    candidate_rows: int | None
    expression_timings: Mapping[str, object]
    _max_check: ibis.Table
    _max_events: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", MappingProxyType(dict(self.columns)))
        object.__setattr__(
            self, "expression_timings", MappingProxyType(dict(self.expression_timings))
        )

    def validate(self, *, execute: Callable[[ibis.Table], pa.Table]) -> None:
        result = execute(self._max_check).to_pylist()
        if result and result[0]["exceeded"]:
            raise _MAX_EVENTS_EXCEEDED.error(limit=self._max_events)


def event_sequence(
    events: ibis.Table,
    anchors: ibis.Table,
    *,
    window_start: int,
    window_end: int,
    max_events: int,
    representation: Literal["positions", "arrays"],
    properties: tuple[str, ...],
    internal_properties: tuple[str, ...],
    label_property: str | None,
    unit_column: str,
    anchor_id_column: str,
    timestamp_column: str,
    event_id_column: str,
    observed_from_column: str | None,
    observed_until_column: str | None,
    complete_from: datetime,
    complete_until: datetime,
    session_gap_seconds: int | None = None,
) -> SequenceResult:
    """Select complete, ordered source events in each anchor's exact half-open window."""
    if window_start >= window_end or max_events <= 0:
        raise _INVALID_BOUNDS.error()
    if (
        complete_from.tzinfo is None
        or complete_until.tzinfo is None
        or complete_from >= complete_until
    ):
        raise _INVALID_BOUNDS.error()
    for name in (unit_column, anchor_id_column, "t0"):
        if name not in anchors.columns:
            raise ValueError(f"anchor relation is missing {name!r}")
    required = (unit_column, timestamp_column, event_id_column, *properties, *internal_properties)
    if any(name not in events.columns for name in required):
        raise ValueError("event relation is missing a requested column")

    anchor_fields = {
        "_sequence_unit": anchors[unit_column],
        "_sequence_anchor": anchors[anchor_id_column],
        "_sequence_t0": anchors.t0,
    }
    if observed_from_column is not None:
        if observed_from_column not in anchors.columns:
            raise ValueError("anchor observation lower-bound column is missing")
        anchor_fields["_sequence_observed_from"] = anchors[observed_from_column]
    if observed_until_column is not None:
        if observed_until_column not in anchors.columns:
            raise ValueError("anchor observation upper-bound column is missing")
        anchor_fields["_sequence_observed_until"] = anchors[observed_until_column]
    anchor = anchors.select(**anchor_fields)
    start_time = anchor._sequence_t0 + ibis.interval(seconds=window_start)
    end_time = anchor._sequence_t0 + ibis.interval(seconds=window_end)
    matches = [
        anchor._sequence_unit == events[unit_column],
        events[timestamp_column] >= start_time,
        events[timestamp_column] < end_time,
    ]
    joined = anchor.join(events, predicates=matches, how="left")
    order = [joined[timestamp_column], joined[event_id_column]]
    ranked = joined.mutate(
        sequence_position=ibis.row_number().over(
            group_by=[joined._sequence_unit, joined._sequence_anchor], order_by=order
        )
    )
    present = ranked.filter(ranked[event_id_column].notnull())
    counts = present.group_by([present._sequence_unit, present._sequence_anchor]).aggregate(
        event_count=ibis._.count()
    )
    over_limit = counts.filter(counts.event_count > max_events).aggregate(
        exceeded=ibis._.count() > 0
    )

    coverage_complete = (
        anchor._sequence_t0 + ibis.interval(seconds=window_start) >= ibis.literal(complete_from)
    ) & (anchor._sequence_t0 + ibis.interval(seconds=window_end) <= ibis.literal(complete_until))
    observation_complete = ibis.literal(True)
    if observed_from_column is not None:
        observation_complete = observation_complete & (
            anchor._sequence_observed_from.isnull()
            | (
                anchor._sequence_observed_from
                <= anchor._sequence_t0 + ibis.interval(seconds=window_start)
            )
        )
    if observed_until_column is not None:
        observation_complete = observation_complete & (
            anchor._sequence_observed_until.isnull()
            | (
                anchor._sequence_observed_until
                >= anchor._sequence_t0 + ibis.interval(seconds=window_end)
            )
        )
    complete_expr = coverage_complete & observation_complete
    complete = anchor.mutate(
        _sequence_coverage_complete=coverage_complete,
        _sequence_complete=complete_expr,
    )
    counts = complete.left_join(
        counts,
        predicates=[
            complete._sequence_unit == counts._sequence_unit,
            complete._sequence_anchor == counts._sequence_anchor,
        ],
    ).select(
        _sequence_unit=complete._sequence_unit,
        _sequence_anchor=complete._sequence_anchor,
        _sequence_t0=complete._sequence_t0,
        _sequence_coverage_complete=complete._sequence_coverage_complete,
        _sequence_complete=complete._sequence_complete,
        event_count=ibis.ifelse(
            complete._sequence_complete,
            ibis.coalesce(counts.event_count, 0),
            ibis.literal(None, type="int64"),
        ),
    )
    complete_children = present.join(
        complete,
        predicates=[
            present._sequence_unit == complete._sequence_unit,
            present._sequence_anchor == complete._sequence_anchor,
        ],
    ).filter(complete._sequence_complete)
    if session_gap_seconds is not None:
        complete_children = sessionize(
            complete_children,
            unit_column="_sequence_unit",
            anchor_id_column="_sequence_anchor",
            timestamp_column=timestamp_column,
            event_id_column=event_id_column,
            gap_seconds=session_gap_seconds,
            max_events=max_events,
        )

    if representation == "arrays":
        struct_values = ibis.struct(
            {
                "event_id": complete_children[event_id_column],
                "event_time": complete_children[timestamp_column],
                **{
                    field: (
                        ibis.ifelse(
                            complete_children[name].isnull(),
                            "known_null",
                            ibis.literal(None, type="string"),
                        )
                        if is_reason
                        else complete_children[name]
                    )
                    for name in properties
                    for field, is_reason in ((name, False), (f"{name}_reason", True))
                },
                **(
                    {
                        "session_index": complete_children.session_index,
                        "previous_event_id": complete_children.previous_event_id,
                        "previous_event_time": complete_children.previous_event_time,
                        "transition_reason": complete_children.transition_reason,
                    }
                    if session_gap_seconds is not None
                    else {}
                ),
            }
        )
        arrays = complete_children.group_by(
            [complete_children._sequence_unit, complete_children._sequence_anchor]
        ).aggregate(
            event_values=struct_values.collect(
                order_by=[complete_children[timestamp_column], complete_children[event_id_column]]
            )
        )
        header = counts.left_join(
            arrays,
            predicates=[
                counts._sequence_unit == arrays._sequence_unit,
                counts._sequence_anchor == arrays._sequence_anchor,
            ],
        ).select(
            unit=counts._sequence_unit,
            anchor_id=counts._sequence_anchor,
            t0=counts._sequence_t0,
            is_event=ibis.literal(False),
            sequence_position=ibis.literal(None, type="int64"),
            event_id=ibis.literal(None, type=events.schema()[event_id_column]),
            event_time=ibis.literal(None, type=events.schema()[timestamp_column]),
            event_count=counts.event_count,
            sequence_complete=counts._sequence_complete,
            sequence_reason=ibis.ifelse(
                counts._sequence_complete,
                ibis.literal(None, type="string"),
                ibis.ifelse(
                    counts._sequence_coverage_complete, "unit_observation", "source_coverage"
                ),
            ),
            event_values=ibis.ifelse(
                counts._sequence_complete,
                ibis.coalesce(
                    arrays.event_values, ibis.literal([], type=arrays.schema()["event_values"])
                ),
                ibis.literal(None, type=arrays.schema()["event_values"]),
            ),
        )
        table = header
        feature_names = ("event_values",)
    else:
        header = counts.select(
            unit=counts._sequence_unit,
            anchor_id=counts._sequence_anchor,
            t0=counts._sequence_t0,
            is_event=ibis.literal(False),
            sequence_position=ibis.literal(None, type="int64"),
            event_id=ibis.literal(None, type=events.schema()[event_id_column]),
            event_time=ibis.literal(None, type=events.schema()[timestamp_column]),
            event_count=counts.event_count,
            sequence_complete=counts._sequence_complete,
            sequence_reason=ibis.ifelse(
                counts._sequence_complete,
                ibis.literal(None, type="string"),
                ibis.ifelse(
                    counts._sequence_coverage_complete, "unit_observation", "source_coverage"
                ),
            ),
            **{
                output: expression
                for name in properties
                for output, expression in (
                    (name, ibis.literal(None, type=events.schema()[name])),
                    (f"{name}_reason", ibis.literal(None, type="string")),
                )
            },
            **(
                {
                    "session_index": ibis.literal(None, type="int64"),
                    "previous_event_id": ibis.literal(None, type=events.schema()[event_id_column]),
                    "previous_event_time": ibis.literal(
                        None, type=events.schema()[timestamp_column]
                    ),
                    "transition_reason": ibis.literal(None, type="string"),
                }
                if session_gap_seconds is not None
                else {}
            ),
        )
        children = complete_children.select(
            unit=complete_children._sequence_unit,
            anchor_id=complete_children._sequence_anchor,
            t0=complete_children._sequence_t0,
            is_event=ibis.literal(True),
            sequence_position=complete_children.sequence_position,
            event_id=complete_children[event_id_column],
            event_time=complete_children[timestamp_column],
            event_count=ibis.literal(None, type="int64"),
            sequence_complete=ibis.literal(True),
            sequence_reason=ibis.literal(None, type="string"),
            **{
                output: expression
                for name in properties
                for output, expression in (
                    (name, complete_children[name]),
                    (
                        f"{name}_reason",
                        ibis.ifelse(
                            complete_children[name].isnull(),
                            "known_null",
                            ibis.literal(None, type="string"),
                        ),
                    ),
                )
            },
            **(
                {
                    "session_index": complete_children.session_index,
                    "previous_event_id": complete_children.previous_event_id,
                    "previous_event_time": complete_children.previous_event_time,
                    "transition_reason": complete_children.transition_reason,
                }
                if session_gap_seconds is not None
                else {}
            ),
        )
        table = header.union(children, distinct=False).order_by(
            ["unit", "anchor_id", "is_event", "sequence_position"]
        )
        feature_names = tuple(properties)
        feature_names = tuple(properties) + (
            ("session_index", "previous_event_id", "previous_event_time", "transition_reason")
            if session_gap_seconds is not None
            else ()
        )
    roles = Roles("unit", "anchor_id", "t0", None, feature_names, ())
    columns = {
        name: ColumnMeta(Window(window_start, window_end), "event_property", None, None, None, None)
        for name in feature_names
    }
    columns.update(
        {
            "event_count": ColumnMeta(
                None, "event_count", None, None, "sequence_complete", "sequence_reason"
            ),
            "sequence_complete": ColumnMeta(
                None, "completeness", None, None, None, "sequence_reason"
            ),
        }
    )
    return SequenceResult(table, roles, columns, None, {}, over_limit, max_events)
