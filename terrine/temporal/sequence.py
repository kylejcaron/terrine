"""Sparse deterministic event selection relative to candidate anchors."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Literal, cast

import ibis
import ibis.expr.types as ir
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


def funnel_progress(
    events: ibis.Table,
    *,
    event_property: str,
    steps: tuple[str, ...],
    within_seconds: int | None,
    sequence_end_column: str,
    unit_column: str,
    anchor_id_column: str,
    timestamp_column: str,
    event_id_column: str,
    position_column: str,
    event_property_found_column: str | None = None,
) -> ibis.Table:
    """Return one bounded attempt row per step-zero event."""
    required = (
        event_property,
        sequence_end_column,
        unit_column,
        anchor_id_column,
        timestamp_column,
        event_id_column,
        position_column,
    )
    if event_property_found_column is not None:
        required = (*required, event_property_found_column)
    if (
        len(steps) < 2
        or any(not isinstance(step, str) or not step for step in steps)
        or len(set(steps)) != len(steps)
        or any(name not in events.columns for name in required)
        or (within_seconds is not None and (isinstance(within_seconds, bool) or within_seconds < 0))
    ):
        raise ValueError("invalid funnel parameters or event relation")
    deadline = (
        events[sequence_end_column]
        if within_seconds is None
        else events[timestamp_column] + ibis.interval(seconds=within_seconds)
    )
    attempts = events.filter(events[event_property] == steps[0]).select(
        _f_unit=events[unit_column],
        _f_anchor=events[anchor_id_column],
        _f_start_id=events[event_id_column],
        _f_start_time=events[timestamp_column],
        _f_start_position=events[position_column],
        _f_last_id=events[event_id_column],
        _f_last_time=events[timestamp_column],
        _f_last_position=events[position_column],
        _f_event_ids=ibis.array([events[event_id_column]]),
        _f_event_times=ibis.array([events[timestamp_column]]),
        _f_progress=ibis.literal(1, type="int64"),
        _f_deadline=deadline,
        _f_sequence_end=events[sequence_end_column],
    )
    keys = ("_f_unit", "_f_anchor", "_f_start_id")
    attempt_columns = tuple(attempts.columns)
    for progress, step in enumerate(steps[1:], start=2):
        attempt = attempts.view()
        candidate = events.view()
        matches = attempt.join(
            candidate,
            [
                attempt._f_progress == progress - 1,
                attempt._f_unit == candidate[unit_column],
                attempt._f_anchor == candidate[anchor_id_column],
                candidate[event_property] == step,
                candidate[timestamp_column] >= attempt._f_last_time,
                candidate[timestamp_column] < attempt._f_deadline,
                candidate[position_column] > attempt._f_last_position,
            ],
        ).select(
            **{name: attempt[name] for name in attempt_columns},
            _candidate_id=candidate[event_id_column],
            _candidate_time=candidate[timestamp_column],
            _candidate_position=candidate[position_column],
        )
        earliest = matches.group_by(list(keys)).aggregate(
            _next_position=matches._candidate_position.min()
        )
        matches_left = matches.view()
        earliest_right = earliest.view()
        matched = matches_left.join(
            earliest_right,
            [matches_left[key] == earliest_right[key] for key in keys]
            + [matches_left._candidate_position == earliest_right._next_position],
        ).select(
            **{name: matches_left[name] for name in attempt_columns},
            _next_id=matches_left._candidate_id,
            _next_time=matches_left._candidate_time,
            _next_position=matches_left._candidate_position,
        )
        left = attempts.view()
        right = matched.view()
        joined = left.left_join(right, [left[key] == right[key] for key in keys])
        attempts = joined.select(
            _f_unit=left._f_unit,
            _f_anchor=left._f_anchor,
            _f_start_id=left._f_start_id,
            _f_start_time=left._f_start_time,
            _f_start_position=left._f_start_position,
            _f_last_id=ibis.coalesce(right._next_id, left._f_last_id),
            _f_last_time=ibis.coalesce(right._next_time, left._f_last_time),
            _f_last_position=ibis.coalesce(right._next_position, left._f_last_position),
            _f_event_ids=ibis.ifelse(
                right._next_position.isnull(),
                left._f_event_ids,
                left._f_event_ids.concat(ibis.array([right._next_id])),
            ),
            _f_event_times=ibis.ifelse(
                right._next_position.isnull(),
                left._f_event_times,
                left._f_event_times.concat(ibis.array([right._next_time])),
            ),
            _f_progress=ibis.ifelse(
                right._next_position.isnull(),
                left._f_progress,
                ibis.literal(progress, type="int64"),
            ),
            _f_deadline=left._f_deadline,
            _f_sequence_end=left._f_sequence_end,
        )
    if event_property_found_column is not None:
        attempt = attempts.view()
        candidate = events.view()
        unknown = (
            attempt.join(
                candidate,
                [
                    attempt._f_unit == candidate[unit_column],
                    attempt._f_anchor == candidate[anchor_id_column],
                    candidate[position_column] > attempt._f_start_position,
                    candidate[timestamp_column] < attempt._f_deadline,
                    ~candidate[event_property_found_column],
                ],
            )
            .group_by(list(keys))
            .aggregate(_unknown_count=ibis._.count())
        )
        left = attempts.view()
        right = unknown.view()
        joined = left.left_join(right, [left[key] == right[key] for key in keys])
        attempts = joined.select(
            **{name: left[name] for name in attempts.columns},
            _f_unknown=right._unknown_count.notnull(),
        )
    complete = attempts._f_progress == len(steps)
    unknown_label = cast(
        ir.BooleanValue,
        attempts._f_unknown
        if "_f_unknown" in attempts.columns
        else ibis.literal(False, type="boolean"),
    )
    failed = (attempts._f_deadline <= attempts._f_sequence_end) & ~unknown_label
    return attempts.select(
        unit=attempts._f_unit,
        anchor_id=attempts._f_anchor,
        step0_event_id=attempts._f_start_id,
        start_time=attempts._f_start_time,
        matched_event_ids=attempts._f_event_ids,
        matched_event_times=attempts._f_event_times,
        deadline=attempts._f_deadline,
        progress=attempts._f_progress,
        status=ibis.ifelse(
            complete,
            "complete",
            ibis.ifelse(failed, "failed", "unavailable"),
        ),
        reason=ibis.ifelse(
            complete,
            ibis.literal(None, type="string"),
            ibis.ifelse(
                unknown_label,
                "missing_history",
                ibis.ifelse(
                    failed,
                    "deadline_elapsed",
                    "sequence_window_end_before_deadline",
                ),
            ),
        ),
    )


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
    funnel_event_property: str | None = None,
    funnel_steps: tuple[str, ...] | None = None,
    funnel_within_seconds: int | None = None,
    property_columns: Mapping[str, str] | None = None,
    label_property_column: str | None = None,
    property_found_columns: Mapping[str, str] | None = None,
    funnel_event_property_found_column: str | None = None,
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
    property_columns = dict(property_columns or {name: name for name in properties})
    property_found_columns = dict(property_found_columns or {})
    if set(property_columns) != set(properties) or not set(property_found_columns) <= set(
        properties
    ):
        raise ValueError("invalid property output or availability mapping")
    helper_columns = {"_sequence_unit", "_sequence_anchor", "_sequence_t0"}
    property_output_columns = {name: name for name in properties}
    occupied_outputs = set(events.columns) | set(anchors.columns) | set(properties)
    for index, name in enumerate(properties):
        if name in helper_columns:
            alias = f"__terrine_sequence_output_property_{index}"
            suffix = 0
            while alias in occupied_outputs:
                suffix += 1
                alias = f"__terrine_sequence_output_property_{index}_{suffix}"
            occupied_outputs.add(alias)
            property_output_columns[name] = alias
    label_column = label_property_column or label_property
    required = (
        unit_column,
        timestamp_column,
        event_id_column,
        *property_columns.values(),
        *property_found_columns.values(),
        *internal_properties,
        *((label_column,) if label_column is not None else ()),
        *((funnel_event_property_found_column,) if funnel_event_property_found_column else ()),
    )
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
            label_column=label_column,
        )

    if representation == "arrays":
        struct_values = ibis.struct(
            {
                "event_id": complete_children[event_id_column],
                "event_time": complete_children[timestamp_column],
                **{
                    field: (
                        (
                            ibis.ifelse(
                                complete_children[property_found_columns[name]],
                                ibis.ifelse(
                                    complete_children[property_columns[name]].isnull(),
                                    "known_null",
                                    ibis.literal(None, type="string"),
                                ),
                                "missing_history",
                            )
                            if name in property_found_columns
                            else ibis.ifelse(
                                complete_children[property_columns[name]].isnull(),
                                "known_null",
                                ibis.literal(None, type="string"),
                            )
                        )
                        if is_reason
                        else complete_children[property_columns[name]]
                    )
                    for name in properties
                    for field, is_reason in ((name, False), (f"{name}_reason", True))
                },
                **(
                    {
                        "session_index": complete_children.session_index,
                        "session_event_count": complete_children.session_event_count,
                        "session_start_time": complete_children.session_start_time,
                        "session_end_time": complete_children.session_end_time,
                        "previous_event_id": complete_children.previous_event_id,
                        "previous_event_time": complete_children.previous_event_time,
                        "transition_reason": complete_children.transition_reason,
                        "transition": complete_children.transition,
                    }
                    if session_gap_seconds is not None
                    else {}
                ),
                **(
                    {
                        "transition_from": complete_children.transition_from,
                        "transition_to": complete_children.transition_to,
                    }
                    if session_gap_seconds is not None and label_property is not None
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
                    (
                        property_output_columns[name],
                        ibis.literal(None, type=events.schema()[property_columns[name]]),
                    ),
                    (f"{name}_reason", ibis.literal(None, type="string")),
                )
            },
            **(
                {
                    "session_index": ibis.literal(None, type="int64"),
                    "session_event_count": ibis.literal(None, type="int64"),
                    "session_start_time": ibis.literal(
                        None, type=events.schema()[timestamp_column]
                    ),
                    "session_end_time": ibis.literal(None, type=events.schema()[timestamp_column]),
                    "previous_event_id": ibis.literal(None, type=events.schema()[event_id_column]),
                    "previous_event_time": ibis.literal(
                        None, type=events.schema()[timestamp_column]
                    ),
                    "transition_reason": ibis.literal(None, type="string"),
                    "transition": ibis.literal(
                        None,
                        type=ibis.struct(
                            {
                                "from_event_id": events[event_id_column],
                                "to_event_id": events[event_id_column],
                            }
                        ).type(),
                    ),
                }
                if session_gap_seconds is not None
                else {}
            ),
            **(
                {
                    "transition_from": ibis.literal(None, type=events.schema()[label_column]),
                    "transition_to": ibis.literal(None, type=events.schema()[label_column]),
                }
                if session_gap_seconds is not None and label_column is not None
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
                    (property_output_columns[name], complete_children[property_columns[name]]),
                    (
                        f"{name}_reason",
                        (
                            ibis.ifelse(
                                complete_children[property_found_columns[name]],
                                ibis.ifelse(
                                    complete_children[property_columns[name]].isnull(),
                                    "known_null",
                                    ibis.literal(None, type="string"),
                                ),
                                "missing_history",
                            )
                            if name in property_found_columns
                            else ibis.ifelse(
                                complete_children[property_columns[name]].isnull(),
                                "known_null",
                                ibis.literal(None, type="string"),
                            )
                        ),
                    ),
                )
            },
            **(
                {
                    "session_index": complete_children.session_index,
                    "session_event_count": complete_children.session_event_count,
                    "session_start_time": complete_children.session_start_time,
                    "session_end_time": complete_children.session_end_time,
                    "previous_event_id": complete_children.previous_event_id,
                    "previous_event_time": complete_children.previous_event_time,
                    "transition_reason": complete_children.transition_reason,
                    "transition": complete_children.transition,
                }
                if session_gap_seconds is not None
                else {}
            ),
            **(
                {
                    "transition_from": complete_children.transition_from,
                    "transition_to": complete_children.transition_to,
                }
                if session_gap_seconds is not None and label_property is not None
                else {}
            ),
        )
        table = header.union(children, distinct=False).order_by(
            ["unit", "anchor_id", "is_event", "sequence_position"]
        )
        output_renames = {
            alias: name for name, alias in property_output_columns.items() if alias != name
        }
        if output_renames:
            table = table.select(
                **{output_renames.get(name, name): table[name] for name in table.columns}
            )
        feature_names = tuple(properties) + (
            (
                "session_index",
                "session_event_count",
                "session_start_time",
                "session_end_time",
                "previous_event_id",
                "previous_event_time",
                "transition_reason",
                "transition",
            )
            if session_gap_seconds is not None
            else ()
        )
        if session_gap_seconds is not None and label_property is not None:
            feature_names = (*feature_names, "transition_from", "transition_to")
    if session_gap_seconds is not None:
        session_rows = complete_children.group_by(
            [
                complete_children._sequence_unit,
                complete_children._sequence_anchor,
                complete_children.session_index,
            ]
        ).aggregate(
            session_event_count=complete_children[event_id_column].count(),
            session_start_time=complete_children[timestamp_column].min(),
            session_end_time=complete_children[timestamp_column].max(),
        )
        session_values = ibis.struct(
            {
                "session_index": session_rows.session_index,
                "session_event_count": session_rows.session_event_count,
                "session_start_time": session_rows.session_start_time,
                "session_end_time": session_rows.session_end_time,
            }
        )
        session_groups = session_rows.group_by(
            [session_rows._sequence_unit, session_rows._sequence_anchor]
        ).aggregate(_sessions=session_values.collect(order_by=session_rows.session_index))
        occupied = set(table.columns) | set(session_groups.columns)

        def fresh_session_column(base: str) -> str:
            name = base
            suffix = 0
            while name in occupied:
                suffix += 1
                name = f"{base}_{suffix}"
            occupied.add(name)
            return name

        session_unit_column = fresh_session_column("__terrine_session_unit")
        session_anchor_column = fresh_session_column("__terrine_session_anchor")
        session_list_column = fresh_session_column("__terrine_session_values")
        sessions = session_groups.select(
            **{
                session_unit_column: session_groups._sequence_unit,
                session_anchor_column: session_groups._sequence_anchor,
                session_list_column: session_groups._sessions,
            }
        )
        session_type = sessions.schema()[session_list_column]
        table_columns = tuple(table.columns)
        joined = table.left_join(
            sessions,
            [
                table.unit == sessions[session_unit_column],
                table.anchor_id == sessions[session_anchor_column],
            ],
        )
        session_payload = ibis.ifelse(
            joined.is_event,
            ibis.literal(None, type=session_type),
            ibis.ifelse(
                joined.sequence_complete,
                ibis.coalesce(joined[session_list_column], ibis.literal([], type=session_type)),
                ibis.literal(None, type=session_type),
            ),
        )
        table = joined.select(
            **{name: joined[name] for name in table_columns},
            sessions=session_payload,
            sessions_complete=ibis.ifelse(
                joined.is_event,
                ibis.literal(None, type="boolean"),
                joined.sequence_complete,
            ),
            sessions_reason=ibis.ifelse(
                joined.is_event,
                ibis.literal(None, type="string"),
                joined.sequence_reason,
            ),
        )
        feature_names = (*feature_names, "sessions", "sessions_complete", "sessions_reason")
    if funnel_event_property is not None:
        if (
            not funnel_steps
            or funnel_event_property not in complete_children.columns
            or (funnel_within_seconds is not None and funnel_within_seconds <= 0)
        ):
            raise ValueError("invalid funnel parameters or missing funnel property")
        funnel_events = complete_children.mutate(
            _funnel_sequence_end=complete_children._sequence_t0 + ibis.interval(seconds=window_end)
        )
        attempts = funnel_progress(
            funnel_events,
            event_property=funnel_event_property,
            steps=funnel_steps,
            within_seconds=funnel_within_seconds,
            sequence_end_column="_funnel_sequence_end",
            unit_column="_sequence_unit",
            anchor_id_column="_sequence_anchor",
            timestamp_column=timestamp_column,
            event_id_column=event_id_column,
            position_column="sequence_position",
            event_property_found_column=funnel_event_property_found_column,
        )
        attempt_values = ibis.struct(
            {
                "start_time": attempts.start_time,
                "matched_event_ids": attempts.matched_event_ids,
                "matched_event_times": attempts.matched_event_times,
                "step0_event_id": attempts.step0_event_id,
                "deadline": attempts.deadline,
                "progress": attempts.progress,
                "status": attempts.status,
                "reason": attempts.reason,
            }
        )
        table_columns = tuple(table.columns)
        used_columns = set(table_columns)

        def fresh_summary_column(base: str) -> str:
            name = base
            suffix = 0
            while name in used_columns:
                suffix += 1
                name = f"{base}_{suffix}"
            used_columns.add(name)
            return name

        attempts_unit_column = fresh_summary_column("_terrine_funnel_unit")
        attempts_anchor_column = fresh_summary_column("_terrine_funnel_anchor")
        attempts_values_column = fresh_summary_column("_terrine_funnel_values")
        attempts_by_anchor = attempts.group_by(["unit", "anchor_id"]).aggregate(
            **{attempts_values_column: attempt_values.collect(order_by=[attempts.step0_event_id])}
        )
        attempts_by_anchor = attempts_by_anchor.select(
            **{
                attempts_unit_column: attempts_by_anchor.unit,
                attempts_anchor_column: attempts_by_anchor.anchor_id,
                attempts_values_column: attempts_by_anchor[attempts_values_column],
            }
        )
        joined_table = table.left_join(
            attempts_by_anchor,
            [
                table.unit == attempts_by_anchor[attempts_unit_column],
                table.anchor_id == attempts_by_anchor[attempts_anchor_column],
            ],
        )
        funnel_missing_history_column = fresh_summary_column("_terrine_funnel_missing_history")
        if funnel_event_property_found_column is not None:
            unknown_unit_column = fresh_summary_column("_terrine_funnel_unknown_unit")
            unknown_anchor_column = fresh_summary_column("_terrine_funnel_unknown_anchor")
            unknown_by_anchor = complete_children.group_by(
                [complete_children._sequence_unit, complete_children._sequence_anchor]
            ).aggregate(
                **{
                    funnel_missing_history_column: (
                        ~complete_children[funnel_event_property_found_column]
                    ).any()
                }
            )
            unknown_by_anchor = unknown_by_anchor.select(
                **{
                    unknown_unit_column: unknown_by_anchor._sequence_unit,
                    unknown_anchor_column: unknown_by_anchor._sequence_anchor,
                    funnel_missing_history_column: unknown_by_anchor[funnel_missing_history_column],
                }
            )
            joined_table = joined_table.left_join(
                unknown_by_anchor,
                [
                    joined_table.unit == unknown_by_anchor[unknown_unit_column],
                    joined_table.anchor_id == unknown_by_anchor[unknown_anchor_column],
                ],
            )
        funnel_type = attempts_by_anchor.schema()[attempts_values_column]
        funnel_missing_history = cast(
            ir.BooleanValue,
            joined_table[funnel_missing_history_column].fill_null(False)
            if funnel_missing_history_column in joined_table.columns
            else ibis.literal(False, type="boolean"),
        )
        funnel_complete = joined_table.sequence_complete & ~funnel_missing_history
        if "is_event" in joined_table.columns:
            payload = ibis.ifelse(
                joined_table.is_event,
                ibis.literal(None, type=funnel_type),
                ibis.ifelse(
                    funnel_complete,
                    ibis.coalesce(
                        joined_table[attempts_values_column],
                        ibis.literal([], type=funnel_type),
                    ),
                    ibis.literal(None, type=funnel_type),
                ),
            )
        else:
            payload = ibis.ifelse(
                funnel_complete,
                ibis.coalesce(
                    joined_table[attempts_values_column],
                    ibis.literal([], type=funnel_type),
                ),
                ibis.literal(None, type=funnel_type),
            )
        table = joined_table.select(
            **{name: joined_table[name] for name in table_columns},
            funnel_attempts=payload,
            funnel_attempts_complete=ibis.ifelse(
                joined_table.is_event,
                ibis.literal(None, type="boolean"),
                funnel_complete,
            ),
            funnel_attempts_reason=ibis.ifelse(
                joined_table.is_event,
                ibis.literal(None, type="string"),
                ibis.ifelse(
                    funnel_missing_history,
                    "missing_history",
                    joined_table.sequence_reason,
                ),
            ),
        )
        feature_names = (
            *feature_names,
            "funnel_attempts",
            "funnel_attempts_complete",
            "funnel_attempts_reason",
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
