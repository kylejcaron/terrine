"""Strict point lookups over coherent sparse temporal states."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.numeric import is_exact_type
from terrine.backends.timestamps import timestamp_order_key

__all__ = ["PrefixLookup", "lookup_prefix"]


@dataclass(frozen=True, slots=True)
class PrefixLookup:
    """A point-preserving lookup and its generated state-field aliases."""

    table: ibis.Table
    columns: Mapping[str, str]
    matched_column: str


def _allocate(used: set[str], stem: str) -> str:
    candidate = stem
    index = 0
    while candidate.casefold() in used:
        index += 1
        candidate = f"{stem}_{index}"
    used.add(candidate.casefold())
    return candidate


def _numeric(dtype: dt.DataType) -> bool:
    return dtype.is_numeric() or is_exact_type(dtype)


def lookup_prefix(
    points: ibis.Table,
    prefix: ibis.Table,
    *,
    unit_column: str,
    timestamp_column: str,
    strategy: Literal["native", "union_window"] = "native",
) -> PrefixLookup:
    """Attach the latest sparse state strictly before each point cutoff.

    Prefix keys are the canonical ``_unit`` and ``_timestamp`` fields. The
    producer must supply unique, non-null, timezone-compatible state keys.
    """
    if strategy not in {"native", "union_window"}:
        raise ValueError("strategy must be 'native' or 'union_window'")
    for column in (unit_column, timestamp_column):
        if column not in points.columns:
            raise ValueError(f"point column {column!r} is missing")
    for column in ("_unit", "_timestamp"):
        if column not in prefix.columns:
            raise ValueError(f"prefix column {column!r} is missing")

    # Each cutoff needs a distinct relation, even when points already contain this prefix.
    prefix = prefix.view()
    state_fields = [name for name in prefix.columns if name not in {"_unit", "_timestamp"}]
    used = {name.casefold() for name in (*points.columns, *prefix.columns)}
    point_unit = _allocate(used, "__terrine_point_unit")
    point_time = _allocate(used, "__terrine_point_time")
    state_unit = _allocate(used, "__terrine_state_unit")
    state_time = _allocate(used, "__terrine_state_time")
    point_present = _allocate(used, "__terrine_point_timestamp_present")
    state_present = _allocate(used, "__terrine_state_timestamp_present")
    kind_name = _allocate(used, "__terrine_kind")
    marker_name = _allocate(used, "__terrine_marker")
    matched_name = _allocate(used, "__terrine_matched")
    timeline_points = {
        name: _allocate(used, f"__terrine_point_{index}")
        for index, name in enumerate(points.columns)
    }
    state_aliases = {
        field: _allocate(used, f"__terrine_state_{index}")
        for index, field in enumerate(state_fields)
    }
    output_aliases = {
        field: _allocate(used, f"__terrine_lookup_{index}")
        for index, field in enumerate(state_fields)
    }

    left = points.select(
        **{name: points[name] for name in points.columns},
        **{
            point_unit: points[unit_column],
            point_time: timestamp_order_key(points[timestamp_column]),
        },
        **{point_present: points[timestamp_column].notnull()},
    )
    right = prefix.select(
        **{state_unit: prefix._unit, state_time: timestamp_order_key(prefix._timestamp)},
        **{state_present: ibis.literal(True)},
        **{state_aliases[name]: prefix[name] for name in state_fields},
    )

    if strategy == "native":
        joined = left.asof_join(
            right,
            on=left[point_time] > right[state_time],
            predicates=[
                left[point_unit] == right[state_unit],
                left[point_present] == right[state_present],
            ],
        )
    else:
        states = right.select(
            **{
                point_unit: right[state_unit],
                point_time: right[state_time],
                kind_name: ibis.literal(1).cast("int8"),
                **{
                    timeline_points[name]: ibis.null().cast(points[name].type())
                    for name in points.columns
                },
            }
        )
        requests = left.select(
            **{
                point_unit: left[point_unit],
                point_time: left[point_time],
                kind_name: ibis.literal(0).cast("int8"),
                **{timeline_points[name]: left[name] for name in points.columns},
            }
        )
        timeline = states.union(requests, distinct=False)
        frame = ibis.window(
            group_by=timeline[point_unit],
            order_by=[timeline[point_time], timeline[kind_name]],
            rows=(None, 0),
        )
        timeline = timeline.mutate(
            **{
                marker_name: cast(
                    ir.StructColumn,
                    ibis.ifelse(timeline[kind_name] == 1, timeline[point_time], None),
                )
                .max()
                .over(frame)
            }
        )
        markers = timeline.filter(timeline[kind_name] == 0).select(
            **{
                point_unit: timeline[point_unit],
                marker_name: timeline[point_time].notnull().ifelse(timeline[marker_name], None),
                **{
                    timeline_points[name]: timeline[timeline_points[name]]
                    for name in points.columns
                },
            }
        )
        joined = markers.left_join(
            right,
            [
                markers[point_unit] == right[state_unit],
                markers[marker_name] == right[state_time],
            ],
        )

    matched = joined[state_time].notnull()
    projection = {
        name: joined[name] if strategy == "native" else joined[timeline_points[name]]
        for name in points.columns
    }
    projection[matched_name] = matched
    for field in state_fields:
        value = joined[state_aliases[field]]
        dtype = prefix[field].type()
        if field == "value_scale":
            output = value
        elif _numeric(dtype):
            output = matched.ifelse(value, ibis.literal(0).cast(dtype))
        elif dtype.is_boolean():
            output = matched.ifelse(value, ibis.literal(False))
        else:
            output = value
        projection[output_aliases[field]] = output
    table = joined.select(**projection)
    return PrefixLookup(
        table=table,
        columns=MappingProxyType(output_aliases),
        matched_column=matched_name,
    )
