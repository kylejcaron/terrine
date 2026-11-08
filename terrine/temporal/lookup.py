"""Strict point lookups over coherent sparse temporal states."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.dialect import current_dialect
from terrine.backends.joins import has_native_asof, union_unit_keys
from terrine.backends.numeric import is_exact_type
from terrine.backends.timestamps import lookup_keys

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
    fields: Sequence[str] | None = None,
) -> PrefixLookup:
    """Attach the latest sparse state strictly before each point cutoff.

    Prefix keys are the canonical ``_unit`` and ``_timestamp`` fields. The
    producer must supply unique, non-null, timezone-compatible state keys.
    ``fields`` names the state columns to carry; by default every non-key
    column is carried.
    """
    if strategy not in {"native", "union_window"}:
        raise ValueError("strategy must be 'native' or 'union_window'")
    if strategy == "native" and not has_native_asof(points[timestamp_column]):
        strategy = "union_window"
    for column in (unit_column, timestamp_column):
        if column not in points.columns:
            raise ValueError(f"point column {column!r} is missing")
    for column in ("_unit", "_timestamp"):
        if column not in prefix.columns:
            raise ValueError(f"prefix column {column!r} is missing")
    state_fields = [name for name in prefix.columns if name not in {"_unit", "_timestamp"}]
    if fields is not None:
        for name in fields:
            if name not in state_fields:
                raise ValueError(f"prefix state field {name!r} is missing")
        state_fields = list(fields)

    # Each cutoff needs a distinct relation, even when points already contain this prefix.
    prefix = prefix.view()
    point_key, state_key = lookup_keys(points[timestamp_column], prefix._timestamp)
    used = {name.casefold() for name in (*points.columns, *prefix.columns)}
    # Short internal aliases: each is listed at every level of the statement.
    matched_name = _allocate(used, "__m")
    output_aliases = {
        field: _allocate(used, f"__l{index}") for index, field in enumerate(state_fields)
    }

    if strategy == "native":
        # Successive lookups over the same points join into one chain of ASOF joins.
        # A NULL cutoff matches no state; an engine may order a NULL order key last.
        joined = points.asof_join(
            prefix,
            on=cast(ir.BooleanColumn, point_key > state_key),
            predicates=[
                points[unit_column] == prefix._unit,
                points[timestamp_column].notnull() == prefix._timestamp.notnull(),
            ],
        )
        projection = {name: points[name] for name in points.columns}
        found = prefix._timestamp
        states = {field: prefix[field] for field in state_fields}
    else:
        point_unit = _allocate(used, "__pu")
        point_time = _allocate(used, "__pt")
        state_unit = _allocate(used, "__su")
        state_time = _allocate(used, "__st")
        kind_name = _allocate(used, "__k")
        marker_name = _allocate(used, "__mk")
        timeline_points = {
            name: _allocate(used, f"__p{index}") for index, name in enumerate(points.columns)
        }
        state_aliases = {
            field: _allocate(used, f"__s{index}") for index, field in enumerate(state_fields)
        }
        right = prefix.select(
            **{state_unit: prefix._unit, state_time: state_key},
            **{state_aliases[name]: prefix[name] for name in state_fields},
        )
        left_unit_key, right_unit_key = union_unit_keys(points[unit_column], right[state_unit])
        policy = current_dialect(points[timestamp_column], prefix._timestamp)
        states = right.select(
            **{
                point_unit: right_unit_key,
                point_time: right[state_time],
                kind_name: ibis.literal(1).cast("int8"),
                **{
                    timeline_points[name]: policy.typed_null(points[name].type())
                    for name in points.columns
                },
            }
        )
        requests = points.select(
            **{
                point_unit: left_unit_key,
                point_time: point_key,
                kind_name: ibis.literal(0).cast("int8"),
                **{timeline_points[name]: points[name] for name in points.columns},
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
                    ir.Column, ibis.ifelse(timeline[kind_name] == 1, timeline[point_time], None)
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
        projection = {name: joined[timeline_points[name]] for name in points.columns}
        found = joined[state_time]
        states = {field: joined[state_aliases[field]] for field in state_fields}

    matched = found.notnull()
    projection[matched_name] = matched
    for field in state_fields:
        value = states[field]
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
