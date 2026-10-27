"""First eligible event occurrences for candidate half-open windows."""

from __future__ import annotations

from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.dialect import dialect_of
from terrine.backends.joins import asof_join
from terrine.backends.timestamps import (
    _struct_key,
    lossless_timestamp,
    timestamp_order_key,
    timestamp_parts,
)
from terrine.data import ColumnMeta
from terrine.errors import RefusalSpec
from terrine.temporal.aggregate import TemporalResult, _Check, _Failure

__all__ = ["first_occurrence"]

_DUPLICATE_EVENT_ID = RefusalSpec(
    "terrine.temporal.duplicate_event_id",
    "Event identifiers must be non-null and unique within each unit.",
)
_UNSUPPORTED_BACKEND = RefusalSpec(
    "terrine.temporal.first_occurrence_unsupported_backend",
    "First-occurrence point lookup requires the DuckDB, PostgreSQL, BigQuery or "
    "Snowflake backend; got {backend!r}.",
)
_SUPPORTED_BACKENDS = frozenset({"duckdb", "postgres", "bigquery", "snowflake"})
_INVALID = RefusalSpec(
    "terrine.temporal.first_occurrence_invalid_relation",
    "First-occurrence input relation is invalid ({reason}).",
)


def _validate_timestamp(table: ibis.Table, column: str, name: str) -> dt.Timestamp:
    if column not in table.columns:
        raise _INVALID.error(reason=f"missing_{name}_column")
    dtype = table.schema()[column]
    if not isinstance(dtype, dt.Timestamp) or dtype.timezone not in (None, "UTC"):
        raise _INVALID.error(reason=f"{name}_must_be_utc_timestamp")
    if dtype.scale is not None and dtype.scale > 9:
        raise _INVALID.error(reason=f"{name}_timestamp_precision_unsupported")
    return dtype


def _lossless(table: ibis.Table, *columns: str) -> ibis.Table:
    """``table`` with each timestamp column at the supported scale holding it
    exactly: a scale below 6 promotes to microseconds, 7 or 8 to nanoseconds."""
    promoted = {
        column: value
        for column in columns
        if (value := lossless_timestamp(table[column])).type() != table.schema()[column]
    }
    return table.mutate(**promoted) if promoted else table


def _reverse_time_key(value: ir.TimestampValue) -> ir.Value:
    """A key whose ascending order is the instants' descending order."""
    dialect = dialect_of(value)
    if dialect in {"postgres", "bigquery", "snowflake"}:
        # One exact scalar: finite instants' nanoseconds stay far below 10**38.
        rank, seconds, nanos = timestamp_parts(value, backend=dialect)
        wide = dt.Decimal(38, 0)
        billion = cast(ir.DecimalValue, ibis.literal(10**9, type=wide))
        rank_weight = cast(ir.DecimalValue, ibis.literal(10**23, type=wide))
        coordinate = cast(ir.DecimalValue, seconds.cast(wide)) * billion + cast(
            ir.DecimalValue, nanos.cast(wide)
        )
        return cast(ir.DecimalValue, ibis.literal(0, type=wide)) - (
            cast(ir.DecimalValue, rank.cast(wide)) * rank_weight + coordinate
        )
    key = _struct_key(value)
    return ibis.struct(
        {
            "rank": -cast(ir.IntegerValue, key["rank"]),
            "seconds": -cast(ir.IntegerValue, key["seconds"]),
            "nanos": -cast(ir.IntegerValue, key["nanos"]),
        }
    )


def first_occurrence(
    events: ibis.Table,
    candidates: ibis.Table,
    *,
    unit: str,
    timestamp: str,
    lower: str,
    upper: str,
    event_id: str | None = None,
) -> TemporalResult:
    """Select the earliest event in each candidate's exact ``[lower, upper)`` window.

    Candidate rows are retained even when no event qualifies. A reverse-time
    as-of point lookup avoids a candidate-by-event interval join.
    """
    if not isinstance(events, ibis.Table) or not isinstance(candidates, ibis.Table):
        raise TypeError("events and candidates must be Ibis tables")
    for relation, column, label in (
        (events, unit, "event_unit"),
        (candidates, unit, "candidate_unit"),
        (candidates, "anchor_id", "candidate_anchor_id"),
        (candidates, lower, "candidate_lower"),
        (candidates, upper, "candidate_upper"),
    ):
        if not isinstance(column, str) or column not in relation.columns:
            raise _INVALID.error(reason=f"missing_{label}_column")
    _validate_timestamp(events, timestamp, "event")
    _validate_timestamp(candidates, lower, "lower_bound")
    _validate_timestamp(candidates, upper, "upper_bound")
    events = _lossless(events, timestamp)
    candidates = _lossless(candidates, lower, upper)
    if event_id is not None:
        if not isinstance(event_id, str) or event_id not in events.columns:
            raise _INVALID.error(reason="missing_event_id_column")
        if event_id.casefold() in {name.casefold() for name in candidates.columns}:
            raise _INVALID.error(reason="event_id_collides_with_candidate_column")

    try:
        backend = events._find_backend(use_default=False)
        candidate_backend = candidates._find_backend(use_default=False)
        backend_name = backend.name
    except Exception as exc:
        raise _UNSUPPORTED_BACKEND.error(backend="unbound") from exc
    if backend_name not in _SUPPORTED_BACKENDS or candidate_backend is not backend:
        raise _UNSUPPORTED_BACKEND.error(backend=backend_name)

    used = {name.casefold() for name in candidates.columns}
    for name in ("first_timestamp", "first_event_id", "no_event"):
        if name.casefold() in used:
            raise _INVALID.error(reason=f"output_column_collision_{name}")

    def fresh_name(base: str) -> str:
        candidate = base
        suffix = 0
        while candidate.casefold() in used:
            suffix += 1
            candidate = f"{base}_{suffix}"
        used.add(candidate.casefold())
        return candidate

    first_lower_key = fresh_name("__terrine_first_lower_key")
    event_unit = fresh_name("__terrine_event_unit")
    event_key = fresh_name("__terrine_event_key")
    event_time = fresh_name("__terrine_event_time")
    event_identifier = fresh_name("__terrine_event_id")
    event_value_id = fresh_name("__terrine_event_id_value")

    checks: tuple[_Check, ...] = ()
    if event_id is not None:
        projected = events.select(_unit=events[unit], _event_id=events[event_id])
        duplicates = (
            projected.group_by("_unit", "_event_id")
            .aggregate(_count=ibis._.count())
            .filter(ibis._._event_id.isnull() | (ibis._._count > 1))
            .aggregate(_duplicate_count=ibis._.count().cast("string"))
        )
        checks = (_Check(duplicates, (_Failure("_duplicate_count", _DUPLICATE_EVENT_ID, {}),)),)

    events_view = events.view()
    event_columns = {event_unit: events_view[unit], event_time: events_view[timestamp]}
    if event_id is None:
        event_relation = (
            events_view.filter(events_view[timestamp].notnull()).select(**event_columns).distinct()
        )
        event_relation = event_relation.mutate(**{event_identifier: ibis.null().cast("string")})
    else:
        event_columns[event_value_id] = events_view[event_id]
        tie_rank = fresh_name("__terrine_event_tie_rank")
        ranked = events_view.filter(events_view[timestamp].notnull()).select(**event_columns)
        # Ordered numbering, not MIN: PostgreSQL orders UUIDs but has no min(uuid).
        ranked = ranked.mutate(
            **{
                tie_rank: ibis.row_number().over(
                    ibis.window(
                        group_by=[ranked[event_unit], ranked[event_time]],
                        order_by=[ranked[event_value_id]],
                    )
                )
            }
        )
        event_relation = ranked.filter(ranked[tie_rank] == 0).select(
            **{
                event_unit: ranked[event_unit],
                event_time: ranked[event_time],
                event_identifier: ranked[event_value_id],
            }
        )

    candidates_view = candidates.view()
    left = candidates_view.select(
        **{name: candidates_view[name] for name in candidates_view.columns},
        **{first_lower_key: _reverse_time_key(candidates_view[lower])},
    )
    events_view = event_relation.view()
    right = events_view.select(
        **{
            event_unit: events_view[event_unit],
            event_key: _reverse_time_key(events_view[event_time]),
            event_time: events_view[event_time],
            event_identifier: events_view[event_identifier],
        }
    )
    joined = asof_join(
        left,
        right,
        left_unit=unit,
        left_key=first_lower_key,
        right_unit=event_unit,
        right_key=event_key,
        inclusive=True,
    )
    upper_value = joined[upper]
    eligible = (
        joined[event_time].notnull()
        & joined[lower].notnull()
        & upper_value.notnull()
        & (timestamp_order_key(joined[event_time]) < timestamp_order_key(upper_value))
    )
    output = {name: joined[name] for name in candidates.columns}
    output["first_timestamp"] = eligible.ifelse(
        joined[event_time],
        ibis.null().cast(event_relation[event_time].type()),
    )
    if event_id is not None:
        output["first_event_id"] = eligible.ifelse(
            joined[event_identifier],
            ibis.null().cast(event_relation[event_identifier].type()),
        )
    output["no_event"] = ~eligible
    result = joined.select(**output)
    meta = ColumnMeta(None, "event", None, None, None, None)
    return TemporalResult(result, {"first_timestamp": meta}, checks)
