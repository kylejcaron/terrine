"""First eligible event occurrences for candidate half-open windows."""

from __future__ import annotations

import ibis
import ibis.expr.datatypes as dt

from terrine.data import ColumnMeta
from terrine.errors import RefusalSpec
from terrine.temporal.aggregate import TemporalResult, _Check, _Failure

__all__ = ["first_occurrence"]

_DUPLICATE_EVENT_ID = RefusalSpec(
    "terrine.temporal.duplicate_event_id",
    "Event identifiers must be non-null and unique before first-occurrence selection.",
)
_UNSUPPORTED_BACKEND = RefusalSpec(
    "terrine.temporal.first_occurrence_unsupported_backend",
    "First-occurrence point lookup requires the DuckDB backend; got {backend!r}.",
)
_INVALID = RefusalSpec(
    "terrine.temporal.first_occurrence_invalid_relation",
    "First-occurrence input relation is invalid ({reason}).",
)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _validate_timestamp(table: ibis.Table, column: str, name: str) -> dt.Timestamp:
    if column not in table.columns:
        raise _INVALID.error(reason=f"missing_{name}_column")
    dtype = table.schema()[column]
    if not isinstance(dtype, dt.Timestamp) or dtype.timezone not in (None, "UTC"):
        raise _INVALID.error(reason=f"{name}_must_be_utc_timestamp")
    if dtype.scale not in (6, 9):
        raise _INVALID.error(reason=f"{name}_timestamp_precision_unsupported")
    return dtype


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

    Candidate rows are retained even when no event qualifies. The point lookup is
    implemented as a correlated ordered limit, avoiding candidate-by-event fanout.
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
    lower_type = _validate_timestamp(candidates, lower, "lower_bound")
    upper_type = _validate_timestamp(candidates, upper, "upper_bound")
    if lower_type != upper_type:
        raise _INVALID.error(reason="candidate_window_timestamp_types_differ")
    if timestamp not in events.columns:
        raise _INVALID.error(reason="missing_event_timestamp_column")
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
    if backend_name != "duckdb" or candidate_backend is not backend:
        raise _UNSUPPORTED_BACKEND.error(backend=backend_name)

    used = {name.casefold() for name in candidates.columns}
    for name in ("first_timestamp", "first_event_id", "no_event"):
        if name.casefold() in used:
            raise _INVALID.error(reason=f"output_column_collision_{name}")
    event_id_projection = (
        f", selected.{_quote(event_id)} AS {_quote('first_event_id')}" if event_id else ""
    )
    tie_order = f", event.{_quote(event_id)} ASC" if event_id else ""
    events_sql = ibis.to_sql(events, dialect="duckdb")
    candidates_sql = ibis.to_sql(candidates, dialect="duckdb")
    event_unit = _quote(unit)
    candidate_unit = _quote(unit)
    event_time = _quote(timestamp)
    candidate_lower = _quote(lower)
    candidate_upper = _quote(upper)
    sql = (
        f"SELECT candidate.*, selected.{event_time} AS {_quote('first_timestamp')}"
        f"{event_id_projection}, selected.{event_time} IS NULL AS {_quote('no_event')} "
        f"FROM ({candidates_sql}) AS candidate "
        f"LEFT JOIN LATERAL (SELECT event.{event_time}"
        f"{', event.' + _quote(event_id) if event_id else ''} "
        f"FROM ({events_sql}) AS event "
        f"WHERE event.{event_unit} = candidate.{candidate_unit} "
        f"AND event.{event_time} >= candidate.{candidate_lower} "
        f"AND event.{event_time} < candidate.{candidate_upper} "
        f"ORDER BY event.{event_time} ASC{tie_order} LIMIT 1) AS selected ON TRUE"
    )
    result = backend.sql(sql)
    checks: tuple[_Check, ...] = ()
    if event_id is not None:
        projected = events.select(_event_id=events[event_id])
        duplicates = (
            projected.group_by("_event_id")
            .aggregate(_count=ibis._.count())
            .filter(ibis._._event_id.isnull() | (ibis._._count > 1))
            .aggregate(_duplicate_count=ibis._.count().cast("string"))
        )
        checks = (
            _Check(
                duplicates,
                (_Failure("_duplicate_count", _DUPLICATE_EVENT_ID, {}),),
            ),
        )
    meta = ColumnMeta(None, "event", None, None, None, None)
    return TemporalResult(
        result,
        {"first_timestamp": meta},
        checks,
    )
