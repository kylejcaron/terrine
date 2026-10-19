"""Lazy candidate-anchor compilation over bound Catalog inputs."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import pyarrow as pa

from terrine.errors import RefusalSpec
from terrine.model import Model
from terrine.semantic.models import TableRef
from terrine.spec.anchors import AnchorRange, EventAnchor, FixedDate, Schedule
from terrine.spec.design import Design
from terrine.spec.validate import _MISSING, _UNIT

__all__ = ["AnchorTable", "compile_anchors"]

_INVALID = RefusalSpec("terrine.compile.invalid_anchor", "Anchor compilation is invalid: {reason}.")
_INTEGRITY = RefusalSpec(
    "terrine.compile.anchor_integrity",
    "Candidate anchor integrity check failed ({check_key}).",
)
_TIMESTAMP_PRECISION = RefusalSpec(
    "terrine.compile.anchor_timestamp_precision",
    "Anchor timestamp column {column!r} contains submicrosecond values that cannot be "
    "represented exactly as UTC; normalize those values to microseconds.",
)


_UTC_CONVERSION_UNSUPPORTED = RefusalSpec(
    "terrine.compile.anchor_utc_conversion_unsupported",
    "Exact conversion of naive timestamps to UTC is unsupported for backend {backend!r}.",
)


@dataclass(frozen=True, slots=True)
class _AnchorCheck:
    query: ibis.Table
    refusal: RefusalSpec
    context: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class AnchorTable:
    """Lazy candidate relation and deferred source-integrity validation."""

    table: ibis.Table
    _checks: tuple[_AnchorCheck, ...]

    def validate(self, *, execute: Callable[[ibis.Table], pa.Table]) -> None:
        """Execute aggregate checks through the caller's pinned Arrow executor."""
        if not callable(execute):
            raise TypeError("execute must be callable")
        for check in self._checks:
            result = execute(check.query)
            if (
                not isinstance(result, pa.Table)
                or result.num_rows != 1
                or result.column_names != ["invalid_count"]
            ):
                raise _INTEGRITY.error(check_key="invalid_executor_result")
            count = result.column(0)[0].as_py()
            if type(count) is not int or count < 0:
                raise _INTEGRITY.error(check_key="invalid_executor_result")
            if count:
                raise check.refusal.error(**dict(check.context))


def _integrity_check(key: str, query: ibis.Table) -> _AnchorCheck:
    return _AnchorCheck(query, _INTEGRITY, (("check_key", key),))


def _null_check(table: ibis.Table, column: str, key: str) -> _AnchorCheck:
    query = table.filter(table[column].isnull()).aggregate(invalid_count=ibis._.count())
    return _integrity_check(key, query)


def _unique_checks(
    table: ibis.Table,
    column: str,
    entity: str,
    *,
    scope: tuple[str, ...] = (),
) -> list[_AnchorCheck]:
    null_key = "null_roster_key" if entity == "roster" else "null_event_id"
    nulls = _null_check(table, column, null_key)
    # Isolate physical names before grouping; event IDs may be local to a unit.
    keys = {"_anchor_check_key": table[column]}
    keys.update({f"_anchor_check_scope_{index}": table[name] for index, name in enumerate(scope)})
    projected = table.select(**keys)
    duplicates = (
        projected.group_by(*keys)
        .aggregate(_anchor_check_count=ibis._.count())
        .filter(ibis._._anchor_check_count > 1)
        .aggregate(invalid_count=ibis._.count())
    )
    duplicate_key = "duplicate_roster_key" if entity == "roster" else "duplicate_event_id"
    return [nulls, _integrity_check(duplicate_key, duplicates)]


def _relation(model: Model, source: Any) -> Any:
    return model._relation(source.relation)


def _bound(model: Model, value: date | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    return model.day_boundary(value).astimezone(UTC)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _precision_check(
    model: Model, table: ibis.Table, column: str, *, source_column: str | None = None
) -> _AnchorCheck | None:
    dtype = table.schema()[column]
    assert isinstance(dtype, dt.Timestamp)
    if dtype.scale is None or dtype.scale <= 6:
        return None
    if model.catalog.dialect.casefold() != "duckdb":
        raise _UTC_CONVERSION_UNSUPPORTED.error(backend=model.catalog.dialect)
    relation_sql = ibis.to_sql(table, dialect="duckdb")
    col = f"source.{_quote(column)}"
    query = model._sql(
        f"SELECT count(*) AS invalid_count FROM ({relation_sql}) AS source "
        f"WHERE {col} IS NOT NULL AND epoch_ns({col}) % 1000 <> 0"
    )
    reported_column = source_column if source_column is not None else column
    return _AnchorCheck(query, _TIMESTAMP_PRECISION, (("column", reported_column),))


def _utc_column(model: Model, table: Any, column: str, output: str) -> Any:
    """Append UTC conversion; caller defers submicrosecond rejection."""
    dtype = table.schema()[column]
    if dtype.timezone is not None:
        return table.mutate(**{output: table[column].cast("timestamp('UTC')")})
    if model.catalog.dialect.casefold() != "duckdb":
        raise _UTC_CONVERSION_UNSUPPORTED.error(backend=model.catalog.dialect)
    relation_sql = ibis.to_sql(table, dialect="duckdb")
    expression = f"timezone('UTC', source.{_quote(column)})"
    return model._sql(
        f"SELECT source.*, {expression} AS {_quote(output)} FROM ({relation_sql}) AS source"
    )


def _range_filter_raw(
    model: Model, table: Any, column: str, anchor_range: AnchorRange | None
) -> Any:
    if anchor_range is None:
        return table
    dtype = table.schema()[column]
    naive = dtype.timezone is None
    if naive and model.catalog.dialect.casefold() != "duckdb":
        raise _UTC_CONVERSION_UNSUPPORTED.error(backend=model.catalog.dialect)
    start, end = _bound(model, anchor_range.start), _bound(model, anchor_range.end)
    for bound, lower in ((start, True), (end, False)):
        if bound is None:
            continue
        value = bound.replace(tzinfo=None) if naive else bound
        timestamp = table[column]
        if dtype.scale is not None and dtype.scale > 6:
            delta = bound - datetime(1970, 1, 1, tzinfo=UTC)
            nanoseconds = (
                (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
            ) * 1000
            if not -(2**63) <= nanoseconds < 2**63:
                # All finite NS instants lie on the same side of this bound.
                # Widening here also preserves native timestamp infinities.
                timestamp = timestamp.cast(dt.Timestamp(timezone=dtype.timezone, scale=6))
        cutoff = ibis.literal(value)
        table = table.filter(timestamp >= cutoff if lower else timestamp < cutoff)
    return table


def _constant_table(t0: datetime, anchor_id: str, provenance: str) -> Any:
    return (
        ibis.literal(1)
        .as_table()
        .select(
            unit=ibis.literal(None, type="string"),
            anchor_id=ibis.literal(anchor_id),
            t0=ibis.literal(t0),
            anchor_provenance=ibis.literal(provenance),
        )
    )


def _range_filter(table: Any, anchor_range: AnchorRange | None, model: Model) -> Any:
    if anchor_range is None:
        return table
    start, end = _bound(model, anchor_range.start), _bound(model, anchor_range.end)
    if start is not None:
        table = table.filter(table.t0 >= ibis.literal(start))
    if end is not None:
        table = table.filter(table.t0 < ibis.literal(end))
    return table


def _roster_candidates(
    model: Model,
    unit_source: Any,
    anchor_rows: Any,
    *,
    filter_range: AnchorRange | None,
    include_membership: bool = True,
    match_unit: bool = True,
) -> tuple[ibis.Table, list[_AnchorCheck]]:
    raw = model._relation(TableRef(kind="table", name=unit_source.table))
    checks = _unique_checks(raw, unit_source.key, "roster")
    for column in dict.fromkeys(
        (
            unit_source.entered_at,
            unit_source.exited_at,
            unit_source.observed_from,
            unit_source.observed_until,
        )
    ):
        if column is not None:
            check = _precision_check(model, raw, column)
            if check is not None:
                checks.append(check)

    projection: dict[str, Any] = {"unit": raw[unit_source.key]}
    for name, column in (
        ("entered_raw", unit_source.entered_at),
        ("exited_raw", unit_source.exited_at),
        ("observed_from_raw", unit_source.observed_from),
        ("observed_until_raw", unit_source.observed_until),
    ):
        if column is not None:
            projection[name] = raw[column]
    roster = raw.select(**projection)
    for raw_name, utc_name in (
        ("entered_raw", "entered_utc"),
        ("exited_raw", "exited_utc"),
        ("observed_from_raw", "observed_from_utc"),
        ("observed_until_raw", "observed_until_utc"),
    ):
        if raw_name in roster.columns:
            roster = _utc_column(model, roster, raw_name, utc_name)

    if match_unit:
        candidates = anchor_rows.left_join(roster, anchor_rows.unit == roster.unit)
        unit_value = anchor_rows.unit
    else:
        candidates = roster.cross_join(anchor_rows)
        unit_value = roster.unit
    if include_membership and "entered_utc" in roster.columns:
        entered = roster.entered_utc
        candidates = candidates.filter(entered.isnull() | (entered <= candidates.t0))
    if include_membership and "exited_utc" in roster.columns:
        exited = roster.exited_utc
        candidates = candidates.filter(exited.isnull() | (candidates.t0 < exited))
    observed_from = (
        roster.observed_from_utc
        if "observed_from_utc" in roster.columns
        else ibis.literal(None, type="timestamp('UTC')")
    )
    observed_until = (
        roster.observed_until_utc
        if "observed_until_utc" in roster.columns
        else ibis.literal(None, type="timestamp('UTC')")
    )
    result = candidates.select(
        unit=unit_value,
        anchor_id=candidates.anchor_id,
        t0=candidates.t0,
        _observed_from=observed_from,
        _observed_until=observed_until,
        anchor_provenance=candidates.anchor_provenance,
    )
    return _range_filter(result, filter_range, model), checks


def _roster_anchors(
    model: Model, design: Design, *, include_membership: bool
) -> tuple[ibis.Table, list[_AnchorCheck]]:
    unit_source = model.units.get(design.unit)
    if unit_source is None:
        raise _UNIT.error(unit=design.unit, name="roster")
    if isinstance(design.anchor, FixedDate):
        instant = _bound(model, design.anchor.date)
        assert instant is not None
        base = _constant_table(instant, "fixed", "fixed_date")
    else:
        schedule = design.anchor
        assert isinstance(schedule, Schedule)
        start = model.day_boundary(schedule.start).astimezone(UTC)
        end = model.day_boundary(schedule.end).astimezone(UTC)
        elapsed_seconds = int((end - start).total_seconds())
        if elapsed_seconds <= 0:
            raise _INVALID.error(reason="schedule interval has no elapsed time")
        stop = (elapsed_seconds + schedule.every - 1) // schedule.every
        if model.catalog.dialect.casefold() != "duckdb":
            raise _INVALID.error(
                reason="lazy Schedule expansion currently requires DuckDB range support"
            )
        sequence = model._sql(f"SELECT unnest(range(0, {stop})) AS ordinal")
        ordinal = sequence.ordinal
        t0 = ibis.literal(start) + ibis.interval(seconds=schedule.every) * ordinal
        base = sequence.select(
            unit=ibis.literal(None, type="string"),
            anchor_id=cast(ir.StringValue, ibis.literal("schedule:")).concat(
                ordinal.cast("string")
            ),
            t0=t0,
            anchor_provenance=ibis.literal("schedule"),
        ).filter(t0 < ibis.literal(end))
    return _roster_candidates(
        model,
        unit_source,
        base,
        filter_range=design.anchor_range,
        match_unit=False,
        include_membership=include_membership,
    )


def _event_anchors(model: Model, design: Design) -> tuple[ibis.Table, list[_AnchorCheck]]:
    anchor = design.anchor
    assert isinstance(anchor, EventAnchor)
    fact = model.catalog.facts.get(anchor.fact)
    if fact is None:
        raise _MISSING.error(kind="fact", name=anchor.fact)
    source = model.sources.get(fact.source)
    if source is None:
        raise _MISSING.error(kind="event source", name=fact.source)
    if anchor.fact not in source.facts:
        raise _MISSING.error(kind="bound fact", name=anchor.fact)
    physical_unit = source.entities.get(design.unit)
    if physical_unit is None:
        raise _UNIT.error(unit=design.unit, name=anchor.fact)
    events = _relation(model, source)
    event_id_column = model.event_ids.get(source.name)

    identity_column = event_id_column or source.timestamp_column
    identity_dtype = events.schema()[identity_column]
    identity_text = None
    if identity_dtype.is_timestamp() and identity_dtype.timezone is not None:
        if model.catalog.dialect.casefold() != "duckdb":
            raise _UTC_CONVERSION_UNSUPPORTED.error(backend=model.catalog.dialect)
        alias = "_anchor_identity_text"
        names = {name.casefold() for name in events.columns}
        while alias.casefold() in names:
            alias = f"_{alias}"
        relation_sql = ibis.to_sql(events, dialect="duckdb")
        events = model._sql(
            f"SELECT source.*, CAST(timezone('UTC', source.{_quote(identity_column)}) "
            f"AS VARCHAR) AS {_quote(alias)} FROM ({relation_sql}) AS source"
        )
        identity_text = events[alias]
    raw_timestamp = events[source.timestamp_column]
    checks: list[_AnchorCheck] = [
        _null_check(events, physical_unit, "null_event_unit"),
        _null_check(events, source.timestamp_column, "null_event_timestamp"),
    ]

    if event_id_column is not None:
        event_id = events[event_id_column]
        checks.extend(_unique_checks(events, event_id_column, "event_id", scope=(physical_unit,)))
        dtype = events.schema()[event_id_column]
        text = identity_text if identity_text is not None else event_id.cast("string")
        type_tag = str(dtype)
    else:
        event_id = raw_timestamp
        dtype = events.schema()[source.timestamp_column]
        text = identity_text if identity_text is not None else raw_timestamp.cast("string")
        type_tag = str(dtype)
    identity = cast(ir.StringValue, ibis.literal(f"event:{len(type_tag)}:{type_tag}:")).concat(
        text.length().cast("string"), ":", text
    )
    base = events.select(
        unit=events[physical_unit],
        anchor_id=identity,
        _raw_t0=raw_timestamp,
        _event_id=event_id,
        anchor_provenance=ibis.literal(f"event:{anchor.fact}:{anchor.which}"),
    )
    if anchor.which in ("first", "nth"):
        rank = (
            ibis.row_number().over(group_by=[base.unit], order_by=[base._raw_t0, base._event_id])
            + 1
        )
        selected = 1 if anchor.which == "first" else anchor.n
        base = base.mutate(_anchor_ordinal=rank)
        base = base.filter(base._anchor_ordinal == selected)
        if event_id_column is None:
            keys = events.select(
                _anchor_check_unit=events[physical_unit],
                _anchor_check_timestamp=raw_timestamp,
            )
            ties = (
                keys.group_by("_anchor_check_unit", "_anchor_check_timestamp")
                .aggregate(_anchor_check_count=ibis._.count())
                .filter(ibis._._anchor_check_count > 1)
                .aggregate(invalid_count=ibis._.count())
            )
            checks.append(
                _integrity_check(
                    "tied_event_timestamps",
                    ties,
                )
            )
    base = _range_filter_raw(model, base, "_raw_t0", design.anchor_range)
    precision = _precision_check(model, base, "_raw_t0", source_column=source.timestamp_column)
    if precision is not None:
        checks.append(precision)
    base = _utc_column(model, base, "_raw_t0", "t0")
    if design.unit in model.units:
        candidates, roster_checks = _roster_candidates(
            model, model.units[design.unit], base, filter_range=None
        )
        checks.extend(roster_checks)
    else:
        candidates = base.select(
            unit=base.unit,
            anchor_id=base.anchor_id,
            t0=base.t0,
            _observed_from=ibis.literal(None, type="timestamp('UTC')"),
            _observed_until=ibis.literal(None, type="timestamp('UTC')"),
            anchor_provenance=base.anchor_provenance,
        )
    duplicates = (
        candidates.group_by([candidates.unit, candidates.anchor_id])
        .aggregate(_anchor_check_count=ibis._.count())
        .filter(ibis._._anchor_check_count > 1)
        .aggregate(invalid_count=ibis._.count())
    )
    checks.append(_integrity_check("ambiguous_event_identity", duplicates))
    return candidates, checks


def compile_anchors(
    design: Design,
    model: Model,
    *,
    include_membership: bool = True,
    persisted: ibis.Table | None = None,
) -> AnchorTable:
    """Build or validate a lazy candidate relation without executing data-dependent checks."""
    if not isinstance(design, Design) or not isinstance(model, Model):
        raise TypeError("compile_anchors requires a Design and a bound Model")
    from terrine.execution.backend import backend_provider

    backend_provider(model.con).capabilities.require(
        "exact_timestamps",
        backend=model.catalog.dialect,
        route="Use verified native timestamp semantics before compiling dataset anchors.",
    )
    if persisted is not None:
        if isinstance(design.anchor, (FixedDate, Schedule)):
            _, source_checks = _roster_anchors(model, design, include_membership=include_membership)
        elif isinstance(design.anchor, EventAnchor):
            _, source_checks = _event_anchors(model, design)
        else:
            raise _INVALID.error(reason="unsupported anchor variant")
        if not isinstance(persisted, ibis.Table):
            raise TypeError("persisted anchor relation must be an Ibis table")
        required = {
            "unit",
            "anchor_id",
            "t0",
            "_observed_from",
            "_observed_until",
            "anchor_provenance",
        }
        missing = required.difference(persisted.columns)
        if missing:
            raise ValueError(f"persisted anchor relation is missing columns {sorted(missing)!r}")
        timestamp_type = persisted.schema()["t0"]
        if not isinstance(timestamp_type, dt.Timestamp) or timestamp_type.timezone != "UTC":
            raise ValueError("persisted anchor timestamp must be UTC")
        checks = list(source_checks)
        checks.extend(_unique_checks(persisted, "anchor_id", "event", scope=("unit",)))
        checks.extend(
            (
                _null_check(persisted, "unit", "null_unit"),
                _null_check(persisted, "t0", "null_anchor_time"),
            )
        )
        return AnchorTable(table=persisted, _checks=tuple(checks))
    if isinstance(design.anchor, (FixedDate, Schedule)):
        table, checks = _roster_anchors(model, design, include_membership=include_membership)
    elif isinstance(design.anchor, EventAnchor):
        table, checks = _event_anchors(model, design)
    else:
        raise _INVALID.error(reason="unsupported anchor variant")
    return AnchorTable(table=table, _checks=tuple(checks))
