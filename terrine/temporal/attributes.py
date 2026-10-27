"""Lazy SCD attribute lookup and explicit dimension-history integrity checks."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Mapping
from typing import Any, cast

import ibis
import ibis.expr.types as ir
import pyarrow as pa

from terrine.backends.timestamps import compare_timestamps, timestamp_order_key
from terrine.errors import RefusalSpec
from terrine.semantic.bind import _MISSING, _SCHEMA
from terrine.semantic.models import BoundDimension

__all__ = ["attribute_asof", "enrich_events", "history_checks", "validate_history"]

_ALIAS_COLLISION = RefusalSpec(
    "terrine.temporal.alias_collision",
    "Temporal output alias conflicts with an existing point column.",
)
_MISSING_KEY = RefusalSpec(
    "terrine.temporal.missing_join_key",
    "Dimension {dimension!r} cannot resolve its declared event-side entity key.",
)
_INVALID_HISTORY = RefusalSpec(
    "terrine.temporal.malformed_history",
    "Dimension history contains null keys, null starts, or malformed validity intervals.",
)
_DUPLICATE_HISTORY = RefusalSpec(
    "terrine.temporal.duplicate_history", "Dimension history contains duplicate entity/start keys."
)
_OVERLAPPING_HISTORY = RefusalSpec(
    "terrine.temporal.overlapping_history",
    "Dimension history contains overlapping validity intervals.",
)
_STATIC_MULTIPLICITY = RefusalSpec(
    "terrine.temporal.static_key_multiplicity",
    "Static dimension has multiple rows for an entity key.",
)


def _enriched_property_columns(
    dimensions: Iterable[BoundDimension], raw_columns: Iterable[str]
) -> dict[tuple[str, str], tuple[str, str]]:
    """Resolve ``(dimension, property)`` to deterministic ``(value, found)`` columns.

    Existing physical columns and all generated aliases are reserved case-insensitively.
    """
    reserved = {column.casefold() for column in raw_columns}
    result: dict[tuple[str, str], tuple[str, str]] = {}
    for dimension in sorted(dimensions, key=lambda item: item.name.casefold()):
        for name in sorted(dimension.properties, key=str.casefold):
            key = (dimension.name, name)
            digest = hashlib.sha256((dimension.name + "\0" + name).encode("utf-8")).hexdigest()
            attempt = 0
            while True:
                suffix = digest if attempt == 0 else f"{digest}_{attempt}"
                value_name = f"__terrine_attribute_{suffix}_value"
                found_name = f"__terrine_attribute_{suffix}_found"
                folded = (value_name.casefold(), found_name.casefold())
                if not any(alias in reserved for alias in folded):
                    break
                attempt += 1
            reserved.update(folded)
            result[key] = (value_name, found_name)
    return result


def _fresh_alias(prefix: str, reserved: set[str]) -> str:
    candidate = prefix
    suffix = 0
    while candidate.casefold() in reserved:
        suffix += 1
        candidate = f"{prefix}_{suffix}"
    reserved.add(candidate.casefold())
    return candidate


def _check_columns(table: ibis.Table, columns: Iterable[str], *, field: str) -> None:
    available = set(table.columns)
    for column in columns:
        if column not in available:
            raise _SCHEMA.error(field=field, column=column)


def _check_matching_types(
    left: ibis.Table, left_column: str, right: ibis.Table, right_column: str, *, field: str
) -> None:
    left_type = left[left_column].type().copy(nullable=True)
    right_type = right[right_column].type().copy(nullable=True)
    compatible = left_type == right_type
    if left_type.is_timestamp() and right_type.is_timestamp():
        compatible = True
    if not compatible:
        raise _SCHEMA.error(field=field, column=right_column)


def _validity_columns(dimension: BoundDimension) -> tuple[str | None, str | None]:
    validity = dimension.validity
    if validity is None:
        return None, None
    if validity.changed_at is not None:
        return validity.changed_at, None
    if validity.valid_from is None:
        raise _INVALID_HISTORY.error()
    return validity.valid_from, validity.valid_to


def _as_ranges(
    history: ibis.Table, dimension: BoundDimension
) -> tuple[ibis.Table, str | None, str | None]:
    start, end = _validity_columns(dimension)
    validity = dimension.validity
    if validity is None or validity.changed_at is None:
        return history, start, end
    reserved = {column.casefold() for column in history.columns}
    normalized_start = _fresh_alias("__terrine_valid_from", reserved)
    normalized_end = _fresh_alias("__terrine_valid_to", reserved)
    changed = validity.changed_at
    normalized = history.mutate(
        **{
            normalized_start: history[changed],
            normalized_end: history[changed]
            .lead()
            .over(
                group_by=[history[dimension.entity_column]],
                order_by=[timestamp_order_key(history[changed])],
            ),
        }
    )
    return normalized, normalized_start, normalized_end


def _point_output_aliases(points: ibis.Table) -> None:
    existing = {column.casefold() for column in points.columns}
    if "__terrine_attribute_value" in existing or "__terrine_attribute_found" in existing:
        raise _ALIAS_COLLISION.error()


def attribute_asof(
    points: ibis.Table,
    history: ibis.Table,
    dimension: BoundDimension,
    property_name: str,
    *,
    unit_column: str,
    cutoff_column: str,
    strict: bool = True,
    matched_at_column: str | None = None,
) -> ibis.Table:
    """Lazily attach one property's as-of value and a match flag to point rows."""
    _point_output_aliases(points)
    prop = dimension.properties.get(property_name)
    if prop is None:
        raise _MISSING.error(reference=f"property {property_name} dimension {dimension.name}")
    start, end = _validity_columns(dimension)
    _check_columns(points, (unit_column, cutoff_column), field="point")
    _check_columns(
        history,
        (
            dimension.entity_column,
            prop.column,
            *((start,) if start else ()),
            *((end,) if end else ()),
        ),
        field="dimension",
    )
    _check_matching_types(
        points, unit_column, history, dimension.entity_column, field="dimension entity"
    )
    if start:
        _check_matching_types(points, cutoff_column, history, start, field="dimension validity")
    if end:
        _check_matching_types(points, cutoff_column, history, end, field="dimension validity")
    normalized, start_column, end_column = _as_ranges(history, dimension)
    suffix = hashlib.sha256((dimension.name + "\0" + property_name).encode()).hexdigest()[:16]
    reserved = {column.casefold() for column in (*points.columns, *normalized.columns)}
    right_key = _fresh_alias(f"__terrine_join_{suffix}_key", reserved)
    right_value = _fresh_alias(f"__terrine_join_{suffix}_value", reserved)
    right_found = _fresh_alias(f"__terrine_join_{suffix}_found", reserved)
    right_fields = [normalized[dimension.entity_column].name(right_key)]
    right_start = right_end = None
    if start_column:
        right_start = _fresh_alias(f"__terrine_join_{suffix}_from", reserved)
        right_fields.append(normalized[start_column].name(right_start))
    if end_column:
        right_end = _fresh_alias(f"__terrine_join_{suffix}_to", reserved)
        right_fields.append(normalized[end_column].name(right_end))
    right_fields.extend(
        (normalized[prop.column].name(right_value), ibis.literal(True).name(right_found))
    )
    right = normalized.select(*right_fields)
    predicates = [points[unit_column] == right[right_key]]
    if right_start:
        predicates.append(
            compare_timestamps(points[cutoff_column], right[right_start], "gt" if strict else "ge")
        )
    if right_end:
        predicates.append(
            right[right_end].isnull()
            | compare_timestamps(points[cutoff_column], right[right_end], "le" if strict else "lt")
        )
    joined = points.left_join(right, predicates)
    return joined.select(
        *(joined[column] for column in points.columns),
        joined[right_value].name("__terrine_attribute_value"),
        joined[right_found].fill_null(False).name("__terrine_attribute_found"),
        *(
            [joined[right_start].name(matched_at_column)]
            if matched_at_column and right_start
            else []
        ),
    )


def enrich_events(
    events: ibis.Table,
    source: Any,
    dimension_relations: Mapping[str, ibis.Table],
) -> ibis.Table:
    """Lazily enrich events at ``_timestamp`` using each dimension's declared key."""
    if "_timestamp" not in events.columns:
        raise _SCHEMA.error(field="event", column="_timestamp")
    dimensions = tuple(source.dimensions)
    for dimension in dimensions:
        if dimension.name not in dimension_relations:
            raise _MISSING.error(reference=f"dimension relation {dimension.name}")
    aliases = _enriched_property_columns(dimensions, events.columns)
    result = events
    for dimension in dimensions:
        event_key = source.entities.get(dimension.source_entity)
        if event_key is None or event_key not in result.columns:
            raise _MISSING_KEY.error(dimension=dimension.name)
        history = dimension_relations[dimension.name]
        _check_columns(history, (dimension.entity_column,), field=f"dimension {dimension.name}")
        _check_columns(
            history,
            (prop.column for prop in dimension.properties.values()),
            field=f"dimension {dimension.name}",
        )
        _check_matching_types(
            result, event_key, history, dimension.entity_column, field="dimension entity"
        )
        if dimension.validity is not None:
            for time_column in (
                dimension.validity.valid_from,
                dimension.validity.valid_to,
                dimension.validity.changed_at,
            ):
                if time_column:
                    _check_columns(history, (time_column,), field="dimension validity")
                    _check_matching_types(
                        result, "_timestamp", history, time_column, field="dimension validity"
                    )
        normalized, start, end = _as_ranges(history, dimension)
        reserved = {column.casefold() for column in (*result.columns, *normalized.columns)}
        suffix = hashlib.sha256(dimension.name.encode("utf-8")).hexdigest()[:16]
        key_alias = _fresh_alias(f"__terrine_event_{suffix}_key", reserved)
        start_alias = _fresh_alias(f"__terrine_event_{suffix}_from", reserved)
        end_alias = _fresh_alias(f"__terrine_event_{suffix}_to", reserved)
        right_fields = [normalized[dimension.entity_column].name(key_alias)]
        if start:
            right_fields.append(normalized[start].name(start_alias))
        if end:
            right_fields.append(normalized[end].name(end_alias))
        field_aliases = {}
        for name, prop in dimension.properties.items():
            value_alias, found_alias = aliases[(dimension.name, name)]
            field_aliases[name] = (value_alias, found_alias)
            right_fields.extend(
                (normalized[prop.column].name(value_alias), ibis.literal(True).name(found_alias))
            )
        right = normalized.select(*right_fields)
        predicates = [result[event_key] == right[key_alias]]
        if start:
            predicates.append(compare_timestamps(result["_timestamp"], right[start_alias], "ge"))
        if end:
            predicates.append(
                right[end_alias].isnull()
                | compare_timestamps(result["_timestamp"], right[end_alias], "lt")
            )
        joined = result.left_join(right, predicates)
        result = joined.select(
            *(joined[column] for column in result.columns),
            *(joined[value].name(value) for value, _ in field_aliases.values()),
            *(joined[found].fill_null(False).name(found) for _, found in field_aliases.values()),
        )
    return result


def history_checks(
    history: ibis.Table, dimension: BoundDimension
) -> tuple[tuple[ibis.Table, RefusalSpec], ...]:
    """The bounded count queries that validate ``history``, each with its refusal.

    Every query is an aggregate over the columns it checks, so admission
    inspects exactly what executes rather than the whole dimension table.
    """
    validity = dimension.validity
    start = None if validity is None else (validity.changed_at or validity.valid_from)
    end = None if validity is None or validity.changed_at else validity.valid_to
    columns = [dimension.entity_column, *([start] if start else []), *([end] if end else [])]
    _check_columns(history, columns, field=f"dimension {dimension.name}")
    key = history[dimension.entity_column]
    reserved = {column.casefold() for column in history.columns}

    def count(rows: ibis.Table) -> ibis.Table:
        return rows.aggregate(__terrine_count=rows.count())

    if validity is None:
        count_alias = _fresh_alias("__terrine_n", reserved)
        repeated = history.group_by(dimension.entity_column).aggregate(
            **{count_alias: history.count()}
        )
        return (
            (count(history.filter(key.isnull())), _INVALID_HISTORY),
            (count(repeated.filter(repeated[count_alias] > 1)), _STATIC_MULTIPLICITY),
        )

    assert start is not None
    began = history[start]
    malformed = key.isnull() | began.isnull()
    if end:
        finished = history[end]
        malformed = malformed | (finished.notnull() & compare_timestamps(finished, began, "le"))
    count_alias = _fresh_alias("__terrine_n", reserved)
    grouped = history.group_by(dimension.entity_column, start).aggregate(
        **{count_alias: history.count()}
    )
    checks = [
        (count(history.filter(malformed)), _INVALID_HISTORY),
        (count(grouped.filter(grouped[count_alias] > 1)), _DUPLICATE_HISTORY),
    ]
    if end:
        prior_end_alias = _fresh_alias("__terrine_prior_max_end", reserved)
        prior_open_alias = _fresh_alias("__terrine_prior_open", reserved)
        # Only the key and validity columns take part; project them so the
        # window carries no unrelated column into the query.
        ranges = history.select(dimension.entity_column, start, end)
        prior_window = ibis.window(
            group_by=[ranges[dimension.entity_column]],
            order_by=[timestamp_order_key(ranges[start])],
            rows=(None, -1),
        )
        annotated = ranges.mutate(
            **{
                prior_end_alias: cast(ir.StructColumn, timestamp_order_key(ranges[end]))
                .max()
                .over(prior_window),
                prior_open_alias: ranges[end].isnull().any().over(prior_window),
            }
        )
        overlaps = annotated.filter(
            annotated[prior_open_alias]
            | (annotated[prior_end_alias] > timestamp_order_key(annotated[start]))
        )
        checks.append((count(overlaps), _OVERLAPPING_HISTORY))
    elif validity.changed_at is None:
        entity_count_alias = _fresh_alias("__terrine_n", reserved)
        multiple_ranges = history.group_by(dimension.entity_column).aggregate(
            **{entity_count_alias: history.count()}
        )
        checks.append(
            (
                count(multiple_ranges.filter(multiple_ranges[entity_count_alias] > 1)),
                _OVERLAPPING_HISTORY,
            )
        )
    return tuple(checks)


def validate_history(
    history: ibis.Table,
    dimension: BoundDimension,
    *,
    execute: Callable[[ibis.Table], Any],
) -> None:
    """Validate history with bounded aggregate queries on the caller's pinned executor."""
    for query, refusal in history_checks(history, dimension):
        if _scalar(execute(query)) > 0:
            raise refusal.error()


def _scalar(result: Any) -> int:
    """Require one exact nonnegative integer count from the pinned Arrow executor."""
    if (
        not isinstance(result, pa.Table)
        or result.num_rows != 1
        or result.column_names != ["__terrine_count"]
        or not pa.types.is_integer(result.column(0).type)
    ):
        raise _INVALID_HISTORY.error()
    result.validate(full=True)
    value = result.column(0)[0].as_py()
    if type(value) is not int or value < 0:
        raise _INVALID_HISTORY.error()
    return value
