"""Catalog reference resolution and static schema binding."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any

from terrine.errors import RefusalSpec, freeze
from terrine.semantic.models import (
    BoundDimension,
    BoundFilter,
    BoundMeasure,
    BoundMetric,
    BoundSource,
    BoundValidity,
    Catalog,
    PropertyBinding,
    RelationSpec,
)
from terrine.semantic.rules import UNSUPPORTED_TEMPORAL_KIND
from terrine.semantic.sql import DIALECT_MISMATCH, admit_read_query
from terrine.semantic.time import boundary_timezone

__all__ = ["normalize_source", "bind_metric", "normalize_events"]

_SCHEMA = RefusalSpec(
    "terrine.semantic.schema_mismatch",
    "Declared {field} column {column!r} is absent or has an incompatible type.",
)
_MISSING = RefusalSpec(
    "terrine.semantic.missing_reference", "Catalog reference {reference!r} is not declared."
)
_AMBIGUOUS = RefusalSpec(
    "terrine.semantic.ambiguous_column",
    "Normalized event aliases conflict with declared physical column {column!r}.",
)


def _reference(registry: Mapping[str, Any], name: str, label: str):
    try:
        return registry[name]
    except KeyError as exc:
        raise _MISSING.error(reference=f"{label}:{name}") from exc


def _relation(con: Any, relation, dialect: str):
    if relation.kind == "table":
        return con.table(relation.name)
    actual = str(getattr(con, "name", "")).casefold()
    sql = admit_read_query(relation.sql, dialect, actual)
    return con.sql(sql)


def _dtype_ok(dtype: Any, expected: str) -> bool:
    if expected == "string":
        return dtype.is_string()
    if expected == "int":
        return dtype.is_integer()
    if expected == "float":
        return dtype.is_floating() or dtype.is_decimal()
    if expected == "bool":
        return dtype.is_boolean()
    if expected == "date":
        return dtype.is_date()
    return False


def normalize_source(
    con: Any,
    catalog: Catalog,
    source_name: str,
    *,
    resolver: Callable[[RelationSpec], Any] | None = None,
) -> BoundSource:
    """Bind one source against declared schema metadata without reading rows."""
    boundary_timezone(catalog.day_boundary)
    execution_dialect = str(getattr(con, "name", "")).casefold()
    if catalog.dialect.casefold() != execution_dialect:
        raise DIALECT_MISMATCH.error(declared=catalog.dialect, execution=execution_dialect)
    source = _reference(catalog.sources, source_name, "source")
    relation = (
        resolver(source.relation)
        if resolver is not None
        else _relation(con, source.relation, catalog.dialect)
    )
    schema = relation.schema()
    columns = dict(schema.items())
    if source.timestamp not in columns or not columns[source.timestamp].is_timestamp():
        raise _SCHEMA.error(field="timestamp", column=source.timestamp)
    for name, column in source.entities.items():
        if column not in columns:
            raise _SCHEMA.error(field=f"entity {name}", column=column)
    facts = {}
    for name in source.facts:
        fact = _reference(catalog.facts, name, "fact")
        if fact.source != source_name:
            raise _MISSING.error(reference=f"fact {name} owned by {fact.source}")
        facts[name] = fact.column
        if fact.column is not None and fact.column not in columns:
            raise _SCHEMA.error(field=f"fact {name}", column=fact.column)
    properties = {}
    for name in source.properties:
        prop = _reference(catalog.properties, name, "property")
        if prop.source != source_name:
            raise _MISSING.error(reference=f"property {name} owned by {prop.source}")
        if prop.dimension is None:
            if prop.column not in columns or not _dtype_ok(columns.get(prop.column), prop.dtype):
                raise _SCHEMA.error(field=f"property {name}", column=prop.column)
        elif (
            prop.dimension not in source.dimensions
            or name not in _reference(catalog.dimensions, prop.dimension, "dimension").properties
        ):
            raise _MISSING.error(reference=f"property {name} dimension {prop.dimension}")
        properties[name] = PropertyBinding(
            name, prop.column, prop.dtype, prop.as_of, prop.dimension
        )
    dimensions = []
    for name in source.dimensions:
        dim = _reference(catalog.dimensions, name, "dimension")
        if dim.source_entity not in source.entities:
            raise _MISSING.error(reference=f"dimension {name} source entity {dim.source_entity}")
        dimension_relation = (
            resolver(dim.relation)
            if resolver is not None
            else _relation(con, dim.relation, catalog.dialect)
        )
        dimension_columns = dict(dimension_relation.schema().items())
        if dim.entity_column not in dimension_columns:
            raise _SCHEMA.error(field=f"dimension {name} entity", column=dim.entity_column)
        validity = None
        if dim.validity is not None:
            validity = BoundValidity(
                dim.validity.valid_from, dim.validity.valid_to, dim.validity.changed_at
            )
            for col in (validity.valid_from, validity.valid_to, validity.changed_at):
                if col is not None and (
                    col not in dimension_columns or not dimension_columns[col].is_timestamp()
                ):
                    raise _SCHEMA.error(field=f"dimension {name} validity", column=col)
        dimension_properties = {}
        for property_name in dim.properties:
            prop = _reference(catalog.properties, property_name, "property")
            if prop.dimension != name:
                raise _MISSING.error(reference=f"property {property_name} dimension {name}")
            if prop.column not in dimension_columns or not _dtype_ok(
                dimension_columns.get(prop.column), prop.dtype
            ):
                raise _SCHEMA.error(field=f"dimension property {property_name}", column=prop.column)
            dimension_properties[property_name] = PropertyBinding(
                property_name, prop.column, prop.dtype, prop.as_of, name
            )
        dimensions.append(
            BoundDimension(
                name,
                dim.relation,
                dim.source_entity,
                dim.entity_column,
                validity,
                MappingProxyType(dimension_properties),
            )
        )
    return BoundSource(
        source_name,
        source.relation,
        source.timestamp,
        MappingProxyType(dict(source.entities)),
        MappingProxyType(facts),
        MappingProxyType(properties),
        tuple(dimensions),
        catalog.dialect,
        catalog.day_boundary,
    )


def bind_metric(catalog: Catalog, metric_name: str) -> BoundMetric:
    """Resolve metric and measure references while preserving declaration meaning."""
    metric = _reference(catalog.metrics, metric_name, "metric")
    if metric.kind not in {"mean", "conversion", "ratio"}:
        raise UNSUPPORTED_TEMPORAL_KIND.error(kind=metric.kind)
    names = (metric.numerator, metric.denominator) if metric.kind == "ratio" else (metric.measure,)
    bound_measures = []
    source_names = []
    entity_columns = {}
    for measure_name in names:
        measure = _reference(catalog.measures, measure_name, "measure")
        fact = _reference(catalog.facts, measure.fact, "fact")
        source = _reference(catalog.sources, fact.source, "source")
        if measure.fact not in source.facts:
            raise _MISSING.error(reference=f"fact {measure.fact} on source {fact.source}")
        if metric.entity not in source.entities:
            raise _MISSING.error(reference=f"entity {metric.entity} on source {fact.source}")
        source_names.append(fact.source)
        entity_columns[fact.source] = source.entities[metric.entity]
        filters = []
        for filt in measure.filters:
            prop = _reference(catalog.properties, filt.property, "property")
            if prop.dimension is None:
                declared = filt.property in source.properties and prop.source == fact.source
            else:
                # The property keeps one canonical owner; any source declaring the
                # shared dimension may filter through that same dimension property.
                dimension = _reference(catalog.dimensions, prop.dimension, "dimension")
                owner = _reference(catalog.sources, prop.source, "property owner")
                declared = (
                    prop.dimension in source.dimensions
                    and filt.property in dimension.properties
                    and prop.dimension in owner.dimensions
                )
            if not declared:
                raise _MISSING.error(
                    reference=f"filter property {filt.property} on source {fact.source}"
                )
            filters.append(BoundFilter(filt.property, filt.op, tuple(filt.values)))
        bound_measures.append(
            BoundMeasure(
                measure.fact,
                fact.source,
                measure.aggregation,
                tuple(filters),
                measure.window_days,
                None,
            )
        )
    transform = (freeze(metric.winsorization),) if metric.winsorization is not None else ()
    return BoundMetric(
        metric_name,
        metric.entity,
        MappingProxyType(entity_columns),
        metric.kind,
        tuple(bound_measures),
        tuple(dict.fromkeys(source_names)),
        transform,
    )


def normalize_events(source_table: Any, source: BoundSource, unit_column: str):
    """Project normalized timestamp/unit names and all other declared physical columns."""
    if unit_column not in source.entities.values():
        raise _MISSING.error(reference=f"physical entity column:{unit_column}")
    physical = list(source.entities.values())
    physical.extend(column for column in source.facts.values() if column is not None)
    physical.extend(
        prop.column for prop in source.properties.values() if prop.dimension_name is None
    )
    physical = list(dict.fromkeys(physical))
    if ("_unit" in physical and unit_column != "_unit") or (
        "_timestamp" in physical and source.timestamp_column != "_timestamp"
    ):
        raise _AMBIGUOUS.error(column="_unit" if "_unit" in physical else "_timestamp")
    columns = {"_timestamp": source.timestamp_column, "_unit": unit_column}
    columns.update((column, column) for column in physical)
    return source_table.select(**{name: source_table[column] for name, column in columns.items()})
