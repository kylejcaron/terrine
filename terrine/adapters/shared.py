"""Project shared metric declarations onto native Terrine bindings."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict

from terrine.errors import RefusalSpec
from terrine.semantic.io import validate_catalog
from terrine.semantic.models import (
    Catalog,
    Dimension,
    Event,
    Filter,
    FilterOp,
    Measure,
    PopulationRequirement,
    Property,
    PropertyType,
    Source,
    TableRef,
)

if TYPE_CHECKING:
    import metric_definitions as shared

_MISSING = RefusalSpec(
    "terrine.shared_metrics.missing_extra",
    "Install the {extra!r} Terrine extra to read shared metric definitions.",
)
_INVALID = RefusalSpec(
    "terrine.shared_metrics.invalid_definition",
    "Cannot read shared metric definitions at {path!r}:{line}:{column} ({reason}). "
    "Provide a valid shared v1 catalog.",
)
_UNSUPPORTED = RefusalSpec(
    "terrine.shared_metrics.unsupported_definition",
    "Terrine cannot bind shared metric {metric!r}: {requirement}. "
    "Select definitions at one unit grain with timestamped event tables.",
)
_TYPES: dict[str, PropertyType] = {
    "string": "string",
    "integer": "int",
    "boolean": "bool",
    "date": "date",
    "number": "number",
    "timestamp": "timestamp",
}
_OPS: dict[str, FilterOp] = {
    "eq": "equals",
    "ne": "not_equals",
    "lt": "lt",
    "le": "lte",
    "gt": "gt",
    "ge": "gte",
    "in": "in",
}


def _lower_additive(
    event: str, aggregation: Literal["sum", "count"], *, filters: tuple[Filter, ...] = ()
) -> Measure:
    """Bind one exact window primitive with no native temporal override."""
    return Measure(event=event, aggregation=aggregation, filters=filters)


def _unsupported(metric: str, requirement: str):
    return _UNSUPPORTED.error(metric=metric, requirement=requirement, target="terrine")


type _FormulaDeclaration = dict[str, str | int | float | _FormulaDeclaration]


class _MetricDeclaration(TypedDict):
    kind: Literal["mean", "conversion", "formula"]
    unit: str
    population: str | None
    measure: NotRequired[str]
    formula: NotRequired[_FormulaDeclaration]


def _formula(node: shared.Formula, catalog: shared.Catalog) -> _FormulaDeclaration:
    from metric_definitions import Binary, Literal, Negation, Reference

    match node:
        case Literal(value=value):
            return {"kind": "literal", "value": value}
        case Reference(name=name):
            return {"kind": "measure" if name in catalog.measures else "metric", "name": name}
        case Negation(operand=operand):
            return {"kind": "negation", "operand": _formula(operand, catalog)}
        case Binary(operator=operator, left=left, right=right):
            return {
                "kind": "binary",
                "operator": operator,
                "left": _formula(left, catalog),
                "right": _formula(right, catalog),
            }
    raise AssertionError("unrecognized shared formula node")


def _metrics(catalog: shared.Catalog, units: frozenset[str]) -> dict[str, _MetricDeclaration]:
    from metric_definitions import ConversionMetric, FormulaMetric, MeanMetric, references

    lowered: dict[str, _MetricDeclaration] = {}

    def bind(name: str) -> _MetricDeclaration:
        if name in lowered:
            return lowered[name]
        metric = catalog.metrics[name]
        match metric:
            case MeanMetric(per_unit_mean=rule):
                result = _MetricDeclaration(
                    kind="mean",
                    unit=catalog.populations[rule.population].entity,
                    measure=rule.measure,
                    population=rule.population,
                )
            case ConversionMetric(conversion=rule):
                result = _MetricDeclaration(
                    kind="conversion",
                    unit=catalog.populations[rule.population].entity,
                    measure=rule.event_count,
                    population=rule.population,
                )
            case FormulaMetric(formula=formula):
                choices: set[str] | None = None
                populations = set()
                for ref in dict.fromkeys(references(formula)):
                    if ref in catalog.measures:
                        candidates = set(catalog.sources[catalog.measures[ref].source].entities)
                    else:
                        dependency = bind(ref)
                        candidates = {dependency["unit"]}
                        if dependency["population"] is not None:
                            populations.add(dependency["population"])
                    choices = candidates if choices is None else choices & candidates
                choices = set(units) if choices is None else choices
                if len(choices) > 1 and len(matched := choices & units) == 1:
                    choices = matched
                if len(choices) != 1 or len(populations) > 1:
                    raise _unsupported(name, "operands must share one entity and population")
                result = _MetricDeclaration(
                    kind="formula",
                    unit=next(iter(choices)),
                    formula=_formula(formula, catalog),
                    population=next(iter(populations), None),
                )
        lowered[name] = result
        return result

    return {name: bind(name) for name in catalog.metrics}


def _source(
    catalog: shared.Catalog,
    name: str,
    measure_names: list[str],
    properties: dict[str, Property],
    dimensions: dict[str, Dimension],
) -> Source:
    source = catalog.sources[name]
    if source.time is None:
        metric = next(
            metric for metric in catalog.metrics if name in catalog.select([metric]).sources
        )
        raise _unsupported(metric, f"source {name!r} must declare an event timestamp")
    property_names = []
    dimension_names = []
    for dim_name, dim in source.dimensions.items():
        qualified = f"{name}.{dim_name}"
        properties[qualified] = Property(
            source=name, column=dim.column, dtype=_TYPES[dim.type], as_of="event_time"
        )
        property_names.append(qualified)
    for alias, join in source.joins.items():
        target = catalog.sources[join.source]
        dimension_name = f"{name}.{alias}"
        dimension_properties = []
        for dim_name, dim in target.dimensions.items():
            qualified = f"{dimension_name}.{dim_name}"
            properties[qualified] = Property(
                source=name,
                column=dim.column,
                dtype=_TYPES[dim.type],
                as_of="event_time",
                dimension=dimension_name,
            )
            dimension_properties.append(qualified)
            property_names.append(qualified)
        dimensions[dimension_name] = Dimension(
            relation=TableRef(kind="table", name=target.table),
            unit=None,
            key=target.row_key,
            foreign_key=join.foreign_key,
            properties=tuple(dimension_properties),
        )
        dimension_names.append(dimension_name)
    return Source(
        relation=TableRef(kind="table", name=source.table),
        time=source.time,
        units=source.entities,
        events=tuple(f"{name}.{measure}" for measure in measure_names),
        properties=tuple(property_names),
        dimensions=tuple(dimension_names),
        row_key=source.row_key,
    )


def to_catalog(
    path_or_catalog: str | Path | shared.Catalog,
    *,
    dialect: str,
    units: frozenset[str],
    metrics: Sequence[str] | None,
) -> Catalog:
    """Select and translate declarations without connecting to their relations."""
    try:
        from metric_definitions import Catalog as SharedCatalog
        from metric_definitions import CatalogError, load
    except ModuleNotFoundError as exc:
        if exc.name != "metric_definitions":
            raise
        raise _MISSING.error(extra="metric-definitions") from exc
    try:
        catalog = (
            path_or_catalog if isinstance(path_or_catalog, SharedCatalog) else load(path_or_catalog)
        )
    except CatalogError as exc:
        raise _INVALID.error(
            path=str(exc.path), line=exc.line, column=exc.column, reason=exc.code
        ) from exc
    names = tuple(catalog.metrics) if metrics is None else metrics
    if (
        not isinstance(names, Sequence)
        or isinstance(names, (str, bytes))
        or not all(isinstance(name, str) for name in names)
    ):
        raise _unsupported("selection", "pass a sequence of metric names")
    try:
        catalog = catalog.select(names)
    except KeyError as exc:
        raise _unsupported(str(exc.args[0]), "metric is not declared") from exc
    lowered_metrics = _metrics(catalog, units)
    grouped: dict[str, list[str]] = {}
    events = {}
    measures = {}
    for name, measure in catalog.measures.items():
        grouped.setdefault(measure.source, []).append(name)
        event = f"{measure.source}.{name}"
        events[event] = Event(source=measure.source, column=measure.sum)
        conditions = () if measure.filter is None else catalog.filters[measure.filter].conditions
        filters = tuple(
            Filter(
                property=f"{measure.source}.{condition.dimension}",
                op=_OPS[condition.op],
                values=condition.value
                if isinstance(condition.value, tuple)
                else (condition.value,),
            )
            for condition in conditions
        )
        measures[name] = _lower_additive(
            event, "count" if measure.count_rows else "sum", filters=filters
        )
    properties: dict[str, Property] = {}
    dimensions: dict[str, Dimension] = {}
    sources = {
        name: _source(catalog, name, names, properties, dimensions)
        for name, names in grouped.items()
    }
    populations = {
        name: PopulationRequirement(
            relation=TableRef(kind="table", name=catalog.sources[population.source].table),
            key=catalog.sources[population.source].row_key,
            unit=population.entity,
        )
        for name, population in catalog.populations.items()
    }
    return validate_catalog(
        dict(
            schema_version=4,
            day_boundary="UTC",
            dialect=dialect,
            sources=sources,
            events=events,
            properties=properties,
            dimensions=dimensions,
            measures=measures,
            metrics=lowered_metrics,
            populations=populations,
        )
    )
