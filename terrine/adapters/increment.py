"""Optional bridge from published increment definitions into native Catalogs."""

from __future__ import annotations

from collections import Counter
from importlib import import_module
from pathlib import Path
from typing import Any, Literal

import yaml
from yaml.constructor import ConstructorError
from yaml.resolver import BaseResolver

from terrine.errors import RefusalSpec
from terrine.semantic.models import (
    Catalog,
    Dimension,
    Fact,
    Filter,
    Measure,
    Metric,
    Property,
    Source,
    SqlQuery,
    Validity,
)
from terrine.semantic.rules import UNSUPPORTED_TEMPORAL_KIND
from terrine.semantic.sql import admit_read_query

__all__ = ["to_catalog"]

_MISSING = RefusalSpec(
    "terrine.increment.missing_extra",
    "The increment compatibility extra is required; install terrine[increment].",
)
_DUPLICATE_YAML_KEY = RefusalSpec(
    "terrine.increment.duplicate_yaml_key",
    "increment YAML repeats mapping key {key!r} in {path!r}.",
)
_UNSUPPORTED = RefusalSpec(
    "terrine.increment.unsupported_definition",
    "increment declaration {declaration!r} has semantics not represented by the native Catalog.",
)
_INVALID_DEFINITIONS = RefusalSpec(
    "terrine.increment.invalid_definitions",
    "increment definitions were rejected with upstream code {upstream_code!r}.",
)
_PROPERTY_COLLISION = RefusalSpec(
    "terrine.increment.property_name_collision",
    "increment property names cannot be qualified without a Catalog name collision: {name!r}.",
)
_AMBIGUOUS_PROPERTY = RefusalSpec(
    "terrine.increment.ambiguous_property",
    "increment filter property {property!r} is ambiguous in source {source!r}.",
)


class _DuplicateKey(Exception):
    def __init__(self, key: object) -> None:
        self.key = key


class _UniqueSafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader: _UniqueSafeLoader, node, deep: bool = False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "unhashable mapping key",
                key_node.start_mark,
            ) from exc
        if duplicate:
            raise _DuplicateKey(key)
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueSafeLoader.add_constructor(BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping)


def _check_duplicate_keys(path: Path) -> None:
    yaml_files = (
        (path,) if path.is_file() else tuple(sorted((*path.rglob("*.yaml"), *path.rglob("*.yml"))))
    )
    for yaml_path in yaml_files:
        try:
            with yaml_path.open(encoding="utf-8") as stream:
                yaml.load(stream, Loader=_UniqueSafeLoader)
        except _DuplicateKey as exc:
            raise _DUPLICATE_YAML_KEY.error(key=str(exc.key), path=str(yaml_path)) from exc
        except yaml.YAMLError:
            # Let increment's loader provide its coded YAML refusal.
            continue


def _add_property_name(
    raw_name: str,
    scope_kind: str,
    scope: str,
    counts: Counter[str],
    scope_counts: Counter[str],
    names: dict[tuple[str, str, str], str],
) -> None:
    if counts[raw_name] == 1:
        name = raw_name
    elif scope_counts[scope] > 1:
        name = f"{scope_kind}.{scope}.{raw_name}"
    else:
        name = f"{scope}.{raw_name}"
    names[(scope_kind, scope, raw_name)] = name


def _filter_records(
    filters: Any,
    *,
    source: str,
    available_scopes: tuple[tuple[str, str], ...],
    names: dict[tuple[str, str, str], str],
):
    records = []
    for source_filter in filters:
        matching = [
            names[(scope_kind, scope, source_filter.property)]
            for scope_kind, scope in available_scopes
            if (scope_kind, scope, source_filter.property) in names
        ]
        if len(matching) > 1:
            raise _AMBIGUOUS_PROPERTY.error(property=source_filter.property, source=source)
        property_name = matching[0] if matching else source_filter.property
        records.append(
            Filter(
                property=property_name,
                op=source_filter.op,
                values=tuple(source_filter.values),
            )
        )
    return tuple(records)


def _measure(
    fact: str,
    aggregation: Literal[
        "sum", "count", "avg_event", "avg_calendar_day", "min", "max", "count_distinct"
    ],
    filters: Any,
    window_days: int | None,
    *,
    source: str,
    scopes: tuple[tuple[str, str], ...],
    names: dict[tuple[str, str, str], str],
) -> Measure:
    return Measure(
        fact=fact,
        aggregation=aggregation,
        filters=_filter_records(filters, source=source, available_scopes=scopes, names=names),
        window_days=window_days,
    )


def _load_semantics() -> tuple[Any, Any]:
    try:
        semantics = import_module("increment.semantics")
        metric_models = import_module("increment.semantics.models")
    except (ImportError, ModuleNotFoundError) as exc:
        raise _MISSING.error(extra="increment") from exc
    return semantics, metric_models


def to_catalog(path_or_definitions: Any, *, dialect: str) -> Catalog:
    """Convert increment's published semantic definitions to a native Catalog.

    The optional dependency is loaded only when this bridge is invoked. The bridge
    projects fact sources, metrics, and referenced dimensions; exposure, experiment,
    week-start, and unreferenced-dimension declarations are not interpreted. Source
    SQL is re-admitted by Terrine and execution remains the native Model.from_catalog
    path.
    """
    semantics, metric_models = _load_semantics()

    if isinstance(path_or_definitions, semantics.Definitions):
        definitions = path_or_definitions
    else:
        path = Path(path_or_definitions)
        _check_duplicate_keys(path)
        try:
            definitions = semantics.load(path)
        except semantics.DefinitionError as exc:
            raise _INVALID_DEFINITIONS.error(upstream_code=exc.code) from exc

    fact_sources = {source.name: source for source in definitions.fact_sources}
    dimension_sources = {source.name: source for source in definitions.dim_sources}
    referenced_dimensions = {
        dimension_name for source in definitions.fact_sources for dimension_name in source.dims
    }

    scopes_by_property: dict[str, list[tuple[str, str]]] = {}
    for source in definitions.fact_sources:
        for prop in source.properties:
            scopes_by_property.setdefault(prop.name, []).append(("source", source.name))
    for dimension in definitions.dim_sources:
        if dimension.name in referenced_dimensions:
            for prop in dimension.properties:
                scopes_by_property.setdefault(prop.name, []).append(("dimension", dimension.name))
    counts = Counter({name: len(scopes) for name, scopes in scopes_by_property.items()})
    property_names: dict[tuple[str, str, str], str] = {}
    for raw_name, scopes in scopes_by_property.items():
        scope_counts = Counter(scope for _, scope in scopes)
        for scope_kind, scope in scopes:
            _add_property_name(raw_name, scope_kind, scope, counts, scope_counts, property_names)
    projected_names = list(property_names.values())
    if len(set(projected_names)) != len(projected_names):
        duplicate = next(name for name, count in Counter(projected_names).items() if count > 1)
        raise _PROPERTY_COLLISION.error(name=duplicate)

    sources: dict[str, Source] = {}
    facts: dict[str, Fact] = {}
    properties: dict[str, Property] = {}
    for source in definitions.fact_sources:
        source_properties = []
        for source_fact in source.facts:
            facts[source_fact.name] = Fact(source=source.name, column=source_fact.column)
        for source_property in source.properties:
            name = property_names[("source", source.name, source_property.name)]
            source_properties.append(name)
            properties[name] = Property(
                source=source.name,
                column=source_property.column,
                dtype=source_property.dtype,
                as_of=source_property.as_of,
            )
        sources[source.name] = Source(
            relation=SqlQuery(kind="sql", sql=source.sql),
            timestamp=source.timestamp_column,
            entities={column: column for column in source.entities},
            facts=tuple(fact.name for fact in source.facts),
            properties=tuple(source_properties),
            dimensions=tuple(source.dims),
        )

    dimensions: dict[str, Dimension] = {}
    for dimension_name in sorted(referenced_dimensions):
        dimension = dimension_sources[dimension_name]
        owners = sorted(
            source.name for source in definitions.fact_sources if dimension_name in source.dims
        )
        owner = owners[0]
        dimension_properties = []
        for source_property in dimension.properties:
            name = property_names[("dimension", dimension.name, source_property.name)]
            dimension_properties.append(name)
            properties[name] = Property(
                source=owner,
                column=source_property.column,
                dtype=source_property.dtype,
                as_of=source_property.as_of,
                dimension=dimension.name,
            )
        validity = (
            Validity(
                valid_from=dimension.validity.valid_from,
                valid_to=dimension.validity.valid_to,
                changed_at=dimension.validity.changed_at,
            )
            if dimension.validity is not None
            else None
        )
        dimensions[dimension.name] = Dimension(
            relation=SqlQuery(kind="sql", sql=dimension.sql),
            source_entity=dimension.entity,
            entity_column=dimension.entity,
            validity=validity,
            properties=tuple(dimension_properties),
        )

    measures: dict[str, Measure] = {}
    metrics: dict[str, Metric] = {}
    for metric in definitions.metrics:
        kind = metric.type
        if not isinstance(
            metric,
            (metric_models.MeanMetric, metric_models.ConversionMetric, metric_models.RatioMetric),
        ):
            raise UNSUPPORTED_TEMPORAL_KIND.error(kind=kind)
        if isinstance(metric, metric_models.RatioMetric):
            numerator_name = f"{metric.name}.numerator"
            denominator_name = f"{metric.name}.denominator"
            for name, part in (
                (numerator_name, metric.numerator),
                (denominator_name, metric.denominator),
            ):
                if name in measures:
                    raise _UNSUPPORTED.error(declaration=f"measure name collision {name!r}")
                owner = next(
                    source.name
                    for source in definitions.fact_sources
                    if any(fact.name == part.fact for fact in source.facts)
                )
                source = fact_sources[owner]
                measures[name] = _measure(
                    part.fact,
                    part.aggregation,
                    part.filters,
                    part.window_days,
                    source=owner,
                    scopes=(("source", owner), *(("dimension", name) for name in source.dims)),
                    names=property_names,
                )
            metrics[metric.name] = Metric(
                kind=kind,
                entity=metric.entity,
                numerator=numerator_name,
                denominator=denominator_name,
            )
        else:
            name = metric.name
            if name in measures:
                raise _UNSUPPORTED.error(declaration=f"measure name collision {name!r}")
            owner = next(
                source.name
                for source in definitions.fact_sources
                if any(fact.name == metric.fact for fact in source.facts)
            )
            source = fact_sources[owner]
            aggregation = (
                metric.aggregation if isinstance(metric, metric_models.MeanMetric) else "count"
            )
            measures[name] = _measure(
                metric.fact,
                aggregation,
                metric.filters,
                metric.window_days,
                source=owner,
                scopes=(("source", owner), *(("dimension", dim_name) for dim_name in source.dims)),
                names=property_names,
            )
            winsorization = (
                metric.winsorization.model_dump(mode="json")
                if isinstance(metric, metric_models.MeanMetric) and metric.winsorization is not None
                else None
            )
            metrics[metric.name] = Metric(
                kind=kind, entity=metric.entity, measure=name, winsorization=winsorization
            )

    catalog_dialect = definitions.dialect or dialect
    for source in definitions.fact_sources:
        admit_read_query(source.sql, catalog_dialect, dialect)
    for dimension in definitions.dim_sources:
        if dimension.name in referenced_dimensions:
            admit_read_query(dimension.sql, catalog_dialect, dialect)

    return Catalog(
        schema_version=1,
        day_boundary=definitions.day_boundary,
        dialect=catalog_dialect,
        sources=sources,
        facts=facts,
        properties=properties,
        dimensions=dimensions,
        measures=measures,
        metrics=metrics,
    )
