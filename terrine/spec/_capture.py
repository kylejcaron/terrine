"""Declaration-level root selection for retained inputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from typing import TYPE_CHECKING, Any

from terrine.spec.anchors import EventAnchor
from terrine.spec.expressions import (
    ApproxMetricDistinct,
    AttributeAsOf,
    EventTime,
    MetricAgg,
    MetricQuantile,
    RetentionMetric,
)

if TYPE_CHECKING:
    from terrine.model import Model
    from terrine.spec.design import Design


def _design_root_names(
    model: Model, design: Design
) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    from terrine.spec.validate import resolve_unit, validate_design

    design = resolve_unit(design, model)
    sources: set[str] = (
        set(validate_design(design, model).required_sources)
        if design.unit in model.units
        else set()
    )
    properties: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, (EventAnchor, EventTime)):
            event = model.catalog.events.get(value.event)
            if event is not None:
                sources.add(event.source)
            if isinstance(value, EventTime):
                if value.label_property is not None:
                    properties.add(value.label_property)
                if value.relative_to is not None:
                    visit(value.relative_to)
        elif isinstance(value, RetentionMetric):
            event = model.catalog.events.get(value.event)
            if event is not None:
                sources.add(event.source)
            properties.add(value.label_property)
        elif isinstance(value, AttributeAsOf):
            properties.add(value.attribute)
            if value.relative_to is not None:
                visit(value.relative_to)
        elif isinstance(value, (MetricAgg, MetricQuantile, ApproxMetricDistinct)):
            metric = model.catalog.metrics.get(value.metric)
            if metric is not None:
                for measure_name in (metric.measure, metric.numerator, metric.denominator):
                    if measure_name is None:
                        continue
                    measure = model.catalog.measures.get(measure_name)
                    if measure is not None:
                        event = model.catalog.events.get(measure.event)
                        if event is not None:
                            sources.add(event.source)
            if isinstance(value, MetricAgg) and value.relative_to is not None:
                visit(value.relative_to)
        elif isinstance(value, Mapping):
            for child in value.values():
                visit(child)
        elif isinstance(value, (tuple, list)):
            for child in value:
                visit(child)
        elif is_dataclass(value) and not isinstance(value, type):
            for item in fields(value):
                visit(getattr(value, item.name))

    for role in ("features", "outcomes", "eligibility"):
        visit(getattr(design, role, {}))
    visit(design.anchor)
    visit(design.treatment)
    visit(design.shape)
    dimension_names: set[str] = set()
    for property_name in properties:
        prop = model.catalog.properties.get(property_name)
        if prop is None:
            continue
        sources.add(prop.source)
        if prop.dimension is not None:
            dimension_names.add(prop.dimension)
    for source_name in sources:
        source = model.catalog.sources.get(source_name)
        if source is not None:
            dimension_names.update(source.dimensions)
    unit_names = {design.unit} if design.unit is not None and design.unit in model.units else set()
    return frozenset(sources), frozenset(dimension_names), frozenset(unit_names)
