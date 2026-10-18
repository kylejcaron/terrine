from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Literal

import ibis

from terrine.spec.design import Design

LayerResolution = Literal["day", "event", "point", "period"]
LayerName = Literal["daily", "prefix", "anchor", "state_point", "state_period"]


@dataclass(frozen=True, slots=True)
class LayerKey:
    layer: LayerName
    source: str
    measure: str | None
    unit: str
    grain: str
    resolution: LayerResolution
    definition_hash: str
    snapshot_id: str


@dataclass(frozen=True, slots=True)
class LayerSubstitution:
    key: LayerKey
    relation: ibis.Table


@dataclass(frozen=True, slots=True)
class LayerPlan:
    keys: tuple[LayerKey, ...]


def measure_identity(measure) -> str:
    """Stable identity for one bound measure, excluding schema-derived value type."""
    fields = (
        measure.source_name,
        measure.fact,
        measure.aggregation,
        tuple((item.property_name, item.op, item.values) for item in measure.filters),
        measure.window_days,
    )
    return hashlib.sha256(repr(fields).encode()).hexdigest()


def plan_layers(design: Design, model) -> LayerPlan:
    """Describe compiler input layers for a design, without consulting runtime state."""
    from terrine.semantic.rules import calendar_average_alignment
    from terrine.spec.anchors import FixedDate, Schedule
    from terrine.spec.expressions import And, AttributeAsOf, Eq, EventTime, Gt, IsNull, MetricAgg
    from terrine.spec.validate import _fixed_anchors

    measures: dict[tuple[str, str, str, LayerResolution], None] = {}

    def expression(node) -> None:
        if isinstance(node, And):
            for term in node.terms:
                expression(term)
        elif isinstance(node, (Eq, Gt)):
            expression(node.left if isinstance(node, Eq) else node.expr)
        elif isinstance(node, IsNull):
            expression(node.expr)
        elif isinstance(node, (AttributeAsOf, EventTime)):
            return
        elif isinstance(node, MetricAgg):
            metric = model.metrics[node.metric]
            fixed = (
                _fixed_anchors(design, model)
                if isinstance(design.anchor, (FixedDate, Schedule))
                else None
            )
            resolution: LayerResolution = (
                "day"
                if fixed
                and all(
                    calendar_average_alignment(
                        node.window, anchor=instant, day_boundary=model.catalog.day_boundary
                    )
                    == "aligned"
                    for instant in fixed
                )
                else "event"
            )
            for measure in metric.measures:
                identity = measure_identity(measure)
                measures[(measure.source_name, identity, design.unit, resolution)] = None
        else:
            raise TypeError(f"unsupported design expression: {type(node).__name__}")

    for node in (
        *design.features.values(),
        *design.outcomes.values(),
        *design.eligibility.values(),
    ):
        expression(node)
    source_schema = {
        name: {
            column: str(dtype)
            for column, dtype in model._resolver(source.relation).schema().items()
        }
        for name, source in model.sources.items()
    }
    definition_hash = hashlib.sha256(
        json.dumps(
            [design.spec_hash, model.model_hash, source_schema],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    anchor = LayerKey(
        "anchor",
        "__design__",
        None,
        design.unit,
        "unit_anchor",
        "point",
        definition_hash,
        "unbound",
    )
    prefixes = tuple(
        LayerKey("prefix", source, measure, unit, "unit", resolution, definition_hash, "unbound")
        for source, measure, unit, resolution in sorted(measures)
    )
    return LayerPlan((anchor, *prefixes))
