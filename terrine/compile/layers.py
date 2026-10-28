from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Literal

import ibis

from terrine.execution._metadata import metadata_operation
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
        measure.event,
        measure.aggregation,
        tuple((item.property_name, item.op, item.values) for item in measure.filters),
        measure.window_days,
    )
    return hashlib.sha256(repr(fields).encode()).hexdigest()


def build_measure_layer(
    events: ibis.Table,
    measure,
    source,
    *,
    boundary: str,
    resolution: Literal["day", "event"],
) -> ibis.Table:
    """Build one canonical sparse measure prefix from normalized source events."""
    from terrine.temporal.daily import _sparse_group, resolve_measure, sparse_daily
    from terrine.temporal.prefix import sparse_prefix

    measure = resolve_measure(events, measure, source=source)
    grouped = (
        sparse_daily(events, measure, boundary, "_unit", source=source)
        if resolution == "day"
        else _sparse_group(
            events,
            measure,
            {"_timestamp": events._timestamp},
            "_unit",
            source=source,
        )
    )
    return sparse_prefix(grouped, measure, resolution=resolution)


@metadata_operation()
def build_layer(design: Design, model, key: LayerKey, *, relation_resolver=None) -> ibis.Table:
    """Build one compiler-planned anchor or measure-prefix relation."""
    from dataclasses import replace

    from terrine.compile.anchors import compile_anchors
    from terrine.model import Model
    from terrine.semantic.bind import normalize_events
    from terrine.temporal.attributes import enrich_events

    if relation_resolver is None:
        bound_model = model
    else:
        from terrine.model import Model

        bound_model = Model._bind(
            model.catalog,
            con=model.con,
            readiness=model.readiness,
            units=model.units,
            event_ids=model.event_ids,
            resolver=relation_resolver,
            input_binding=model.input_binding,
            source_names=set(model.sources),
            engine=model._engine,
        )
    expected = plan_layers(design, bound_model).keys

    if replace(key, snapshot_id="unbound") not in expected:
        raise ValueError("layer key does not belong to this Design and Model")
    if key.layer == "anchor":
        return compile_anchors(design, bound_model).table
    if key.resolution not in {"day", "event"}:
        raise ValueError("prefix layer resolution must be day or event")

    measure = next(
        (
            measure
            for metric in model.metrics.values()
            for measure in metric.measures
            if measure_identity(measure) == key.measure and measure.source_name == key.source
        ),
        None,
    )
    if measure is None:
        raise ValueError("layer measure is absent from the bound Model")
    source = model.sources[key.source]
    resolve = relation_resolver or model._relation
    raw = resolve(source.relation)
    events = normalize_events(raw, source, source.units[design.unit])
    histories = {dimension.name: resolve(dimension.relation) for dimension in source.dimensions}
    events = enrich_events(events, source, histories)
    return build_measure_layer(
        events,
        measure,
        source,
        boundary=model.catalog.day_boundary,
        resolution=key.resolution,
    )


@metadata_operation()
def plan_layers(design: Design, model) -> LayerPlan:
    """Describe compiler input layers for a design, without consulting runtime state."""
    from terrine.semantic.rules import calendar_average_alignment
    from terrine.spec.anchors import FixedDate, Schedule
    from terrine.spec.expressions import (
        ApproxMetricDistinct,
        ApproxMetricQuantile,
        AttributeAsOf,
        EventTime,
        MetricAgg,
        MetricQuantile,
        RetentionMetric,
        _output_leaves,
        agg,
    )
    from terrine.spec.state_panel import BinnedMetricState, StatePanel
    from terrine.spec.validate import _fixed_anchors

    measures: dict[tuple[str, str, str, LayerResolution], None] = {}
    anchor_points = (
        _fixed_anchors(design, model) if isinstance(design.anchor, (FixedDate, Schedule)) else None
    )

    def expression(node, points=anchor_points) -> None:
        if isinstance(
            node,
            (
                AttributeAsOf,
                EventTime,
                MetricQuantile,
                ApproxMetricQuantile,
                ApproxMetricDistinct,
                RetentionMetric,
            ),
        ):
            return
        elif isinstance(node, MetricAgg):
            metric = model.metrics[node.metric]
            resolution: LayerResolution = (
                "day"
                if points
                and node.relative_to is None
                and all(
                    calendar_average_alignment(
                        node.window, anchor=instant, day_boundary=model.catalog.day_boundary
                    )
                    == "aligned"
                    for instant in points
                )
                else "event"
            )
            for measure in metric.measures:
                if measure.aggregation in {"min", "max", "count_distinct"}:
                    continue
                identity = measure_identity(measure)
                measures[(measure.source_name, identity, design.unit, resolution)] = None
        else:
            raise TypeError(f"unsupported design expression: {type(node).__name__}")

    for node in (
        *design.features.values(),
        *design.outcomes.values(),
        *design.eligibility.values(),
    ):
        for leaf in _output_leaves(node):
            expression(leaf)
    if isinstance(design.shape, StatePanel):
        assert anchor_points is not None
        baseline_points = anchor_points[:1]
        for node in design.shape.baseline_features.values():
            expression(node, baseline_points)
        if isinstance(design.shape.state, BinnedMetricState):
            assert isinstance(design.anchor, Schedule)
            expression(agg(design.shape.state.metric, window=(-design.anchor.every, 0)))
    source_schema = {
        name: {
            column: str(dtype.copy(nullable=True))
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
