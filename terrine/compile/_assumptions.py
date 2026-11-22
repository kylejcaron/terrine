"""Describe the bindings actually used by a study, without opening any relation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from terrine.compile._predicates import native_decision_dependencies, native_metric_events
from terrine.data import (
    NativeFloatDecision,
    RosterAssumption,
    RosterAttributeAssumption,
    SourceAssumption,
    StudyAssumptions,
)
from terrine.semantic.attributes import resolve_attribute
from terrine.spec.anchors import EventAnchor
from terrine.spec.expressions import (
    ApproxMetricDistinct,
    ApproxMetricQuantile,
    AttributeAsOf,
    EventTime,
    Expr,
    MetricAgg,
    MetricQuantile,
    Predicate,
    RetentionMetric,
    _output_leaves,
)
from terrine.spec.panels import CalendarComparisonSpec, SequentialSpec
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import BinnedMetricState, StatePanel
from terrine.spec.trajectory import CohortSpec, LifeTable, TimeToEvent
from terrine.spec.validate import resolve_selector_source

if TYPE_CHECKING:
    from terrine.model import Model
    from terrine.semantic.models import BoundDimension
    from terrine.spec.design import Design

type _Role = Literal["anchor", "feature", "outcome", "eligibility", "treatment", "shape"]
_METRIC_NODES = (MetricAgg, MetricQuantile, ApproxMetricQuantile, ApproxMetricDistinct)


class _Uses:
    def __init__(self, model: Model, unit: str) -> None:
        self.model = model
        self.unit = unit
        self.sources: dict[SourceAssumption, None] = {}
        self.roster_attributes: dict[RosterAttributeAssumption, None] = {}

    def add(self, use: SourceAssumption) -> None:
        self.sources[use] = None

    def event(self, event: str, role: _Role, output: str) -> str:
        source = self.model.catalog.events[event].source
        self.add(SourceAssumption(source, role, output, "event_time"))
        return source

    def dimension(self, source: str, role: _Role, output: str, dimension: BoundDimension) -> None:
        axis = "effective_time" if dimension.validity is not None else "static"
        self.add(SourceAssumption(source, role, output, axis, dimension=dimension.name))

    def property_dimension(self, source: str, role: _Role, output: str, property_name: str) -> None:
        for dimension in self.model.sources[source].dimensions:
            if property_name in dimension.properties:
                self.dimension(source, role, output, dimension)
                return

    def metric(self, metric: str, role: _Role, output: str) -> None:
        for measure in self.model.metrics[metric].measures:
            source = measure.source_name
            self.add(SourceAssumption(source, role, output, "event_time"))
            for filt in measure.filters:
                self.property_dimension(source, role, output, filt.property_name)

    def output(self, role: _Role, output: str, node: Expr | Predicate) -> None:
        for leaf in _output_leaves(node):
            if isinstance(leaf, (MetricAgg, EventTime, AttributeAsOf)) and leaf.relative_to:
                self.output(role, output, leaf.relative_to)
            if isinstance(leaf, _METRIC_NODES):
                self.metric(leaf.metric, role, output)
            elif isinstance(leaf, (EventTime, RetentionMetric)):
                source = self.event(leaf.event, role, output)
                if leaf.label_property is not None:
                    self.property_dimension(source, role, output, leaf.label_property)
            elif isinstance(leaf, AttributeAsOf):
                resolved = resolve_attribute(
                    sources=self.model.sources,
                    properties=self.model.catalog.properties,
                    roster=self.model._roster_attributes(self.unit),
                    attribute=leaf.attribute,
                    unit=self.unit,
                )
                if resolved.source is None:
                    use = RosterAttributeAssumption(leaf.attribute, role, output)
                    self.roster_attributes[use] = None
                elif resolved.prop.dimension_name is not None:
                    self.dimension(resolved.source.name, role, output, resolved.dimension)
                else:
                    axis = "effective_time" if resolved.dimension.validity else "static"
                    self.add(SourceAssumption(resolved.source.name, role, output, axis))

    def shape(self, design: Design, features: tuple[str, ...] | None) -> None:
        shape = design.shape
        if isinstance(shape, (SequentialSpec, CalendarComparisonSpec)):
            selector = shape.treatment if isinstance(shape, SequentialSpec) else shape.cohort
            source = resolve_selector_source(self.model, selector.event)
            if selector.knowledge == "recorded_at":
                self.add(
                    SourceAssumption(
                        source,
                        "treatment",
                        selector.event,
                        "recorded_at",
                        knowledge="declared_recorded_at",
                    )
                )
            else:
                self.add(SourceAssumption(source, "treatment", selector.event, "effective_time"))
        elif isinstance(shape, StatePanel):
            if isinstance(shape.state, BinnedMetricState):
                self.metric(shape.state.metric, "shape", "state")
            else:
                source = self.model.catalog.properties[shape.state.property].source
                self.add(SourceAssumption(source, "shape", "state", "event_time"))
            for name, expr in shape.baseline_features.items():
                if features is None or name in features:
                    self.output("feature", name, expr)
        elif isinstance(shape, EventSequence):
            source = self.event(shape.event, "shape", "sequence")
            for name in (
                *shape.properties,
                shape.label_property,
                shape.funnel.event_property if shape.funnel is not None else None,
            ):
                if name is not None:
                    self.property_dimension(source, "shape", "sequence", name)
        elif isinstance(shape, CohortSpec):
            self.metric(shape.trajectory.metric, "shape", "trajectory")
        elif isinstance(shape, (LifeTable, TimeToEvent)):
            survival = shape.time_to_event if isinstance(shape, LifeTable) else shape
            for role, event in (
                ("entry", survival.entry_event),
                ("end", survival.end_event),
                ("exit", survival.exit_event),
            ):
                if event is not None:
                    self.event(event, "shape", role)


def _roster(design: Design, model: Model, unit: str) -> RosterAssumption:
    roster = model.units.get(unit)
    if roster is None:
        return RosterAssumption(
            "event_anchored_no_roster" if isinstance(design.anchor, EventAnchor) else "unknown"
        )
    return RosterAssumption(
        "bounded" if roster.entered_at or roster.exited_at else "current_unbounded",
        entered_at=roster.entered_at is not None,
        exited_at=roster.exited_at is not None,
        observed_from=roster.observed_from is not None,
        observed_until=roster.observed_until is not None,
    )


def study_assumptions(
    design: Design,
    model: Model,
    *,
    features: tuple[str, ...] | None = None,
    labels: tuple[str, ...] | None = None,
) -> StudyAssumptions:
    """Report a validated design or the selected training values and their eligibility rules."""
    unit = design.unit
    assert unit is not None  # preview and Frame resolve the unit before compiling
    uses = _Uses(model, unit)
    if isinstance(design.anchor, EventAnchor):
        uses.event(design.anchor.event, "anchor", design.anchor.event)
    uses.shape(design, features)
    if design.treatment is not None:
        treatment = design.treatment
        uses.output("treatment", "treatment", AttributeAsOf(treatment.attribute, treatment.at))
    # Sampling consumes strata before the training projection drops unselected features.
    strata = () if design.sample is None else design.sample.strata
    for name, node in design.features.items():
        if features is None or name in features or name in strata:
            uses.output("feature", name, node)
    decisions = []
    for role, outputs in (("eligibility", design.eligibility), ("outcome", design.outcomes)):
        for name, node in outputs.items():
            if role == "outcome" and labels is not None and name not in labels:
                continue
            uses.output(role, name, node)
            decisions.extend(
                NativeFloatDecision(role, name, metric, events)
                for metric, events in native_decision_dependencies(node, model).items()
            )
    if (
        isinstance(design.shape, StatePanel)
        and isinstance(design.shape.state, BinnedMetricState)
        and (labels is None or "to_state" in labels)
    ):
        metric = design.shape.state.metric
        if events := native_metric_events(metric, model):
            decisions.append(NativeFloatDecision("outcome", "to_state", metric, events))
    return StudyAssumptions(
        tuple(uses.sources),
        _roster(design, model, unit),
        tuple(decisions),
        roster_attributes=tuple(uses.roster_attributes),
    )
