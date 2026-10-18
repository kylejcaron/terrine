"""Metadata-only static validation of a study design."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

from terrine.errors import RefusalSpec
from terrine.semantic.rules import (
    UNALIGNED_CALENDAR_AVERAGE,
    UNSUPPORTED_EVENT_RESOLUTION,
    UNSUPPORTED_TEMPORAL_AGGREGATION,
    UNSUPPORTED_TEMPORAL_KIND,
    UNSUPPORTED_TRANSFORM,
    calendar_average_alignment,
    cutoff_alignment,
    supports_event_resolution,
    supports_temporal_aggregation,
)
from terrine.spec.anchors import EventAnchor, FixedDate, Schedule
from terrine.spec.design import Design
from terrine.spec.expressions import (
    And,
    AttributeAsOf,
    Eq,
    EventTime,
    Expr,
    Gt,
    IsNull,
    MetricAgg,
    Predicate,
)
from terrine.spec.sequence import _UNSUPPORTED_SHAPE, EventSequence
from terrine.windows import Window

if TYPE_CHECKING:
    from terrine.model import Model
__all__ = ["ValidationReport", "validate_design"]

_INVALID = RefusalSpec("terrine.design.invalid", "Design validation failed: {reason}.")
_MISSING = RefusalSpec(
    "terrine.design.missing_reference", "Design references unknown {kind} {name!r}."
)
_AMBIGUOUS = RefusalSpec(
    "terrine.design.ambiguous_dimension", "Property {name!r} resolves to multiple dimensions."
)
_UNIT = RefusalSpec(
    "terrine.design.incompatible_unit", "Analysis unit {unit!r} is incompatible with {name!r}."
)
_WINDOW = RefusalSpec("terrine.design.invalid_window", "Window for {name!r} must have start < end.")
_GAP = RefusalSpec("terrine.design.gap_violation", "{role} window or cutoff violates gap {gap}.")
_METRIC_WINDOW = RefusalSpec(
    "terrine.design.metric_window_conflict",
    "Metric {metric!r} declares a conflicting window_days value.",
)


_SEQUENCE_INVALID = RefusalSpec(
    "terrine.sequence.invalid_spec", "Invalid event sequence design: {reason}."
)
_REANCHOR_LEAK = RefusalSpec(
    "terrine.sequence.reanchor_leak",
    "{role} expression support may cross the required gap {gap}.",
)


@dataclass(frozen=True, slots=True)
class ValidationReport:
    """Row-free summary of required sources and conditions deferred to execution."""

    required_sources: tuple[str, ...]
    runtime_obligations: tuple[str, ...]


def _metric(model: Model, name: str):
    try:
        return model.metrics[name]
    except KeyError as exc:
        raise _MISSING.error(kind="metric", name=name) from exc


def _event_property(model: Model, source_name: str, name: str) -> str:
    prop = model.catalog.properties.get(name)
    if prop is None or prop.source != source_name:
        raise _MISSING.error(kind="event property", name=name)
    source = model.sources.get(source_name)
    if source is None or name not in source.properties:
        raise _MISSING.error(kind="bound event property", name=name)
    return source.properties[name].dtype


def _check_event_scalar(model: Model, source_name: str, name: str, value: object) -> None:
    dtype = _event_property(model, source_name, name)
    matches = {
        "string": isinstance(value, str),
        "int": isinstance(value, int) and not isinstance(value, bool),
        "float": isinstance(value, (int, float)) and not isinstance(value, bool),
        "bool": isinstance(value, bool),
    }
    if value is not None and not matches.get(dtype, False):
        raise _SEQUENCE_INVALID.error(
            reason=f"filter value is incompatible with event property {name!r}"
        )


def _fixed_anchors(design: Design, model: Model) -> tuple[datetime, ...] | None:
    anchor = design.anchor
    if isinstance(anchor, FixedDate):
        value = anchor.date
        if isinstance(value, datetime):
            if value.tzinfo is None or value.utcoffset() is None:
                raise _INVALID.error(reason="fixed datetime anchor must be timezone-aware")
            resolved = value
        else:
            resolved = model.day_boundary(value)
        return (resolved,)
    if isinstance(anchor, Schedule):
        first = model.day_boundary(anchor.start)
        span = (anchor.end - anchor.start).days * 86400
        count = (span - 1) // anchor.every + 1
        if count > 1:
            return first, first + timedelta(seconds=anchor.every)
        return (first,)
    return None


def _required(model: Model, source_name: str, required: set[str]) -> None:
    model.readiness.for_fact(source_name)
    required.add(source_name)


def _resolve_property(
    expr: AttributeAsOf,
    role: str,
    design: Design,
    model: Model,
    required: set[str],
    obligations: list[str],
) -> None:
    if role != "treatment" and expr.before > -design.gap:
        raise _GAP.error(role=role, gap=design.gap)
    declaration = model.catalog.properties.get(expr.attribute)
    owner = declaration.source if declaration is not None else None
    declared_dimension = declaration.dimension if declaration is not None else None
    candidates = []
    for source in model.sources.values():
        if owner is not None and source.name != owner:
            continue
        binding = source.properties.get(expr.attribute)
        if binding is not None and binding.dimension_name is None and declared_dimension is None:
            candidates.append((source, binding, None))
        for dimension in source.dimensions:
            prop = dimension.properties.get(expr.attribute)
            if prop is not None and (
                declared_dimension is None or dimension.name == declared_dimension
            ):
                candidates.append((source, prop, dimension))
    if not candidates:
        raise _MISSING.error(kind="property", name=expr.attribute)
    dimension_names = {
        dimension.name if dimension is not None else None for _, _, dimension in candidates
    }
    if len(dimension_names) > 1:
        raise _AMBIGUOUS.error(name=expr.attribute)
    # An inline binding and a dimension's concrete binding describe the same property;
    # only the latter is retained above. Repeated uses of the same dimension are one
    # logical dimension, with Catalog ownership selecting its source when available.
    unique = {}
    for source, prop, dimension in candidates:
        key = dimension.name if dimension is not None else None
        unique.setdefault(key, (source, prop, dimension))
    source, prop, dimension = next(iter(unique.values()))
    _required(model, source.name, required)
    if dimension is None:
        if design.unit not in source.entities:
            raise _UNIT.error(unit=design.unit, name=expr.attribute)
    else:
        unit_key = source.entities.get(design.unit)
        dimension_key = source.entities.get(dimension.source_entity)
        if unit_key is None or dimension_key is None or unit_key != dimension_key:
            raise _UNIT.error(unit=design.unit, name=expr.attribute)
    temporal_property = (
        dimension.validity is not None if dimension is not None else prop.as_of != "static"
    )
    if temporal_property:
        anchors = _fixed_anchors(design, model)
        if anchors is None:
            obligations.append("asof_cutoff_alignment")
        else:
            alignment = any(
                cutoff_alignment(
                    expr.before, anchor=anchor, day_boundary=model.catalog.day_boundary
                )
                == "unaligned"
                for anchor in anchors
            )
            if alignment and not supports_event_resolution(source.dialect):
                raise UNSUPPORTED_EVENT_RESOLUTION.error(dialect=source.dialect)


def _requires_event_resolution(win: Window, design: Design, model: Model) -> bool:
    anchors = _fixed_anchors(design, model)
    if anchors is None:
        return False
    for instant in anchors:
        if (
            calendar_average_alignment(win, anchor=instant, day_boundary=model.catalog.day_boundary)
            != "aligned"
        ):
            return True
    return False


def _expression(
    expr: Expr, role: str, design: Design, model: Model, required: set[str], obligations: list[str]
) -> None:
    if isinstance(expr, EventTime) and expr.relative_to is not None:
        raise _UNSUPPORTED_SHAPE.error()
    if isinstance(expr, (MetricAgg, AttributeAsOf)) and expr.relative_to is not None:
        raise _UNSUPPORTED_SHAPE.error()
    if isinstance(expr, AttributeAsOf):
        _resolve_property(expr, role, design, model, required, obligations)
        return
    if isinstance(expr, EventTime):
        fact = model.catalog.facts.get(expr.fact)
        if fact is None:
            raise _MISSING.error(kind="fact", name=expr.fact)
        source = model.sources.get(fact.source)
        if source is None or expr.fact not in source.facts:
            raise _MISSING.error(kind="bound fact", name=expr.fact)
        if design.unit not in source.entities:
            raise _UNIT.error(unit=design.unit, name=expr.fact)
        if expr.label_property is not None:
            _check_event_scalar(model, source.name, expr.label_property, expr.equals)
        _required(model, source.name, required)
        obligations.append("coverage_completeness")
        lower, upper = expr.window.start, expr.window.end
        if role in {"feature", "eligibility"} and upper > -design.gap:
            raise _REANCHOR_LEAK.error(role=role, gap=design.gap)
        if role == "outcome" and lower < 0:
            raise _GAP.error(role=role, gap=design.gap)
        return
    metric = _metric(model, expr.metric)
    if metric.entity != design.unit:
        raise _UNIT.error(unit=design.unit, name=expr.metric)
    win = expr.window
    if not isinstance(win, Window) or win.start >= win.end:
        raise _WINDOW.error(name=expr.metric)
    for source_name in metric.source_names:
        _required(model, source_name, required)
    obligations.append("coverage_completeness")
    if _requires_event_resolution(win, design, model):
        for source_name in metric.source_names:
            source = model.sources[source_name]
            if not supports_event_resolution(source.dialect):
                raise UNSUPPORTED_EVENT_RESOLUTION.error(dialect=source.dialect)
    if role in {"feature", "eligibility"} and win.end > -design.gap:
        raise _GAP.error(role=role, gap=design.gap)
    if role == "outcome" and win.start < 0:
        raise _GAP.error(role=role, gap=design.gap)
    if metric.kind not in {"mean", "conversion", "ratio"}:
        raise UNSUPPORTED_TEMPORAL_KIND.error(kind=metric.kind)
    if metric.transforms:
        raise UNSUPPORTED_TRANSFORM.error(transform=type(metric.transforms[0]).__name__)
    for measure in metric.measures:
        if not supports_temporal_aggregation(measure.aggregation):
            raise UNSUPPORTED_TEMPORAL_AGGREGATION.error(aggregation=measure.aggregation)
        if measure.window_days is not None and win.end - win.start != measure.window_days * 86400:
            raise _METRIC_WINDOW.error(metric=expr.metric)
        if measure.aggregation == "avg_calendar_day":
            anchors = _fixed_anchors(design, model)
            if anchors is None:
                alignment = calendar_average_alignment(
                    win, anchor=None, day_boundary=model.catalog.day_boundary
                )
                if alignment == "unaligned":
                    raise UNALIGNED_CALENDAR_AVERAGE.error()
                obligations.append("calendar_average_alignment")
            else:
                for anchor in anchors:
                    alignment = calendar_average_alignment(
                        win, anchor=anchor, day_boundary=model.catalog.day_boundary
                    )
                    if alignment == "unaligned":
                        raise UNALIGNED_CALENDAR_AVERAGE.error()


def _predicate(
    predicate: Predicate, design: Design, model: Model, required: set[str], obligations: list[str]
) -> None:
    if isinstance(predicate, And):
        for item in predicate.terms:
            _predicate(item, design, model, required, obligations)
    elif isinstance(predicate, Gt):
        _expression(predicate.expr, "eligibility", design, model, required, obligations)
    elif isinstance(predicate, Eq):
        _expression(predicate.left, "eligibility", design, model, required, obligations)
    elif isinstance(predicate, IsNull):
        _expression(predicate.expr, "eligibility", design, model, required, obligations)
    else:
        raise _INVALID.error(reason="unsupported predicate")


def _range_bound(value: date | datetime | None, model: Model) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise _INVALID.error(reason="anchor range datetime bounds must be timezone-aware")
        return value.astimezone(UTC)
    return model.day_boundary(value)


def _validate_sequence_shape(design: Design, model: Model, required: set[str]) -> None:
    sequence = design.shape
    if not isinstance(sequence, EventSequence):
        return
    if not isinstance(design.anchor, EventAnchor):
        raise _SEQUENCE_INVALID.error(reason="event sequences require an EventAnchor")
    if design.treatment is not None:
        raise _SEQUENCE_INVALID.error(reason="event sequences do not support treatment")
    if any(not isinstance(expr, EventTime) for expr in design.outcomes.values()):
        raise _SEQUENCE_INVALID.error(reason="event sequences only support EventTime outcomes")
    fact = model.catalog.facts.get(sequence.fact)
    if fact is None:
        raise _MISSING.error(kind="fact", name=sequence.fact)
    source = model.sources.get(fact.source)
    if source is None or sequence.fact not in source.facts:
        raise _MISSING.error(kind="bound fact", name=sequence.fact)
    if design.unit not in source.entities:
        raise _UNIT.error(unit=design.unit, name=sequence.fact)
    _required(model, source.name, required)
    requested_properties = [*sequence.properties]
    if sequence.label_property is not None:
        requested_properties.append(sequence.label_property)
    if sequence.funnel is not None:
        requested_properties.append(sequence.funnel.event_property)
    for property_name in requested_properties:
        _event_property(model, source.name, property_name)


def _validate_shape(design: Design, model: Model, required: set[str]) -> None:
    from terrine.spec.state_panel import StatePanel, validate_state_panel

    if isinstance(design.shape, StatePanel):
        validate_state_panel(design, model, required)
    elif isinstance(design.shape, EventSequence):
        _validate_sequence_shape(design, model, required)
    elif design.shape != "cross_section":
        raise _INVALID.error(reason="unsupported design shape")


def validate_design(design: Design, model: Model) -> ValidationReport:
    required: set[str] = set()
    obligations: list[str] = []
    _validate_shape(design, model, required)
    anchor = design.anchor
    if isinstance(anchor, EventAnchor):
        fact = model.catalog.facts.get(anchor.fact)
        if fact is None:
            raise _MISSING.error(kind="fact", name=anchor.fact)
        source = model.sources.get(fact.source)
        if source is None:
            raise _MISSING.error(kind="event source", name=fact.source)
        if anchor.fact not in source.facts:
            raise _MISSING.error(kind="bound fact", name=anchor.fact)
        if not supports_event_resolution(source.dialect):
            raise UNSUPPORTED_EVENT_RESOLUTION.error(dialect=source.dialect)
        if design.unit not in source.entities:
            raise _UNIT.error(unit=design.unit, name=anchor.fact)
        _required(model, source.name, required)
        obligations.append("event_anchor_alignment")
    elif isinstance(anchor, (FixedDate, Schedule)) and design.unit not in model.units:
        raise _UNIT.error(unit=design.unit, name="roster")

    for role, expressions in (("feature", design.features), ("outcome", design.outcomes)):
        for expr in expressions.values():
            _expression(expr, role, design, model, required, obligations)
    from terrine.spec.state_panel import StatePanel

    if isinstance(design.shape, StatePanel):
        for expr in design.shape.baseline_features.values():
            _expression(expr, "feature", design, model, required, obligations)
    for predicate in design.eligibility.values():
        _predicate(predicate, design, model, required, obligations)
    if design.treatment is not None:
        treatment = design.treatment
        _expression(
            AttributeAsOf(treatment.attribute, treatment.at),
            "treatment",
            design,
            model,
            required,
            obligations,
        )

    if design.anchor_range is not None:
        start = _range_bound(design.anchor_range.start, model)
        end = _range_bound(design.anchor_range.end, model)
        if start is not None and end is not None and start >= end:
            raise _INVALID.error(reason="anchor range start must precede exclusive end")
    return ValidationReport(tuple(sorted(required)), tuple(dict.fromkeys(obligations)))
