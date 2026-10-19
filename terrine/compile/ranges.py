"""Metadata-only nonadditive range admission for ordinary and repeated anchors."""

from __future__ import annotations

from terrine.spec.anchors import EventAnchor, FixedDate, Schedule
from terrine.spec.design import Design
from terrine.spec.expressions import (
    And,
    ApproxMetricDistinct,
    ApproxMetricQuantile,
    Eq,
    Expr,
    MetricAgg,
    MetricQuantile,
)
from terrine.spec.panels import EventPanelSpec
from terrine.temporal.ranges import _MAX_CANDIDATES, _RANGE_BOUND, RangePlan

_ROUTE = (
    "use first/nth event anchors, fewer event periods or scheduled anchors, a coarser "
    "schedule/narrower window, or an additive metric using sparse prefix lookups"
)


def plan_ranges(design: Design, model) -> dict[Expr, RangePlan]:
    """Derive candidate bounds without querying relation sizes or event density."""
    expressions = set(design.features.values()) | set(design.outcomes.values())

    def collect(predicate):
        if isinstance(predicate, And):
            for term in predicate.terms:
                collect(term)
        else:
            expressions.add(predicate.left if isinstance(predicate, Eq) else predicate.expr)

    for predicate in design.eligibility.values():
        collect(predicate)
    plans = {}
    for expression in expressions:
        nonadditive = isinstance(
            expression, (MetricQuantile, ApproxMetricQuantile, ApproxMetricDistinct)
        ) or (
            isinstance(expression, MetricAgg)
            and len(model.metrics[expression.metric].measures) == 1
            and model.metrics[expression.metric].measures[0].aggregation
            in {"min", "max", "count_distinct"}
        )
        if not nonadditive:
            continue
        width = expression.window.end - expression.window.start
        anchor = design.anchor
        step = None
        count = None
        if isinstance(anchor, FixedDate):
            count, basis = 1, "fixed"
        elif isinstance(anchor, Schedule):
            span = (anchor.end - anchor.start).days * 86_400
            count = (span - 1) // anchor.every + 1
            step, basis = anchor.every, "schedule"
        elif isinstance(anchor, EventAnchor) and anchor.which in {"first", "nth"}:
            count, basis = 1, "single_event"
        else:
            basis = "unknown_event_multiplicity"
        if isinstance(design.shape, EventPanelSpec) and count is not None:
            count *= len(design.shape.periods)
            step, basis = design.shape.period_seconds, "single_event_periods"
        if getattr(expression, "relative_to", None) is not None:
            step = None
            basis += "/reanchored"
        candidates = (
            min(count, 2 * ((width + step - 1) // step))
            if count is not None and step is not None
            else count
        )
        if candidates is None or candidates > _MAX_CANDIDATES or width >= 2**63:
            reason = (
                "event-anchor multiplicity is unknown"
                if candidates is None
                else "the declared overlap exceeds the supported bound"
            )
            raise _RANGE_BOUND.error(
                expression=repr(expression),
                reason=reason,
                limit=_MAX_CANDIDATES,
                candidates_per_event=candidates,
                route=_ROUTE,
            )
        plans[expression] = RangePlan(width, candidates, basis)
    return plans
