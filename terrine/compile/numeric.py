"""Lowering additive metric windows onto local reductions and sparse prefix states."""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

import ibis

from terrine.compile.layers import build_measure_layer, measure_identity
from terrine.semantic.models import BoundMeasure, BoundMetric
from terrine.semantic.rules import calendar_average_alignment
from terrine.temporal.aggregate import (
    AggregateRequest,
    TemporalResult,
    aggregate_window,
    aggregate_windows,
)
from terrine.temporal.daily import native_arithmetic, resolve_measure
from terrine.windows import Window

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["CompileContext", "LoweredAggregate", "lower_aggregate"]

# (source, measure identity, unit, resolution) of one prefix state.
type StateKey = tuple[str, str, str, str]


@dataclass(frozen=True, slots=True)
class CompileContext:
    """The compilation inputs window lowering reads, and the prefix states it shares.

    ``states`` holds every prefix relation built or substituted so far, by source,
    measure identity, unit and resolution; lowering adds each state it builds, so
    windows over the same measure and resolution read one relation.
    """

    model: Model
    unit: str
    fixed_anchors: Sequence[datetime] | None
    events_for: Callable[[str], ibis.Table]
    states: MutableMapping[StateKey, ibis.Table]


@dataclass(frozen=True, slots=True)
class LoweredAggregate:
    """One lowered additive metric window and the strategy that evaluates it."""

    result: TemporalResult
    strategy: str


def _resolution(ctx: CompileContext, window: Window, *, relative: bool) -> Literal["day", "event"]:
    """Day states answer fixed anchors whose window bounds are all local midnights."""
    fixed = ctx.fixed_anchors
    if (
        fixed
        and not relative
        and all(
            calendar_average_alignment(
                window, anchor=instant, day_boundary=ctx.model.catalog.day_boundary
            )
            == "aligned"
            for instant in fixed
        )
    ):
        return "day"
    return "event"


def lower_aggregate(
    ctx: CompileContext,
    metric: BoundMetric,
    window: Window,
    *,
    anchors: ibis.Table,
    output: str,
    relative: bool,
) -> LoweredAggregate:
    """Lower one additive metric window over ``anchors`` to its value relation.

    Each measure reads the prefix state of its identity and resolution, building
    it once per compilation unless a persisted substitution already supplies it.
    The measure's declaration decides that state's arithmetic: a native float
    measure reads binary64 running sums, any other exact coefficients.
    ``relative`` windows start from a selected event time, so they read event
    states.
    """
    model = ctx.model
    boundary = model.catalog.day_boundary
    resolution = _resolution(ctx, window, relative=relative)
    resolved: list[BoundMeasure] = []
    supplied: dict[BoundMeasure, ibis.Table] = {}
    for measure in metric.measures:
        source = model.sources[measure.source_name]
        events = ctx.events_for(source.name)
        measure = resolve_measure(events, measure, source=source)
        key = (measure.source_name, measure_identity(measure), ctx.unit, resolution)
        if key not in ctx.states:
            ctx.states[key] = build_measure_layer(
                events, measure, source, boundary=boundary, resolution=resolution
            )
        resolved.append(measure)
        supplied[measure] = ctx.states[key]
    result = aggregate_window(
        anchors,
        replace(metric, measures=tuple(resolved)),
        supplied,
        window,
        model.readiness,
        boundary=boundary,
        output=output,
        role="feature",
        resolution=resolution,
    )
    # Native float windows record their arithmetic in the strategy.
    native = any(native_arithmetic(measure) for measure in resolved)
    return LoweredAggregate(
        result, f"sparse_{resolution}_prefix/{'native_float' if native else 'native'}"
    )


@dataclass(frozen=True, slots=True)
class LoweredAggregates:
    """A batch of local additive windows and their declared physical strategies."""

    result: TemporalResult
    strategies: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "strategies", MappingProxyType(dict(self.strategies)))


def lower_aggregates(
    ctx: CompileContext,
    requests: Sequence[AggregateRequest],
    *,
    anchors: ibis.Table,
    relative: bool,
) -> LoweredAggregates:
    """Resolve and batch compatible raw or aligned daily window reductions."""
    sources = {}
    groups: dict[Literal["event", "day"], list[AggregateRequest]] = {}
    strategies = {}
    repeated = ctx.fixed_anchors is not None and len(set(ctx.fixed_anchors)) > 1
    for request in requests:
        resolved = []
        for measure in request.metric.measures:
            if measure.source_name not in sources:
                source = ctx.model.sources[measure.source_name]
                sources[source.name] = (ctx.events_for(source.name), source)
            events, source = sources[measure.source_name]
            resolved.append(resolve_measure(events, measure, source=source))
        resolution = _resolution(ctx, request.window, relative=relative) if repeated else "event"
        groups.setdefault(resolution, []).append(
            replace(request, metric=replace(request.metric, measures=tuple(resolved)))
        )
        native = any(native_arithmetic(measure) for measure in resolved)
        strategies[request.output] = f"local_{resolution}/{'native_float' if native else 'exact'}"
    table = anchors
    columns = {}
    checks = []
    for resolution, group in groups.items():
        result = aggregate_windows(
            table,
            group,
            sources,
            ctx.model.readiness,
            boundary=ctx.model.catalog.day_boundary,
            resolution=resolution,
        )
        table = result.table
        columns.update(result.columns)
        checks.extend(result.row_checks)
    ordered = {request.output: columns[request.output] for request in requests}
    public = list(anchors.columns)
    for output, meta in ordered.items():
        public.extend(
            name
            for name in (
                output,
                meta.observed_from_column,
                meta.observed_until_column,
                meta.complete_column,
                meta.reason_column,
            )
            if name is not None
        )
    public.extend(check.column for check in checks)
    return LoweredAggregates(
        TemporalResult(table.select(*dict.fromkeys(public)), ordered, row_checks=tuple(checks)),
        strategies,
    )
