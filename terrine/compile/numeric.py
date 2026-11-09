"""Lowering of additive metric windows onto shared sparse prefix states."""

from __future__ import annotations

from collections.abc import Callable, MutableMapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import TYPE_CHECKING, Literal

import ibis

from terrine.compile.layers import build_measure_layer, measure_identity
from terrine.semantic.models import BoundMeasure, BoundMetric
from terrine.semantic.rules import calendar_average_alignment
from terrine.temporal.aggregate import TemporalResult, aggregate_window
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
