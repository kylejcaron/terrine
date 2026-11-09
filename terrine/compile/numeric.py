"""Lower additive metric windows onto shared local reductions."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

import ibis

from terrine.semantic.rules import calendar_average_alignment
from terrine.temporal.aggregate import (
    AggregateRequest,
    TemporalResult,
    aggregate_windows,
)
from terrine.temporal.daily import native_arithmetic, resolve_measure
from terrine.windows import Window

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["CompileContext", "LoweredAggregates", "lower_aggregates"]


@dataclass(frozen=True, slots=True)
class CompileContext:
    """Declaration-proven origins and normalized source access for one compilation."""

    model: Model
    unit: str
    fixed_anchors: Sequence[datetime] | None
    events_for: Callable[[str], ibis.Table]


def _resolution(ctx: CompileContext, window: Window, *, relative: bool) -> Literal["day", "event"]:
    """Daily inputs require declaration-proven local-midnight window bounds."""
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
