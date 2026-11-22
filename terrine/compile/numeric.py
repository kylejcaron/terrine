"""Lower additive metric windows onto shared local reductions."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.dialect import current_dialect
from terrine.compile.support import WindowUse, _shared_input_span
from terrine.data import CoverageSummary, _SourceRead
from terrine.execution.backend import _event_partition
from terrine.semantic.models import TableRef
from terrine.semantic.rules import calendar_average_alignment
from terrine.spec.anchors import FixedDate, Schedule
from terrine.temporal.aggregate import (
    AggregateRequest,
    TemporalResult,
    aggregate_windows,
)
from terrine.temporal.daily import resolve_measure
from terrine.windows import Window

if TYPE_CHECKING:
    from terrine.model import Model
    from terrine.semantic.models import BoundSource
    from terrine.spec.design import Design

__all__ = ["CompileContext", "LoweredAggregates", "lower_aggregates"]


@dataclass(frozen=True, slots=True)
class CompileContext:
    """Declaration-proven origin extrema and unpruned source access for a compilation."""

    model: Model
    unit: str
    fixed_anchors: Sequence[datetime] | None
    origin_bounds: tuple[datetime, datetime] | None
    events_for: Callable[[str], ibis.Table]


def _declared_origin_bounds(design: Design, model: Model) -> tuple[datetime, datetime] | None:
    """Conservative UTC extrema from ordinary origin declarations, without reading rows."""
    anchor = design.anchor
    try:
        if isinstance(anchor, FixedDate):
            value = anchor.date
            resolved = value if isinstance(value, datetime) else model.day_boundary(value)
            instant = resolved.astimezone(UTC)
            return instant, instant
        if isinstance(anchor, Schedule):
            count = anchor.count(model.day_boundary)
            if count:
                return (
                    anchor.instant(0, model.day_boundary).astimezone(UTC),
                    anchor.instant(count - 1, model.day_boundary).astimezone(UTC),
                )
    except OverflowError:
        return None
    return None


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


def _bounded_fact_inputs(
    sources: dict[str, tuple[ibis.Table, BoundSource]],
    requests: Sequence[AggregateRequest],
    *,
    ctx: CompileContext,
    resolution: Literal["event", "day"],
    origin_bounds: tuple[datetime, datetime] | None,
) -> tuple[Mapping[str, tuple[ibis.Table, BoundSource]], list[_SourceRead]]:
    uses: dict[str, set[WindowUse]] = {}
    for request in requests:
        for name in request.metric.source_names:
            uses.setdefault(name, set()).add(
                WindowUse(name, ctx.unit, request.window.start.seconds, request.window.end.seconds)
            )
    bounded = {}
    reads = []
    for name, consumers in uses.items():
        events, source = sources[name]
        span = _shared_input_span(consumers, origin_bounds) if origin_bounds is not None else None
        predicate, key, reason = _source_bound(ctx, events, source, span)
        if predicate is not None:
            bounded[name] = (events.filter(*predicate), source)
        reads.append(
            _SourceRead(
                family=f"local_{resolution}",
                source=name,
                event_span=CoverageSummary(*span) if predicate is not None and span else None,
                physical_key=key,
                reason=reason,
            )
        )
    # Each local reduction receives its own envelope. The shared bindings
    # still feed ranking, history and whole-input checks unchanged.
    return sources | bounded if bounded else sources, reads


def _source_bound(
    ctx: CompileContext,
    events: ibis.Table,
    source: BoundSource,
    span: tuple[datetime, datetime] | None,
) -> tuple[tuple[ir.BooleanValue, ir.BooleanValue] | None, str | None, str | None]:
    if span is None:
        return None, None, "event_bounds_unknown"
    if source.dimensions or not isinstance(source.relation, TableRef):
        return None, None, "indirect_source"
    timestamp = events._timestamp
    dtype = timestamp.type()
    if not isinstance(dtype, dt.Timestamp):
        return None, None, "timestamp_comparison_unavailable"
    start, end = span
    if dtype.timezone is None:
        # Naive event clocks denote UTC. Keep the column in its own domain.
        start, end = start.replace(tzinfo=None), end.replace(tzinfo=None)
    dialect = current_dialect(timestamp)
    lower = dialect.literal_bound_keys(timestamp, cast(ir.TimestampValue, ibis.literal(start)))
    upper = dialect.literal_bound_keys(timestamp, cast(ir.TimestampValue, ibis.literal(end)))
    if lower is None or upper is None:
        return None, None, "timestamp_comparison_unavailable"
    key, reason = None, "layout_unverified"
    if lower[0].equals(timestamp) and upper[0].equals(timestamp):
        key, reason = _event_partition(ctx.model._engine, ctx.model.con, source, timestamp)
    return (lower[0] >= lower[1], upper[0] < upper[1]), key, reason


@dataclass(frozen=True, slots=True)
class LoweredAggregates:
    """A batch of local additive windows and their declared physical strategies."""

    result: TemporalResult
    strategies: Mapping[str, str]
    source_reads: tuple[_SourceRead, ...]

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
    sources: dict[str, tuple[ibis.Table, BoundSource]] = {}
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
        strategies[request.output] = resolution
    table = anchors
    columns = {}
    checks = []
    source_reads = []
    origin_bounds = None if relative else ctx.origin_bounds
    for resolution, group in groups.items():
        inputs, reads = _bounded_fact_inputs(
            sources, group, ctx=ctx, resolution=resolution, origin_bounds=origin_bounds
        )
        source_reads.extend(reads)
        result = aggregate_windows(
            table,
            group,
            inputs,
            ctx.model.readiness,
            boundary=ctx.model.catalog.day_boundary,
            resolution=resolution,
            _origin_bounds=origin_bounds,
        )
        table = result.table
        for output, meta in result.columns.items():
            arithmetic = "native_float" if meta.arithmetic == "native" else "exact"
            strategies[output] = f"local_{strategies[output]}/{arithmetic}"
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
        tuple(source_reads),
    )
