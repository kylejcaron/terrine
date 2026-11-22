"""Compact per-age support and reduction for a single elapsed SUM measure."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import ibis

from terrine.backends.dialect import current_dialect
from terrine.backends.numeric import exact_literal, truncating_quotient
from terrine.backends.sequences import integer_sequence
from terrine.data import RowCheck
from terrine.semantic.bind import normalize_events
from terrine.temporal.aggregate import (
    TemporalResult,
    _Builder,
    _finalize_window,
    _local_component,
    _Primitive,
    _reduce_payload,
    _window_context,
)
from terrine.temporal.daily import (
    _require_backend,
    contributions,
    native_arithmetic,
    resolve_measure,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import pyarrow as pa

    from terrine.compile.columns import CompiledStudy
    from terrine.model import Model
    from terrine.semantic.models import BoundMetric
    from terrine.spec.trajectory import Trajectory
    from terrine.windows import Window

_ORIGIN = "cohort_time"
_KEYS = ("unit", "anchor_id", _ORIGIN, "age")
_MICROS = 1_000_000
_OUTPUT = "__terrine_value_0"


@dataclass(frozen=True, slots=True)
class PeriodChecks:
    """Report the same first age, check and failed-anchor count as wide windows."""

    counts: ibis.Table
    checks: tuple[RowCheck, ...]

    @classmethod
    def from_result(cls, result: TemporalResult) -> PeriodChecks:
        periods = result.table
        counts = periods.group_by("age").aggregate(
            **{
                check.column: periods[check.column].ifelse(1, 0).sum().cast("int64")
                for check in result.row_checks
            }
        )
        failed = ibis.or_(*(counts[check.column] > 0 for check in result.row_checks))
        return cls(counts.filter(failed).order_by("age").limit(1), result.row_checks)

    def reads(self) -> tuple[ibis.Table, ...]:
        return (self.counts,)

    def validate(self, execute: Callable[[ibis.Table], pa.Table]) -> None:
        summary = execute(self.counts)
        if summary.num_rows == 0:
            return
        (row,) = summary.to_pylist()
        for check in self.checks:
            count = row[check.column]
            if count:
                context = {**check.context, "output": f"__terrine_value_{row['age']}"}
                raise check.refusal.error(**context, failed_count=count)


def compact_periods(
    compiled: CompiledStudy,
    model: Model,
    unit: str,
    trajectory: Trajectory,
    metric: BoundMetric,
    window: Window,
) -> TemporalResult | None:
    """Lower the proven native microsecond route; leave other semantics to wide windows."""
    measure = metric.measures[0]
    source = model.sources[measure.source_name]
    if source.dimensions or source.row_key is not None or measure.filters:
        return None
    # The wide compiler admits the clock and source after anchor compilation.
    raw = model._relation(source.relation)
    anchors = compiled._anchors.table
    dialect = current_dialect(anchors.t0)
    period = trajectory.period.seconds
    horizon = period * trajectory.periods
    coverage = model.readiness.for_fact(source.name)
    if (
        dialect.native_shifted_comparison_keys(
            raw[source.time_column],
            anchors.t0,
            offset_seconds=horizon,
            right_bounds=(coverage.start, coverage.end_exclusive),
        )
        is None
    ):
        return None
    events = normalize_events(raw, source, source.units[unit])
    measure = resolve_measure(events, measure, source=source)
    if not native_arithmetic(measure):
        _require_backend(events)
    metric = replace(metric, measures=(measure,))

    ages = integer_sequence(trajectory.periods, "age", dialect=dialect.name)
    points = anchors.select(
        "unit", "anchor_id", anchors.t0.name(_ORIGIN), "_observed_from", "_observed_until"
    ).cross_join(ages)
    # Rebase only this period clock, never any baseline feature or anchor identity.
    points = points.mutate(t0=points[_ORIGIN] + (points.age * (period * _MICROS)).as_interval("us"))
    builder = _Builder(points, _OUTPUT)
    builder.used.update(name.casefold() for name in events.columns)
    context, checks = _window_context(
        builder, metric, window, model.readiness, model.catalog.day_boundary, _OUTPUT, shared={}
    )
    scaffold = builder.table
    supported = scaffold.filter(scaffold[context.available])
    # Readiness and roster observation are single intervals. The complete periods
    # therefore form one contiguous run; no event can belong to two of its ages.
    lower, upper = (builder.name(name) for name in ("range_start", "range_end"))
    ranges = supported.group_by(*_KEYS[:3]).aggregate(
        **{lower: supported[context.start].min(), upper: supported[context.end].max()}
    )
    value_column = source.events[measure.event]
    assert value_column is not None
    events = events.select(*dict.fromkeys(("_unit", "_timestamp", value_column)))
    # Project the anchor keys to a separate namespace; physical fact names are user-owned.
    keys = [builder.name(name) for name in _KEYS]
    fact_unit, _, fact_origin, fact_age = keys
    ranges = ranges.rename(**dict(zip(keys[:3], _KEYS[:3], strict=True)))
    joined = events.inner_join(
        ranges,
        (events._unit == ranges[fact_unit])
        & (events._timestamp >= ranges[lower])
        & (events._timestamp < ranges[upper]),
    )
    elapsed = joined._timestamp.delta(joined[fact_origin], unit="microsecond")
    age = truncating_quotient(elapsed, period * _MICROS, like=joined._timestamp)
    joined = joined.mutate(**{fact_age: age.cast("int64")})
    payload, fields = contributions(joined, measure, source=source, _name=builder.name)
    names = {field: builder.name(field) for field in fields}
    leaf = payload.select(*keys, **{names[field]: value for field, value in fields.items()})
    reduced = _reduce_payload(leaf, {_Primitive(measure, window, False): names}, keys)
    attached = scaffold.left_join(
        reduced,
        [scaffold[key] == reduced[alias] for key, alias in zip(_KEYS, keys, strict=True)],
    ).drop(*keys)
    builder.table = attached.mutate(
        **{
            name: attached[name].fill_null(
                ibis.literal(0.0)
                if attached[name].type().is_floating()
                else exact_literal(0, attached[name])
            )
            for name in names.values()
        }
    )
    quantity = _local_component(
        builder,
        measure,
        names,
        days=period // 86400,
        conversion=False,
        context={"source": source.name, "measure": measure.event, "output": _OUTPUT},
    )
    return _finalize_window(
        builder,
        points,
        metric,
        window,
        _OUTPUT,
        "outcome",
        (quantity,),
        context.available,
        context.unavailable,
        context.observed_from,
        context.observed_until,
        list(checks),
        (),
    )
