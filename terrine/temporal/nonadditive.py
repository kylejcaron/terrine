"""Exact nonadditive window operations and local order-statistic references."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from fractions import Fraction
from typing import Any, Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.timestamps import compare_timestamps
from terrine.data import ColumnMeta
from terrine.errors import RefusalSpec
from terrine.semantic.models import BoundMeasure, BoundSource
from terrine.semantic.rules import UNKNOWN_FILTER
from terrine.temporal.aggregate import TemporalResult, _Check, _Failure
from terrine.temporal.daily import _predicate
from terrine.temporal.ranges import RangePlan, blocked_range_join
from terrine.temporal.readiness import Readiness
from terrine.windows import Window

__all__ = ["aggregate_nonadditive_window", "exact_quantile"]

_EXACT_PRECISION = RefusalSpec(
    "terrine.metric.exact_precision_unavailable",
    "Exact {operation} requires output precision unavailable for source type {value_type!r}; "
    "use an exactly representable type/probability or a wider supported numeric backend.",
)
_UNSUPPORTED_TYPE = RefusalSpec(
    "terrine.metric.nonadditive_type_unsupported",
    "Exact {operation} does not support source type {value_type!r}; use an ordered numeric fact.",
)


def _validated_probability(value: Decimal) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError("probability must be a Decimal")
    if not value.is_finite() or not Decimal(0) <= value <= Decimal(1):
        raise ValueError("probability must be finite and in [0, 1]")
    return value


def _max(value: ir.NumericValue) -> ir.NumericValue:
    """Use Ibis' dynamically provided aggregate method with a numeric output type."""
    return cast(ir.NumericValue, cast(Any, value).max())


def _decimal_exact(value: Fraction) -> Decimal:
    denominator = value.denominator
    twos = fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator != 1:
        raise ArithmeticError("exact decimal interpolation produced a non-decimal rational")
    scale = max(twos, fives)
    coefficient = value.numerator * 2 ** (scale - twos) * 5 ** (scale - fives)
    digits = tuple(int(digit) for digit in str(abs(coefficient)))
    return Decimal((int(coefficient < 0), digits, -scale))


def exact_quantile(values: Iterable[int | Decimal | None], probability: Decimal) -> int | Decimal:
    """Return Hyndman--Fan type 7 with exact integer/Decimal arithmetic."""
    probability = _validated_probability(probability)
    ordered = sorted(value for value in values if value is not None)
    if not ordered:
        raise ValueError("quantile is undefined for an empty or all-null window")
    if any(isinstance(value, bool) or not isinstance(value, (int, Decimal)) for value in ordered):
        raise TypeError("exact quantile supports integer and Decimal values only")
    rank = Fraction(len(ordered) - 1) * Fraction(probability)
    lower = rank.numerator // rank.denominator
    remainder = rank - lower
    if remainder == 0:
        return ordered[lower]
    left, right = ordered[lower], ordered[lower + 1]
    return _decimal_exact(Fraction(left) + (Fraction(right) - Fraction(left)) * remainder)


def _exact_numeric_dtype(value_type: str, operation: str, probability: Decimal | None):
    dtype = dt.dtype(value_type)
    if operation in {"min", "max", "count_distinct"}:
        if dtype.is_integer() or dtype.is_decimal() or dtype.is_floating():
            return dtype
        raise _UNSUPPORTED_TYPE.error(operation=operation, value_type=value_type)
    if operation == "approx_count_distinct":
        return dt.int64
    if not (dtype.is_integer() or isinstance(dtype, dt.Decimal)):
        raise _UNSUPPORTED_TYPE.error(operation=operation, value_type=value_type)
    probability_scale = (
        max(0, -int(probability.as_tuple().exponent)) if probability is not None else 0
    )
    if isinstance(dtype, dt.Decimal):
        source_scale = dtype.scale or 0
        source_precision = dtype.precision or 38
        integer_digits = source_precision - source_scale
    else:
        source_scale = 0
        source_precision = len(str(cast(Any, dtype).bounds.upper))
        integer_digits = source_precision
    output_scale = source_scale + probability_scale
    output_precision = integer_digits + output_scale
    interpolation_scale = source_scale + 2 * probability_scale
    interpolation_precision = source_precision + 2 * probability_scale + 4
    if (
        output_scale > 38
        or output_precision > 38
        or interpolation_scale > 38
        or interpolation_precision > 38
        or 21 + 2 * probability_scale > 38
    ):
        raise _EXACT_PRECISION.error(operation=operation, value_type=value_type)
    return dt.Decimal(output_precision, output_scale)


def aggregate_nonadditive_window(
    anchors: ibis.Table,
    events: ibis.Table,
    measure: BoundMeasure,
    source: BoundSource,
    window: Window,
    readiness: Readiness,
    *,
    output: str,
    role: str,
    operation: Literal["min", "max", "count_distinct", "approx_count_distinct", "quantile"],
    probability: Decimal | None = None,
    unit_column: str = "unit",
    range_plan: RangePlan | None = None,
) -> TemporalResult:
    """Aggregate only matched sparse range-join rows, retaining completeness metadata."""
    if operation == "quantile":
        if probability is None:
            raise ValueError("quantile requires a probability")
        probability = _validated_probability(probability)
    if measure.value_type is None:
        raise ValueError("nonadditive aggregation requires a resolved measure")
    value_type = measure.value_type
    output_type = _exact_numeric_dtype(value_type, operation, probability)
    value_column = source.facts[measure.fact]
    if value_column is None:
        raise _UNSUPPORTED_TYPE.error(operation=operation, value_type="event_count")
    if (
        unit_column not in anchors.columns
        or "_unit" not in events.columns
        or "_timestamp" not in events.columns
    ):
        raise ValueError("nonadditive window inputs lack unit or timestamp columns")

    predicate, *_ = _predicate(events, measure, source)
    selected = events.filter(predicate.fill_null(False)).select(
        __terrine_nonadditive_event_unit=events._unit,
        __terrine_nonadditive_event_timestamp=events._timestamp,
        __terrine_nonadditive_value=events[value_column],
    )
    start = anchors.t0 + ibis.interval(seconds=window.start)
    end = anchors.t0 + ibis.interval(seconds=window.end)
    event_unit = "__terrine_nonadditive_event_unit"
    event_time = "__terrine_nonadditive_event_timestamp"
    event_value = "__terrine_nonadditive_value"
    conditions = [
        anchors[unit_column] == selected[event_unit],
        compare_timestamps(selected[event_time], start, "ge"),
        compare_timestamps(selected[event_time], end, "lt"),
    ]
    matched = (
        anchors.inner_join(selected, conditions)
        if range_plan is None
        else blocked_range_join(
            anchors,
            selected,
            plan=range_plan,
            window=window,
            event_unit=event_unit,
            event_timestamp=event_time,
            unit_column=unit_column,
        ).matched
    )
    value = matched[event_value]
    groups = [matched[unit_column], matched.anchor_id]
    summary = matched.group_by(*groups).aggregate(
        __terrine_nonadditive_event_count=matched.count(),
        __terrine_nonadditive_nonnull_count=value.count(),
    )
    if operation == "min":
        grouped = matched.group_by(*groups).aggregate(**{output: value.min()})
    elif operation == "max":
        grouped = matched.group_by(*groups).aggregate(**{output: value.max()})
    elif operation == "count_distinct":
        grouped = matched.group_by(*groups).aggregate(**{output: value.nunique()})
    elif operation == "approx_count_distinct":
        grouped = matched.group_by(*groups).aggregate(**{output: value.approx_nunique()})
    else:
        assert probability is not None
        nonnull = matched.filter(value.notnull())
        ordered_groups = [nonnull[unit_column], nonnull.anchor_id]
        ordering_window = ibis.window(group_by=ordered_groups, order_by=nonnull[event_value])
        grouping_window = ibis.window(group_by=ordered_groups)
        ranked = nonnull.mutate(
            __terrine_nonadditive_rank=ibis.row_number().over(ordering_window),
            __terrine_nonadditive_n=nonnull[event_value].count().over(grouping_window),
        )
        probability_scale = max(0, -int(probability.as_tuple().exponent))
        probability_literal = ibis.literal(
            probability,
            type=dt.Decimal(probability_scale + 1, probability_scale),
        )
        type7_rank = (
            (ranked.__terrine_nonadditive_n - 1).cast(
                dt.Decimal(19 + probability_scale, probability_scale)
            )
            * probability_literal
        ).cast(dt.Decimal(19 + probability_scale, probability_scale))
        lower_rank = type7_rank.floor()
        order_value = ranked[event_value].cast(output_type)
        lower = _max(
            cast(
                ir.NumericValue,
                ibis.ifelse(
                    ranked.__terrine_nonadditive_rank == lower_rank,
                    order_value,
                    ibis.null().cast(output_type),
                ),
            )
        )
        upper = _max(
            cast(
                ir.NumericValue,
                ibis.ifelse(
                    ranked.__terrine_nonadditive_rank == type7_rank.ceil(),
                    order_value,
                    ibis.null().cast(output_type),
                ),
            )
        )
        fraction = _max(
            cast(
                ir.NumericValue,
                (type7_rank - lower_rank).cast(
                    dt.Decimal(probability_scale + 1, probability_scale)
                ),
            )
        )
        quantiles = ranked.group_by(*ordered_groups).aggregate(
            **{output: (lower + (upper - lower) * fraction).cast(output_type)}
        )
        grouped = summary.left_join(
            quantiles,
            [
                summary[unit_column] == quantiles[unit_column],
                summary.anchor_id == quantiles.anchor_id,
            ],
        ).select(
            summary[unit_column].name(unit_column),
            summary.anchor_id,
            summary.__terrine_nonadditive_event_count,
            summary.__terrine_nonadditive_nonnull_count,
            quantiles[output],
        )
    if operation != "quantile":
        grouped = summary.join(
            grouped,
            [summary[unit_column] == grouped[unit_column], summary.anchor_id == grouped.anchor_id],
        ).select(
            summary[unit_column].name(unit_column),
            summary.anchor_id,
            summary.__terrine_nonadditive_event_count,
            summary.__terrine_nonadditive_nonnull_count,
            grouped[output],
        )
    base = anchors
    coverage = readiness.for_fact(measure.source_name)
    source_complete = (start >= ibis.literal(coverage.start)) & (
        end <= ibis.literal(coverage.end_exclusive)
    )
    observed_from = ibis.literal(coverage.start)
    observed_until = ibis.literal(coverage.end_exclusive)
    observation_complete = ibis.literal(True)
    if "_observed_from" in anchors.columns:
        bound = anchors._observed_from
        observed_from = ibis.greatest(observed_from, bound.fill_null(observed_from))
        observation_complete = observation_complete & (bound.isnull() | (start >= bound))
    if "_observed_until" in anchors.columns:
        bound = anchors._observed_until
        observed_until = ibis.least(observed_until, bound.fill_null(observed_until))
        observation_complete = observation_complete & (bound.isnull() | (end <= bound))
    complete = (source_complete & observation_complete).fill_null(False)
    names = {
        "observed_from": f"{output}_observed_from",
        "observed_until": f"{output}_observed_until",
        "complete": f"{output}_complete",
        "reason": f"{output}_reason",
        "source_complete": f"{output}_source_complete",
        "observation_complete": f"{output}_observation_complete",
        "event_count": f"{output}_event_count",
        "nonnull_count": f"{output}_nonnull_count",
    }
    base = anchors.select(
        *anchors.columns,
        **{
            names["observed_from"]: observed_from,
            names["observed_until"]: observed_until,
            names["complete"]: complete,
            names["source_complete"]: source_complete,
            names["observation_complete"]: observation_complete,
        },
    )
    joined = base.left_join(
        grouped,
        [base[unit_column] == grouped[unit_column], base.anchor_id == grouped.anchor_id],
    )
    event_count = joined.__terrine_nonadditive_event_count.fill_null(0)
    nonnull_count = joined.__terrine_nonadditive_nonnull_count.fill_null(0)
    if operation in {"count_distinct", "approx_count_distinct"}:
        value = joined[output].fill_null(0).cast("int64")
    else:
        value = joined[output]
    complete_name = names["complete"]
    reason = ibis.cases(
        (~joined[complete_name] & ~joined[names["source_complete"]], "incomplete_coverage"),
        (~joined[complete_name], "incomplete_observation"),
        (event_count == 0, "no_observations"),
        ((event_count > 0) & (nonnull_count == 0), "all_values_null"),
        else_=ibis.null().cast("string"),
    )
    reason_name = names["reason"]
    result_value = ibis.ifelse(joined[complete_name], value, ibis.null().cast(value.type()))
    result = joined.select(
        *[joined[name] for name in anchors.columns],
        result_value.name(output),
        joined[names["observed_from"]],
        joined[names["observed_until"]],
        joined[complete_name],
        reason.name(reason_name),
        event_count.name(names["event_count"]),
        nonnull_count.name(names["nonnull_count"]),
    )
    checks: tuple[_Check, ...] = ()
    if measure.filters:
        unknown = events.filter(predicate.isnull())
        if range_plan is None:
            unknown_match = anchors.inner_join(
                unknown,
                [
                    anchors[unit_column] == unknown._unit,
                    compare_timestamps(unknown._timestamp, start, "ge"),
                    compare_timestamps(unknown._timestamp, end, "lt"),
                ],
            )
        else:
            unknown_match = blocked_range_join(
                anchors,
                unknown,
                plan=range_plan,
                window=window,
                event_unit="_unit",
                event_timestamp="_timestamp",
                unit_column=unit_column,
            ).matched
        unknown_match = unknown_match.filter(
            (unknown_match.t0 + ibis.interval(seconds=window.start) >= ibis.literal(coverage.start))
            & (
                unknown_match.t0 + ibis.interval(seconds=window.end)
                <= ibis.literal(coverage.end_exclusive)
            )
        )
        if "_observed_from" in anchors.columns:
            unknown_match = unknown_match.filter(
                unknown_match._observed_from.isnull()
                | (
                    unknown_match.t0 + ibis.interval(seconds=window.start)
                    >= unknown_match._observed_from
                )
            )
        if "_observed_until" in anchors.columns:
            unknown_match = unknown_match.filter(
                unknown_match._observed_until.isnull()
                | (
                    unknown_match.t0 + ibis.interval(seconds=window.end)
                    <= unknown_match._observed_until
                )
            )
        failure_column = "failed_unknown_filter"
        failed_count = unknown_match.count().cast("string")
        checks = (
            _Check(
                unknown_match.aggregate(failed_unknown_filter=failed_count),
                (
                    _Failure(
                        failure_column,
                        UNKNOWN_FILTER,
                        {"source": measure.source_name, "measure": measure.fact, "output": output},
                    ),
                ),
            ),
        )
    meta = ColumnMeta(
        window,
        role,
        names["observed_from"],
        names["observed_until"],
        complete_name,
        reason_name,
        numeric_policy="exact",
    )
    return TemporalResult(result, {output: meta}, checks)


def approximate_quantile_window_values(
    anchors: ibis.Table,
    events: ibis.Table,
    measure: BoundMeasure,
    source: BoundSource,
    window: Window,
    *,
    unit_column: str = "unit",
    range_plan: RangePlan | None = None,
) -> ibis.Table:
    """Return canonical-ordered event values per anchor for sketch sidecar execution."""
    value_column = source.facts[measure.fact]
    if value_column is None:
        raise _UNSUPPORTED_TYPE.error(operation="quantile", value_type="event_count")
    predicate, *_ = _predicate(events, measure, source)
    selected = events.filter(predicate.fill_null(False)).select(
        __sketch_unit=events._unit,
        __sketch_time=events._timestamp,
        __sketch_value=events[value_column],
    )
    start = anchors.t0 + ibis.interval(seconds=window.start)
    end = anchors.t0 + ibis.interval(seconds=window.end)
    matched = (
        anchors.inner_join(
            selected,
            [
                anchors[unit_column] == selected.__sketch_unit,
                compare_timestamps(selected.__sketch_time, start, "ge"),
                compare_timestamps(selected.__sketch_time, end, "lt"),
            ],
        )
        if range_plan is None
        else blocked_range_join(
            anchors,
            selected,
            plan=range_plan,
            window=window,
            event_unit="__sketch_unit",
            event_timestamp="__sketch_time",
            unit_column=unit_column,
        ).matched
    )
    return (
        matched.filter(matched.__sketch_value.notnull())
        .select(
            unit=matched[unit_column],
            anchor_id=matched.anchor_id,
            timestamp=matched.__sketch_time,
            value=matched.__sketch_value,
        )
        .order_by("unit", "anchor_id", "timestamp", "value")
    )
