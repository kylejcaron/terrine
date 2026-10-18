"""Exact nonadditive window operations and local order-statistic references."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from fractions import Fraction
from typing import Any, Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.data import ColumnMeta
from terrine.errors import RefusalSpec
from terrine.semantic.models import BoundMeasure, BoundSource
from terrine.semantic.rules import UNKNOWN_FILTER
from terrine.temporal.aggregate import TemporalResult, _Check, _Failure
from terrine.temporal.daily import _predicate
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
    if not (dtype.is_integer() or dtype.is_decimal()):
        raise _UNSUPPORTED_TYPE.error(operation=operation, value_type=value_type)
    scale = max(0, -int(probability.as_tuple().exponent)) if probability is not None else 0
    if isinstance(dtype, dt.Decimal):
        source_scale = dtype.scale or 0
        scale = max(scale, source_scale)
        integer_digits = (dtype.precision or 38) - source_scale
    else:
        integer_digits = 19
    if scale > 38 or integer_digits + scale > 38:
        raise _EXACT_PRECISION.error(operation=operation, value_type=value_type)
    return dt.Decimal(38, scale)


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
    operation: Literal["min", "max", "count_distinct", "quantile"],
    probability: Decimal | None = None,
    unit_column: str = "unit",
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
    selected = events.filter(predicate.fill_null(False))
    start = anchors.t0 + ibis.interval(seconds=window.start)
    end = anchors.t0 + ibis.interval(seconds=window.end)
    conditions = [
        anchors[unit_column] == selected._unit,
        selected._timestamp >= start,
        selected._timestamp < end,
    ]
    matched = anchors.inner_join(selected, conditions)
    value = matched[value_column]
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
    else:
        assert probability is not None
        nonnull = matched.filter(value.notnull())
        ordered_groups = [nonnull[unit_column], nonnull.anchor_id]
        ordering_window = ibis.window(
            group_by=ordered_groups, order_by=nonnull[value_column].cast(output_type)
        )
        grouping_window = ibis.window(group_by=ordered_groups)
        ranked = nonnull.mutate(
            __terrine_nonadditive_rank=ibis.row_number().over(ordering_window),
            __terrine_nonadditive_n=nonnull[value_column].count().over(grouping_window),
        )
        type7_rank = (ranked.__terrine_nonadditive_n - 1).cast(dt.Decimal(38, 0)) * ibis.literal(
            probability
        )
        lower_rank = type7_rank.floor()
        order_value = ranked[value_column].cast(output_type)
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
        fraction = _max(cast(ir.NumericValue, type7_rank - lower_rank))
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
    if operation == "count_distinct":
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
        unknown_match = anchors.inner_join(
            unknown,
            [
                anchors[unit_column] == unknown._unit,
                unknown._timestamp >= start,
                unknown._timestamp < end,
            ],
        ).filter(source_complete & observation_complete)
        failure_column = "failed_unknown_filter"
        failed_count = unknown_match.count().cast("string").name(failure_column)
        checks = (
            _Check(
                unknown_match.aggregate(failed_count),
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
