"""Deterministic cohort aggregation over a compiled LTV relation."""

from __future__ import annotations

from typing import cast

import ibis
import ibis.expr.types as ir

from terrine.compile.columns import CompiledStudy
from terrine.data import ColumnMeta, Roles
from terrine.errors import RefusalSpec
from terrine.spec.trajectory import CohortGrouping
from terrine.temporal._timestamps import timestamp_order_key
from terrine.temporal.aggregate import _EXACT_OVERFLOW, _Check, _Failure, _scaled_text
from terrine.temporal.prefix import (
    FLOAT64_PRECISION,
    _isfinite,
    exact_literal,
    exact_sum,
    float64_rounding_radius,
    float64_tolerance,
    outward_add,
    outward_multiply,
)

__all__ = ["compile_cohorts"]

_INVALID = RefusalSpec(
    "terrine.cohort.invalid_operation",
    "Cohort aggregation cannot be compiled ({reason}).",
)


def compile_cohorts(
    study: CompiledStudy, *, grouping: CohortGrouping, value_column: str
) -> CompiledStudy:
    """Group an already compiled LTV relation; this function never recompiles a Frame."""
    if not isinstance(grouping, CohortGrouping):
        raise TypeError("grouping must be CohortGrouping")
    if value_column not in study.table.columns:
        raise _INVALID.error(reason="unknown_value_column")
    complete_column = f"{value_column}_complete"
    if complete_column not in study.table.columns:
        raise _INVALID.error(reason="missing_value_completeness")
    table = study.table
    time_key = timestamp_order_key(table.cohort_time)
    origin = cast(ir.TimestampValue, ibis.literal(grouping.bucket_origin))
    origin_key = timestamp_order_key(origin)
    elapsed_seconds = cast(ir.IntegerValue, time_key["seconds"]) - cast(
        ir.IntegerValue, origin_key["seconds"]
    )
    elapsed_nanos = cast(ir.IntegerValue, time_key["nanos"]) - cast(
        ir.IntegerValue, origin_key["nanos"]
    )
    carry = elapsed_nanos // 1_000_000_000
    quotient = (elapsed_seconds + carry) // grouping.bucket
    table = table.mutate(bucket_index=quotient.cast("int64"))
    entrants = (
        table.filter(table.age == 0)
        .select(
            bucket_index=table.bucket_index,
            unit=table.unit,
        )
        .distinct()
    )
    risk = entrants.group_by("bucket_index").aggregate(at_risk=ibis._.count())
    value_meta = study.columns.get(value_column)
    error_column = value_meta.absolute_error_column if value_meta is not None else None
    checked_float = (
        value_meta is not None
        and value_meta.numeric_policy == "checked_float64_v1"
        and error_column is not None
        and error_column in table.columns
    )
    per_unit = table.group_by("bucket_index", "age", "unit").aggregate(
        _value=table[value_column].first(),
        _absolute_error=table[error_column].first()
        if checked_float
        else ibis.null().cast("float64"),
        _complete=table[complete_column].first().fill_null(ibis.literal(False)),
        _mature=table.period_value_mature.first(),
        _reason=table[f"{value_column}_reason"].first()
        if f"{value_column}_reason" in table.columns
        else ibis.null().cast("string"),
    )
    numeric_type = per_unit._value.type()
    reduction_table = per_unit
    decimal_mean_failure = None
    if grouping.statistic == "total":
        observed_value = per_unit._value.sum()
    elif numeric_type.is_integer():
        observed_value = (
            per_unit._value.cast("decimal(38, 0)").sum()
            / per_unit._value.notnull().cast("int64").sum()
        )
    elif numeric_type.is_decimal():
        scale = numeric_type.scale
        assert scale is not None
        keys = ["bucket_index", "age"]
        exact_type = exact_literal(0).type()
        exact_count = per_unit._value.notnull().cast("int64").sum()
        counts = per_unit.group_by(*keys).aggregate(_mean_count=exact_count)
        mean_inputs = per_unit.left_join(counts, keys)
        decimal_text = mean_inputs._value.cast("string")
        separator = decimal_text.find(".")
        fractional_digits = ibis.ifelse(separator < 0, 0, decimal_text.length() - separator - 1)
        coefficient_text = decimal_text.replace(".", "")
        exact_coefficient = coefficient_text.rpad(
            coefficient_text.length() + scale - fractional_digits, "0"
        ).cast(exact_type)
        mean_state = mean_inputs.group_by(*keys).aggregate(
            _mean_total=exact_sum(exact_coefficient),
            _mean_count=mean_inputs._mean_count.first(),
        )
        exact_total = _scaled_text(mean_state._mean_total, scale).try_cast("float64")
        decimal_value = exact_total / mean_state._mean_count.cast("float64")
        mean_state = mean_state.mutate(
            _mean_value=ibis.ifelse(
                mean_state._mean_count == 0,
                ibis.null().cast("float64"),
                decimal_value,
            ),
            _mean_unrepresentable=(mean_state._mean_count > 0)
            & (decimal_value.isnull() | ~_isfinite(decimal_value)),
        )
        reduction_table = per_unit.left_join(mean_state, keys)
        observed_value = reduction_table._mean_value.first()
        decimal_mean_failure = reduction_table._mean_unrepresentable.first()
    else:
        scales = per_unit.group_by("bucket_index", "age").aggregate(
            _scale=per_unit._value.abs().max()
        )
        scaled = per_unit.left_join(scales, ["bucket_index", "age"])
        reduction_table = scaled.mutate(
            _magnitude=scaled._scale,
            _scale=ibis.ifelse(scaled._scale == 0.0, 1.0, scaled._scale),
        )
        normalized = reduction_table._value / reduction_table._scale
        observed_value = normalized.mean() * reduction_table._scale.first()
    if decimal_mean_failure is None:
        decimal_mean_failure = ibis.literal(False)
    if checked_float and grouping.statistic == "total":
        error_count = per_unit._value.notnull().cast("float64").sum()
        inherited_error = per_unit._absolute_error.sum()
        inherited_rounding = outward_multiply(
            float64_rounding_radius(per_unit._absolute_error.abs().sum()),
            outward_multiply(error_count, 2.0),
        )
        value_rounding = outward_multiply(
            float64_rounding_radius(per_unit._value.abs().sum()),
            outward_multiply(error_count, 2.0),
        )
        absolute_error = outward_add(
            outward_add(inherited_error, inherited_rounding), value_rounding
        )
    elif checked_float:
        input_error = per_unit._absolute_error.max()
        error_count = per_unit._value.notnull().cast("float64").sum()
        reduction_error = outward_multiply(
            float64_rounding_radius(reduction_table._magnitude.first()),
            outward_multiply(error_count, 4.0),
        )
        absolute_error = outward_add(input_error, reduction_error)
    else:
        absolute_error = ibis.null().cast("float64")
    groups = reduction_table.group_by("bucket_index", "age").aggregate(
        observable=reduction_table._complete.cast("int64").sum(),
        _value_count=reduction_table._value.notnull().cast("int64").sum(),
        _observed_value=observed_value,
        _absolute_error=absolute_error,
        _mature=reduction_table._mature.max(),
        _reason=reduction_table._reason.first(),
        _mean_unrepresentable=decimal_mean_failure,
    )
    joined = groups.left_join(risk, "bucket_index")
    mature = joined._mature.fill_null(False)
    complete = (
        mature & (joined.observable == joined.at_risk) & (joined._value_count == joined.at_risk)
    )
    bucket_origin = cast(ir.TimestampValue, ibis.literal(grouping.bucket_origin))
    bucket_start = bucket_origin + joined.bucket_index * ibis.interval(seconds=grouping.bucket)
    result = joined.select(
        bucket_start=bucket_start,
        bucket_index=joined.bucket_index,
        age=joined.age,
        value=complete.ifelse(
            joined._observed_value, ibis.null().cast(joined._observed_value.type())
        ),
        at_risk=mature.ifelse(joined.at_risk, ibis.null().cast("int64")),
        observable=mature.ifelse(joined.observable, ibis.null().cast("int64")),
        complete=complete,
        reason=complete.ifelse(
            ibis.null().cast("string"),
            ibis.ifelse(
                mature,
                joined._reason.fill_null("incomplete_observation"),
                "administrative_cutoff",
            ),
        ),
        **(
            {
                "value_absolute_error": complete.ifelse(
                    joined._absolute_error, ibis.null().cast("float64")
                )
            }
            if checked_float
            else {}
        ),
    )
    roles = Roles("bucket_index", "bucket_index", "bucket_start", None, (), ("value",))
    columns = {
        "value": ColumnMeta(
            None,
            "outcome",
            None,
            None,
            "complete",
            "reason",
            numeric_policy=value_meta.numeric_policy if checked_float else None,
            absolute_error_column="value_absolute_error" if checked_float else None,
        )
    }
    checks = list(study._checks)
    if numeric_type.is_decimal() and grouping.statistic == "mean":
        failure_column = "failed_cohort_exact_mean"
        failed = joined.filter(complete & joined._mean_unrepresentable)
        checks.append(
            _Check(
                failed.aggregate(ibis._.count().cast("string").name(failure_column)),
                (
                    _Failure(
                        failure_column,
                        _EXACT_OVERFLOW,
                        {"source": "cohort", "measure": value_column, "output": "value"},
                    ),
                ),
            )
        )
    if checked_float:
        failed = result.value.notnull() & (
            result.value_absolute_error.isnull()
            | (result.value_absolute_error < 0.0)
            | ~_isfinite(result.value)
            | ~_isfinite(result.value_absolute_error)
            | (result.value_absolute_error > float64_tolerance(result.value))
        )
        failure_column = "failed_cohort_tolerance"
        checks.append(
            _Check(
                result.filter(failed).aggregate(ibis._.count().cast("string").name(failure_column)),
                (
                    _Failure(
                        failure_column,
                        FLOAT64_PRECISION,
                        {"failure": "tolerance", "output": "value"},
                    ),
                ),
            )
        )
    return CompiledStudy(
        table=result,
        audit_table=study.audit_table,
        roles=roles,
        columns=columns,
        feature_cutoffs=study.feature_cutoffs,
        eligibility_cutoffs=study.eligibility_cutoffs,
        strategy=study.strategy,
        _anchors=study._anchors,
        _checks=tuple(checks),
    )
