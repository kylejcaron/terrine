"""Deterministic cohort aggregation over a compiled LTV relation."""

from __future__ import annotations

from typing import cast

import ibis
import ibis.expr.types as ir

from terrine.compile.columns import CompiledStudy
from terrine.data import ColumnMeta, Roles
from terrine.errors import RefusalSpec
from terrine.spec.trajectory import CohortGrouping
from terrine.temporal.aggregate import _Check, _Failure
from terrine.temporal.prefix import (
    FLOAT64_PRECISION,
    float64_rounding_radius,
    float64_tolerance,
    outward_add,
)
from terrine.temporal._timestamps import timestamp_order_key

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
        grouping.statistic == "total"
        and value_meta is not None
        and value_meta.numeric_policy == "checked_float64_v1"
        and error_column is not None
        and error_column in table.columns
    )
    per_unit = table.group_by("bucket_index", "age", "unit").aggregate(
        _value=table[value_column].first(),
        _absolute_error=table[error_column].first() if checked_float else ibis.null().cast("float64"),
        _complete=table[complete_column].first().fill_null(ibis.literal(False)),
        _mature=table.period_value_mature.first(),
        _reason=table[f"{value_column}_reason"].first()
        if f"{value_column}_reason" in table.columns
        else ibis.null().cast("string"),
    )
    numeric_type = per_unit._value.type()
    if grouping.statistic == "total":
        observed_value = per_unit._value.sum()
    elif numeric_type.is_integer():
        observed_value = (
            per_unit._value.cast("decimal(38, 0)").sum()
            / per_unit._value.notnull().cast("int64").sum()
        )
    elif numeric_type.is_decimal():
        observed_value = (
            per_unit._value.cast(f"decimal(38, {numeric_type.scale})").sum()
            / per_unit._value.notnull().cast("int64").sum()
        )
    else:
        observed_value = (
            per_unit._value.sum() / per_unit._value.notnull().cast("int64").sum()
        )
    groups = per_unit.group_by("bucket_index", "age").aggregate(
        observable=per_unit._complete.cast("int64").sum(),
        _value_count=per_unit._value.notnull().cast("int64").sum(),
        _observed_value=observed_value,
        _absolute_error=(
            ibis.ifelse(
                grouping.statistic == "total",
                outward_add(
                    per_unit._absolute_error.sum(),
                    float64_rounding_radius(per_unit._value.abs().sum()),
                ),
                per_unit._absolute_error.sum(),
            )
            if checked_float
            else ibis.null().cast("float64")
        ),
        _mature=per_unit._mature.max(),
        _reason=per_unit._reason.first(),
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
    if checked_float:
        failed = result.value.notnull() & (
            result.value_absolute_error > float64_tolerance(result.value)
        )
        failure_column = "failed_cohort_tolerance"
        checks.append(
            _Check(
                result.filter(failed).aggregate(
                    ibis._.count().cast("string").name(failure_column)
                ),
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
