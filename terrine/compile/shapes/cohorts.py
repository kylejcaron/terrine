"""Deterministic cohort aggregation over a compiled LTV relation."""

from __future__ import annotations

from typing import cast

import ibis
import ibis.expr.types as ir

from terrine.backends.dialect import current_dialect, dialect_scope
from terrine.backends.numeric import (
    EXACT_OVERFLOW,
    exact_literal,
    exact_sum,
    float_is_finite,
)
from terrine.backends.timestamps import timestamp_parts
from terrine.compile.columns import CompiledStudy
from terrine.data import ColumnMeta, Roles
from terrine.errors import RefusalSpec
from terrine.spec.trajectory import CohortGrouping
from terrine.temporal.aggregate import (
    _Check,
    _Failure,
    _scaled_text,
    narrow_exact,
)
from terrine.temporal.prefix import FLOAT64_PRECISION

__all__ = ["compile_cohorts"]

_INVALID = RefusalSpec(
    "terrine.cohort.invalid_operation",
    "Cohort aggregation cannot be compiled ({reason}).",
)


def compile_cohorts(
    study: CompiledStudy, *, grouping: CohortGrouping, value_column: str
) -> CompiledStudy:
    """Group an already compiled LTV relation; this function never recompiles a Frame."""
    # Summaries compile after the Design's compile scope ends; use the table's backend.
    with dialect_scope(
        current_dialect(*(study.table[name] for name in study.table.columns[:1])).name
    ):
        return _compile_cohorts(study, grouping=grouping, value_column=value_column)


def _compile_cohorts(
    study: CompiledStudy, *, grouping: CohortGrouping, value_column: str
) -> CompiledStudy:
    if not isinstance(grouping, CohortGrouping):
        raise TypeError("grouping must be CohortGrouping")
    if value_column not in study.table.columns:
        raise _INVALID.error(reason="unknown_value_column")
    complete_column = study.columns[value_column].complete_column
    reason_column = study.columns[value_column].reason_column
    if complete_column not in study.table.columns:
        raise _INVALID.error(reason="missing_value_completeness")
    table = study.table
    _rank, time_seconds, time_nanos = timestamp_parts(table.cohort_time)
    origin = cast(ir.TimestampValue, ibis.literal(grouping.bucket_origin))
    backend = current_dialect(table.cohort_time).name
    _origin_rank, origin_seconds, origin_nanos = timestamp_parts(origin, backend=backend)
    elapsed_seconds = time_seconds - origin_seconds
    elapsed_nanos = time_nanos - origin_nanos
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
    # One first-event anchor per unit makes each (bucket, age, unit) a single row,
    # and later joined state is constant per group, so min() selects that value;
    # unlike FIRST it exists on every backend. Reasons choose deterministically.
    per_unit = table.group_by("bucket_index", "age", "unit").aggregate(
        _value=table[value_column].min(),
        _complete=table[complete_column].min().fill_null(ibis.literal(False)),
        _mature=table.period_value_mature.min(),
        _reason=table[reason_column].min()
        if reason_column is not None
        else ibis.null().cast("string"),
    )
    numeric_type = per_unit._value.type()
    reduction_table = per_unit
    decimal_total_failure = None
    decimal_mean_failure = None
    if grouping.statistic == "total" and numeric_type.is_decimal():
        scale = numeric_type.scale
        assert scale is not None
        keys = ["bucket_index", "age"]
        exact_type = exact_literal(0).type()
        total_inputs = per_unit
        decimal_text = total_inputs._value.cast("string")
        separator = decimal_text.find(".")
        fractional_digits = ibis.ifelse(separator < 0, 0, decimal_text.length() - separator - 1)
        coefficient_text = decimal_text.replace(".", "")
        exact_coefficient = coefficient_text.rpad(
            coefficient_text.length() + scale - fractional_digits, "0"
        ).cast(exact_type)
        # Booleans count through INT: PostgreSQL cannot cast boolean to BIGINT, and
        # its SUM(integer) is BIGINT where SUM(bigint) would be NUMERIC.
        total_state = total_inputs.group_by(*keys).aggregate(
            _total=exact_sum(exact_coefficient),
            _total_count=total_inputs._value.notnull().cast("int32").sum(),
        )
        decimal_total = narrow_exact(total_state._total, scale, numeric_type)
        total_state = total_state.mutate(
            _total_value=decimal_total,
            _total_unrepresentable=(total_state._total_count > 0) & decimal_total.isnull(),
        )
        reduction_table = per_unit.left_join(total_state, keys)
        observed_value = reduction_table._total_value.min()
        decimal_total_failure = reduction_table._total_unrepresentable.min()
    elif grouping.statistic == "total":
        observed_value = per_unit._value.sum()
    elif numeric_type.is_integer():
        observed_value = (
            per_unit._value.cast("decimal(38, 0)").sum()
            / per_unit._value.notnull().cast("int32").sum()
        )
    elif numeric_type.is_decimal():
        scale = numeric_type.scale
        assert scale is not None
        keys = ["bucket_index", "age"]
        exact_type = exact_literal(0).type()
        exact_count = per_unit._value.notnull().cast("int32").sum()
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
            _mean_count=mean_inputs._mean_count.min(),
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
            & (decimal_value.isnull() | ~float_is_finite(decimal_value)),
        )
        reduction_table = per_unit.left_join(mean_state, keys)
        observed_value = reduction_table._mean_value.min()
        decimal_mean_failure = reduction_table._mean_unrepresentable.min()
    else:
        scales = per_unit.group_by("bucket_index", "age").aggregate(
            _scale=per_unit._value.abs().max()
        )
        scaled = per_unit.left_join(scales, ["bucket_index", "age"])
        reduction_table = scaled.mutate(
            _scale=ibis.ifelse(scaled._scale == 0.0, 1.0, scaled._scale),
        )
        normalized = reduction_table._value / reduction_table._scale
        observed_value = normalized.mean() * reduction_table._scale.min()
    if decimal_mean_failure is None:
        decimal_mean_failure = ibis.literal(False)
    if decimal_total_failure is None:
        decimal_total_failure = ibis.literal(False)
    groups = reduction_table.group_by("bucket_index", "age").aggregate(
        observable=reduction_table._complete.cast("int32").sum(),
        _value_count=reduction_table._value.notnull().cast("int32").sum(),
        _observed_value=observed_value,
        _mature=reduction_table._mature.max(),
        _reason=reduction_table._reason.min(),
        _mean_unrepresentable=decimal_mean_failure,
        _total_unrepresentable=decimal_total_failure,
    )
    joined = groups.left_join(risk, "bucket_index")
    mature = joined._mature.fill_null(False)
    complete = (
        mature & (joined.observable == joined.at_risk) & (joined._value_count == joined.at_risk)
    )
    bucket_origin = cast(ir.TimestampValue, ibis.literal(grouping.bucket_origin))
    # Integer-seconds interval; see compile/anchors.py for the BigQuery lowering.
    bucket_start = bucket_origin + (joined.bucket_index * grouping.bucket).as_interval("s")
    result = joined.select(
        bucket_start=bucket_start,
        bucket_index=joined.bucket_index,
        age=joined.age,
        value=complete.ifelse(
            joined._observed_value, ibis.null().cast(joined._observed_value.type())
        ),
        at_risk=mature.ifelse(joined.at_risk, ibis.null().cast("int64")),
        observable=mature.ifelse(joined.observable, ibis.null().cast("int64")),
        denominators_complete=mature,
        denominators_reason=mature.ifelse(ibis.null().cast("string"), "administrative_cutoff"),
        complete=complete,
        reason=complete.ifelse(
            ibis.null().cast("string"),
            ibis.ifelse(
                mature,
                joined._reason.fill_null("incomplete_observation"),
                "administrative_cutoff",
            ),
        ),
    )
    roles = Roles("bucket_index", "bucket_index", "bucket_start", None, (), ("value",))
    columns = {
        "value": ColumnMeta(None, "outcome", None, None, "complete", "reason"),
        **{
            name: ColumnMeta(
                None, "denominator", None, None, "denominators_complete", "denominators_reason"
            )
            for name in ("at_risk", "observable")
        },
    }
    checks = list(study._checks)
    if numeric_type.is_decimal() and grouping.statistic == "total":
        failure_column = "failed_cohort_exact_total"
        failed = joined.filter(complete & joined._total_unrepresentable)
        checks.append(
            _Check(
                failed.aggregate(ibis._.count().cast("string").name(failure_column)),
                (
                    _Failure(
                        failure_column,
                        EXACT_OVERFLOW,
                        {"source": "cohort", "measure": value_column, "output": "value"},
                    ),
                ),
            )
        )
    if numeric_type.is_decimal() and grouping.statistic == "mean":
        failure_column = "failed_cohort_exact_mean"
        failed = joined.filter(complete & joined._mean_unrepresentable)
        checks.append(
            _Check(
                failed.aggregate(ibis._.count().cast("string").name(failure_column)),
                (
                    _Failure(
                        failure_column,
                        EXACT_OVERFLOW,
                        {"source": "cohort", "measure": value_column, "output": "value"},
                    ),
                ),
            )
        )
    if numeric_type.is_floating():
        # Native binary64 reductions: a total or mean that leaves the finite range
        # refuses instead of publishing an infinity or NaN.
        failure_column = "failed_cohort_nonfinite"
        failed = result.value.notnull() & ~float_is_finite(result.value)
        checks.append(
            _Check(
                result.filter(failed).aggregate(ibis._.count().cast("string").name(failure_column)),
                (
                    _Failure(
                        failure_column,
                        FLOAT64_PRECISION,
                        {
                            "failure": "bound_overflow",
                            "source": "cohort",
                            "measure": value_column,
                            "output": "value",
                        },
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
        range_plans=study.range_plans,
    )
