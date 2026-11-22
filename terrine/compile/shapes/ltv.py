"""Compiler for bounded additive lifetime-value trajectories."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

import terrine.reasons as reason_codes
from terrine.backends.numeric import (
    EXACT_OVERFLOW,
    FLOAT64_PRECISION,
    exact_literal,
    exact_sum,
    float_is_finite,
    guard_accumulation,
)
from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.compile.shapes._ltv_periods import PeriodChecks, compact_periods
from terrine.data import ColumnMeta, Roles, RowCheck
from terrine.errors import RefusalSpec
from terrine.spec._columns import TRAJECTORY
from terrine.spec.design import Design
from terrine.spec.expressions import MetricAgg
from terrine.spec.trajectory import CohortSpec
from terrine.temporal.aggregate import _Check, narrow_exact
from terrine.windows import Window, _elapsed

_INVALID = RefusalSpec(
    "terrine.trajectory.invalid_compilation",
    "Trajectory compilation failed ({reason}).",
)

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["compile_ltv"]


def compile_ltv(design: Design, model: Model) -> CompiledStudy:
    """Compile a bounded trajectory, using wide windows where age lowering is unproven."""
    if not isinstance(design.shape, CohortSpec):
        raise TypeError("compile_ltv requires a CohortSpec design")
    design.validate(model)
    trajectory = design.shape.trajectory
    reserved = {
        "unit",
        "anchor_id",
        "cohort_time",
        "age",
        "period_value",
        "period_value_complete",
        "period_value_reason",
        "period_value_observed_from",
        "period_value_observed_until",
        "period_value_mature",
        "cumulative_value",
        "cumulative_value_complete",
        "cumulative_value_reason",
    }
    for feature in (*design.features, *design.eligibility):
        if feature.casefold() in reserved:
            from terrine.compile.columns import _RESERVED_NAME

            raise _RESERVED_NAME.error(name=feature)
    period = trajectory.period.seconds
    _elapsed(trajectory.periods * period, field="window")
    window = Window(0, trajectory.period)
    base = Design(
        design.unit,
        design.anchor,
        features=design.features,
        eligibility=design.eligibility,
        treatment=design.treatment,
        gap=design.gap,
        shape="cross_section",
        anchor_range=design.anchor_range,
    )
    compact = None
    # The wide path sequences shared-source checks among feature, eligibility
    # and trajectory requests. Keep it when that schedule cannot be separated.
    if not (design.features or design.eligibility or design.treatment):
        compiled = compile_columns(base, model)
        assert design.unit is not None
        compact = compact_periods(
            compiled, model, design.unit, trajectory, model.metrics[trajectory.metric], window
        )
    if compact is not None:
        source = compact.table
        period_rows = [
            (
                source,
                next(iter(compact.columns)),
                next(iter(compact.columns.values())),
                source.age,
                source.cohort_time,
                source.t0,
                source.t0 + ibis.interval(seconds=period),
            )
        ]
    else:
        outcomes = {
            f"__trajectory_age_{age}": MetricAgg(
                trajectory.metric,
                Window(
                    _elapsed(age * period, field="window"),
                    _elapsed((age + 1) * period, field="window"),
                ),
            )
            for age in range(trajectory.periods)
        }
        compiled = compile_columns(replace(base, outcomes=outcomes), model)
        source = compiled.table
        period_rows = [
            (
                source,
                name,
                compiled.columns[name],
                ibis.literal(age, type="int64"),
                source.t0,
                source.t0 + ibis.interval(seconds=age * period),
                source.t0 + ibis.interval(seconds=(age + 1) * period),
            )
            for age, name in enumerate(outcomes)
        ]
    age_tables = []
    for source, name, meta, age, origin, start_time, end_time in period_rows:
        if (
            meta.complete_column is None
            or meta.reason_column is None
            or meta.observed_from_column is None
            or meta.observed_until_column is None
        ):
            raise _INVALID.error(reason="missing_period_metadata")
        value = source[name]
        complete = source[meta.complete_column].fill_null(ibis.literal(False))
        reason = source[meta.reason_column]
        mature = ibis.literal(True)
        if design.administrative_cutoff is not None:
            immature = end_time > ibis.literal(design.administrative_cutoff)
            value = immature.ifelse(ibis.null().cast(value.type()), value)
            complete = complete & ~immature
            mature = start_time < ibis.literal(design.administrative_cutoff)
            reason = immature.ifelse(reason_codes.ADMINISTRATIVE_CUTOFF, reason)
        projection = dict(
            zip(
                TRAJECTORY,
                (
                    source.unit,
                    source.anchor_id,
                    origin,
                    age,
                    value,
                    mature,
                ),
                strict=True,
            )
        )
        projection.update(
            period_value_complete=complete,
            period_value_reason=reason,
            period_value_observed_from=source[meta.observed_from_column],
            period_value_observed_until=source[meta.observed_until_column],
        )
        for feature in (*compiled.roles.features, *design.eligibility):
            feature_meta = compiled.columns[feature]
            for column in (
                feature_meta.observed_from_column,
                feature_meta.observed_until_column,
                feature_meta.complete_column,
                feature_meta.reason_column,
            ):
                if column is not None:
                    projection[column] = source[column]
            projection[feature] = source[feature]
        if compiled.roles.treatment is not None:
            treatment_meta = compiled.columns.get(compiled.roles.treatment)
            if treatment_meta is not None:
                for column in (
                    treatment_meta.observed_from_column,
                    treatment_meta.observed_until_column,
                    treatment_meta.complete_column,
                    treatment_meta.reason_column,
                ):
                    if column is not None:
                        projection[column] = source[column]
            projection[compiled.roles.treatment] = source[compiled.roles.treatment]
        for feature in compiled.roles.features:
            timing = compiled.expression_timings.get(feature)
            if timing is None:
                continue
            for column in (
                timing.point_time_column,
                timing.start_bound_column,
                timing.end_bound_column,
            ):
                if column is not None and column != compiled.roles.t0:
                    projection[column] = source[column]
        age_tables.append(source.select(**projection))
    table = age_tables[0]
    for age_table in age_tables[1:]:
        table = table.union(age_table, distinct=False)

    output_columns = {
        name: meta
        for name, meta in compiled.columns.items()
        if name in compiled.roles.features
        or name in design.eligibility
        or name == compiled.roles.treatment
    }
    output_columns["period_value"] = ColumnMeta(
        window=window,
        role="outcome",
        observed_from_column="period_value_observed_from",
        observed_until_column="period_value_observed_until",
        complete_column="period_value_complete",
        reason_column="period_value_reason",
        numeric_policy=meta.numeric_policy,
        arithmetic=meta.arithmetic,
    )
    outcome_names = ["period_value"]
    checks = list(compiled._checks)
    if compact is not None and compact.row_checks:
        checks.append(PeriodChecks.from_result(compact))
    if trajectory.cumulative:
        partition = ["unit", "anchor_id", "cohort_time"]
        numeric_type = table.period_value.type()
        scale = None
        if isinstance(numeric_type, dt.Decimal):
            scale = numeric_type.scale
            assert scale is not None
            decimal_text = table.period_value.cast("string")
            separator = decimal_text.find(".")
            fractional_digits = ibis.ifelse(separator < 0, 0, decimal_text.length() - separator - 1)
            coefficient_text = decimal_text.replace(".", "")
            term: ir.Value = coefficient_text.rpad(
                coefficient_text.length() + scale - fractional_digits, "0"
            ).cast(exact_literal(0).type())
        else:
            term = table.period_value
        # A bounding policy withholds each period's decimal coefficient or
        # binary64 value once the magnitudes accumulated through it may leave
        # the domain (a fixed-precision type, or half the binary64 range where
        # float arithmetic raises), so the backend never evaluates a running
        # sum outside it; any withheld period at or before an age makes that
        # age's total NULL, which the checks below refuse. Integer periods sum
        # in their native domain.
        original = table.columns
        admitted = None
        if scale is not None or numeric_type.is_floating():
            table, term, admitted = guard_accumulation(
                table, term, partition=partition, order="age"
            )
        scratch = [name for name in table.columns if name not in original]
        frame = ibis.window(
            group_by=[table[name] for name in partition], order_by=[table.age], rows=(None, 0)
        )
        running = (
            exact_sum(term, admitted=admitted is not None).over(frame)
            if scale is not None
            else cast(ir.NumericColumn, term).sum().over(frame)
        )
        if admitted is not None:
            flags = cast(ir.IntegerColumn, (~admitted).cast("int32"))
            withheld = cast(ir.IntegerColumn, flags.sum().over(frame)) > 0
            running = withheld.ifelse(ibis.null().cast(running.type()), running)
        total = narrow_exact(running, scale, numeric_type) if scale is not None else running
        # Through INT: PostgreSQL cannot cast boolean to BIGINT.
        missing = table.period_value.isnull().cast("int32").sum().over(frame) > 0
        if scale is not None:
            failure_column = "failed_cumulative_exact_overflow"
            check_table = table.filter(~missing & total.isnull()).aggregate(
                ibis._.count().cast("string").name(failure_column)
            )
            checks.append(
                _Check(
                    check_table,
                    (
                        RowCheck(
                            failure_column,
                            EXACT_OVERFLOW,
                            {"output": "cumulative_value"},
                        ),
                    ),
                )
            )
        elif numeric_type.is_floating():
            # A native binary64 running total that leaves the finite range, or
            # one a bounding policy withheld, refuses instead of publishing an
            # infinity, NaN or incomplete sum.
            failure_column = "failed_cumulative_nonfinite"
            check_table = table.filter(
                ~missing & (total.isnull() | ~float_is_finite(total))
            ).aggregate(ibis._.count().cast("string").name(failure_column))
            checks.append(
                _Check(
                    check_table,
                    (
                        RowCheck(
                            failure_column,
                            FLOAT64_PRECISION,
                            {"reason": "bound_overflow", "output": "cumulative_value"},
                        ),
                    ),
                )
            )
        cumulative = missing.ifelse(ibis.null().cast(total.type()), total)
        table = table.mutate(cumulative_value=cumulative)
        if scratch:
            table = table.drop(*scratch)
        table = table.mutate(
            cumulative_value_complete=~missing & table.period_value_complete.fill_null(False)
        )
        # A period's reason either explains its missing value or, beside a
        # published value, notes what it omits (``below_resolution``). The
        # cumulative value takes the earliest missing period's reason, else the
        # earliest note among the periods it adds.
        no_reason = ibis.null().cast("string")
        period_missing = table.period_value.isnull()
        age_width = len(str(trajectory.periods - 1))

        def earliest(reason):
            if compact is not None:
                # A fixed-width age prefix orders the nonnull reasons. MIN
                # ignores nulls, unlike Ibis FIRST_VALUE on this window.
                prefix = table.age.cast("string").lpad(age_width, "0")
                return (
                    reason.notnull()
                    .ifelse(prefix.concat(reason), no_reason)
                    .min()
                    .over(frame)
                    .substr(age_width)
                )
            return ibis.coalesce(
                *(
                    reason.lag(offset=offset).over(frame)
                    for offset in reversed(range(trajectory.periods))
                )
            )

        first_missing = earliest(period_missing.ifelse(table.period_value_reason, no_reason))
        first_note = earliest(period_missing.ifelse(no_reason, table.period_value_reason))
        table = table.mutate(
            cumulative_value_reason=ibis.ifelse(
                missing,
                first_missing.fill_null(ibis.literal(reason_codes.INCOMPLETE_CONTRIBUTION)),
                first_note,
            )
        )
        output_columns["cumulative_value"] = replace(
            output_columns["period_value"],
            complete_column="cumulative_value_complete",
            reason_column="cumulative_value_reason",
        )
        outcome_names.append("cumulative_value")
    expression_timings = {}
    for feature in design.features:
        timing = compiled.expression_timings.get(feature)
        if timing is None:
            continue
        replacements = {
            field: "cohort_time"
            for field in ("point_time_column", "start_bound_column", "end_bound_column")
            if getattr(timing, field) == compiled.roles.t0
        }
        expression_timings[feature] = replace(timing, **replacements)
    roles = Roles(
        unit="unit",
        anchor="anchor_id",
        t0="cohort_time",
        treatment=compiled.roles.treatment,
        features=compiled.roles.features,
        outcomes=tuple(outcome_names),
    )
    return CompiledStudy(
        table=table,
        audit_table=compiled.audit_table,
        roles=roles,
        columns=output_columns,
        feature_cutoffs=compiled.feature_cutoffs,
        eligibility_cutoffs=compiled.eligibility_cutoffs,
        strategy=compiled.strategy,
        _anchors=compiled._anchors,
        _checks=tuple(checks),
        expression_timings=expression_timings,
        range_plans=compiled.range_plans,
        source_reads=compiled.source_reads,
    )
