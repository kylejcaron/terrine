"""Compiler for bounded additive lifetime-value trajectories."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import ibis

from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.data import ColumnMeta, Roles
from terrine.errors import RefusalSpec
from terrine.spec.design import Design
from terrine.spec.expressions import MetricAgg
from terrine.spec.trajectory import CohortSpec
from terrine.windows import Window

_INVALID = RefusalSpec(
    "terrine.trajectory.invalid_compilation",
    "Trajectory compilation failed ({reason}).",
)

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["compile_ltv"]


def compile_ltv(design: Design, model: Model) -> CompiledStudy:
    """Compile each requested half-open age from the regular temporal compiler."""
    if not isinstance(design.shape, CohortSpec):
        raise TypeError("compile_ltv requires a CohortSpec design")
    design.validate(model)
    trajectory = design.shape.trajectory
    names = tuple(f"__trajectory_age_{age}" for age in range(trajectory.periods))
    outcomes = {
        name: MetricAgg(
            trajectory.metric,
            Window(age * trajectory.period, (age + 1) * trajectory.period),
        )
        for age, name in enumerate(names)
    }
    source_design = Design(
        design.unit,
        design.anchor,
        features=design.features,
        outcomes=outcomes,
        eligibility=design.eligibility,
        treatment=design.treatment,
        gap=design.gap,
        shape="cross_section",
        anchor_range=design.anchor_range,
    )
    compiled = compile_columns(source_design, model)
    source = compiled.table
    age_tables = []
    for age, name in enumerate(names):
        meta = compiled.columns[name]
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
        end_time = source.t0 + ibis.interval(seconds=(age + 1) * trajectory.period)
        if design.administrative_cutoff is not None:
            immature = end_time > ibis.literal(design.administrative_cutoff)
            value = immature.ifelse(ibis.null().cast(value.type()), value)
            complete = complete & ~immature
            reason = immature.ifelse("administrative_cutoff", reason)
        mature = (
            ibis.literal(True)
            if design.administrative_cutoff is None
            else source.t0 + ibis.interval(seconds=age * trajectory.period)
            < ibis.literal(design.administrative_cutoff)
        )
        projection = {
            "unit": source.unit,
            "anchor_id": source.anchor_id,
            "cohort_time": source.t0,
            "age": ibis.literal(age).cast("int64"),
            "period_value": value,
            "period_value_complete": complete,
            "period_value_reason": reason,
            "period_value_observed_from": source[meta.observed_from_column],
            "period_value_observed_until": source[meta.observed_until_column],
            "period_value_mature": mature,
        }
        if meta.numeric_policy == "checked_float64_v1":
            if meta.absolute_error_column is None:
                raise _INVALID.error(reason="missing_numeric_certificate")
            projection["period_value_absolute_error"] = source[meta.absolute_error_column]
        for feature in compiled.roles.features:
            projection[feature] = source[feature]
        if compiled.roles.treatment is not None:
            projection[compiled.roles.treatment] = source[compiled.roles.treatment]
        age_tables.append(source.select(**projection))
    table = age_tables[0]
    for age_table in age_tables[1:]:
        table = table.union(age_table, distinct=False)

    output_columns = {
        name: meta for name, meta in compiled.columns.items() if name in compiled.roles.features
    }
    output_columns["period_value"] = ColumnMeta(
        Window(0, trajectory.period),
        "outcome",
        "period_value_observed_from",
        "period_value_observed_until",
        "period_value_complete",
        "period_value_reason",
        numeric_policy=(
            "checked_float64_v1"
            if any(
                meta.numeric_policy == "checked_float64_v1" for meta in compiled.columns.values()
            )
            else None
        ),
        absolute_error_column=(
            "period_value_absolute_error"
            if "period_value_absolute_error" in table.columns
            else None
        ),
    )
    outcome_names = ["period_value"]
    if trajectory.cumulative:
        frame = ibis.window(
            group_by=[table.unit, table.anchor_id, table.cohort_time],
            order_by=[table.age],
            rows=(None, 0),
        )
        total = table.period_value.sum().over(frame)
        missing = table.period_value.isnull().cast("int64").sum().over(frame) > 0
        cumulative = missing.ifelse(ibis.null().cast(total.type()), total)
        table = table.mutate(cumulative_value=cumulative)
        table = table.mutate(
            cumulative_value_complete=~missing & table.period_value_complete.fill_null(False)
        )
        first_reason = ibis.coalesce(
            *(
                table.period_value_reason.lag(offset=offset).over(frame)
                for offset in reversed(range(trajectory.periods))
            )
        )
        table = table.mutate(
            cumulative_value_reason=ibis.ifelse(
                missing,
                first_reason.fill_null(ibis.literal("incomplete_contribution")),
                ibis.null().cast("string"),
            )
        )
        if "period_value_absolute_error" in table.columns:
            cumulative_error = table.period_value_absolute_error.sum().over(frame)
            cumulative_error = missing.ifelse(
                ibis.null().cast(cumulative_error.type()), cumulative_error
            )
            table = table.mutate(cumulative_value_absolute_error=cumulative_error)
        output_columns["cumulative_value"] = replace(
            output_columns["period_value"],
            complete_column="cumulative_value_complete",
            reason_column="cumulative_value_reason",
            absolute_error_column=(
                "cumulative_value_absolute_error"
                if "cumulative_value_absolute_error" in table.columns
                else None
            ),
        )
        outcome_names.append("cumulative_value")
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
        _checks=compiled._checks,
    )
