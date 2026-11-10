"""Compiler for bounded repeated-measures state panels."""

from __future__ import annotations

import math
from dataclasses import replace
from datetime import UTC, date, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import ibis
import ibis.expr.datatypes as dt

import terrine.reasons as reason_codes
from terrine.backends.dialect import dialect_for_name
from terrine.compile.anchors import AnchorTable
from terrine.compile.columns import CompiledStudy, compile_columns, expression_policy
from terrine.data import ColumnMeta, Roles
from terrine.spec._columns import IDENTITY, STATE_PANEL
from terrine.spec.anchors import FixedDate, Schedule
from terrine.spec.design import Design
from terrine.spec.expressions import MetricAgg
from terrine.spec.state_panel import (
    _UNSUPPORTED,
    BinnedMetricState,
    StateObservations,
    StatePanel,
)
from terrine.windows import Duration, Window

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["compile_state_panel"]


def _state_panel_design(
    design: Design,
    *,
    anchor: FixedDate | Schedule,
    features,
    filter_population: bool = False,
    gap: Duration | None = None,
) -> Design:
    return Design(
        design.unit,
        anchor,
        features=features,
        eligibility=design.eligibility if filter_population else None,
        gap=design.gap if gap is None else gap,
        shape="cross_section",
        anchor_range=design.anchor_range if filter_population else None,
    )


def _cut_literal(cut: int | float | Decimal, source_type: dt.DataType):
    if source_type.is_floating():
        if source_type != dt.float64:
            raise _UNSUPPORTED.error(reason="Float64 bins require Float64 metric output")
        decimal_cut = Decimal.from_float(cut) if isinstance(cut, float) else Decimal(cut)
        try:
            floating_cut = float(cut)
        except OverflowError:
            raise _UNSUPPORTED.error(
                reason="cutpoint cannot be represented exactly as Float64"
            ) from None
        if not math.isfinite(floating_cut) or Decimal.from_float(floating_cut) != decimal_cut:
            raise _UNSUPPORTED.error(reason="cutpoint cannot be represented exactly as Float64")
        return ibis.literal(floating_cut, type=dt.float64)

    decimal_cut = Decimal.from_float(cut) if isinstance(cut, float) else Decimal(cut)
    sign, digits, exponent = decimal_cut.as_tuple()
    if not isinstance(exponent, int):
        raise _UNSUPPORTED.error(reason="non-finite Decimal cutpoint")
    if all(digit == 0 for digit in digits):
        digits, exponent = (0,), 0
    else:
        while len(digits) > 1 and digits[-1] == 0:
            digits = digits[:-1]
            exponent += 1
    decimal_cut = Decimal((sign, digits, exponent))
    scale = max(-exponent, 0)
    integer_digits = max(len(digits) + exponent, 1)
    precision = integer_digits + scale
    if precision > 38:
        raise _UNSUPPORTED.error(reason="cutpoint exceeds exact DuckDB Decimal precision")
    if isinstance(source_type, dt.Decimal):
        if source_type.scale is None or source_type.precision is None:
            raise _UNSUPPORTED.error(reason="metric Decimal precision is unknown")
        source_scale = source_type.scale
        source_precision = source_type.precision
        source_integer_digits = source_precision - source_scale
        common_scale = max(source_scale, scale)
        common_integer_digits = max(source_integer_digits, integer_digits)
        if common_integer_digits + common_scale > 38:
            raise _UNSUPPORTED.error(reason="cutpoint comparison exceeds exact Decimal precision")
    return ibis.literal(format(decimal_cut, "f")).cast(dt.Decimal(precision, scale))


def _label(value, state: BinnedMetricState):
    """Classify each value by the bin its exact comparison with the cutpoints selects."""
    dtype = value.schema()["_state_period"]
    cuts = tuple(_cut_literal(cut, dtype) for cut in state.cutpoints)
    period = value._state_period
    branches = []
    for index, cut in enumerate(cuts):
        condition = period <= cut if index == 0 else (period > cuts[index - 1]) & (period <= cut)
        branches.append((condition, ibis.literal(state.levels[index])))
    branches.append((period > cuts[-1], ibis.literal(state.levels[-1])))
    return ibis.cases(*branches, else_=ibis.null().cast("string"))


def _state_value(relation, state: BinnedMetricState):
    raw = relation._state_period
    label = _label(relation, state)
    available = relation.__terrine_state_complete.fill_null(False)
    value = ibis.ifelse(available, label, ibis.literal(None, type="string"))
    reason = ibis.cases(
        (
            ~available,
            relation.__terrine_state_reason.fill_null(
                ibis.literal(reason_codes.INCOMPLETE_OBSERVATION)
            ),
        ),
        (
            available & raw.isnull(),
            relation.__terrine_state_reason.fill_null(reason_codes.STATE_VALUE_NULL),
        ),
        else_=ibis.null().cast("string"),
    )
    return value.cast("string"), (available & label.notnull()), reason


def _compact_state_schedule(schedule: Schedule, model: Model) -> Schedule | None:
    """Include the endpoint only when the elapsed and calendar clocks coincide."""
    if not isinstance(schedule.every, Duration) or schedule.every.seconds % 86_400:
        return None
    step = schedule.every.seconds
    try:
        old_count = schedule.count(model.day_boundary)
        start_utc = model.day_boundary(schedule.start).astimezone(UTC)
        endpoint_utc = model.day_boundary(schedule.end_exclusive).astimezone(UTC)
        if start_utc + timedelta(seconds=old_count * step) != endpoint_utc:
            return None
        extended_end = schedule.end + timedelta(days=step // 86_400)
        # Schedule includes its end day, so its exclusive end must also exist.
        if extended_end == date.max:
            return None
        extended = Schedule(schedule.every, schedule.start, extended_end)
        if extended.count(model.day_boundary) != old_count + 1:
            return None
    except OverflowError:
        return None
    return extended


def _state_period_relation(
    design: Design,
    model: Model,
    metric: str,
) -> tuple[ibis.Table, CompiledStudy, CompiledStudy]:
    schedule = design.anchor
    assert isinstance(schedule, Schedule) and isinstance(schedule.every, Duration)
    period = Window(Duration(-schedule.every.seconds), 0)
    node = MetricAgg(metric, period)
    state_design = _state_panel_design(
        design, anchor=schedule, features={"_state_period": node}, gap=Duration(0)
    )
    compiled = compile_columns(state_design, model, include_membership=False)
    endpoint = _state_panel_design(
        design,
        anchor=FixedDate(schedule.end_exclusive),
        features={"_state_period": node},
        gap=Duration(0),
    )
    final = compile_columns(endpoint, model, include_membership=False)
    resolved = model.metrics[metric]
    policy = dialect_for_name(expression_policy(model))
    if (
        resolved.kind != "mean"
        or len(resolved.measures) != 1
        or resolved.measures[0].aggregation != "sum"
        or resolved.transforms
        or compiled.table.schema()["_state_period"] != dt.Decimal(38, 0)
        or not policy._unbounded_coefficients
    ):
        return compiled.table.union(final.table), compiled, final
    extended = _compact_state_schedule(schedule, model)
    if extended is None:
        return compiled.table.union(final.table), compiled, final
    # Scale-zero coefficients add exactly in an unbounded domain. Count payloads
    # stay below physical source rows < 2**63 (see the policy's count_type).
    # Keep both original studies: their checks retain separate failure domains
    # and certify the same quantities and final checked narrowing.
    compact = compile_columns(
        _state_panel_design(
            design, anchor=extended, features={"_state_period": node}, gap=Duration(0)
        ),
        model,
        include_membership=False,
    )
    projection = {name: compact.table[name] for name in ("unit", "t0", "_state_period")}
    original_meta = compiled.columns["_state_period"]
    compact_meta = compact.columns["_state_period"]
    for attribute in (
        "complete_column",
        "reason_column",
        "observed_from_column",
        "observed_until_column",
    ):
        original = getattr(original_meta, attribute)
        if original is not None:
            projection[original] = compact.table[getattr(compact_meta, attribute)]
    return compact.table.select(**projection), compiled, final


def _binned(
    design: Design,
    model: Model,
    panel: StatePanel,
    *,
    persisted_anchors: ibis.Table | None,
) -> CompiledStudy:
    spec = panel.state
    assert isinstance(spec, BinnedMetricState)
    schedule = design.anchor
    assert isinstance(schedule, Schedule) and isinstance(schedule.every, Duration)
    every = schedule.every.seconds
    period_relation, period_compiled, endpoint_compiled = _state_period_relation(
        design, model, spec.metric
    )
    state_meta = period_compiled.columns["_state_period"]
    assert state_meta.complete_column is not None and state_meta.reason_column is not None
    period_relation = period_relation.mutate(
        __terrine_state_complete=period_relation[state_meta.complete_column].fill_null(False),
        __terrine_state_reason=period_relation[state_meta.reason_column],
    )
    output_design = _state_panel_design(
        design,
        anchor=schedule,
        features=design.features,
        filter_population=True,
    )
    compiled = compile_columns(
        output_design,
        model,
        persisted_anchors=persisted_anchors,
    )

    def attach(source):
        prior = period_relation.view()
        response = period_relation.view()
        joined = source.left_join(
            prior,
            (source.unit == prior.unit) & (source.t0 == prior.t0),
        ).left_join(
            response,
            (source.unit == response.unit)
            & (source.t0 + ibis.interval(seconds=every) == response.t0),
        )
        from_state, from_complete, from_reason = _state_value(prior, spec)
        to_state, to_complete, to_reason = _state_value(response, spec)
        projection = {name: source[name] for name in source.columns}
        projection.update(
            from_time=source.t0,
            to_time=source.t0 + ibis.interval(seconds=every),
            scheduled_to=source.t0 + ibis.interval(seconds=every),
            period_index=(source.anchor_id.re_replace("^schedule:", "").cast("int64")),
            gap=every * 1_000_000,
            from_state=from_state,
            to_state=to_state,
            terminal=ibis.literal(False),
            from_state_complete=from_complete,
            from_state_reason=from_reason,
            to_state_complete=to_complete,
            to_state_reason=to_reason,
        )
        for prefix, relation in (("from_state", prior), ("to_state", response)):
            for attribute, suffix in (
                ("observed_from_column", "observed_from"),
                ("observed_until_column", "observed_until"),
            ):
                name = getattr(state_meta, attribute)
                if name is not None:
                    projection[f"{prefix}_{suffix}"] = relation[name]
        return joined.select(**projection)

    retained = attach(compiled.table)
    audit = attach(compiled.audit_table)
    # Baseline values are evaluated at the common panel start, then broadcast by unit.
    baseline_expression_timings = {}
    baseline_compiled = None
    if panel.baseline_features:
        baseline_design = _state_panel_design(
            design,
            anchor=FixedDate(schedule.start),
            features=panel.baseline_features,
        )
        baseline_compiled = compile_columns(
            baseline_design,
            model,
            include_membership=False,
        )
        raw_baseline = baseline_compiled.table
        baseline_projection = {"unit": raw_baseline.unit}
        remapped = {}
        remapped_timings = {}
        for index, name in enumerate(panel.baseline_features):
            baseline_projection[name] = raw_baseline[name]
            meta = baseline_compiled.columns[name]
            replacements = {}
            for attribute in (
                "observed_from_column",
                "observed_until_column",
                "complete_column",
                "reason_column",
            ):
                original = getattr(meta, attribute)
                if original is not None:
                    alias = f"__terrine_baseline_{index}_{attribute}"
                    baseline_projection[alias] = raw_baseline[original]
                    replacements[attribute] = alias
            timing = baseline_compiled.expression_timings.get(name)
            if timing is not None:
                timing_replacements = {}
                for field in (
                    "point_time_column",
                    "start_bound_column",
                    "end_bound_column",
                ):
                    original = getattr(timing, field)
                    if field == "point_time_column" and timing.kind == "fixed_t0":
                        continue
                    if original is not None:
                        alias = f"__terrine_baseline_{index}_{field}"
                        baseline_projection[alias] = raw_baseline[original]
                        timing_replacements[field] = alias
                remapped_timings[name] = replace(timing, **timing_replacements)
            remapped[name] = replace(meta, **replacements)
        baseline_expression_timings = remapped_timings
        baseline = raw_baseline.select(**baseline_projection)
        retained = retained.left_join(baseline, retained.unit == baseline.unit)
        audit = audit.left_join(baseline, audit.unit == baseline.unit)
    feature_names = tuple(design.features) + tuple(panel.baseline_features)
    columns = dict(compiled.columns)
    cutoffs = dict(compiled.feature_cutoffs)
    if baseline_compiled is not None:
        columns.update(remapped)
        cutoffs.update(
            {
                name: replace(value, reference="panel_start")
                for name, value in baseline_compiled.feature_cutoffs.items()
            }
        )
    state_columns = {
        "from_state": ColumnMeta(
            window=Window(Duration(-every), 0),
            role="prior_state",
            observed_from_column=None,
            observed_until_column=None,
            complete_column="from_state_complete",
            reason_column="from_state_reason",
        ),
        "to_state": ColumnMeta(
            window=Window(0, Duration(every)),
            role="outcome",
            observed_from_column=None,
            observed_until_column=None,
            complete_column="to_state_complete",
            reason_column="to_state_reason",
        ),
    }
    for state_name in ("from_state", "to_state"):
        state_columns[state_name] = replace(
            state_columns[state_name],
            observed_from_column=f"{state_name}_observed_from"
            if state_meta.observed_from_column
            else None,
            observed_until_column=f"{state_name}_observed_until"
            if state_meta.observed_until_column
            else None,
        )
    columns.update(state_columns)
    roles = Roles(
        unit="unit",
        anchor="anchor_id",
        t0="t0",
        treatment=None,
        features=feature_names,
        outcomes=("to_state",),
        cluster_unit="unit",
        prior_state="from_state",
    )
    required_columns = [
        *IDENTITY,
        *STATE_PANEL,
        *feature_names,
        *design.eligibility,
        "from_state_complete",
        "from_state_reason",
        "to_state_complete",
        "to_state_reason",
    ]
    for meta in columns.values():
        for name in (
            meta.observed_from_column,
            meta.observed_until_column,
            meta.complete_column,
            meta.reason_column,
        ):
            if name is not None and name not in required_columns:
                required_columns.append(name)
    for timing in (
        *compiled.expression_timings.values(),
        *(baseline_expression_timings.values() if baseline_compiled is not None else ()),
    ):
        for name in (
            timing.point_time_column,
            timing.start_bound_column,
            timing.end_bound_column,
        ):
            if name is not None and name not in required_columns:
                required_columns.append(name)
    panel_start = ibis.literal(model.day_boundary(schedule.start))
    retained = retained.mutate(__terrine_panel_start=panel_start)
    audit = audit.mutate(__terrine_panel_start=panel_start)
    required_columns.append("__terrine_panel_start")
    retained = retained.select(**{name: retained[name] for name in required_columns})
    audit = audit.select(
        **{name: audit[name] for name in required_columns},
        __terrine_eligible=audit.__terrine_eligible,
    )
    checks = (
        list(compiled._checks) + list(period_compiled._checks) + list(endpoint_compiled._checks)
    )
    anchor_checks = (
        list(compiled._anchors._checks)
        + list(period_compiled._anchors._checks)
        + list(endpoint_compiled._anchors._checks)
    )
    if baseline_compiled is not None:
        checks.extend(baseline_compiled._checks)
        anchor_checks.extend(baseline_compiled._anchors._checks)
    anchors = AnchorTable(compiled._anchors.table, _checks=tuple(anchor_checks))
    return CompiledStudy(
        retained,
        audit,
        roles,
        columns,
        cutoffs,
        compiled.eligibility_cutoffs,
        {
            **compiled.strategy,
            "from_state": "reused_period_metric",
            "to_state": "reused_period_metric",
        },
        anchors,
        tuple(checks),
        {
            **(
                {
                    name: (
                        replace(timing, point_time_column="__terrine_panel_start")
                        if timing.kind == "fixed_t0"
                        else timing
                    )
                    for name, timing in baseline_expression_timings.items()
                }
                if baseline_compiled is not None
                else {}
            ),
            **compiled.expression_timings,
        },
        range_plans={
            **(baseline_compiled.range_plans if baseline_compiled is not None else {}),
            **compiled.range_plans,
        },
    )


def compile_state_panel(
    design: Design,
    model: Model,
    *,
    persisted_anchors: ibis.Table | None = None,
) -> CompiledStudy:
    """Compile a bounded schedule into paired unit-period state rows."""
    panel = design.shape
    assert isinstance(panel, StatePanel)
    from terrine.compile.columns import _validate_names

    _validate_names(design)
    design.validate(model)
    if isinstance(panel.state, StateObservations):
        from terrine.compile.shapes._observations import compile_observed_state_panel

        return compile_observed_state_panel(
            design,
            model,
            panel,
            persisted_anchors=persisted_anchors,
        )
    return _binned(
        design,
        model,
        panel,
        persisted_anchors=persisted_anchors,
    )
