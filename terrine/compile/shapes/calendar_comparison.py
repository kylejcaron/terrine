"""Compiler for roster-backed calendar comparison stacks."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import TYPE_CHECKING, cast

import ibis
from ibis.expr import types as ir

from terrine.backends.extrema import greatest
from terrine.backends.timestamps import compare_timestamps
from terrine.compile.anchors import AnchorTable, _precision_check, _utc_column
from terrine.compile.columns import CompiledStudy, _validate_names, compile_columns
from terrine.data import ColumnMeta, Roles, RowCheck
from terrine.errors import RefusalSpec
from terrine.semantic.bind import normalize_events
from terrine.spec.anchors import Schedule
from terrine.spec.design import Design
from terrine.spec.panels import CalendarComparisonSpec
from terrine.spec.validate import _outcome_bounds, resolve_selector_source
from terrine.temporal.aggregate import _Check

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["compile_calendar_comparison"]

_UNSUPPORTED = RefusalSpec(
    "terrine.compile.unsupported_calendar_comparison",
    "Calendar comparison compilation is unsupported: {reason}.",
)


_INCOMPLETE_HISTORY = RefusalSpec(
    "terrine.design.incomplete_unit_treatment_history",
    "Roster observation bounds do not cover the required calendar treatment history.",
)


def _deferred_refusal(query):
    return _Check(
        query.aggregate(failed_0=query.count().cast("string")),
        (
            RowCheck(
                "failed_0",
                _UNSUPPORTED,
                {"reason": "requested comparison period is outside the schedule"},
            ),
        ),
    )


def _followup_bound(
    t0,
    reference_t0,
    *,
    outcome_window_end: int,
    outcome_cutoff: datetime | None,
    anticipation_seconds: int,
) -> tuple[ir.TimestampValue, bool]:
    end = cast(ir.TimestampValue, greatest(t0, reference_t0))
    if outcome_window_end:
        end += ibis.interval(seconds=outcome_window_end)
    if outcome_cutoff is not None:
        end = cast(ir.TimestampValue, greatest(end, ibis.literal(outcome_cutoff)))
    if anticipation_seconds:
        end += ibis.interval(seconds=anticipation_seconds)
    return end, outcome_window_end > 0 or outcome_cutoff is not None


def _history_check(
    design: Design, model: Model, rows, outcome_window_end: int, outcome_cutoff: datetime | None
):
    spec = design.shape
    assert isinstance(spec, CalendarComparisonSpec)
    anchor = design.anchor
    assert isinstance(anchor, Schedule)
    selector = spec.cohort
    participants = rows
    if "__terrine_eligible" in rows.columns:
        participants = participants.filter(rows.__terrine_eligible)
    reference_t0 = participants.cohort + ibis.interval(seconds=spec.reference_period * anchor.every)
    anticipation = max((abs(value) for value in spec.anticipation_periods), default=0)
    latest, exclusive = _followup_bound(
        participants.t0,
        reference_t0,
        outcome_window_end=outcome_window_end,
        outcome_cutoff=outcome_cutoff,
        anticipation_seconds=anticipation * anchor.every,
    )
    schedule_end = ibis.literal(model.day_boundary(anchor.end_exclusive))
    never_control = participants.control_eligible & ~participants.treatment
    missing = cast(ir.BooleanValue, ibis.literal(False))
    if "_observed_from" in participants.columns:
        missing = missing | (
            participants._observed_from.notnull()
            & (participants._observed_from > ibis.literal(selector.history_start))
        )
    if "_observed_until" in participants.columns:
        observation_cutoff_failure = (
            participants._observed_until < latest
            if exclusive
            else participants._observed_until <= latest
        )
        missing = missing | (participants._observed_until.notnull() & observation_cutoff_failure)
        if spec.control == "never_treated":
            missing = missing | (
                never_control
                & participants._observed_until.notnull()
                & (participants._observed_until < schedule_end)
            )
    query = participants.filter(missing).select("unit").distinct()
    return _Check(
        query.aggregate(failed_0=query.count().cast("string")),
        (RowCheck("failed_0", _INCOMPLETE_HISTORY, {}),),
    )


def _period_check(cohorts, anchors, periods, every):
    cohort_values = cohorts.select(focal_cohort=cohorts.cohort_time).distinct()
    shifted = [
        cohort_values.select(t0=cohort_values.focal_cohort + ibis.interval(seconds=period * every))
        for period in periods
    ]
    required = shifted[0]
    for period_table in shifted[1:]:
        required = required.union(period_table, distinct=False)
    scheduled = anchors.table.select(t0=anchors.table.t0).distinct()
    missing = required.join(scheduled, required.t0 == scheduled.t0, how="left").filter(
        scheduled.t0.isnull()
    )
    return _deferred_refusal(missing)


def _cohort_units(design: Design, model: Model, anchors: AnchorTable):
    spec = design.shape
    assert isinstance(spec, CalendarComparisonSpec)
    selector = spec.cohort
    source = model.sources[resolve_selector_source(model, selector.event)]
    events = normalize_events(model._relation(source.relation), source, source.units[design.unit])
    precision = _precision_check(model, events, "_timestamp", source_column=source.time_column)
    events = _utc_column(model, events, "_timestamp", "_timestamp_utc")
    timestamp = events._timestamp_utc
    if selector.event in source.events:
        observed = timestamp.notnull()
    else:
        prop = source.properties[selector.event]
        if prop.dimension_name is not None:
            raise _UNSUPPORTED.error(reason="cohort properties bound to dimensions are unsupported")
        observed = events[prop.column].notnull()
    initiation = (
        events.filter(
            observed
            & compare_timestamps(
                timestamp,
                cast(ir.TimestampScalar, ibis.literal(selector.history_start)),
                "ge",
            )
        )
        .group_by("_unit")
        .aggregate(__terrine_initiation_time=timestamp.min())
    )
    candidates = anchors.table.join(
        initiation,
        [
            anchors.table.unit == initiation._unit,
            compare_timestamps(anchors.table.t0, initiation.__terrine_initiation_time, "ge"),
        ],
    )
    cohorts = candidates.group_by("unit").aggregate(cohort_time=candidates.t0.min())
    return cohorts, initiation, precision


def _stack(
    table,
    *,
    anchors,
    cohorts,
    initiations,
    spec: CalendarComparisonSpec,
    every: int,
    outcome_window_end: int,
    outcome_cutoff: datetime | None,
):
    period_values = tuple(
        dict.fromkeys((spec.reference_period, *spec.comparison_periods, *spec.anticipation_periods))
    )
    cohort_values = cohorts.select(focal_cohort=cohorts.cohort_time).distinct()
    pieces = []
    for period in period_values:
        period_table = cohort_values.select(
            focal_cohort=cohort_values.focal_cohort,
            t0=cohort_values.focal_cohort + ibis.interval(seconds=period * every),
            period=ibis.literal(period),
        )
        pieces.append(period_table)
    times = pieces[0]
    for piece in pieces[1:]:
        times = times.union(piece, distinct=False)
    unit_times = anchors.table.select(unit=anchors.table.unit, calendar_t0=anchors.table.t0)
    matched = times.join(unit_times, times.t0 == unit_times.calendar_t0)
    scaffold = matched.select(
        unit=unit_times.unit,
        focal_cohort=times.focal_cohort,
        t0=times.t0,
        period=times.period,
    )
    cohort_map = cohorts.select(unit_key=cohorts.unit, unit_cohort=cohorts.cohort_time)
    initiation_map = initiations.select(
        initiation_unit=initiations._unit,
        __terrine_initiation_time=initiations.__terrine_initiation_time,
    )
    joined_cohorts = scaffold.join(cohort_map, scaffold.unit == cohort_map.unit_key, how="left")
    joined_initiations = joined_cohorts.join(
        initiation_map, joined_cohorts.unit == initiation_map.initiation_unit, how="left"
    )
    expanded = joined_initiations.select(
        unit=scaffold.unit,
        focal_cohort=scaffold.focal_cohort,
        t0=scaffold.t0,
        period=scaffold.period,
        unit_cohort=cohort_map.unit_cohort,
        __terrine_initiation_time=initiation_map.__terrine_initiation_time,
    )
    source_values = table.select(
        value_unit=table.unit,
        value_t0=table.t0,
        **{name: table[name] for name in table.columns if name not in ("unit", "t0", "anchor_id")},
    )
    with_values = expanded.join(
        source_values,
        [expanded.unit == source_values.value_unit, expanded.t0 == source_values.value_t0],
        how="inner",
    )
    reference_t0 = with_values.focal_cohort + ibis.interval(seconds=spec.reference_period * every)
    anticipation = max((abs(value) for value in spec.anticipation_periods), default=0)
    comparison_t0 = with_values.t0
    latest, exclusive = _followup_bound(
        comparison_t0,
        reference_t0,
        outcome_window_end=outcome_window_end,
        outcome_cutoff=outcome_cutoff,
        anticipation_seconds=anticipation * every,
    )
    treated = (with_values.unit_cohort == with_values.focal_cohort).fill_null(False)
    if spec.control == "never_treated":
        control = with_values.__terrine_initiation_time.isnull()
    else:
        control_after_cutoff = (
            with_values.__terrine_initiation_time >= latest
            if exclusive
            else with_values.__terrine_initiation_time > latest
        )
        control = with_values.__terrine_initiation_time.isnull() | control_after_cutoff
    eligible = treated | control
    filtered = with_values.filter(eligible)
    columns = {
        name: with_values[name]
        for name in table.columns
        if name not in ("unit", "t0", "anchor_id", "__terrine_eligible")
    }
    output = {
        "unit": with_values.unit,
        "anchor_id": ibis.struct({"cohort": with_values.focal_cohort, "period": with_values.t0}),
        "t0": with_values.t0,
        "cohort": with_values.focal_cohort,
        "period": with_values.t0,
        "event_period": with_values.period,
        "treatment": treated,
        "control_eligible": control,
        "__terrine_treatment_initiation_time": with_values.__terrine_initiation_time,
        **columns,
    }
    if "__terrine_eligible" in table.columns:
        output["__terrine_eligible"] = with_values.__terrine_eligible
    return filtered.select(**output)


def compile_calendar_comparison(design: Design, model: Model) -> CompiledStudy:
    """Compile one roster-backed focal stack per adopter cohort."""
    spec = design.shape
    if not isinstance(spec, CalendarComparisonSpec):
        raise TypeError("compile_calendar_comparison requires CalendarComparisonSpec")
    if design.sample is not None:
        raise _UNSUPPORTED.error(reason="sampled analysis populations are not admissible")
    _validate_names(design)
    design.validate(model)
    if not isinstance(design.anchor, Schedule):
        raise _UNSUPPORTED.error(reason="calendar comparisons require a schedule anchor")
    schedule = design.anchor
    base = Design(
        design.unit,
        design.anchor,
        features=design.features,
        outcomes=design.outcomes,
        eligibility=design.eligibility,
        gap=design.gap,
        anchor_range=design.anchor_range,
    )
    compiled = compile_columns(base, model)
    anchors = compiled._anchors
    cohort_units, initiation_times, cohort_precision = _cohort_units(design, model, anchors)
    if cohort_precision is not None:
        anchors = replace(anchors, _checks=(*anchors._checks, cohort_precision))
    outcome_offsets, outcome_cutoffs = _outcome_bounds(design, model)
    outcome_window_end = max(outcome_offsets, default=0)
    outcome_cutoff = max(outcome_cutoffs) if outcome_cutoffs else None

    def stack(source):
        return _stack(
            source,
            anchors=anchors,
            cohorts=cohort_units,
            initiations=initiation_times,
            spec=spec,
            every=schedule.every,
            outcome_window_end=outcome_window_end,
            outcome_cutoff=outcome_cutoff,
        )

    retained = stack(compiled.table)
    audit = stack(compiled.audit_table)
    columns = dict(compiled.columns)
    columns.update(
        {
            "cohort": ColumnMeta(None, "cohort", None, None, None, None),
            "period": ColumnMeta(None, "period", None, None, None, None),
            "event_period": ColumnMeta(None, "event period", None, None, None, None),
            "treatment": ColumnMeta(None, "treatment", None, None, None, None),
            "control_eligible": ColumnMeta(None, "control eligibility", None, None, None, None),
            "__terrine_treatment_initiation_time": ColumnMeta(
                None, "treatment initiation time", None, None, None, None
            ),
        }
    )
    return CompiledStudy(
        retained,
        audit,
        Roles(
            "unit",
            "anchor_id",
            "t0",
            "treatment",
            tuple(design.features),
            tuple(design.outcomes),
            cluster_unit="unit",
            trial="cohort",
        ),
        columns,
        compiled.feature_cutoffs,
        compiled.eligibility_cutoffs,
        {**compiled.strategy, "cohort": "calendar_initiation", "control_eligible": "risk_set"},
        anchors,
        (
            *compiled._checks,
            _history_check(design, model, audit, outcome_window_end, outcome_cutoff),
            _period_check(
                cohort_units,
                anchors,
                (spec.reference_period, *spec.comparison_periods, *spec.anticipation_periods),
                schedule.every,
            ),
        ),
        expression_timings=compiled.expression_timings,
        range_plans=compiled.range_plans,
    )
