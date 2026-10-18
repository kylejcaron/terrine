"""Compiler for roster-backed calendar comparison stacks."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import ibis
from ibis.expr import types as ir

from terrine.compile.anchors import AnchorTable
from terrine.compile.columns import CompiledStudy, _validate_names, compile_columns
from terrine.data import ColumnMeta, Roles
from terrine.errors import RefusalSpec
from terrine.semantic.bind import normalize_events
from terrine.spec.anchors import Schedule
from terrine.spec.design import Design
from terrine.spec.panels import CalendarComparisonSpec
from terrine.temporal.aggregate import _Check, _Failure

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
            _Failure(
                "failed_0",
                _UNSUPPORTED,
                {"reason": "requested comparison period is outside the schedule"},
            ),
        ),
    )


def _history_check(design: Design, model: Model, anchors: AnchorTable):
    spec = design.shape
    assert isinstance(spec, CalendarComparisonSpec)
    anchor = design.anchor
    assert isinstance(anchor, Schedule)
    selector = spec.cohort
    units = anchors.table.select("unit", "_observed_from", "_observed_until").distinct()
    missing = cast(ir.BooleanValue, ibis.literal(False))
    if "_observed_from" in units.columns:
        missing = missing | (
            units._observed_from.notnull()
            & (units._observed_from > ibis.literal(selector.history_start))
        )
    if "_observed_until" in units.columns:
        missing = missing | (
            units._observed_until.notnull()
            & (units._observed_until < ibis.literal(model.day_boundary(anchor.end)))
        )
    query = units.filter(missing).select("unit")
    return _Check(
        query.aggregate(failed_0=query.count().cast("string")),
        (_Failure("failed_0", _INCOMPLETE_HISTORY, {}),),
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
    source = model.sources[selector.source]
    events = normalize_events(
        model._relation(source.relation), source, source.entities[design.unit]
    )
    if selector.property in source.facts:
        observed = events._timestamp.notnull()
    else:
        prop = source.properties[selector.property]
        if prop.dimension_name is not None:
            raise _UNSUPPORTED.error(reason="cohort properties bound to dimensions are unsupported")
        observed = events[prop.column].notnull()
    initiation = (
        events.filter(observed & (events._timestamp >= ibis.literal(selector.history_start)))
        .group_by("_unit")
        .aggregate(initiation_time=events._timestamp.min())
    )
    candidates = anchors.table.join(
        initiation,
        [
            anchors.table.unit == initiation._unit,
            anchors.table.t0 >= initiation.initiation_time,
        ],
    )
    cohorts = candidates.group_by("unit").aggregate(cohort_time=candidates.t0.min())
    return cohorts, initiation


def _stack(table, *, anchors, cohorts, initiations, spec: CalendarComparisonSpec, every: int):
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
        initiation_time=initiations.initiation_time,
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
        initiation_time=initiation_map.initiation_time,
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
    latest = cast(ir.TimestampValue, ibis.greatest(comparison_t0, reference_t0)) + ibis.interval(
        seconds=anticipation * every
    )
    treated = (with_values.unit_cohort == with_values.focal_cohort).fill_null(False)
    if spec.control == "never_treated":
        control = with_values.initiation_time.isnull()
    else:
        control = with_values.initiation_time.isnull() | (with_values.initiation_time > latest)
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
    cohort_units, initiation_times = _cohort_units(design, model, anchors)

    def stack(source):
        return _stack(
            source,
            anchors=anchors,
            cohorts=cohort_units,
            initiations=initiation_times,
            spec=spec,
            every=schedule.every,
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
            _history_check(design, model, anchors),
            _period_check(
                cohort_units,
                anchors,
                (spec.reference_period, *spec.comparison_periods, *spec.anticipation_periods),
                schedule.every,
            ),
        ),
    )
