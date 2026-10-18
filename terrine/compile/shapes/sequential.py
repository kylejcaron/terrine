"""Compiler for schedule-anchored sequential trials."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from typing import cast

import ibis
from ibis.expr import types as ir

from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.compile.shapes.matched_t0 import sample_risk_sets
from terrine.data import ColumnMeta, Roles
from terrine.errors import RefusalSpec
from terrine.semantic.bind import normalize_events
from terrine.spec.anchors import Schedule
from terrine.spec.design import Design
from terrine.spec.expressions import MetricAgg
from terrine.spec.panels import Initiation, PersistentAdoption, SequentialSpec
from terrine.temporal._timestamps import compare_timestamps
from terrine.temporal.aggregate import _Check, _Failure

_INVALID_TRIAL_WEIGHT = RefusalSpec(
    "terrine.design.invalid_trial_weight",
    "Trial weights must name every scheduled trial exactly once.",
)
_CONTRADICTORY_ADOPTION = RefusalSpec(
    "terrine.compile.contradictory_persistent_adoption_history",
    "Persistent adoption history contradicts absorbing state.",
)


def _followup_end(design: Design, model):
    assert isinstance(design.anchor, Schedule)
    horizon = max(
        (
            expression.window.end
            for expression in design.outcomes.values()
            if isinstance(expression, MetricAgg)
        ),
        default=0,
    )
    return model.day_boundary(design.anchor.end) + timedelta(seconds=horizon)


def _trial_columns(table, design: Design):
    assert isinstance(design.shape, SequentialSpec)
    assert isinstance(design.anchor, Schedule)
    table = table.mutate(trial_id=table.anchor_id.cast("string"))
    weight = design.shape.trial_weight
    if weight is None:
        return table
    schedule = design.anchor
    span = (schedule.end - schedule.start).days * 86_400
    count = (span - 1) // schedule.every + 1
    trial_ids = tuple(f"schedule:{index}" for index in range(count))
    if weight.kind == "equal_trial":
        values = {trial: 1.0 / count for trial in trial_ids}
    else:
        assert weight.values is not None
        if set(weight.values) != set(trial_ids):
            raise _INVALID_TRIAL_WEIGHT.error()
        values = weight.values
    branches = [(table.trial_id == trial, ibis.literal(values[trial])) for trial in trial_ids]
    return table.mutate(trial_weight=ibis.cases(*branches, else_=ibis.null().cast("float64")))


_LATER_INITIATION = RefusalSpec(
    "terrine.compile.later_control_initiation",
    "Control initiates during the sustained no-initiation follow-up.",
)

_INCOMPLETE_UNIT_HISTORY = RefusalSpec(
    "terrine.design.incomplete_unit_treatment_history",
    "Roster observation bounds do not cover a scheduled treatment history.",
)


def _unit_history_gaps(design: Design, model, anchors):
    assert isinstance(design.shape, SequentialSpec)
    assert isinstance(design.anchor, Schedule)
    selector = design.shape.treatment
    table = anchors.table
    history_start = cast(ir.TimestampScalar, ibis.literal(selector.history_start))
    boundary = (
        _followup_end(design, model)
        if isinstance(selector, Initiation)
        else model.day_boundary(design.anchor.end)
    )
    end = cast(ir.TimestampScalar, ibis.literal(boundary))
    missing = ibis.literal(False)
    if "_observed_from" in table.columns:
        missing |= table._observed_from.notnull() & (table._observed_from > history_start)
    if "_observed_until" in table.columns:
        missing |= table._observed_until.notnull() & (table._observed_until <= end)
    return table.filter(missing).select("unit").distinct()


def _persistent_contradictions(design: Design, model):
    assert isinstance(design.shape, SequentialSpec)
    assert isinstance(design.shape.treatment, PersistentAdoption)
    assert isinstance(design.anchor, Schedule)
    selector = design.shape.treatment
    source = model.sources[selector.source]
    prop = source.properties[selector.property]
    events = normalize_events(
        model._relation(source.relation), source, source.entities[design.unit]
    )
    end = ibis.literal(model.day_boundary(design.anchor.end))
    value = events[prop.column]
    adopted = events.filter(
        (events._timestamp >= ibis.literal(selector.history_start))
        & (events._timestamp < end)
        & (value == selector.adopted_value)
    ).select(_unit=events._unit, adopted_at=events._timestamp)
    reversed_state = events.filter(
        (events._timestamp >= ibis.literal(selector.history_start))
        & (events._timestamp < end)
        & value.notnull()
        & (value != selector.adopted_value)
    ).select(_unit=events._unit, reversed_at=events._timestamp)
    return (
        adopted.join(
            reversed_state,
            [
                adopted._unit == reversed_state._unit,
                reversed_state.reversed_at > adopted.adopted_at,
            ],
        )
        .select(unit=adopted._unit)
        .distinct()
    )


def _deferred_refusal(query, refusal: RefusalSpec) -> _Check:
    return _Check(
        query.aggregate(failed_0=query.count().cast("string")),
        (_Failure("failed_0", refusal, {}),),
    )


def _treatment_rows(design: Design, model, anchors):
    assert isinstance(design.shape, SequentialSpec)
    selector = design.shape.treatment
    source = model.sources[selector.source]
    events = normalize_events(
        model._relation(source.relation), source, source.entities[design.unit]
    )
    if selector.property in source.facts:
        observed = events._timestamp.notnull()
    else:
        prop = source.properties[selector.property]
        if prop.dimension_name is not None:
            raise ValueError("sequential treatment selectors cannot bind dimension properties")
        value = events[prop.column]
        observed = value.notnull()
        if isinstance(selector, PersistentAdoption):
            observed &= value == selector.adopted_value
    relevant = anchors.table.join(
        events,
        [
            anchors.table.unit == events._unit,
            compare_timestamps(
                events._timestamp,
                cast(ir.TimestampScalar, ibis.literal(selector.history_start)),
                "ge",
            ),
            compare_timestamps(events._timestamp, anchors.table.t0, "le"),
            observed,
        ],
        how="left",
    )
    return relevant.group_by("unit", "anchor_id").aggregate(
        treatment=relevant._timestamp.notnull().max().cast("bool")
    )


def _later_initiations(design: Design, model, eligible_table, treatment_rows):
    assert isinstance(design.shape, SequentialSpec)
    assert isinstance(design.shape.treatment, Initiation)
    selector = design.shape.treatment
    source = model.sources[selector.source]
    events = normalize_events(
        model._relation(source.relation), source, source.entities[design.unit]
    )
    if selector.property in source.facts:
        observed = events._timestamp.notnull()
    else:
        prop = source.properties[selector.property]
        observed = events[prop.column].notnull()
    control = eligible_table.join(
        treatment_rows,
        [
            eligible_table.unit == treatment_rows.unit,
            eligible_table.anchor_id.identical_to(treatment_rows.anchor_id),
        ],
    ).filter(~treatment_rows.treatment.fill_null(False))
    followup_end = cast(ir.TimestampScalar, ibis.literal(_followup_end(design, model)))
    return (
        control.join(
            events,
            [
                control.unit == events._unit,
                compare_timestamps(events._timestamp, control.t0, "gt"),
                compare_timestamps(events._timestamp, followup_end, "lt"),
                observed,
            ],
        )
        .select(unit=control.unit, t0=control.t0)
        .distinct()
    )


def _attach_treatment(table, values):
    joined = table.join(
        values,
        [
            table.unit == values.unit,
            table.anchor_id.identical_to(values.anchor_id),
        ],
        how="left",
    )
    return joined.select(
        **{name: table[name] for name in table.columns},
        treatment=joined.treatment.fill_null(False),
    )


def compile_sequential(design: Design, model) -> CompiledStudy:
    if not isinstance(design.shape, SequentialSpec):
        raise TypeError("compile_sequential requires a Sequential Design")
    compiled = compile_columns(design, model)
    values = _treatment_rows(design, model, compiled._anchors)
    sample = design.sample
    table = _trial_columns(_attach_treatment(compiled.table, values), design)
    checks = list(compiled._checks)
    checks.append(
        _deferred_refusal(
            _unit_history_gaps(design, model, compiled._anchors), _INCOMPLETE_UNIT_HISTORY
        )
    )
    if isinstance(design.shape.treatment, Initiation):
        checks.append(
            _deferred_refusal(
                _later_initiations(design, model, compiled.table, values), _LATER_INITIATION
            )
        )
    elif isinstance(design.shape.treatment, PersistentAdoption):
        checks.append(
            _deferred_refusal(_persistent_contradictions(design, model), _CONTRADICTORY_ADOPTION)
        )
    if sample is not None:
        table, check = sample_risk_sets(table, sample)
        checks.append(check)
    columns = dict(compiled.columns)
    columns["treatment"] = ColumnMeta(None, "treatment", None, None, None, None)
    columns["trial_id"] = ColumnMeta(None, "trial", None, None, None, None)
    roles = Roles(
        "unit",
        "anchor_id",
        "t0",
        "treatment",
        tuple(design.features),
        tuple(design.outcomes),
        cluster_unit="unit",
        trial="trial_id",
        trial_weight="trial_weight" if design.shape.trial_weight is not None else None,
        target_weight="target_weight" if "target_weight" in design.features else None,
        assignment_probability="assignment_probability" if sample is not None else None,
    )
    if design.shape.trial_weight is not None:
        columns["trial_weight"] = ColumnMeta(None, "trial_weight", None, None, None, None)
    if sample is not None:
        columns["assignment_probability"] = ColumnMeta(
            None, "sampling_probability", None, None, None, None
        )
    return replace(
        compiled,
        table=table,
        audit_table=_attach_treatment(compiled.audit_table, values),
        roles=roles,
        columns=columns,
        _checks=tuple(checks),
    )
