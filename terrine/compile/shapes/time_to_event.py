"""Compile one bounded first-event history per analysis unit."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import ibis

from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.data import ColumnMeta, Roles
from terrine.errors import RefusalSpec
from terrine.semantic.bind import normalize_events
from terrine.spec.design import Design
from terrine.spec.trajectory import TimeToEvent
from terrine.temporal.first import first_occurrence

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["compile_time_to_event"]

_INVALID = RefusalSpec(
    "terrine.survival.invalid_compilation",
    "Time-to-event compilation failed ({reason}).",
)
_INCOMPLETE_HISTORY = "incomplete_initial_history"


def _source(model: Model, unit: str, fact_name: str):
    fact = model.catalog.facts[fact_name]
    source = model.sources[fact.source]
    return fact, source, source.entities.get(unit)


def _events(model: Model, source, unit_column: str):
    raw = model._relation(source.relation)
    normalized = normalize_events(raw, source, unit_column)
    columns = {"unit": normalized._unit, "_timestamp": normalized._timestamp}
    event_id = model.event_ids.get(source.name)
    if event_id is not None:
        columns["_event_id"] = raw[event_id]
    return normalized.select(**columns), event_id


def compile_time_to_event(design: Design, model: Model) -> CompiledStudy:
    """Build an event/censor relation with explicit half-open supported history."""
    if not isinstance(design.shape, TimeToEvent):
        raise TypeError("compile_time_to_event requires a TimeToEvent design")
    if design.shape.exit is not None and design.shape.exit == design.shape.event:
        raise _INVALID.error(reason="event_and_exit_facts_must_be_distinct")
    design.validate(model)
    shape = design.shape
    base_design = Design(
        design.unit,
        design.anchor,
        features=design.features,
        eligibility=design.eligibility,
        treatment=design.treatment,
        gap=design.gap,
        shape="cross_section",
        anchor_range=design.anchor_range,
    )
    base = compile_columns(base_design, model)
    if base.roles.unit != "unit":
        raise _INVALID.error(reason="missing_unit_role")
    _, event_source, event_unit = _source(model, design.unit, shape.event)
    if event_unit is None:
        raise _INVALID.error(reason="event_source_missing_unit")
    event_relation, event_id = _events(model, event_source, event_unit)
    coverage = model.readiness.sources.get(event_source.name)
    if coverage is None:
        raise _INVALID.error(reason="event_source_coverage_required")
    start = ibis.literal(coverage.start)
    end = ibis.literal(coverage.end_exclusive)
    candidates = base.table.select(
        **{name: base.table[name] for name in base.table.columns if name != "__terrine_eligible"}
    )
    supported_start = ibis.greatest(start, candidates.t0)
    if "_observed_from" in candidates.columns:
        supported_start = ibis.greatest(supported_start, candidates._observed_from)
    supported_end = ibis.least(end, candidates._observed_until)
    if design.administrative_cutoff is not None:
        supported_end = ibis.least(supported_end, ibis.literal(design.administrative_cutoff))
    history_start = start
    checks = list(base._checks)
    if shape.entry is not None:
        _, entry_source, entry_unit = _source(model, design.unit, shape.entry)
        if entry_unit is None:
            raise _INVALID.error(reason="entry_source_missing_unit")
        entry_events, entry_event_id = _events(model, entry_source, entry_unit)
        entry_candidates = candidates.select(
            unit=candidates.unit,
            anchor_id=candidates.anchor_id,
            lower=ibis.literal(model.readiness.sources[entry_source.name].start),
            upper=supported_end,
        )
        entered = first_occurrence(
            entry_events,
            entry_candidates,
            unit="unit",
            timestamp="_timestamp",
            lower="lower",
            upper="upper",
            event_id="_event_id" if entry_event_id is not None else None,
        )
        candidates = candidates.left_join(
            entered.table.select(
                unit=entered.table.unit,
                anchor_id=entered.table.anchor_id,
                _entry_time=entered.table.first_timestamp,
            ),
            [
                candidates.unit == entered.table.unit,
                candidates.anchor_id == entered.table.anchor_id,
            ],
        )
        checks.append(entered)
        supported_start = ibis.greatest(supported_start, candidates._entry_time)

    if shape.exit is not None:
        _, exit_source, exit_unit = _source(model, design.unit, shape.exit)
        if exit_unit is None:
            raise _INVALID.error(reason="exit_source_missing_unit")
        exit_coverage = model.readiness.sources.get(exit_source.name)
        if exit_coverage is None:
            raise _INVALID.error(reason="exit_source_coverage_required")
        exit_start = ibis.literal(exit_coverage.start)
        history_start = ibis.greatest(history_start, exit_start)
        supported_start = ibis.greatest(supported_start, exit_start)
        supported_end = ibis.least(supported_end, ibis.literal(exit_coverage.end_exclusive))
        exit_events, exit_event_id = _events(model, exit_source, exit_unit)
        exit_candidates = candidates.select(
            unit=candidates.unit,
            anchor_id=candidates.anchor_id,
            lower=exit_start,
            upper=supported_end,
        )
        exited = first_occurrence(
            exit_events,
            exit_candidates,
            unit="unit",
            timestamp="_timestamp",
            lower="lower",
            upper="upper",
            event_id="_event_id" if exit_event_id is not None else None,
        )
        candidates = candidates.left_join(
            exited.table.select(
                unit=exited.table.unit,
                anchor_id=exited.table.anchor_id,
                _exit_time=exited.table.first_timestamp,
            ),
            [candidates.unit == exited.table.unit, candidates.anchor_id == exited.table.anchor_id],
        )
        checks.append(exited)
        supported_end = ibis.least(supported_end, candidates._exit_time)
    candidates = candidates.mutate(
        _followup_start=supported_start,
        _followup_end=supported_end,
    )
    prior_candidates = candidates.select(
        unit=candidates.unit,
        anchor_id=candidates.anchor_id,
        lower=start,
        upper=candidates._followup_start,
    )
    prior = first_occurrence(
        event_relation,
        prior_candidates,
        unit="unit",
        timestamp="_timestamp",
        lower="lower",
        upper="upper",
        event_id="_event_id" if event_id is not None else None,
    )
    checks.append(prior)

    event_candidates = candidates.select(
        unit=candidates.unit,
        anchor_id=candidates.anchor_id,
        lower=candidates._followup_start,
        upper=candidates._followup_end,
    )
    selected = first_occurrence(
        event_relation,
        event_candidates,
        unit="unit",
        timestamp="_timestamp",
        lower="lower",
        upper="upper",
        event_id="_event_id" if event_id is not None else None,
    )
    checks.append(selected)
    result = candidates.left_join(
        selected.table.select(
            unit=selected.table.unit,
            anchor_id=selected.table.anchor_id,
            _event_time=selected.table.first_timestamp,
        ),
        [candidates.unit == selected.table.unit, candidates.anchor_id == selected.table.anchor_id],
    )
    result = result.left_join(
        prior.table.select(
            prior_unit=prior.table.unit,
            prior_anchor=prior.table.anchor_id,
            _prior_event_time=prior.table.first_timestamp,
        ),
        [result.unit == prior.table.unit, result.anchor_id == prior.table.anchor_id],
    )
    initial_gap = candidates.t0 < history_start
    if shape.delayed_entry:
        initial_gap = ibis.literal(False)
    if "_observed_from" in candidates.columns:
        initial_gap = initial_gap | (candidates._observed_from > candidates.t0).fill_null(False)
    entry_missing = candidates._entry_time.isnull() if shape.delayed_entry else ibis.literal(False)
    complete = ibis.ifelse(
        initial_gap,
        ibis.literal(False),
        ibis.ifelse(entry_missing, ibis.literal(False), ibis.literal(True)),
    )
    reason = ibis.ifelse(
        initial_gap,
        _INCOMPLETE_HISTORY,
        ibis.ifelse(entry_missing, "missing_delayed_entry", ibis.null().cast("string")),
    )
    prevalent = result._prior_event_time.notnull()
    retained = result.filter(~prevalent)
    output_projection = {
        name: retained[name]
        for name in (*design.features, *((base.roles.treatment,) if base.roles.treatment else ()))
    }
    output_projection.update(
        unit=retained.unit,
        anchor_id=retained.anchor_id,
        t0=retained.t0,
        event_time=retained._event_time,
        censor_time=retained._followup_end,
        duration=ibis.coalesce(retained._event_time, retained._followup_end),
        event=retained._event_time.notnull(),
        entry_time=retained._followup_start,
        complete=complete,
        reason=reason,
    )
    output = retained.select(**output_projection)
    audit = base.audit_table
    columns = {
        name: ColumnMeta(None, "outcome", None, None, "complete", "reason")
        for name in ("event", "event_time", "duration")
    }
    roles = Roles(
        "unit",
        "anchor_id",
        "t0",
        base.roles.treatment,
        tuple(design.features),
        ("duration", "event"),
    )
    return replace(
        base,
        table=output,
        audit_table=audit,
        roles=roles,
        columns=columns,
        _checks=tuple(checks),
    )
