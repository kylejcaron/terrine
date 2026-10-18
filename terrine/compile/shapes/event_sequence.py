"""Adapter from typed EventSequence designs to normalized temporal operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import ibis
import pyarrow as pa

from terrine.compile.anchors import (
    AnchorTable,
    _integrity_check,
    _null_check,
    _precision_check,
    _unique_checks,
    _utc_column,
)
from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.data import Roles, SequenceMeta
from terrine.semantic.bind import normalize_events
from terrine.spec.design import Design
from terrine.spec.sequence import _UNSUPPORTED_SHAPE, EventSequence
from terrine.temporal.attributes import _enriched_property_columns, enrich_events
from terrine.temporal.sequence import SequenceResult, event_sequence

if TYPE_CHECKING:
    from collections.abc import Callable

    from terrine.model import Model

__all__ = ["compile_event_sequence"]


@dataclass(frozen=True, slots=True)
class _SequencePreflight:
    result: SequenceResult

    def validate(self, execute: Callable[[ibis.Table], pa.Table]) -> None:
        self.result.validate(execute=execute)


def _property_binding(source, name: str):
    binding = source.properties.get(name)
    if binding is not None:
        return binding
    for dimension in source.dimensions:
        binding = dimension.properties.get(name)
        if binding is not None:
            return binding
    raise KeyError(name)


def _identity_checks(events: ibis.Table, unit: str, identity: str, *, configured: bool):
    checks = [
        _null_check(events, unit, "null_event_unit"),
        _null_check(events, "_timestamp", "null_event_timestamp"),
    ]
    if configured:
        return [*checks, *_unique_checks(events, identity, "event_id", scope=(unit,))]
    ties = (
        events.select(_unit=events[unit], _timestamp=events["_timestamp"])
        .group_by("_unit", "_timestamp")
        .aggregate(_count=ibis._.count())
        .filter(ibis._._count > 1)
        .aggregate(invalid_count=ibis._.count())
    )
    return [*checks, _integrity_check("tied_event_timestamps", ties)]


def compile_event_sequence(design: Design, model: Model) -> CompiledStudy:
    """Compile a bounded EventSequence through the shared anchor and column paths."""
    sequence = design.shape
    if not isinstance(sequence, EventSequence):
        raise TypeError("design shape must be EventSequence")
    design.validate(model)
    if design.features or design.outcomes:
        raise _UNSUPPORTED_SHAPE.error()
    # Existing cross-section machinery retains candidate identity, eligibility, and audit roles.
    base_design = Design(
        design.unit,
        design.anchor,
        eligibility=design.eligibility,
        gap=design.gap,
        anchor_range=design.anchor_range,
    )
    base = compile_columns(base_design, model)
    anchors = base.table.select(*base._anchors.table.columns)
    fact = model.catalog.facts[sequence.fact]
    source = model.sources[fact.source]
    physical_unit = source.entities[design.unit]
    configured_id = model.event_ids.get(source.name)
    identity_alias = "__terrine_sequence_event_id"
    raw = model._relation(source.relation)
    normalized = normalize_events(
        raw,
        source,
        physical_unit,
        extra_columns={identity_alias: configured_id} if configured_id is not None else None,
    )
    if fact.column is not None:
        normalized = normalized.filter(normalized[fact.column].notnull())
    timestamp_check = _precision_check(
        model, normalized, "_timestamp", source_column=source.timestamp_column
    )
    timestamp_alias = "_sequence_raw_timestamp"
    suffix = 0
    while timestamp_alias in normalized.columns:
        suffix += 1
        timestamp_alias = f"_sequence_raw_timestamp_{suffix}"
    normalized = normalized.mutate(**{timestamp_alias: normalized["_timestamp"]}).drop("_timestamp")
    normalized = _utc_column(model, normalized, timestamp_alias, "_timestamp")

    internal_names = (sequence.label_property,) + (
        (sequence.funnel.event_property,) if sequence.funnel is not None else ()
    )
    internal_properties: tuple[str, ...] = tuple(
        name
        for name in dict.fromkeys(internal_names)
        if name is not None and name not in sequence.properties
    )
    funnel_event_property = sequence.funnel.event_property if sequence.funnel is not None else None
    funnel_event_property_column = None
    if funnel_event_property is not None:
        reserved = set(normalized.columns) | set(sequence.properties) | set(internal_properties)
        funnel_event_property_column = "__terrine_funnel_event_property"
        suffix = 0
        while funnel_event_property_column in reserved:
            suffix += 1
            funnel_event_property_column = f"__terrine_funnel_event_property_{suffix}"
    internal_property_columns_list: list[str] = []
    for name in internal_properties:
        if name == funnel_event_property:
            if funnel_event_property_column is None:
                raise _UNSUPPORTED_SHAPE.error()
            internal_property_columns_list.append(funnel_event_property_column)
        else:
            internal_property_columns_list.append(name)
    internal_property_columns = tuple(internal_property_columns_list)
    direct = {}
    for property_name in (*sequence.properties, *internal_properties):
        prop = _property_binding(source, property_name)
        if prop.dimension_name is None:
            if prop.column is None:
                raise _UNSUPPORTED_SHAPE.error()
            targets = (
                []
                if property_name == funnel_event_property
                and property_name not in sequence.properties
                else [property_name]
            )
            if property_name == funnel_event_property:
                if funnel_event_property_column is None:
                    raise _UNSUPPORTED_SHAPE.error()
                targets.append(funnel_event_property_column)
            for target in dict.fromkeys(targets):
                direct[target] = normalized[prop.column]
    if direct:
        normalized = normalized.mutate(**direct)
    aliases = _enriched_property_columns(source.dimensions, normalized.columns)
    dimensions = {
        dimension.name: model._relation(dimension.relation) for dimension in source.dimensions
    }
    if dimensions:
        normalized = enrich_events(normalized, source, dimensions)
        derived = {}
        for property_name in (*sequence.properties, *internal_properties):
            prop = _property_binding(source, property_name)
            if prop.dimension_name is not None:
                value_alias, _ = aliases[(prop.dimension_name, property_name)]
                targets = (
                    []
                    if property_name == funnel_event_property
                    and property_name not in sequence.properties
                    else [property_name]
                )
                if property_name == funnel_event_property:
                    if funnel_event_property_column is None:
                        raise _UNSUPPORTED_SHAPE.error()
                    targets.append(funnel_event_property_column)
                for target in dict.fromkeys(targets):
                    derived[target] = normalized[value_alias]
        if derived:
            normalized = normalized.mutate(**derived)
    normalized = normalized.mutate(unit=normalized["_unit"])

    if configured_id is None:
        event_id_column = "_timestamp"
    else:
        event_id_column = identity_alias
    readiness = model.readiness.for_fact(source.name)
    result = event_sequence(
        normalized,
        anchors,
        window_start=sequence.window.start,
        window_end=sequence.window.end,
        max_events=sequence.max_events,
        representation=sequence.representation,
        properties=sequence.properties,
        internal_properties=internal_property_columns,
        label_property=sequence.label_property,
        unit_column="unit",
        anchor_id_column="anchor_id",
        timestamp_column="_timestamp",
        event_id_column=event_id_column,
        observed_from_column="_observed_from" if "_observed_from" in anchors.columns else None,
        observed_until_column="_observed_until" if "_observed_until" in anchors.columns else None,
        complete_from=readiness.start,
        complete_until=readiness.end_exclusive,
        session_gap_seconds=(
            sequence.sessions.gap_seconds if sequence.sessions is not None else None
        ),
        funnel_event_property=funnel_event_property_column,
        funnel_steps=sequence.funnel.steps if sequence.funnel is not None else None,
        funnel_within_seconds=(
            sequence.funnel.within_seconds if sequence.funnel is not None else None
        ),
    )
    integrity_checks = list(base._anchors._checks)
    if timestamp_check is not None:
        integrity_checks.append(timestamp_check)
    integrity_checks.extend(
        _identity_checks(
            normalized,
            "_unit",
            event_id_column,
            configured=configured_id is not None,
        )
    )
    anchor_table = AnchorTable(base._anchors.table, tuple(integrity_checks))
    checks = (*base._checks, _SequencePreflight(result))
    return CompiledStudy(
        result.table,
        base.audit_table,
        Roles("unit", "anchor_id", "t0", None, result.roles.features, tuple(design.outcomes)),
        result.columns,
        base.feature_cutoffs,
        {},
        {**base.strategy, **{name: "bounded_event_sequence" for name in result.roles.features}},
        anchor_table,
        tuple(checks),
        sequence_meta=SequenceMeta(
            fact=sequence.fact,
            window=sequence.window,
            max_events=sequence.max_events,
            representation=sequence.representation,
            event_fields=(
                ("event_values",) if sequence.representation == "arrays" else sequence.properties
            ),
            event_id_column="event_id" if sequence.representation == "positions" else None,
            event_time_column="event_time" if sequence.representation == "positions" else None,
            position_column="sequence_position" if sequence.representation == "positions" else None,
            event_count_column="event_count",
            complete_column="sequence_complete",
            reason_column="sequence_reason",
            property_reason_columns={
                name: (
                    f"{name}_reason"
                    if sequence.representation == "positions"
                    else f"event_values.{name}_reason"
                )
                for name in sequence.properties
            },
        ),
    )
