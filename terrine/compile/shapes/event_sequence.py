"""Adapter from typed EventSequence designs to normalized temporal operations."""

from __future__ import annotations

import unicodedata
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
from terrine.compile.columns import (
    _RESERVED_NAME,
    CompiledStudy,
    _HistoryValidation,
    compile_columns,
)
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


def _validate_sequence_column_names(design: Design, sequence: EventSequence) -> None:
    generated = {
        "unit",
        "anchor_id",
        "t0",
        "is_event",
        "sequence_position",
        "event_id",
        "event_time",
        "event_count",
        "sequence_complete",
        "sequence_reason",
        "event_values",
        "funnel_attempts",
        "funnel_attempts_complete",
        "funnel_attempts_reason",
        *sequence.properties,
        *(f"{name}_reason" for name in sequence.properties),
    }
    if sequence.sessions is not None:
        generated.update(
            {
                "session_index",
                "session_event_count",
                "session_start_time",
                "session_end_time",
                "previous_event_id",
                "previous_event_time",
                "transition_reason",
                "transition",
                "transition_from",
                "transition_to",
                "sessions",
                "sessions_complete",
                "sessions_reason",
            }
        )
    canonical = {unicodedata.normalize("NFC", name).casefold() for name in generated}
    for name in (*design.features, *design.outcomes, *design.eligibility):
        if unicodedata.normalize("NFC", name).casefold() in canonical:
            raise _RESERVED_NAME.error(name=name)


def compile_event_sequence(design: Design, model: Model) -> CompiledStudy:
    """Compile a bounded EventSequence through the shared anchor and column paths."""
    sequence = design.shape
    if not isinstance(sequence, EventSequence):
        raise TypeError("design shape must be EventSequence")
    _validate_sequence_column_names(design, sequence)
    design.validate(model)
    # Column expressions remain anchor-grain values; event payloads are joined to them
    # only after sequence selection, using the same compiled anchor relation.
    base_design = Design(
        design.unit,
        design.anchor,
        features=design.features,
        outcomes=design.outcomes,
        eligibility=design.eligibility,
        gap=design.gap,
        anchor_range=design.anchor_range,
    )
    base = compile_columns(base_design, model)
    anchors = base.table.select(
        *(name for name in base._anchors.table.columns if name in base.table.columns)
    )
    fact = model.catalog.facts[sequence.fact]
    source = model.sources[fact.source]
    physical_unit = source.entities[design.unit]
    configured_id = model.event_ids.get(source.name)
    raw = model._relation(source.relation)
    identity_alias = "__terrine_sequence_event_id"
    suffix = 0
    while identity_alias in raw.columns:
        suffix += 1
        identity_alias = f"__terrine_sequence_event_id_{suffix}"
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
    generated_sequence_names = {
        "unit",
        "anchor_id",
        "t0",
        "is_event",
        "sequence_position",
        "event_id",
        "event_time",
        "event_count",
        "sequence_complete",
        "sequence_reason",
        "event_values",
        "funnel_attempts",
        "funnel_attempts_complete",
        "funnel_attempts_reason",
        "sessions",
        "sessions_complete",
        "sessions_reason",
        "session_index",
        "session_event_count",
        "session_start_time",
        "session_end_time",
        "previous_event_id",
        "previous_event_time",
        "transition_reason",
        "transition",
        "transition_from",
        "transition_to",
        "_sequence_unit",
        "_sequence_anchor",
        "_sequence_t0",
        "_sequence_event_id",
        "_sequence_event_time",
        "_funnel_has_missing_history",
        "_funnel_sequence_end",
        "__terrine_funnel_event_property",
    }
    reserved_property_names = (
        set(normalized.columns) | set(sequence.properties) | set(internal_properties)
    )
    property_aliases = {}
    for index, name in enumerate(dict.fromkeys((*sequence.properties, *internal_properties))):
        alias = name
        if name in normalized.columns or name in generated_sequence_names:
            alias = f"__terrine_sequence_property_{index}"
            suffix = 0
            while alias in reserved_property_names:
                suffix += 1
                alias = f"__terrine_sequence_property_{index}_{suffix}"
            reserved_property_names.add(alias)
        property_aliases[name] = alias
    property_columns = {name: property_aliases[name] for name in sequence.properties}
    label_property_column = (
        property_aliases[sequence.label_property] if sequence.label_property is not None else None
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
            internal_property_columns_list.append(property_aliases[name])
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
                and property_name != sequence.label_property
                else [property_aliases[property_name]]
            )
            if property_name == funnel_event_property:
                if funnel_event_property_column is None:
                    raise _UNSUPPORTED_SHAPE.error()
                targets.append(funnel_event_property_column)
            for target in dict.fromkeys(targets):
                direct[target] = normalized[prop.column]
    if direct:
        normalized = normalized.mutate(**direct)
    identity_events = normalized
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
                    and property_name != sequence.label_property
                    else [property_aliases[property_name]]
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
    property_found_columns = {
        name: aliases[(binding.dimension_name, name)][1]
        for name in sequence.properties
        if (binding := _property_binding(source, name)).dimension_name is not None
    }
    funnel_event_property_found_column = None
    if funnel_event_property is not None:
        funnel_binding = _property_binding(source, funnel_event_property)
        if funnel_binding.dimension_name is not None:
            funnel_event_property_found_column = aliases[
                (funnel_binding.dimension_name, funnel_event_property)
            ][1]

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
        property_found_columns=property_found_columns,
        funnel_event_property_found_column=funnel_event_property_found_column,
        property_columns=property_columns,
        label_property_column=label_property_column,
    )
    integrity_checks = list(base._anchors._checks)
    if timestamp_check is not None:
        integrity_checks.append(timestamp_check)
    integrity_checks.extend(
        _identity_checks(
            identity_events,
            "_unit",
            event_id_column,
            configured=configured_id is not None,
        )
    )
    anchor_table = AnchorTable(base._anchors.table, tuple(integrity_checks))
    history_checks = tuple(
        _HistoryValidation(dimensions[dimension.name], dimension) for dimension in source.dimensions
    )
    checks = (*base._checks, *history_checks, _SequencePreflight(result))
    output = result.table
    if any(name not in output.columns for name in base.table.columns):
        base_columns = tuple(name for name in base.table.columns if name not in output.columns)
        output = output.left_join(
            base.table,
            predicates=[
                output.unit == base.table.unit,
                output.anchor_id == base.table.anchor_id,
            ],
        ).select(
            **{name: output[name] for name in output.columns},
            **{name: base.table[name] for name in base_columns},
        )
    return CompiledStudy(
        output,
        base.audit_table,
        Roles(
            "unit",
            "anchor_id",
            "t0",
            None,
            (*result.roles.features, *base.roles.features),
            tuple(base.roles.outcomes),
        ),
        {**result.columns, **base.columns},
        base.feature_cutoffs,
        base.eligibility_cutoffs,
        {
            **base.strategy,
            **{name: "bounded_event_sequence" for name in result.roles.features},
        },
        anchor_table,
        tuple(checks),
        base.expression_timings,
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
            label_property=sequence.label_property,
            session_gap_seconds=(
                sequence.sessions.gap_seconds if sequence.sessions is not None else None
            ),
            session_fields=(
                (
                    "session_index",
                    "session_event_count",
                    "session_start_time",
                    "session_end_time",
                    "sessions",
                    "sessions_complete",
                    *(
                        ("transition_from", "transition_to")
                        if sequence.label_property is not None
                        and sequence.representation == "positions"
                        else ()
                    ),
                )
                if sequence.sessions is not None
                else ()
            ),
            sessions_column="sessions" if sequence.sessions is not None else None,
            sessions_complete_column=(
                "sessions_complete" if sequence.sessions is not None else None
            ),
            sessions_reason_column="sessions_reason" if sequence.sessions is not None else None,
            transition_from_column=(
                (
                    "event_values.transition_from"
                    if sequence.representation == "arrays"
                    else "transition_from"
                )
                if sequence.sessions is not None and sequence.label_property
                else None
            ),
            transition_to_column=(
                (
                    "event_values.transition_to"
                    if sequence.representation == "arrays"
                    else "transition_to"
                )
                if sequence.sessions is not None and sequence.label_property
                else None
            ),
            funnel_event_property=(
                sequence.funnel.event_property if sequence.funnel is not None else None
            ),
            funnel_steps=sequence.funnel.steps if sequence.funnel is not None else (),
            funnel_within_seconds=(
                sequence.funnel.within_seconds if sequence.funnel is not None else None
            ),
            funnel_attempts_column="funnel_attempts" if sequence.funnel is not None else None,
            funnel_attempts_complete_column=(
                "funnel_attempts_complete" if sequence.funnel is not None else None
            ),
            funnel_attempts_reason_column=(
                "funnel_attempts_reason" if sequence.funnel is not None else None
            ),
            funnel_fields=(
                (
                    "start_time",
                    "matched_event_ids",
                    "matched_event_times",
                    "step0_event_id",
                    "deadline",
                    "progress",
                    "status",
                    "reason",
                )
                if sequence.funnel is not None
                else ()
            ),
            identity_kind="configured" if configured_id is not None else "timestamp",
        ),
    )
