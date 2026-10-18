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
    _unique_checks,
)
from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.data import Roles
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
    if configured:
        return _unique_checks(events, identity, "event_id", scope=(unit,))
    ties = (
        events.select(_unit=events[unit], _timestamp=events["_timestamp"])
        .group_by("_unit", "_timestamp")
        .aggregate(_count=ibis._.count())
        .filter(ibis._._count > 1)
        .aggregate(invalid_count=ibis._.count())
    )
    return [
        _null_check(events, unit, "null_event_unit"),
        _null_check(events, "_timestamp", "null_event_timestamp"),
        _integrity_check("tied_event_timestamps", ties),
    ]


def compile_event_sequence(design: Design, model: Model) -> CompiledStudy:
    """Compile a bounded EventSequence through the shared anchor and column paths."""
    sequence = design.shape
    if not isinstance(sequence, EventSequence):
        raise TypeError("design shape must be EventSequence")
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

    internal_properties = tuple(
        name
        for name in (
            sequence.label_property,
            sequence.funnel.event_property if sequence.funnel is not None else None,
        )
        if name is not None and name not in sequence.properties
    )
    for property_name in (*sequence.properties, *internal_properties):
        prop = _property_binding(source, property_name)
        if prop.dimension_name is None:
            normalized = normalized.mutate(**{property_name: normalized[prop.column]})
    dimensions = {
        dimension.name: model._relation(dimension.relation) for dimension in source.dimensions
    }
    if dimensions:
        normalized = enrich_events(normalized, source, dimensions)
        aliases = _enriched_property_columns(source.dimensions, normalized.columns)
        derived = {}
        for property_name in (*sequence.properties, *internal_properties):
            prop = _property_binding(source, property_name)
            if prop.dimension_name is not None:
                value_alias, _ = aliases[(prop.dimension_name, property_name)]
                derived[property_name] = normalized[value_alias]
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
        internal_properties=internal_properties,
        label_property=sequence.label_property,
        unit_column="unit",
        anchor_id_column="anchor_id",
        timestamp_column="_timestamp",
        event_id_column=event_id_column,
        observed_from_column="_observed_from" if "_observed_from" in anchors.columns else None,
        observed_until_column="_observed_until" if "_observed_until" in anchors.columns else None,
        complete_from=readiness.start,
        complete_until=readiness.end_exclusive,
    )
    anchor_table = AnchorTable(
        base._anchors.table,
        base._anchors._checks
        + tuple(
            _identity_checks(
                normalized,
                "_unit",
                event_id_column,
                configured=configured_id is not None,
            )
        ),
    )
    checks = (*base._checks, _SequencePreflight(result))
    columns = dict(base.columns)
    columns.update(result.columns)
    return CompiledStudy(
        result.table,
        base.audit_table,
        Roles("unit", "anchor_id", "t0", None, result.roles.features, tuple(design.outcomes)),
        columns,
        base.feature_cutoffs,
        base.eligibility_cutoffs,
        {**base.strategy, **{name: "bounded_event_sequence" for name in result.roles.features}},
        anchor_table,
        tuple(checks),
    )
