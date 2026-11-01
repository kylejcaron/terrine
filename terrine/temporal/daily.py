"""Sparse local-day sufficient states over normalized, optionally enriched events."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.dialect import current_dialect
from terrine.backends.numeric import (
    count_literal,
    decimal_coefficient,
    exact_subtract,
    exact_sum,
    float_is_finite,
    guard_accumulation,
    quantized_float,
)
from terrine.backends.timestamps import local_day
from terrine.semantic.bind import _MISSING, _SCHEMA
from terrine.semantic.filters import lower_filter
from terrine.semantic.models import BoundMeasure, BoundSource, Filter
from terrine.semantic.rules import (
    UNSUPPORTED_TEMPORAL_AGGREGATION,
    supports_temporal_aggregation,
)
from terrine.semantic.time import boundary_timezone
from terrine.temporal.attributes import _enriched_property_columns
from terrine.temporal.prefix import _UNKNOWN_FIELDS, _require_backend

__all__ = ["sparse_daily"]


def resolve_measure(
    events: ibis.Table,
    measure: BoundMeasure,
    *,
    source: BoundSource,
    allow_nonadditive: bool = False,
) -> BoundMeasure:
    """Resolve an event's dtype, optionally admitting direct nonadditive windows."""
    if measure.source_name != source.name or measure.event not in source.events:
        raise _MISSING.error(reference=f"event {measure.event} on source {source.name}")
    supported_nonadditive = {"min", "max", "count_distinct"}
    if not supports_temporal_aggregation(measure.aggregation) and not (
        allow_nonadditive and measure.aggregation in supported_nonadditive
    ):
        raise UNSUPPORTED_TEMPORAL_AGGREGATION.error(aggregation=measure.aggregation)
    column = source.events[measure.event]
    if column is None:
        if measure.aggregation != "count":
            raise _SCHEMA.error(field=f"numeric event {measure.event}", column=measure.event)
        dtype = dt.int64
    else:
        if column not in events.columns:
            raise _SCHEMA.error(field=f"event {measure.event}", column=column)
        dtype = events[column].type()
        if measure.aggregation != "count" and not (
            dtype.is_integer() or dtype.is_decimal() or dtype.is_floating()
        ):
            raise _SCHEMA.error(field=f"numeric event {measure.event}", column=column)
        if dtype.is_floating() and not (dtype.is_float32() or dtype.is_float64()):
            raise _SCHEMA.error(field=f"binary64 event {measure.event}", column=column)
    value_type = str(dtype)
    if measure.value_type is not None and dt.dtype(measure.value_type) != dtype:
        raise _SCHEMA.error(
            field=f"event {measure.event} type drift", column=column or measure.event
        )
    return measure if measure.value_type is not None else replace(measure, value_type=value_type)


def _raw_columns(source: BoundSource) -> list[str]:
    # Match normalize_events' physical projection, not the post-enrichment aliases.
    return list(
        dict.fromkeys(
            [
                "_timestamp",
                "_unit",
                *source.units.values(),
                *(column for column in source.events.values() if column is not None),
                *(
                    prop.column
                    for prop in source.properties.values()
                    if prop.dimension_name is None
                ),
            ]
        )
    )


def _filter_property(events: ibis.Table, source: BoundSource, name: str, aliases):
    prop = source.properties.get(name)
    if prop is None:
        for dimension in source.dimensions:
            if name in dimension.properties:
                prop = dimension.properties[name]
                break
    if prop is None:
        raise _MISSING.error(reference=f"filter property {name} on source {source.name}")
    if prop.dimension_name is None:
        value_name, found = prop.column, ibis.literal(True)
    else:
        key = (prop.dimension_name, name)
        if key not in aliases:
            raise _MISSING.error(reference=f"dimension property {prop.dimension_name}:{name}")
        value_name, found_name = aliases[key]
        if found_name not in events.columns:
            raise _SCHEMA.error(field=f"enriched filter {name}", column=found_name)
        found = events[found_name].fill_null(False)
    if value_name not in events.columns:
        raise _SCHEMA.error(field=f"filter {name}", column=value_name)
    value = events[value_name]
    return value, found


def _predicate(events: ibis.Table, measure: BoundMeasure, source: BoundSource):
    """Explicit SQL three-valued conjunction, including typed NULL literals."""
    predicate = ibis.literal(True)
    observed_null = missing = ibis.literal(False)
    literal_null = nonfinite = ibis.literal(False)
    aliases = _enriched_property_columns(source.dimensions, _raw_columns(source))
    for filt in measure.filters:
        value, found = _filter_property(events, source, filt.property_name, aliases)
        comparison = lower_filter(
            events, Filter(property=filt.property_name, op=filt.op, values=filt.values), value
        )
        # Missing dimension history is unknown even for an accidental nonnull placeholder.
        comparison = found.ifelse(comparison, ibis.null().cast("boolean"))
        predicate &= comparison
        observed_null |= found & value.isnull()
        missing |= ~found
        if value.type().is_floating():
            nonfinite |= found & value.notnull() & ~float_is_finite(value.cast("float64"))
        if any(item is None for item in filt.values):
            literal_null |= comparison.isnull()
    return predicate, observed_null, missing, literal_null, nonfinite


def _sparse_group(
    events: ibis.Table,
    measure: BoundMeasure,
    keys: Mapping[str, Any],
    unit_column: str,
    *,
    source: BoundSource,
) -> ibis.Table:
    """Shared filter/arithmetic grouping for local days and equal event timestamps.

    Event-resolution callers pass {"_timestamp": events._timestamp}. This groups
    duplicates before prefix accumulation, without mixing another entity key
    into the selected analysis unit. No exposure is inferred from Property.as_of.
    """
    measure = resolve_measure(events, measure, source=source)
    if unit_column not in source.units.values() and unit_column != "_unit":
        raise _MISSING.error(reference=f"selected unit column {unit_column}")
    if unit_column not in events.columns:
        raise _SCHEMA.error(field="selected unit", column=unit_column)
    assert measure.value_type is not None  # resolve_measure always binds it
    dtype = dt.dtype(measure.value_type)
    # Floats quantize once at scale 9, then share declared-decimal coefficients.
    exact_floats = dtype.is_floating() and measure.aggregation != "count"
    _require_backend(events)
    predicate, observed_null, missing, literal_null, nonfinite = _predicate(events, measure, source)
    selected = predicate.fill_null(False)
    unknown = predicate.isnull()
    column = source.events[measure.event]
    value = ibis.literal(1) if column is None else events[column]
    nonnull = selected & value.notnull()
    one, zero = count_literal(1), count_literal(0)
    fields = {
        "event_count": selected.ifelse(one, zero),
        "nonnull_count": nonnull.ifelse(one, zero),
    }
    if measure.filters:
        fields.update(
            zip(
                _UNKNOWN_FIELDS,
                (
                    unknown.ifelse(one, zero),
                    (unknown & observed_null).ifelse(one, zero),
                    (unknown & missing).ifelse(one, zero),
                    (unknown & literal_null).ifelse(one, zero),
                    (unknown & nonfinite).ifelse(one, zero),
                ),
                strict=True,
            )
        )
    # A single projection shares predicates and original leaves across all reductions.
    leaf = events.select(
        **{"_unit": events[unit_column], **keys, **fields},
        __terrine_selected=selected,
        __terrine_possible=predicate.fill_null(True),
        __terrine_value=value,
    )
    leaf = leaf.filter(leaf.__terrine_possible)
    aggregates = {name: exact_sum(leaf[name]) for name in fields}
    if measure.aggregation == "count":
        aggregates["sum_value"] = exact_sum(leaf.event_count)
    else:
        source_value = leaf.__terrine_value
        if exact_floats:
            quantized, nonfinite_value, unrepresentable = quantized_float(
                cast(ir.FloatingValue, source_value)
            )
            exact = decimal_coefficient(quantized, 9)
            selected_value = leaf.__terrine_selected & source_value.notnull()
            aggregates.update(
                float64_nonfinite_count=exact_sum(
                    (selected_value & nonfinite_value).ifelse(one, zero)
                ),
                float64_unrepresentable_count=exact_sum(
                    (selected_value & unrepresentable).ifelse(one, zero)
                ),
            )
        else:
            scale = dtype.scale if isinstance(dtype, dt.Decimal) else 0
            exact = decimal_coefficient(source_value, scale or 0)
        coefficient = leaf.__terrine_selected.ifelse(exact, ibis.null().cast(exact.type()))
        # A fixed-precision policy withholds a group's coefficients before the
        # backend adds them when their magnitudes may leave the domain, and
        # reports the withheld rows so later prefixes refuse instead of
        # reading an incomplete state.
        leaf, coefficient, admitted = guard_accumulation(
            leaf, coefficient, partition=["_unit", *keys]
        )
        aggregates["sum_value"] = exact_sum(coefficient, admitted=admitted is not None)
        if admitted is not None:
            aggregates["sum_bound"] = current_dialect(coefficient).magnitude(coefficient).sum()
            aggregates["sum_withheld"] = exact_sum((~admitted).ifelse(one, zero))
    grouped = leaf.group_by("_unit", *keys).aggregate(**aggregates)
    return grouped.mutate(
        value_type=ibis.literal(measure.value_type),
        value_scale=ibis.literal(
            9
            if exact_floats
            else (
                dtype.scale
                if isinstance(dtype, dt.Decimal) and measure.aggregation != "count"
                else 0
            )
        ),
        numeric_policy=ibis.literal("exact"),
        observed_null_count=exact_subtract(grouped.event_count, grouped.nonnull_count),
    )


def sparse_daily(
    events: ibis.Table,
    measure: BoundMeasure,
    boundary: str,
    unit_column: str,
    *,
    source: BoundSource,
) -> ibis.Table:
    """Group observed events by selected unit and fixed-offset local day, lazily.

    _timestamp is the local bucket's start expressed as an aware UTC instant.
    Daily prefixes are for aligned endpoints only; arbitrary endpoints require
    equal-timestamp states. Naive source timestamps denote UTC, not session time.
    """
    if "_timestamp" not in events.columns:
        raise _SCHEMA.error(field="normalized event", column="_timestamp")
    offset = boundary_timezone(boundary).utcoffset(None)
    minutes = (offset.days * 86400 + offset.seconds) // 60
    timestamp = events._timestamp
    if not timestamp.type().is_timestamp():
        raise _SCHEMA.error(field="normalized event", column="_timestamp")
    day, bucket = local_day(cast(ir.TimestampValue, timestamp), minutes)
    return _sparse_group(
        events, measure, {"local_day": day, "_timestamp": bucket}, unit_column, source=source
    )
