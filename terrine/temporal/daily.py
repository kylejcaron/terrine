"""Sparse local-day sufficient states over normalized, optionally enriched events."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from typing import Any, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.types as ir
from ibis.common.exceptions import IbisError
from ibis.common.graph import Graph

from terrine.backends.dialect import current_dialect
from terrine.backends.numeric import (
    below_resolution,
    count_literal,
    decimal_coefficient,
    exact_subtract,
    exact_sum,
    float_is_finite,
    guard_accumulation,
    quantized_float,
)
from terrine.backends.timestamps import local_day
from terrine.errors import RefusalSpec
from terrine.semantic.bind import _MISSING, _SCHEMA
from terrine.semantic.models import BoundMeasure, BoundSource, Filter
from terrine.semantic.predicates import lower_predicate
from terrine.semantic.rules import (
    UNSUPPORTED_TEMPORAL_AGGREGATION,
    supports_temporal_aggregation,
)
from terrine.semantic.time import boundary_timezone
from terrine.temporal.attributes import _enriched_property_columns

__all__ = ["native_arithmetic", "sparse_daily"]

_EXACT_UNSUPPORTED = RefusalSpec(
    "terrine.temporal.exact_unsupported",
    "Backend {backend!r} lacks exact sparse BIGNUM state (minimum DuckDB {minimum}). "
    "Use DuckDB 1.5 or later.",
)
_UNKNOWN_FIELDS = (
    "unknown_filter_count",
    "unknown_filter_null_count",
    "unknown_filter_missing_count",
    "unknown_filter_literal_count",
    "unknown_filter_nonfinite_count",
)
_EXACT_FLOAT_COUNTS = (
    "float64_nonfinite_count",
    "float64_unrepresentable_count",
    "float64_below_resolution_count",
)
_NATIVE_FLOAT_COUNTS = ("float64_nonfinite_count",)


def _require_backend(table: ibis.Table) -> None:
    """Check static backend/build metadata without running a capability query."""
    try:
        backend = table._find_backend(use_default=False).name
    except IbisError:
        backend = "unbound"
    policy = current_dialect(*(table[name] for name in table.columns))
    if not policy.supports_exact_prefix(backend):
        raise _EXACT_UNSUPPORTED.error(backend=backend, minimum="1.5")


def native_arithmetic(measure: BoundMeasure) -> bool:
    """Whether a resolved non-count measure uses native binary64 arithmetic."""
    return (
        not measure.exact
        and measure.aggregation != "count"
        and measure.value_type is not None
        and dt.dtype(measure.value_type).is_floating()
    )


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
        if measure.aggregation == "count_distinct":
            supported = (
                dtype.is_integer()
                or dtype.is_decimal()
                or dtype.is_floating()
                or dtype.is_string()
                or dtype.is_date()
                or dtype.is_boolean()
            )
        else:
            supported = dtype.is_integer() or dtype.is_decimal() or dtype.is_floating()
        if measure.aggregation != "count" and not supported:
            field = (
                f"distinct-count event column {column}"
                if measure.aggregation == "count_distinct"
                else f"numeric event {measure.event}"
            )
            raise _SCHEMA.error(field=field, column=column)
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
        comparison = lower_predicate(
            Filter(property=filt.property_name, op=filt.op, values=filt.values),
            {filt.property_name: value}.__getitem__,
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


def _batch_projections(base: ibis.Table, branches: Sequence[ibis.Table]) -> ibis.Table:
    """Stage independent temporal projections by named-column dependency depth.

    Every branch contains only projections above ``base`` and assigns fresh
    names. Keep expressions behind their column boundaries rather than inlining
    them, so selection still precedes conversion and invalid-input checks.
    """
    root = base.op()
    fields = {ops.Field(root, name): name for name in base.columns}
    depths = dict.fromkeys(base.columns, 0)
    layers: dict[int, dict[str, tuple[Any, dict[Any, str]]]] = {}

    def scalar(node):
        return not isinstance(node, ops.Relation)

    for branch in branches:
        chain = []
        relation = branch.op()
        while relation != root:
            if not isinstance(relation, ops.Project):
                raise ValueError("temporal projection branches must share their base relation")
            chain.append(relation)
            relation = relation.parent
        for relation in reversed(chain):
            for name, value in relation.values.items():
                if isinstance(value, ops.Field) and fields[value] == name:
                    fields[ops.Field(relation, name)] = name
                    continue
                reads = {
                    node: fields[node]
                    for node in Graph.from_bfs(value, filter=scalar).nodes()
                    if isinstance(node, ops.Field)
                }
                if name in depths:
                    raise ValueError("temporal projection branches must assign fresh names")
                depth = 1 + max((depths[column] for column in reads.values()), default=0)
                layers.setdefault(depth, {})[name] = (value, reads)
                depths[name] = depth
                fields[ops.Field(relation, name)] = name
    table = base
    for depth in sorted(layers):
        parent = table.op()
        values = {}
        for name, (value, reads) in layers[depth].items():
            replacements = {field: ops.Field(parent, column) for field, column in reads.items()}
            values[name] = value.replace(replacements, filter=scalar).to_expr()
        table = table.mutate(**values)
    return table


def contributions(
    events: ibis.Table,
    measure: BoundMeasure,
    *,
    source: BoundSource,
    consumed: ir.BooleanValue | None = None,
    counting: bool = False,
    _name: Callable[[str], str] | None = None,
) -> tuple[ibis.Table, dict[str, ir.Value]]:
    """Per-event sufficient primitives, selected before accumulation.

    Unknown predicates contribute provenance, not values. Exact floating inputs
    quantize individually; native inputs have no magnitude certificate.
    """
    table = events
    used = {name.casefold() for name in events.columns}

    def project(label: str, expression: ir.Value) -> ir.Value:
        nonlocal table
        name = _name(label) if _name is not None else f"__terrine_contribution_{label}"
        while name.casefold() in used:
            name += "_"
        used.add(name.casefold())
        table = table.mutate(**{name: expression})
        return table[name]

    predicate, observed_null, missing, literal_null, nonfinite = _predicate(events, measure, source)
    predicate = cast(ir.BooleanValue, project("predicate", predicate))
    active = cast(
        ir.BooleanValue,
        project(
            "consumed",
            ibis.literal(True) if consumed is None else consumed.fill_null(ibis.literal(False)),
        ),
    )
    selected = cast(
        ir.BooleanValue, project("selected", active & predicate.fill_null(ibis.literal(False)))
    )
    unknown = active & predicate.isnull()
    one, zero = count_literal(1), count_literal(0)
    fields: dict[str, ir.Value] = {"event_count": selected.ifelse(one, zero)}
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
    if counting:
        return table, fields
    column = source.events[measure.event]
    value = ibis.literal(1) if column is None else table[column]
    fields["nonnull_count"] = (selected & value.notnull()).ifelse(one, zero)
    if measure.aggregation == "count":
        fields["sum_value"] = fields["event_count"]
        return table, fields
    assert measure.value_type is not None
    dtype = dt.dtype(measure.value_type)
    if dtype.is_floating():
        value = project("value", selected.ifelse(value, ibis.null(value.type())))
    if native_arithmetic(measure):
        floating = value.cast(dt.float64)
        finite = float_is_finite(floating)
        fields["float64_nonfinite_count"] = (floating.notnull() & ~finite).ifelse(one, zero)
        fields["sum_value"] = finite.ifelse(floating, ibis.null(dt.float64))
    elif dtype.is_floating():
        floating = cast(ir.FloatingValue, value)
        quantized, invalid, unrepresentable = quantized_float(floating)
        coefficient = project("coefficient", decimal_coefficient(quantized, 9))
        fields.update(
            sum_value=coefficient,
            float64_nonfinite_count=invalid.ifelse(one, zero),
            float64_unrepresentable_count=unrepresentable.ifelse(one, zero),
            float64_below_resolution_count=below_resolution(floating, coefficient).ifelse(
                one, zero
            ),
        )
    else:
        scale = dtype.scale if isinstance(dtype, dt.Decimal) else 0
        # A native integer may have a narrower declared decimal alias. Convert
        # directly to its exact coefficient before a typed NULL can narrow it.
        coefficient = decimal_coefficient(value, scale or 0)
        fields["sum_value"] = selected.ifelse(coefficient, ibis.null(coefficient.type()))
    return table, fields


def reduce_payload(
    table: ibis.Table,
    keys: Sequence[str],
    *,
    native_values: Sequence[str],
    exact_values: Sequence[str],
) -> tuple[ibis.Table, dict[str, str]]:
    """Accumulate a selected payload, withholding bounded exact sums before SUM."""
    payload = [name for name in table.columns if name not in keys]
    withheld = {}
    bounded = set()
    for name in exact_values:
        table, value, admitted = guard_accumulation(table, table[name], partition=keys)
        if admitted is not None:
            flag = f"{name}_withheld"
            while flag in table.columns:
                flag += "_"
            table = table.mutate(
                **{name: value, flag: (~admitted).ifelse(count_literal(1), count_literal(0))}
            )
            withheld[name] = flag
            payload.append(flag)
            bounded.add(name)
    aggregates = {
        name: (
            cast(ir.FloatingColumn, table[name]).sum()
            if name in native_values
            else exact_sum(table[name], admitted=name in bounded)
        )
        for name in payload
    }
    return table.group_by(*keys).aggregate(**aggregates), withheld


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
    A native measure sums its finite float values in binary64 and counts the
    non-finite ones; other measures accumulate exact coefficients.
    """
    measure = resolve_measure(events, measure, source=source)
    if unit_column not in source.units.values() and unit_column != "_unit":
        raise _MISSING.error(reference=f"selected unit column {unit_column}")
    if unit_column not in events.columns:
        raise _SCHEMA.error(field="selected unit", column=unit_column)
    assert measure.value_type is not None  # resolve_measure always binds it
    dtype = dt.dtype(measure.value_type)
    native = native_arithmetic(measure)
    # Exact floats quantize once at scale 9, then share declared-decimal coefficients.
    exact_floats = dtype.is_floating() and measure.aggregation != "count" and not native
    if not native:
        _require_backend(events)
    if measure.filters:
        predicate = _predicate(events, measure, source)[0]
        events = events.filter(predicate.fill_null(True))
    events, fields = contributions(events, measure, source=source)
    leaf = events.select(**{"_unit": events[unit_column], **keys, **fields})
    value = leaf.sum_value
    if measure.aggregation == "count":
        admitted = None
    else:
        leaf, value, admitted = guard_accumulation(leaf, value, partition=["_unit", *keys])
    aggregates = {name: exact_sum(leaf[name]) for name in fields if name != "sum_value"}
    aggregates["sum_value"] = (
        cast(ir.FloatingColumn, value).sum()
        if native
        else exact_sum(value, admitted=admitted is not None)
    )
    if admitted is not None:
        policy = current_dialect(value)
        magnitude = policy.float_magnitude if native else policy.magnitude
        aggregates["sum_bound"] = magnitude(value).sum()
        aggregates["sum_withheld"] = exact_sum(
            (~admitted).ifelse(count_literal(1), count_literal(0))
        )
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
        numeric_policy=ibis.literal("native" if native else "exact"),
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
