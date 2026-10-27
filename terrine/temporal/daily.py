"""Sparse local-day sufficient states over normalized, optionally enriched events."""

from __future__ import annotations

import math
import operator
import re
from collections.abc import Mapping
from dataclasses import replace
from datetime import date
from fractions import Fraction
from typing import Any, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.dialect import current_dialect
from terrine.backends.numeric import (
    coefficient_bounds,
    decimal_coefficient,
    exact_float,
    exact_literal,
    exact_subtract,
    exact_sum,
    float_is_finite,
    float_scale,
)
from terrine.backends.timestamps import utc_instant, utc_wall_clock
from terrine.semantic.bind import _MISSING, _SCHEMA
from terrine.semantic.models import BoundMeasure, BoundSource
from terrine.semantic.rules import (
    UNSUPPORTED_TEMPORAL_AGGREGATION,
    supports_temporal_aggregation,
)
from terrine.semantic.time import boundary_timezone
from terrine.temporal.attributes import _enriched_property_columns
from terrine.temporal.prefix import _require_backend

__all__ = ["sparse_daily"]

_UNKNOWN_FIELDS = (
    "unknown_filter_count",
    "unknown_filter_null_count",
    "unknown_filter_missing_count",
    "unknown_filter_literal_count",
    "unknown_filter_nonfinite_count",
)


def resolve_measure(
    events: ibis.Table,
    measure: BoundMeasure,
    *,
    source: BoundSource,
    allow_nonadditive: bool = False,
) -> BoundMeasure:
    """Resolve a fact's dtype, optionally admitting direct nonadditive windows."""
    if measure.source_name != source.name or measure.fact not in source.facts:
        raise _MISSING.error(reference=f"fact {measure.fact} on source {source.name}")
    supported_nonadditive = {"min", "max", "count_distinct"}
    if not supports_temporal_aggregation(measure.aggregation) and not (
        allow_nonadditive and measure.aggregation in supported_nonadditive
    ):
        raise UNSUPPORTED_TEMPORAL_AGGREGATION.error(aggregation=measure.aggregation)
    column = source.facts[measure.fact]
    if column is None:
        if measure.aggregation != "count":
            raise _SCHEMA.error(field=f"numeric fact {measure.fact}", column=measure.fact)
        dtype = dt.int64
    else:
        if column not in events.columns:
            raise _SCHEMA.error(field=f"fact {measure.fact}", column=column)
        dtype = events[column].type()
        if measure.aggregation != "count" and not (
            dtype.is_integer() or dtype.is_decimal() or dtype.is_floating()
        ):
            raise _SCHEMA.error(field=f"numeric fact {measure.fact}", column=column)
        if dtype.is_floating() and not (dtype.is_float32() or dtype.is_float64()):
            raise _SCHEMA.error(field=f"binary64 fact {measure.fact}", column=column)
    value_type = str(dtype)
    if measure.value_type is not None and dt.dtype(measure.value_type) != dtype:
        raise _SCHEMA.error(field=f"fact {measure.fact} type drift", column=column or measure.fact)
    return measure if measure.value_type is not None else replace(measure, value_type=value_type)


def _raw_columns(source: BoundSource) -> list[str]:
    # Match normalize_events' physical projection, not the post-enrichment aliases.
    return list(
        dict.fromkeys(
            [
                "_timestamp",
                "_unit",
                *source.entities.values(),
                *(column for column in source.facts.values() if column is not None),
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


_COMPARISONS = {
    "equals": operator.eq,
    "not_equals": operator.ne,
    "gt": operator.gt,
    "gte": operator.ge,
    "lt": operator.lt,
    "lte": operator.le,
}
_JSON_NUMBER = re.compile(r"(-?)(0|[1-9][0-9]*)(?:\.([0-9]+))?(?:[eE]([+-]?)([0-9]+))?")


def _clip_threshold(floor, ceil, integral, bounds):
    lower, upper = bounds
    return (
        min(max(floor, lower - 1), upper + 1),
        min(max(ceil, lower - 1), upper + 1),
        integral,
    )


def _decimal_threshold(spelling, scale, bounds, name):
    """Lower a JSON decimal spelling using bounded integer parts only.

    Only floor/ceil within the physical coefficient domain matter. Exponents
    beyond that domain are handled symbolically, without constructing enormous
    integer powers or a Fraction denominator. Bounds derive from schema, not
    a user limit on literal size or precision.
    """
    match = _JSON_NUMBER.fullmatch(spelling)
    if match is None:
        raise _SCHEMA.error(field=f"JSON-number filter {name}", column=name)
    sign, integer, fraction, exponent_sign, exponent_digits = match.groups()
    fraction = fraction or ""
    digits = (integer + fraction).lstrip("0")
    if not digits:
        return 0, 0, True
    significant = digits.rstrip("0")
    lower, upper = bounds
    max_digits = len(str(max(abs(lower), abs(upper))))
    limit = len(digits) + len(fraction) + scale + max_digits + 2
    exponent_text = (exponent_digits or "0").lstrip("0") or "0"
    limit_text = str(limit)
    if (len(exponent_text), exponent_text) > (len(limit_text), limit_text):
        exponent = limit + 1
    else:
        exponent = int(exponent_text)
    if exponent_sign == "-":
        exponent = -exponent
    position = len(digits) + exponent - len(fraction) + scale
    if position > max_digits:
        outside = lower - 1 if sign else upper + 1
        return outside, outside, True
    if position <= 0:
        floor, ceil, integral = 0, 1, False
    elif position < len(significant):
        floor = int(significant[:position])
        ceil, integral = floor + 1, False
    else:
        floor = int(significant) * 10 ** (position - len(significant))
        ceil, integral = floor, True
    if sign:
        floor, ceil = -ceil, -floor
    return _clip_threshold(floor, ceil, integral, bounds)


def _numeric_threshold(item, scale, bounds, name):
    if isinstance(item, str):
        return _decimal_threshold(item, scale, bounds, name)
    if isinstance(item, bool) or not isinstance(item, (int, float)):
        raise _SCHEMA.error(field=f"numeric filter {name}", column=name)
    if isinstance(item, float) and not math.isfinite(item):
        raise _SCHEMA.error(field=f"finite filter {name}", column=name)
    lower, upper = bounds
    # Avoid copying a potentially very large user integer merely to scale it.
    if item < lower:
        return lower - 1, lower - 1, True
    if item > upper:
        return upper + 1, upper + 1, True
    rational = Fraction(item) * 10**scale
    floor = rational.numerator // rational.denominator
    ceil = -(-rational.numerator // rational.denominator)
    return _clip_threshold(floor, ceil, rational.denominator == 1, bounds)


# Every coefficient lies below a threshold above the column's range, and above
# one below it; equality never holds either way.
_ABOVE_RANGE = {"gt": False, "gte": False, "lt": True, "lte": True}


def _known(value, result: bool):
    """A comparison settled without a literal, still NULL for a NULL value."""
    return value.isnull().ifelse(ibis.null().cast("boolean"), result)


def _exact_comparison(value, coefficient, scale, bounds, item, op, name):
    """Lower an exact coefficient/rational comparison without native division.

    A threshold outside the column's physical range settles the comparison
    without a literal, which the exact type might not hold (Snowflake's
    NUMBER(38, 0) has no 39th digit for one past a 38-digit column). Literals
    and typed NULLs take the coefficient's own type, so a scale-0 coefficient
    is never promoted into a narrower fixed-scale state.
    """
    if item is None:
        return _COMPARISONS[op](coefficient, ibis.null().cast(coefficient.type()))
    floor, ceil, integral = _numeric_threshold(item, scale, bounds, name)
    if op in {"equals", "not_equals"} and not integral:
        return _known(value, op == "not_equals")
    threshold = ceil if op in {"gte", "lt"} else floor
    lower, upper = bounds
    if threshold > upper or threshold < lower:
        if op in {"equals", "not_equals"}:
            return _known(value, op == "not_equals")
        return _known(value, _ABOVE_RANGE[op] if threshold > upper else not _ABOVE_RANGE[op])
    return _COMPARISONS[op](coefficient, exact_literal(threshold, coefficient))


def _filter_literal(value, item, name):
    dtype = value.type()
    if item is None:
        return ibis.null().cast(dtype)
    if dtype.is_floating():
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise _SCHEMA.error(field=f"numeric filter {name}", column=name)
        try:
            converted = float(item)
        except OverflowError:
            raise _SCHEMA.error(field=f"lossless float filter {name}", column=name) from None
        if not math.isfinite(converted) or converted != item:
            raise _SCHEMA.error(field=f"lossless float filter {name}", column=name)
        return ibis.literal(converted)
    if dtype.is_date() and isinstance(item, str):
        try:
            return ibis.literal(date.fromisoformat(item))
        except ValueError:
            raise _SCHEMA.error(field=f"date filter {name}", column=name) from None
    if (dtype.is_string() and isinstance(item, str)) or (
        dtype.is_boolean() and isinstance(item, bool)
    ):
        return ibis.literal(item)
    raise _SCHEMA.error(field=f"typed filter {name}", column=name)


def _filter_comparison(value, filt):
    """Compare without narrowing integer/decimal properties or numeric literals.

    For an exact coefficient z and rational threshold q, z>q iff z>floor(q),
    z>=q iff z>=ceil(q), with the dual rules for < and <=. Fractional q cannot
    equal an integer coefficient. This also preserves SQL null propagation.
    """
    dtype = value.type()
    exact = dtype.is_integer() or dtype.is_decimal()
    if exact:
        scale = dtype.scale if dtype.is_decimal() else 0
        bounds = coefficient_bounds(value)
        coefficient = decimal_coefficient(value, scale)
    elif dtype.is_floating():
        value = value.cast("float64")
        value = float_is_finite(value).ifelse(value, ibis.null().cast("float64"))

    def compare(item, op):
        if exact:
            return _exact_comparison(
                value, coefficient, scale, bounds, item, op, filt.property_name
            )
        return _COMPARISONS[op](value, _filter_literal(value, item, filt.property_name))

    if filt.op in {"in", "not_in"}:
        result = cast(ir.BooleanValue, ibis.literal(False))
        for item in filt.values:
            result |= compare(item, "equals")
        return ~result if filt.op == "not_in" else result
    if filt.op == "between":
        return compare(filt.values[0], "gte") & compare(filt.values[1], "lte")
    return compare(filt.values[0], filt.op)


def _predicate(events: ibis.Table, measure: BoundMeasure, source: BoundSource):
    """Explicit SQL three-valued conjunction, including typed NULL literals."""
    predicate = ibis.literal(True)
    observed_null = missing = ibis.literal(False)
    literal_null = nonfinite = ibis.literal(False)
    aliases = _enriched_property_columns(source.dimensions, _raw_columns(source))
    for filt in measure.filters:
        value, found = _filter_property(events, source, filt.property_name, aliases)
        comparison = _filter_comparison(value, filt)
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
    if unit_column not in source.entities.values() and unit_column != "_unit":
        raise _MISSING.error(reference=f"selected unit column {unit_column}")
    if unit_column not in events.columns:
        raise _SCHEMA.error(field="selected unit", column=unit_column)
    assert measure.value_type is not None  # resolve_measure always binds it
    dtype = dt.dtype(measure.value_type)
    # A float fact accumulates exactly from each value's shortest round-trip
    # decimal at its unit's scale: window sums cancel history exactly and round
    # once at output. DuckDB derives the scale from the unit's values; the exact
    # types of PostgreSQL and BigQuery hold the text unscaled.
    exact_floats = dtype.is_floating() and measure.aggregation != "count"
    _require_backend(events)
    predicate, observed_null, missing, literal_null, nonfinite = _predicate(events, measure, source)
    selected = predicate.fill_null(False)
    unknown = predicate.isnull()
    column = source.facts[measure.fact]
    value = ibis.literal(1) if column is None else events[column]
    nonnull = selected & value.notnull()
    fields = {
        "event_count": selected.ifelse(exact_literal(1), exact_literal(0)),
        "nonnull_count": nonnull.ifelse(exact_literal(1), exact_literal(0)),
    }
    if measure.filters:
        fields.update(
            zip(
                _UNKNOWN_FIELDS,
                (
                    unknown.ifelse(exact_literal(1), exact_literal(0)),
                    (unknown & observed_null).ifelse(exact_literal(1), exact_literal(0)),
                    (unknown & missing).ifelse(exact_literal(1), exact_literal(0)),
                    (unknown & literal_null).ifelse(exact_literal(1), exact_literal(0)),
                    (unknown & nonfinite).ifelse(exact_literal(1), exact_literal(0)),
                ),
                strict=True,
            )
        )
    unit_scale = {}
    if exact_floats:
        # The scale spans the unit's whole selected history, so every group of
        # the unit holds coefficients that its prefix can add exactly.
        unit_scale["__terrine_scale"] = float_scale(
            selected.ifelse(value, ibis.null().cast(value.type())), [events[unit_column]]
        )
    # A single projection shares predicates and original leaves across all reductions.
    leaf = events.select(
        **{"_unit": events[unit_column], **keys, **fields, **unit_scale},
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
            exact, nonfinite_value, unrepresentable = exact_float(
                source_value, leaf.__terrine_scale
            )
            selected_value = leaf.__terrine_selected & source_value.notnull()
            aggregates.update(
                exact_scale=leaf.__terrine_scale.max(),
                float64_nonfinite_count=exact_sum(
                    (selected_value & nonfinite_value).ifelse(exact_literal(1), exact_literal(0))
                ),
                float64_unrepresentable_count=exact_sum(
                    (selected_value & unrepresentable).ifelse(exact_literal(1), exact_literal(0))
                ),
            )
        else:
            scale = dtype.scale if isinstance(dtype, dt.Decimal) else 0
            exact = decimal_coefficient(source_value, scale or 0)
        coefficient = leaf.__terrine_selected.ifelse(exact, ibis.null().cast(exact.type()))
        aggregates["sum_value"] = exact_sum(coefficient)
        policy = current_dialect(coefficient)
        if policy.bounds_accumulations:
            # Bounds every partial sum, so an accumulation leaving the fixed
            # domain is refused instead of silently narrowed or skipped.
            aggregates["sum_bound"] = policy.magnitude(coefficient).sum()
    grouped = leaf.group_by("_unit", *keys).aggregate(**aggregates)
    constants = {
        "value_type": ibis.literal(measure.value_type),
        "value_scale": ibis.literal(
            dtype.scale if isinstance(dtype, dt.Decimal) and measure.aggregation != "count" else 0
        ),
        "numeric_policy": ibis.literal("exact"),
        "observed_null_count": exact_subtract(grouped.event_count, grouped.nonnull_count),
    }
    if not measure.filters:
        constants.update({name: exact_literal(0) for name in _UNKNOWN_FIELDS})
    return grouped.mutate(**constants)


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
    utc = cast(
        ir.TimestampValue,
        utc_wall_clock(timestamp) if timestamp.type().timezone else timestamp,
    )
    dtype = utc.type()
    if isinstance(dtype, dt.Timestamp) and dtype.scale is not None and dtype.scale > 6:
        # Only this bucket projection needs microseconds: every fixed-offset
        # midnight is aligned. Keep original event instants unchanged.
        micros = cast(ir.TimestampValue, utc.cast("timestamp"))
        utc = cast(
            ir.TimestampValue,
            (utc < micros).ifelse(micros - ibis.interval(microseconds=1), micros),
        )
    day = utc.date()
    midnight = cast(ir.TimestampValue, day.cast("timestamp"))
    if minutes < 0:
        day = (utc < midnight + ibis.interval(minutes=-minutes)).ifelse(
            day - ibis.interval(days=1), day
        )
    elif minutes > 0:
        day = (utc >= midnight + ibis.interval(minutes=1440 - minutes)).ifelse(
            day + ibis.interval(days=1), day
        )
    day = day.cast("date")
    bucket = utc_instant(
        cast(ir.TimestampValue, day.cast("timestamp")) - ibis.interval(minutes=minutes)
    )
    return _sparse_group(
        events, measure, {"local_day": day, "_timestamp": bucket}, unit_column, source=source
    )
