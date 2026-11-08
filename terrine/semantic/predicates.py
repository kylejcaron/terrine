"""Comparison and membership predicates with SQL three-valued semantics.

Design comparisons (whose operands are measured expressions) and ``where=``
filters (whose operands are source columns) share one operator vocabulary, the
``op`` of each record, and lower through :func:`lower_predicate`.
"""

from __future__ import annotations

import math
import operator
import re
from collections.abc import Callable
from datetime import date
from fractions import Fraction
from typing import TYPE_CHECKING, Any, cast, overload

import ibis
import ibis.expr.types as ir

from terrine.backends.numeric import (
    coefficient_bounds,
    decimal_coefficient,
    exact_literal,
    float_is_finite,
)
from terrine.semantic.bind import _SCHEMA
from terrine.semantic.models import Filter, FilterOp

if TYPE_CHECKING:
    from terrine.spec.expressions import Comparison, Expr

__all__ = ["lower_predicate"]


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


type _CompareFn = Callable[[object, str], ir.BooleanValue]


def _comparator(value, name: str) -> _CompareFn:
    """Prepare one operand once; the returned function compares it with a literal."""
    dtype = value.type()
    if dtype.is_integer() or dtype.is_decimal():
        scale = dtype.scale if dtype.is_decimal() else 0
        bounds = coefficient_bounds(value)
        coefficient = decimal_coefficient(value, scale)
        return lambda item, op: _exact_comparison(value, coefficient, scale, bounds, item, op, name)
    if dtype.is_floating():
        value = value.cast("float64")
        value = float_is_finite(value).ifelse(value, ibis.null().cast("float64"))
    return lambda item, op: _COMPARISONS[op](value, _filter_literal(value, item, name))


def _single(compare: _CompareFn, values: tuple, op: str) -> ir.BooleanValue:
    return compare(values[0], op)


def _membership(compare: _CompareFn, values: tuple, op: str) -> ir.BooleanValue:
    result = cast(ir.BooleanValue, ibis.literal(False))
    for item in values:
        result |= compare(item, "equals")
    return ~result if op == "not_in" else result


def _between(compare: _CompareFn, values: tuple, op: str) -> ir.BooleanValue:
    return compare(values[0], "gte") & compare(values[1], "lte")


_HANDLERS: dict[FilterOp, Callable[[_CompareFn, tuple, str], ir.BooleanValue]] = {
    "equals": _single,
    "not_equals": _single,
    "gt": _single,
    "gte": _single,
    "lt": _single,
    "lte": _single,
    "in": _membership,
    "not_in": _membership,
    "between": _between,
}


@overload
def lower_predicate(node: Filter, resolve: Callable[[str], ir.Value]) -> ir.BooleanValue: ...
@overload
def lower_predicate(node: Comparison, resolve: Callable[[Expr], ir.Value]) -> ir.BooleanValue: ...
def lower_predicate(
    node: Comparison | Filter, resolve: Callable[[Any], ir.Value]
) -> ir.BooleanValue:
    """Compare without narrowing integer/decimal operands or numeric literals.

    ``resolve`` returns the column an operand names: a source column for a
    filter's property, the compiled value for a Design expression. For an exact
    coefficient z and rational threshold q, z>q iff z>floor(q), z>=q iff
    z>=ceil(q), with the dual rules for < and <=. Fractional q cannot equal an
    integer coefficient. This also preserves SQL null propagation.
    """
    if isinstance(node, Filter):
        value, name = resolve(node.property), node.property
    else:
        value = resolve(node.expr)
        name = value.get_name()
    return _HANDLERS[node.op](_comparator(value, name), node.values, node.op)
