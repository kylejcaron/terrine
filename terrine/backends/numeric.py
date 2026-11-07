"""Exact coefficient arithmetic and one-time float quantization.

Float inputs of exact measures round once to DECIMAL(38, 9), then share
declared-decimal coefficients: DuckDB BIGNUM, PostgreSQL NUMERIC, BigQuery
BIGNUMERIC and Snowflake NUMBER(38, 0). Counts retain their native exact domains.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.dialect import Dialect, current_dialect, dialect_for_name
from terrine.backends.extrema import greatest
from terrine.backends.registry import ENGINES, GENERIC
from terrine.errors import RefusalSpec

__all__ = [
    "EXACT_OVERFLOW",
    "coefficient_bounds",
    "count_literal",
    "decimal_coefficient",
    "exact_literal",
    "exact_subtract",
    "exact_sum",
    "exact_to_float",
    "exact_type",
    "float_is_finite",
    "float_ratio",
    "guard_accumulation",
    "is_exact_type",
    "quantized_float",
    "truncating_quotient",
]


EXACT_OVERFLOW = RefusalSpec(
    "terrine.temporal.exact_overflow",
    "The requested exact result cannot be represented by the native output type. "
    "Use a supported exact representation or a semantically sufficient window.",
)


def exact_type(*like: ir.Value) -> dt.DataType:
    return current_dialect(*like).exact_type()


def is_exact_type(dtype: dt.DataType) -> bool:
    """Whether ``dtype`` is an internal exact-state type on any policy."""
    policies = [dialect_for_name(name) for name in (*ENGINES, GENERIC.name)]
    return any(dtype in policy.exact_types() for policy in policies)


def exact_literal(value: int, *like: ir.Value) -> ir.Scalar:
    """An exact integer typed like the first exact ``like`` operand, else as a coefficient.

    Thresholds and comparison zeros take their exact operand's type, keeping
    coefficient state and native integer counts in their respective domains.
    """
    dialect = current_dialect(*like)
    exact = dialect.exact_types()
    dtype = next((item.type() for item in like if item.type() in exact), None)
    return cast(ir.Scalar, ibis.literal(str(value)).cast(dtype or dialect.coefficient_type()))


def count_literal(value: int, *like: ir.Value) -> ir.Scalar:
    """A 0/1 count term or a row-count divisor in the policy's count type."""
    return cast(ir.Scalar, ibis.literal(value).cast(current_dialect(*like).count_type()))


def decimal_coefficient(value: ir.Value, scale: int) -> ir.Value:
    """The exact integer ``value * 10**scale`` of an integer or fixed-scale decimal.

    DuckDB and PostgreSQL print every fractional digit of a fixed-scale decimal,
    so its text without the point is the coefficient; BigQuery prints NUMERIC
    1.50 as ``1.5``, so it multiplies instead.
    """
    return current_dialect(value).decimal_coefficient(value, scale)


def coefficient_bounds(value: ir.Value) -> tuple[int, int]:
    """The physical range of an exact column's integer coefficient (its value
    times ``10**scale``).

    Integer types are fixed-width; a ``decimal(p, s)`` holds ``p`` digits, except
    that DuckDB reports its wider HUGEINT as ``decimal(38, 0)``. Snowflake's
    ``NUMBER(p, 0)`` columns beyond int64 resolve as ``decimal(p, 0)``.
    """
    return current_dialect(value).coefficient_bounds(value)


def _scaled_text(coefficient, scale: int):
    text = coefficient.cast("string")
    if scale == 0:
        return text
    sign = text.startswith("-").ifelse("-", "")
    digits = text.replace("-", "")
    digits = digits.lpad(greatest(digits.length(), scale + 1), "0")
    return (
        sign
        + digits.substr(0, digits.length() - scale)
        + "."
        + digits.substr(digits.length() - scale)
    )


def _checked_narrow_exact(coefficient: ir.Value, scale: int, target: str | dt.DataType) -> ir.Value:
    """Check exact coefficient bounds where TRY_CAST would be an ordinary CAST.

    The bounds take the coefficient's own type, so a scale-0 coefficient is
    never promoted into a narrower fixed-scale state for the comparison.
    """
    text = _scaled_text(coefficient, scale)
    target_type = dt.dtype(target)
    if isinstance(target_type, dt.SignedInteger):
        bits = target_type.nbytes * 8
        low, high = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1
    elif isinstance(target_type, dt.Decimal) and target_type.precision is not None:
        low, high = -(10**target_type.precision) + 1, 10**target_type.precision - 1
    else:
        # An unbounded NUMERIC target cannot overflow.
        return text.cast(target_type)
    lower = ibis.literal(str(low)).cast(coefficient.type())
    upper = ibis.literal(str(high)).cast(coefficient.type())
    fits = (coefficient >= lower) & (coefficient <= upper)
    # The cast only ever sees text that fits, whichever branches a backend evaluates.
    return fits.ifelse(text, ibis.null(dt.string)).cast(target_type)


def _not_converted(result: ir.FloatingValue, dialect: Dialect) -> ir.BooleanValue:
    """Whether binary64 conversion had no finite result: NULL beyond its range
    or an infinity."""
    return result.isnull() | ~dialect.float_is_finite(result)


def _scale_text(scale: int | ir.IntegerValue) -> ir.StringValue:
    if isinstance(scale, int):
        return cast(ir.StringValue, ibis.literal(str(scale)))
    return scale.cast("string")


def _scaled_exact_to_float(
    value: ir.Value, scale: int | ir.IntegerValue, dialect: Dialect
) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
    text = value.cast("string") + "e-" + _scale_text(scale)
    result = dialect.binary64(text)
    false = ibis.literal(False)
    out_of_range = (value.notnull() & _not_converted(result, dialect)).fill_null(false)
    rounded = out_of_range.ifelse(ibis.null().cast(dt.float64), result)
    return cast(ir.FloatingValue, rounded), cast(ir.BooleanValue, false), out_of_range


def quantized_float(
    value: ir.FloatingValue,
) -> tuple[ir.DecimalValue, ir.BooleanValue, ir.BooleanValue]:
    """Quantize once at scale 9; flag non-finite and out-of-domain inputs.

    The largest binary64 below 10**29 is float(10**29) itself, so the
    inclusive comparison admits that value without rounding the boundary up.
    Invalid values become NULL before any decimal cast is evaluated.
    """
    dialect = current_dialect(value)
    floating = cast(ir.FloatingValue, value.cast(dt.float64))
    finite = dialect.float_is_finite(floating)
    fits = floating.abs() <= ibis.literal(float(10**29), type=dt.float64)
    safe = cast(ir.FloatingValue, (finite & fits).ifelse(floating, ibis.null(dt.float64)))
    false = ibis.literal(False)
    return (
        dialect.quantize_float(safe),
        (~finite).fill_null(false),
        (finite & ~fits).fill_null(false),
    )


def float_ratio(
    numerator: ir.Value,
    numerator_scale: int | ir.IntegerValue,
    denominator: ir.Value,
    denominator_scale: int | ir.IntegerValue,
) -> ir.FloatingValue:
    """Convert each exact operand at its scale once, then divide once in float64.

    A nested float64 quotient passes through. Zero is tested in the exact
    denominator domain, before conversion.
    """
    zero_value = (
        ibis.literal(0.0) if denominator.type().is_floating() else exact_literal(0, denominator)
    )
    zero = denominator == zero_value
    divisor = zero.ifelse(ibis.null(denominator.type()), denominator)
    n = exact_to_float(numerator, numerator_scale)[0]
    d = exact_to_float(divisor, denominator_scale)[0]
    return cast(ir.FloatingValue, n / d)


def exact_to_float(
    value: ir.Value, scale: int | ir.IntegerValue = 0
) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
    """Convert ``value * 10**-scale`` once to float64; a float passes through.

    Return (float or NULL, nonfinite, out_of_range).
    """
    return current_dialect(value).exact_to_float(value, scale)


def exact_sum(value: ir.Value, *, admitted: bool = False) -> ir.Value:
    """Sum exactly; ``admitted`` operands were withheld by ``guard_accumulation`` already."""
    return current_dialect(value).exact_sum(value, admitted=admitted)


def guard_accumulation(
    table: ibis.Table,
    coefficient: ir.Value,
    *,
    partition: Sequence[str],
    order: str | None = None,
) -> tuple[ibis.Table, ir.Value, ir.BooleanValue | None]:
    """Withhold coefficients whose partition's accumulated magnitudes may not fit.

    A policy that bounds accumulations projects ``coefficient`` onto ``table``
    and, over each partition (running along ``order`` when given), the
    admission of its backend magnitude bound and contributing count by
    ``Dialect.admits_accumulation``. Returns the projected table, the
    coefficient withheld (NULL) where not admitted, and the admission flag,
    both columns of that table: the withheld coefficient keeps every sum the
    backend evaluates inside the type's domain, and the flag lets a running
    consumer refuse rather than publish a partial sum. Other policies return
    the inputs unchanged and no flag.
    """
    policy = current_dialect(coefficient)
    if not policy.bounds_accumulations:
        return table, coefficient, None
    dtype = coefficient.type()
    if not isinstance(dtype, dt.Decimal):
        raise TypeError(f"{dtype} is not a fixed-precision coefficient type")
    used = {name.casefold() for name in table.columns}
    names = []
    for stem in ("coefficient", "admitted"):
        name = f"__terrine_{stem}"
        while name.casefold() in used:
            name = f"_{name}"
        used.add(name.casefold())
        names.append(name)
    value_name, flag_name = names
    staged = table.mutate(**{value_name: coefficient})
    frame = ibis.window(
        group_by=[staged[name] for name in partition],
        order_by=[staged[order]] if order is not None else (),
        rows=(None, 0) if order is not None else None,
    )
    bound = policy.magnitude(staged[value_name]).sum().over(frame)
    count = staged[value_name].count().over(frame)
    flagged = staged.mutate(**{flag_name: policy.admits_accumulation(bound, count, dtype)})
    admitted = cast(ir.BooleanValue, flagged[flag_name])
    return flagged, admitted.ifelse(flagged[value_name], ibis.null().cast(dtype)), admitted


def exact_subtract(left: ir.Value, right: ir.Value) -> ir.Value:
    return current_dialect(left, right).exact_subtract(left, right)


def truncating_quotient(
    value: ir.IntegerValue, divisor: int | ir.IntegerValue, *, like: ir.Value
) -> ir.IntegerValue:
    """Integer division truncating toward zero; Ibis ``//`` floors through doubles."""
    return current_dialect(like, value).truncating_quotient(value, divisor)


def float_is_finite(value: ir.Value) -> ir.BooleanValue:
    return current_dialect(value).float_is_finite(value)
