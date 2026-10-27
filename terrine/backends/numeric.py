"""Numeric expressions Ibis lacks or translates inexactly, per backend.

Exact state uses a native type that adds and subtracts without rounding:
DuckDB ``BIGNUM`` and PostgreSQL ``NUMERIC`` (arbitrary precision), BigQuery
``BIGNUMERIC`` and Snowflake ``NUMBER(38, 0)`` (38 digits; overflow is an
error). On DuckDB and Snowflake a float enters exact state as an integer
coefficient at a data-derived decimal scale, built from its decimal text
(DuckDB's shortest round-trip text; on Snowflake the DECFLOAT value, 18
significant digits), and leaves it through text, which both convert to
binary64 with correct rounding; quotients divide 19-digit normalized
significands as wide integers and round once at output. PostgreSQL and
BigQuery hold the shortest text unscaled in their exact types and divide there.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.dialect import Dialect, current_dialect, dialect_for_name
from terrine.backends.extrema import greatest
from terrine.backends.registry import ENGINES
from terrine.errors import RefusalSpec

__all__ = [
    "EXACT_OVERFLOW",
    "coefficient_bounds",
    "decimal_coefficient",
    "exact_float",
    "exact_is_nonfinite",
    "exact_literal",
    "exact_ratio",
    "exact_subtract",
    "exact_sum",
    "exact_to_float",
    "exact_type",
    "float_is_finite",
    "float_scale",
    "has_scaled_float_state",
    "is_exact_type",
    "normalized",
    "normalized_quotient",
    "rounded_normalized",
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
    """Whether ``dtype`` is an internal exact-state type on any backend."""
    return any(dtype == dialect_for_name(name).exact_type() for name in ENGINES)


def has_scaled_float_state(*like: ir.Value) -> bool:
    """Whether float state is a coefficient at a per-unit scale (DuckDB, Snowflake)."""
    return current_dialect(*like).has_scaled_float_state


def exact_literal(value: int, *like: ir.Value) -> ir.Scalar:
    """An exact native integer, without an inferred fixed-width literal type."""
    return cast(ir.Scalar, ibis.literal(str(value)).cast(exact_type(*like)))


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


def _checked_narrow_exact(
    coefficient: ir.Value, scale: int, target: str | dt.DataType, dialect: Dialect
) -> ir.Value:
    """Check exact coefficient bounds where TRY_CAST would be an ordinary CAST."""
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
    lower = ibis.literal(str(low)).cast(dialect.exact_type())
    upper = ibis.literal(str(high)).cast(dialect.exact_type())
    fits = (coefficient >= lower) & (coefficient <= upper)
    return fits.ifelse(text.cast(target_type), ibis.null().cast(target_type))


def _decimal_parts(
    text: ir.StringValue,
) -> tuple[ir.StringValue, ir.StringValue, ir.IntegerValue, ir.IntegerValue]:
    """``(sign, digits, fraction_digits, exponent)`` of ``[-]D[.F][e±E]`` text.

    ``digits`` is ``D || F`` without sign or point, so the text's value is
    ``sign · digits · 10**(exponent − fraction_digits)``. DuckDB prints a
    binary64 as its shortest round-trip text in this form, and an exact
    coefficient as plain signed digits.
    """
    zero = ibis.literal(0)
    marker = text.find("e")
    has_exponent = marker >= zero
    after_marker = cast(ir.IntegerValue, marker + 1)
    mantissa = cast(ir.StringValue, has_exponent.ifelse(text.substr(0, marker), text))
    exponent = has_exponent.ifelse(text.substr(after_marker).cast(dt.int64), zero)
    point = mantissa.find(".")
    fraction_digits = (point >= zero).ifelse(mantissa.length() - point - 1, zero)
    sign = text.startswith("-").ifelse(ibis.literal("-"), ibis.literal(""))
    digits = mantissa.replace("-", "").replace(".", "")
    return (
        cast(ir.StringValue, sign),
        digits,
        cast(ir.IntegerValue, fraction_digits),
        cast(ir.IntegerValue, exponent),
    )


def _not_converted(result: ir.FloatingValue, dialect: Dialect) -> ir.BooleanValue:
    """Whether binary64 conversion had no finite result: NULL beyond its range
    or an infinity."""
    return result.isnull() | ~dialect.float_is_finite(result)


def _scale_text(scale: int | ir.IntegerValue) -> ir.StringValue:
    if isinstance(scale, int):
        return cast(ir.StringValue, ibis.literal(str(scale)))
    return scale.cast("string")


def _scaled_float_parts(
    value: ir.Value, scale: int | ir.IntegerValue | None, dialect: Dialect
) -> tuple[ir.StringValue, ir.BooleanValue, ir.BooleanValue]:
    if scale is None:
        raise ValueError(f"{dialect.name} exact float state needs the partition's scale")
    floating = cast(ir.FloatingValue, value.cast(dt.float64))
    finite = dialect.float_is_finite(floating)
    sign, digits, fraction_digits, exponent = _decimal_parts(dialect.finite_text(value)[1])
    padding = cast(ir.IntegerValue, scale - (fraction_digits - exponent))
    zeros = cast(ir.StringValue, ibis.literal("0")).repeat(padding)
    return sign + digits + zeros, finite, padding >= ibis.literal(0)


def _scaled_float_result(
    coefficient: ir.Value, finite: ir.BooleanValue, held: ir.BooleanValue
) -> tuple[ir.Value, ir.BooleanValue, ir.BooleanValue]:
    exact = (finite & held).ifelse(coefficient, ibis.null().cast(coefficient.type()))
    nonfinite = (~finite).fill_null(ibis.literal(False))
    unrepresentable = (finite & ~held).fill_null(ibis.literal(False))
    return exact, nonfinite, unrepresentable


def _scaled_exact_to_float(
    value: ir.Value, scale: int | ir.IntegerValue, dialect: Dialect
) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
    text = value.cast("string") + "e-" + _scale_text(scale)
    result = dialect.binary64(text)
    false = ibis.literal(False)
    out_of_range = (value.notnull() & _not_converted(result, dialect)).fill_null(false)
    rounded = out_of_range.ifelse(ibis.null().cast(dt.float64), result)
    return cast(ir.FloatingValue, rounded), cast(ir.BooleanValue, false), out_of_range


def float_scale(value: ir.Column, by: Sequence[ir.Column]) -> ir.IntegerValue:
    """The decimal scale at which every float in each ``by`` partition is an integer.

    DuckDB and Snowflake: the largest ``fraction_digits − exponent`` over the
    partition's non-null finite values' decimal text, floored at
    0, so one tiny value widens only its own partition's coefficients.
    PostgreSQL and BigQuery hold float text as unscaled exact state, so their
    scale is 0.
    """
    if not has_scaled_float_state(value):
        return cast(ir.IntegerValue, ibis.literal(0, type=dt.int64))
    _, text = current_dialect(value).finite_text(value)
    _, _, fraction_digits, exponent = _decimal_parts(text)
    need = cast(ir.IntegerColumn, fraction_digits - exponent)
    largest = need.max().over(ibis.window(group_by=list(by)))
    return cast(ir.IntegerValue, ibis.greatest(largest.fill_null(ibis.literal(0)), 0))


def exact_float(
    value: ir.Value, scale: int | ir.IntegerValue | None = None
) -> tuple[ir.Value, ir.BooleanValue, ir.BooleanValue]:
    """Exact state for a float from its finite decimal text.

    Returns (exact, nonfinite, unrepresentable). DuckDB and Snowflake hold a
    finite binary64 as the coefficient ``value · 10**scale``: the text's digits
    padded with zeros. They need ``scale`` from ``float_scale`` over the same
    partition; a value needing more fractional digits than ``scale`` is NULL
    and ``unrepresentable``, which that scale never produces, as is a
    coefficient beyond Snowflake's 38 digits (DuckDB's BIGNUM is unbounded).
    PostgreSQL NUMERIC keeps NaN and infinities, which poison later arithmetic,
    so both flags are false there. BigQuery BIGNUMERIC has neither and holds
    magnitudes below about 5.7e38 to 38 fractional digits; non-finite and
    out-of-range values become NULL and are flagged so the caller can refuse
    them.
    """
    return current_dialect(value).exact_float(value, scale)


def exact_ratio(numerator: ir.Value, denominator: ir.Value) -> ir.Value:
    """Exact-state ``numerator / denominator``; NULL for a zero denominator.

    Not for DuckDB or Snowflake, which divide normalized significands
    (``normalized_quotient``). PostgreSQL divides to at least 16 significant
    digits; BigQuery to 38 fractional digits.
    """
    return current_dialect(numerator, denominator).exact_ratio(numerator, denominator)


def exact_is_nonfinite(value: ir.Value) -> ir.BooleanValue:
    """Whether an exact value is NaN or infinite; NULL stays NULL.

    Only PostgreSQL NUMERIC has non-finite values; BIGNUMERIC has none.
    """
    return current_dialect(value).exact_is_nonfinite(value)


def exact_to_float(
    value: ir.Value, scale: int | ir.IntegerValue = 0
) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
    """Round the exact value ``value · 10**-scale`` once to binary64.

    Returns (float, nonfinite, out_of_range); the float is NULL for either flag.
    DuckDB and Snowflake parse ``coefficient || 'e-' || scale`` text, which
    their string-to-double conversions round correctly (ties to even); a
    magnitude beyond binary64's range is an infinity on DuckDB and NULL from
    Snowflake's ``TRY_TO_DOUBLE``, flagged either way. PostgreSQL and BigQuery
    hold unscaled exact state, so ``scale`` is 0 there. Magnitudes at or
    below 2**-1075 become 0.0, as round-to-nearest gives.
    """
    return current_dialect(value).exact_to_float(value, scale)


# A normalized value holds this many leading significant digits in a wide
# integer (DuckDB's 128-bit HUGEINT, Snowflake's NUMBER(38, 0)); a quotient of
# two such significands, shifted to as many digits, stays below 10**38 and so
# within either integer's range.
_SIGNIFICAND_DIGITS = 19


def normalized(
    value: ir.Value, scale: int | ir.IntegerValue = 0
) -> tuple[ir.Value, ir.IntegerValue]:
    """``(significand, exponent)`` with ``value · 10**-scale ≈ significand · 10**(exponent − 18)``.

    DuckDB and Snowflake. The significand is a signed wide integer holding the
    value's leading 19 significant digits, so ``10**18 ≤ |significand| < 10**19``
    (0 with exponent 0 for zero); the exponent is that of the leading digit. An
    exact coefficient normalizes from its integer text, an already rounded
    binary64 from its finite decimal text (at most 18 digits, held exactly), so
    it stands for that decimal rather than its binary expansion.
    Digits past the 19th are dropped, perturbing the value by less than
    ``10**-18`` relative. A NULL or non-finite float gives NULL.
    """
    dialect = current_dialect(value)
    wide = dialect.wide_type()
    text = dialect.finite_text(value)[1] if value.type().is_floating() else value.cast("string")
    sign, digits, fraction_digits, exponent = _decimal_parts(text)
    significant = dialect.trim_leading_zeros(digits)
    count = significant.length()
    zero = count == ibis.literal(0)
    leading = significant.substr(0, _SIGNIFICAND_DIGITS).rpad(_SIGNIFICAND_DIGITS, "0")
    significand = zero.ifelse(ibis.literal("0"), sign + leading).cast(wide)
    decimal_exponent = exponent - fraction_digits + count - 1 - scale
    return significand, cast(ir.IntegerValue, zero.ifelse(ibis.literal(0), decimal_exponent))


def normalized_quotient(
    n_significand: ir.Value,
    n_exponent: ir.IntegerValue,
    d_significand: ir.Value,
    d_exponent: ir.IntegerValue,
) -> tuple[ir.Value, ir.IntegerValue]:
    """The quotient of two ``normalized`` operands, itself normalized.

    The significands' magnitudes divide as wide integers, the numerator
    shifted by 18 or 19 digits so the truncated integer quotient has 19 digits,
    and the exponents subtract, so operands beyond binary64's range and
    subnormal results are handled alike, and a quotient can divide again
    losing nothing but its truncation (less than ``10**-18`` relative). A zero
    denominator or a NULL operand gives NULL.
    """
    dialect = current_dialect(n_significand, d_significand)
    wide = dialect.wide_type()
    zero = ibis.literal("0").cast(wide)
    numerator = dialect.wide_abs(n_significand)
    denominator = (d_significand != zero).ifelse(
        dialect.wide_abs(d_significand), ibis.null().cast(wide)
    )
    # |n / d| lies in (0.1, 10): shift so the integer quotient has 19 digits.
    shift = cast(
        ir.IntegerValue,
        (numerator >= denominator).ifelse(
            ibis.literal(_SIGNIFICAND_DIGITS - 1), ibis.literal(_SIGNIFICAND_DIGITS)
        ),
    )
    zeros = cast(ir.StringValue, ibis.literal("0")).repeat(shift)
    digits = dialect.wide_quotient((numerator.cast("string") + zeros).cast(wide), denominator)
    negative = (n_significand < zero) != (d_significand < zero)
    sign = cast(ir.StringValue, negative.ifelse(ibis.literal("-"), ibis.literal("")))
    significand = (sign + digits.cast("string")).cast(wide)
    exponent = n_exponent - d_exponent + (_SIGNIFICAND_DIGITS - 1) - shift
    is_zero = n_significand == zero
    return (
        is_zero.ifelse(zero, significand),
        cast(
            ir.IntegerValue,
            ibis.cases(
                (significand.isnull(), ibis.null(dt.int64)),
                (is_zero, ibis.literal(0)),
                else_=exponent,
            ),
        ),
    )


def rounded_normalized(
    significand: ir.Value, exponent: ir.IntegerValue
) -> tuple[ir.FloatingValue, ir.BooleanValue]:
    """Round a ``normalized`` value once to binary64 on DuckDB or Snowflake.

    Returns (float, overflow): a magnitude beyond binary64's range (an
    infinity on DuckDB, NULL from Snowflake's ``TRY_TO_DOUBLE``) is NULL with
    ``overflow``; a NULL value is NULL without it. Both backends'
    string-to-double conversions round ``significand || 'e' || (exponent − 18)``
    correctly, within ``u = 2**-53`` relative for a normal result and
    ``2**-1075`` absolute for a subnormal one.

    Error model for a quotient of exact ``p`` and ``q``: normalizing each
    operand and dividing truncate to 19 digits, each by less than ``10**-18``
    (``0.01u``) relative, so the value rounded is ``t = (p / q)(1 + δ)`` with
    ``|δ| < 3 · 10**-18``; a quotient of ``k`` nested divisions has
    ``|δ| < (2k + 1) · 10**-18``. Hence
    ``|r − p / q| ≤ (1 + 0.01(2k + 1))u(1 + u) · |p / q| + 2**-1075``, below
    ``1.1u`` for a ratio of two averages (``k = 2``) and inside the published
    ``6u · |p / q| + 2**-1075`` for any practical depth. A representable
    ``p / q`` never rounds to infinity: that needs ``|t| ≥ MAX(1 + u / 2)``.
    """
    text = significand.cast("string") + "e" + (exponent - (_SIGNIFICAND_DIGITS - 1)).cast("string")
    dialect = current_dialect(significand)
    result = dialect.binary64(text)
    overflow = (significand.notnull() & _not_converted(result, dialect)).fill_null(
        ibis.literal(False)
    )
    return cast(ir.FloatingValue, overflow.ifelse(ibis.null().cast(dt.float64), result)), overflow


def exact_sum(value: ir.Value) -> ir.Value:
    return current_dialect(value).exact_sum(value)


def exact_subtract(left: ir.Value, right: ir.Value) -> ir.Value:
    return current_dialect(left, right).exact_subtract(left, right)


def truncating_quotient(
    value: ir.IntegerValue, divisor: int | ir.IntegerValue, *, like: ir.Value
) -> ir.IntegerValue:
    """Integer division truncating toward zero; Ibis ``//`` floors through doubles."""
    return current_dialect(like, value).truncating_quotient(value, divisor)


def float_is_finite(value: ir.Value) -> ir.BooleanValue:
    return current_dialect(value).float_is_finite(value)
