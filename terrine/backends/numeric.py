"""Numeric expressions Ibis lacks or translates inexactly, per backend.

Exact state uses an arbitrary-precision native type: DuckDB ``BIGNUM``,
PostgreSQL ``NUMERIC`` or BigQuery ``BIGNUMERIC``. On DuckDB a float enters
exact state as an integer coefficient at a data-derived decimal scale, built
from its shortest round-trip text, and leaves it through text, which DuckDB
rounds to binary64 correctly. The IEEE binary64 primitives back DuckDB's
certified float reductions only; other backends validate floats against DuckDB
within a tolerance instead.
"""

from __future__ import annotations

import math
import sys
from collections.abc import Sequence
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import sqlglot.expressions as sge

from terrine.backends.dialect import dialect_of, unavailable
from terrine.errors import RefusalSpec

__all__ = [
    "EXACT_OVERFLOW",
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
    "ieee_add",
    "ieee_divide",
    "ieee_multiply",
    "ieee_subtract",
    "is_exact_type",
    "next_toward",
    "normalized",
    "rounded_quotient",
    "truncating_quotient",
]


EXACT_OVERFLOW = RefusalSpec(
    "terrine.temporal.exact_overflow",
    "The requested exact result cannot be represented by the native output type. "
    "Use a supported exact representation or a semantically sufficient window.",
)
_DUCKDB_EXACT = dt.Unknown(raw_type=sge.DataType.build("BIGNUM", dialect="duckdb"))
_PG_EXACT = dt.Unknown(raw_type=sge.DataType.build("NUMERIC", dialect="postgres"))
# BigQuery has no arbitrary precision: BIGNUMERIC holds integers to about 5.7e38
# and 38 fractional digits. Overflow is an error (sums) or NULL (SAFE_ arithmetic),
# both of which surface as terrine.temporal.exact_overflow.
_BQ_EXACT = dt.Unknown(raw_type=sge.DataType.build("BIGNUMERIC", dialect="bigquery"))


def exact_type(*like: ir.Value) -> dt.DataType:
    dialect = dialect_of(*like)
    if dialect == "postgres":
        return _PG_EXACT
    if dialect == "duckdb":
        return _DUCKDB_EXACT
    if dialect == "bigquery":
        return _BQ_EXACT
    raise unavailable("exact_state", dialect)


def is_exact_type(dtype: dt.DataType) -> bool:
    """Whether ``dtype`` is an internal exact-state type on any backend."""
    return dtype in (_DUCKDB_EXACT, _PG_EXACT, _BQ_EXACT)


def exact_literal(value: int, *like: ir.Value) -> ir.Value:
    """An exact native integer, without an inferred fixed-width literal type."""
    return ibis.literal(str(value)).cast(exact_type(*like))


def _exact_text(text: str, *like: ir.Value) -> ir.Value:
    return ibis.literal(text).cast(exact_type(*like))


@ibis.udf.scalar.builtin(
    name="numeric_div", database="pg_catalog", signature=((_PG_EXACT, _PG_EXACT), _PG_EXACT)
)
def _pg_exact_divide(numerator, denominator):
    """PostgreSQL NUMERIC division to at least 16 significant digits."""


@ibis.udf.scalar.builtin(name="safe_divide", signature=((_BQ_EXACT, _BQ_EXACT), _BQ_EXACT))
def _bq_exact_divide(numerator, denominator):
    """BigQuery BIGNUMERIC division to 38 fractional digits; NULL for zero."""


@ibis.udf.scalar.builtin(name="safe_multiply", signature=((_BQ_EXACT, _BQ_EXACT), _BQ_EXACT))
def _bq_exact_multiply(left, right):
    """BigQuery exact BIGNUMERIC product; NULL on overflow."""


def decimal_coefficient(value: ir.Value, scale: int) -> ir.Value:
    """The exact integer ``value * 10**scale`` of an integer or fixed-scale decimal.

    DuckDB and PostgreSQL print every fractional digit of a fixed-scale decimal,
    so its text without the point is the coefficient; BigQuery prints NUMERIC
    1.50 as ``1.5``, so it multiplies instead.
    """
    dialect = dialect_of(value)
    if dialect == "bigquery":
        exact = value.cast(_BQ_EXACT)
        return _bq_exact_multiply(exact, exact_literal(10**scale, value)) if scale else exact
    text = value.cast("string")
    return (text.replace(".", "") if scale else text).cast(exact_type(value))


@ibis.udf.scalar.builtin(name="ltrim", signature=((dt.string, dt.string), dt.string))
def _duckdb_ltrim(value, _characters):
    """DuckDB ``ltrim`` removing the given characters."""


@ibis.udf.scalar.builtin(name="printf", signature=((dt.string, dt.float64), dt.string))
def _duckdb_printf(_template, value):
    """DuckDB ``printf`` of one binary64 value."""


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


def _finite_text(value: ir.Value) -> tuple[ir.FloatingValue, ir.StringValue]:
    """A float widened to binary64 and its shortest text, NULL unless finite."""
    widened = cast(ir.FloatingValue, value.cast(dt.float64))
    text = float_is_finite(widened).ifelse(widened.cast("string"), ibis.null(dt.string))
    return widened, cast(ir.StringValue, text)


def _scale_text(scale: int | ir.IntegerValue) -> ir.StringValue:
    if isinstance(scale, int):
        return cast(ir.StringValue, ibis.literal(str(scale)))
    return scale.cast("string")


def float_scale(value: ir.Column, by: Sequence[ir.Column]) -> ir.IntegerValue:
    """The decimal scale at which every float in each ``by`` partition is an integer.

    DuckDB: the largest ``fraction_digits − exponent`` over the partition's
    non-null finite values' shortest round-trip text, floored at 0, so one tiny
    value widens only its own partition's coefficients. PostgreSQL and BigQuery
    hold float text as unscaled exact state, so their scale is 0.
    """
    if dialect_of(value) != "duckdb":
        return cast(ir.IntegerValue, ibis.literal(0, type=dt.int64))
    _, text = _finite_text(value)
    _, _, fraction_digits, exponent = _decimal_parts(text)
    need = cast(ir.IntegerColumn, fraction_digits - exponent)
    largest = need.max().over(ibis.window(group_by=list(by)))
    return cast(ir.IntegerValue, ibis.greatest(largest.fill_null(ibis.literal(0)), 0))


def exact_float(
    value: ir.Value, scale: int | ir.IntegerValue | None = None
) -> tuple[ir.Value, ir.BooleanValue, ir.BooleanValue]:
    """Exact state for a float from its shortest round-trip text.

    Returns (exact, nonfinite, unrepresentable). DuckDB holds every finite
    binary64 exactly as the BIGNUM coefficient ``value · 10**scale``: the text's
    digits padded with zeros. It needs ``scale`` from ``float_scale`` over the
    same partition; a value needing more fractional digits than ``scale`` is
    NULL and ``unrepresentable``, which that scale never produces. PostgreSQL
    NUMERIC keeps NaN and infinities, which poison later arithmetic, so both
    flags are false there. BigQuery BIGNUMERIC has neither and holds magnitudes
    below about 5.7e38 to 38 fractional digits; non-finite and out-of-range
    values become NULL and are flagged so the caller can refuse them.
    """
    widened = value.cast(dt.float64)
    text = widened.cast("string")
    never = cast(ir.BooleanValue, ibis.literal(False))
    dialect = dialect_of(value)
    if dialect == "postgres":
        return text.cast(_PG_EXACT), never, never
    floating = cast(ir.FloatingValue, widened)
    if dialect == "duckdb":
        if scale is None:
            raise ValueError("DuckDB exact float state needs the partition's scale")
        finite = float_is_finite(floating)
        sign, digits, fraction_digits, exponent = _decimal_parts(_finite_text(value)[1])
        padding = cast(ir.IntegerValue, scale - (fraction_digits - exponent))
        representable = finite & (padding >= ibis.literal(0))
        zeros = cast(ir.StringValue, ibis.literal("0")).repeat(padding)
        coefficient = (sign + digits + zeros).cast(_DUCKDB_EXACT)
        exact = representable.ifelse(coefficient, ibis.null().cast(_DUCKDB_EXACT))
        nonfinite = (~finite).fill_null(ibis.literal(False))
        unrepresentable = (finite & (padding < ibis.literal(0))).fill_null(ibis.literal(False))
        return exact, nonfinite, unrepresentable
    if dialect != "bigquery":
        raise unavailable("exact_float", dialect)
    nonfinite = (floating.isnan() | floating.isinf()).fill_null(ibis.literal(False))
    exact = nonfinite.ifelse(ibis.null().cast(_BQ_EXACT), text.try_cast(_BQ_EXACT))
    unrepresentable = ~nonfinite & widened.notnull() & exact.isnull()
    return exact, nonfinite, unrepresentable


def exact_ratio(numerator: ir.Value, denominator: ir.Value) -> ir.Value:
    """Exact-state ``numerator / denominator``; NULL for a zero denominator.

    Not for DuckDB, whose certified path encloses quotients in float intervals.
    PostgreSQL divides to at least 16 significant digits; BigQuery to 38
    fractional digits.
    """
    dialect = dialect_of(numerator, denominator)
    if dialect == "bigquery":
        return _bq_exact_divide(numerator, denominator)
    if dialect != "postgres":
        raise unavailable("exact_ratio", dialect)
    zero = exact_literal(0, numerator, denominator)
    divisor = (denominator == zero).ifelse(ibis.null().cast(_PG_EXACT), denominator)
    return _pg_exact_divide(numerator, divisor)


# Binary64's largest finite value, and 2**-1075: magnitudes at or below it round
# to zero, which PostgreSQL's float8 input rejects instead of returning.
_FLOAT64_MAX = str(int(sys.float_info.max))
_FLOAT64_ROUNDS_TO_ZERO = "0." + str(5**1075).rjust(1075, "0")
_NONFINITE = ("NaN", "Infinity", "-Infinity")


def exact_is_nonfinite(value: ir.Value) -> ir.BooleanValue:
    """Whether an exact value is NaN or infinite; NULL stays NULL.

    Only PostgreSQL NUMERIC has non-finite values; BIGNUMERIC has none.
    """
    dialect = dialect_of(value)
    if dialect == "bigquery":
        return cast(
            ir.BooleanValue, value.isnull().ifelse(ibis.null(dt.boolean), ibis.literal(False))
        )
    if dialect != "postgres":
        raise unavailable("exact_is_nonfinite", dialect)
    return value.isin([_exact_text(text, value) for text in _NONFINITE])


def exact_to_float(
    value: ir.Value, scale: int | ir.IntegerValue = 0
) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
    """Round the exact value ``value · 10**-scale`` once to binary64.

    Returns (float, nonfinite, out_of_range); the float is NULL for either flag.
    DuckDB parses ``coefficient || 'e-' || scale`` text, which its
    string-to-double conversion rounds correctly (ties to even), and flags a
    magnitude beyond binary64's range. PostgreSQL and BigQuery hold unscaled
    exact state, so ``scale`` is 0 there. Magnitudes at or below 2**-1075 become
    0.0, as round-to-nearest gives.
    """
    false = ibis.literal(False)
    never = cast(ir.BooleanValue, false)
    dialect = dialect_of(value)
    if dialect == "duckdb":
        text = value.cast("string") + "e-" + _scale_text(scale)
        result = cast(ir.FloatingValue, text.cast(dt.float64))
        out_of_range = (value.notnull() & ~float_is_finite(result)).fill_null(false)
        rounded = out_of_range.ifelse(ibis.null().cast(dt.float64), result)
        return cast(ir.FloatingValue, rounded), never, out_of_range
    if not isinstance(scale, int) or scale:
        raise ValueError(f"{dialect} exact state is unscaled; scale must be 0")
    if dialect == "bigquery":
        # BIGNUMERIC is finite and far inside binary64's range.
        return cast(ir.FloatingValue, value.cast(dt.float64)), never, never
    # NUMERIC NaN equals itself and sorts above every number, so test it first.
    nonfinite = exact_is_nonfinite(value)
    limit, tiny = _exact_text(_FLOAT64_MAX, value), _exact_text(_FLOAT64_ROUNDS_TO_ZERO, value)
    negative_limit = _exact_text("-" + _FLOAT64_MAX, value)
    negative_tiny = _exact_text("-" + _FLOAT64_ROUNDS_TO_ZERO, value)
    out_of_range = ~nonfinite & ((value > limit) | (value < negative_limit))
    rounds_to_zero = (value <= tiny) & (value >= negative_tiny)
    missing = ibis.null().cast(dt.float64)
    result = ibis.cases(
        (nonfinite | out_of_range, missing),
        (rounds_to_zero, ibis.literal(0.0)),
        else_=value.cast(dt.float64),
    )
    return (
        cast(ir.FloatingValue, result),
        nonfinite.fill_null(false),
        out_of_range.fill_null(false),
    )


def normalized(
    value: ir.Value, scale: int | ir.IntegerValue = 0
) -> tuple[ir.FloatingValue, ir.IntegerValue]:
    """``(mantissa, exponent)`` with ``value · 10**-scale = mantissa · 10**exponent``.

    DuckDB only. The mantissa is a binary64 with ``1 ≤ |mantissa| < 10`` (0 for
    zero), keeping the sign; the exponent is an integer. An exact coefficient
    normalizes from its integer text; an already rounded binary64 operand from
    its shortest round-trip text, so it stands for that decimal rather than its
    binary expansion. The mantissa is the significant digits rounded once, so
    it is within 2**-53 relative of the exact mantissa; digits that round up to
    10 carry into the exponent as 1 · 10**(exponent + 1), still within 2**-53.
    A NULL or non-finite float gives NULL.
    """
    dialect = dialect_of(value)
    if dialect != "duckdb":
        raise unavailable("normalized", dialect)
    text = _finite_text(value)[1] if value.type().is_floating() else value.cast("string")
    sign, digits, fraction_digits, exponent = _decimal_parts(text)
    significant = cast(ir.StringValue, _duckdb_ltrim(digits, "0"))
    count = significant.length()
    zero = count == ibis.literal(0)
    leading = cast(ir.StringValue, zero.ifelse(ibis.literal("0"), sign + significant))
    mantissa = cast(ir.FloatingValue, (leading + "e" + (1 - count).cast("string")).cast(dt.float64))
    carries = mantissa.abs() == ibis.literal(10.0)
    mantissa = cast(ir.FloatingValue, carries.ifelse(mantissa / 10.0, mantissa))
    carry = cast(ir.IntegerValue, carries.cast(dt.int64))
    decimal_exponent = exponent - fraction_digits + count - 1 + carry - scale
    return mantissa, cast(ir.IntegerValue, zero.ifelse(ibis.literal(0), decimal_exponent))


_DUCKDB_HUGE = dt.Unknown(raw_type=sge.DataType.build("HUGEINT", dialect="duckdb"))
# A mantissa in [1, 10) printed with 18 fractional digits has this many digits.
_MANTISSA_DIGITS = 19
# The 19 significant digits of MAX · (1 + 3.5 · 2**-53) at decimal exponent 308:
# a quotient whose pre-rounding value is at most this returns ±MAX within the bound.
_QUOTIENT_CLAMP = "1797693134862316406"
_FLOAT64_MAX_EXPONENT = 308


@ibis.udf.scalar.builtin(name="divide", signature=((_DUCKDB_HUGE, _DUCKDB_HUGE), _DUCKDB_HUGE))
def _duckdb_huge_quotient(value, divisor):
    """DuckDB 128-bit integer division truncating toward zero."""


def _mantissa_digits(mantissa: ir.FloatingValue) -> ir.Value:
    """``|mantissa| · 10**18`` rounded to an integer: 19 digits as HUGEINT."""
    text = cast(ir.StringValue, _duckdb_printf("%.18f", mantissa.abs()))
    return text.replace(".", "").cast(_DUCKDB_HUGE)


def rounded_quotient(
    n_mantissa: ir.FloatingValue,
    n_exponent: ir.IntegerValue,
    d_mantissa: ir.FloatingValue,
    d_exponent: ir.IntegerValue,
) -> tuple[ir.FloatingValue, ir.BooleanValue]:
    """Binary64 quotient of two ``normalized`` operands on DuckDB.

    Returns (float, overflow). Each mantissa's 19 significant digits divide
    exactly as 128-bit integers, shifted so the quotient has 19 digits, and
    the exponent difference is applied by rounding that digit text once, so
    operands beyond binary64's range and subnormal results are handled alike.
    A zero denominator or a NULL operand gives NULL without ``overflow``. A
    pre-rounding value that rounds to infinity but is within 3.5u of the
    largest finite binary64 returns ±MAX; beyond that gives NULL with
    ``overflow``.

    Error model for exact operands ``p = M_n · 10**e_n`` and ``q = M_d · 10**e_d``
    with ``u = 2**-53``: each mantissa ``m = M(1 + ε)`` with ``|ε| ≤ u``
    (``normalized``); its 19-digit text and the integer quotient's truncation
    each perturb by less than ``10**-18`` relative, so the pre-rounding value
    is ``t = (p / q)(1 + δ)`` with ``|δ| ≤ 2.03u``. A finite text-to-binary64
    cast is correctly rounded, within ``u`` relative for a normal result and
    within ``2**-1075`` absolute for a subnormal one, so
    ``|r − p / q| ≤ 3.03u · |p / q| + 2**-1075``. When ``|t|`` rounds to
    infinity but ``|t| ≤ MAX(1 + 3.5u)``, ``|p / q|`` lies in
    ``[MAX(1 − 1.53u), MAX(1 + 5.53u)]``, so ``±MAX`` is within ``6u · |p / q|``;
    a representable ``p / q`` gives ``|t| ≤ MAX(1 + 2.03u)`` and is never
    flagged. Hence ``|r − p / q| ≤ 6u · |p / q| + 2**-1075`` for every finite
    result.
    """
    zero = ibis.literal(0.0)
    divides = d_mantissa != zero
    numerator = _mantissa_digits(n_mantissa)
    denominator = divides.ifelse(_mantissa_digits(d_mantissa), ibis.null().cast(_DUCKDB_HUGE))
    # |n / d| lies in (0.1, 10): shift so the integer quotient has 19 digits.
    shift = cast(
        ir.IntegerValue,
        (numerator >= denominator).ifelse(
            ibis.literal(_MANTISSA_DIGITS - 1), ibis.literal(_MANTISSA_DIGITS)
        ),
    )
    zeros = cast(ir.StringValue, ibis.literal("0")).repeat(shift)
    digits = _duckdb_huge_quotient(
        (numerator.cast("string") + zeros).cast(_DUCKDB_HUGE), denominator
    )
    # t = digits · 10**(exponent − 18) with 10**exponent ≤ |t| < 10**(exponent + 1).
    exponent = cast(ir.IntegerValue, n_exponent - d_exponent + (_MANTISSA_DIGITS - 1) - shift)
    negative = (n_mantissa < zero) != (d_mantissa < zero)
    sign = cast(ir.StringValue, negative.ifelse(ibis.literal("-"), ibis.literal("")))
    magnitude = digits.cast("string") + "e" + (exponent - (_MANTISSA_DIGITS - 1)).cast("string")
    result = cast(ir.FloatingValue, (sign + magnitude).cast(dt.float64))
    clamp = ibis.literal(_QUOTIENT_CLAMP).cast(_DUCKDB_HUGE)
    largest_exponent = ibis.literal(_FLOAT64_MAX_EXPONENT)
    within_clamp = (exponent < largest_exponent) | (
        (exponent == largest_exponent) & (digits <= clamp)
    )
    beyond = (digits.notnull() & ~float_is_finite(result)).fill_null(ibis.literal(False))
    overflow = beyond & ~within_clamp
    largest = negative.ifelse(ibis.literal(-sys.float_info.max), ibis.literal(sys.float_info.max))
    rounded = ibis.cases(
        (overflow, ibis.null().cast(dt.float64)),
        (beyond, largest),
        else_=result,
    )
    return cast(ir.FloatingValue, rounded), overflow


@ibis.udf.agg.builtin(name="sum", signature=((_DUCKDB_EXACT,), _DUCKDB_EXACT))
def _duckdb_exact_sum(value):
    """Native exact coefficient/count sum."""


@ibis.udf.scalar.builtin(name="subtract", signature=((_DUCKDB_EXACT, _DUCKDB_EXACT), _DUCKDB_EXACT))
def _duckdb_exact_subtract(left, right):
    """Native exact coefficient/count subtraction."""


@ibis.udf.agg.builtin(name="sum", signature=((_PG_EXACT,), _PG_EXACT))
def _pg_exact_sum(value):
    """PostgreSQL exact NUMERIC sum."""


@ibis.udf.scalar.builtin(
    name="numeric_sub", database="pg_catalog", signature=((_PG_EXACT, _PG_EXACT), _PG_EXACT)
)
def _pg_exact_subtract(left, right):
    """PostgreSQL exact NUMERIC subtraction."""


@ibis.udf.agg.builtin(name="sum", signature=((_BQ_EXACT,), _BQ_EXACT))
def _bq_exact_sum(value):
    """BigQuery BIGNUMERIC sum; overflow is a query error."""


@ibis.udf.scalar.builtin(name="safe_subtract", signature=((_BQ_EXACT, _BQ_EXACT), _BQ_EXACT))
def _bq_exact_subtract(left, right):
    """BigQuery BIGNUMERIC subtraction; NULL on overflow."""


def exact_sum(value):
    dialect = dialect_of(value)
    if dialect == "postgres":
        return _pg_exact_sum(value)
    if dialect == "duckdb":
        return _duckdb_exact_sum(value)
    if dialect == "bigquery":
        return _bq_exact_sum(value)
    raise unavailable("exact_state", dialect)


def exact_subtract(left, right):
    dialect = dialect_of(left, right)
    if dialect == "postgres":
        return _pg_exact_subtract(left, right)
    if dialect == "duckdb":
        return _duckdb_exact_subtract(left, right)
    if dialect == "bigquery":
        return _bq_exact_subtract(left, right)
    raise unavailable("exact_state", dialect)


@ibis.udf.scalar.builtin(name="divide", signature=((dt.int64, dt.int64), dt.int64))
def _duckdb_quotient(value, divisor):
    """DuckDB integer division truncating toward zero."""


@ibis.udf.scalar.builtin(name="div", signature=((dt.int64, dt.int64), dt.int64))
def _div(value, divisor):
    """PostgreSQL and BigQuery integer division truncating toward zero."""


def truncating_quotient(
    value: ir.IntegerValue, divisor: int | ir.IntegerValue, *, like: ir.Value
) -> ir.IntegerValue:
    """Integer division truncating toward zero; Ibis ``//`` floors through doubles."""
    dialect = dialect_of(like, value)
    if dialect in {"postgres", "bigquery"}:
        quotient = _div(value, divisor)
    elif dialect == "duckdb":
        quotient = _duckdb_quotient(value, divisor)
    else:
        raise unavailable("integer_quotient", dialect)
    return cast(ir.IntegerValue, cast(ir.IntegerValue, quotient).cast(dt.int64))


@ibis.udf.scalar.builtin(name="isfinite", signature=((dt.float64,), dt.boolean))
def _duckdb_isfinite(value):
    """Native finite-state predicate."""


def float_is_finite(value) -> ir.BooleanValue:
    if dialect_of(value) == "duckdb":
        return cast(ir.BooleanValue, _duckdb_isfinite(value))
    # PostgreSQL has no isfinite(double precision), and NaN equals NaN there.
    nonfinite = [ibis.literal(item, type=dt.float64) for item in (math.inf, -math.inf, math.nan)]
    return cast(ir.FloatingValue, value.cast(dt.float64)).notin(nonfinite)


@ibis.udf.scalar.builtin(name="nextafter", signature=((dt.float64, dt.float64), dt.float64))
def next_toward(value, _direction):
    """DuckDB's adjacent binary64 value toward ``direction``."""


@ibis.udf.scalar.builtin(name="add", signature=((dt.float64, dt.float64), dt.float64))
def ieee_add(left, right):
    """DuckDB binary64 addition."""


@ibis.udf.scalar.builtin(name="subtract", signature=((dt.float64, dt.float64), dt.float64))
def ieee_subtract(left, right):
    """DuckDB binary64 subtraction."""


@ibis.udf.scalar.builtin(name="multiply", signature=((dt.float64, dt.float64), dt.float64))
def ieee_multiply(left, right):
    """DuckDB binary64 multiplication."""


@ibis.udf.scalar.builtin(name="divide", signature=((dt.float64, dt.float64), dt.float64))
def ieee_divide(left, right):
    """DuckDB binary64 division."""
