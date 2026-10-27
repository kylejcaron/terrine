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

import math
import sys
from collections.abc import Sequence
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import sqlglot.expressions as sge

from terrine.backends.dialect import current_dialect, unavailable
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
_DUCKDB_EXACT = dt.Unknown(raw_type=sge.DataType.build("BIGNUM", dialect="duckdb"))
_PG_EXACT = dt.Unknown(raw_type=sge.DataType.build("NUMERIC", dialect="postgres"))
# BigQuery has no arbitrary precision: BIGNUMERIC holds integers to about 5.7e38
# and 38 fractional digits. Overflow is an error (sums) or NULL (SAFE_ arithmetic),
# both of which surface as terrine.temporal.exact_overflow.
_BQ_EXACT = dt.Unknown(raw_type=sge.DataType.build("BIGNUMERIC", dialect="bigquery"))
# Snowflake NUMBER(38, 0) is an ordinary decimal to Ibis, so sums, differences
# and comparisons render natively; a sum or difference beyond 38 digits is a
# query error, which the Snowflake session maps to terrine.temporal.exact_overflow.
_SF_EXACT = dt.Decimal(38, 0)
# Backends whose float state is a coefficient at a per-unit scale and whose
# quotients divide normalized significands.
_SCALED = frozenset({"duckdb", "snowflake"})


def exact_type(*like: ir.Value) -> dt.DataType:
    return current_dialect(*like).exact_type()


def is_exact_type(dtype: dt.DataType) -> bool:
    """Whether ``dtype`` is an internal exact-state type on any backend."""
    return dtype in (_DUCKDB_EXACT, _PG_EXACT, _BQ_EXACT, _SF_EXACT)


def has_scaled_float_state(*like: ir.Value) -> bool:
    """Whether float state is a coefficient at a per-unit scale (DuckDB, Snowflake)."""
    return current_dialect(*like).name in _SCALED


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
    dialect = current_dialect(value).name
    if dialect == "bigquery":
        exact = value.cast(_BQ_EXACT)
        return _bq_exact_multiply(exact, exact_literal(10**scale, value)) if scale else exact
    text = value.cast("string")
    return (text.replace(".", "") if scale else text).cast(exact_type(value))


def coefficient_bounds(value: ir.Value) -> tuple[int, int]:
    """The physical range of an exact column's integer coefficient (its value
    times ``10**scale``).

    Integer types are fixed-width; a ``decimal(p, s)`` holds ``p`` digits, except
    that DuckDB reports its wider HUGEINT as ``decimal(38, 0)``. Snowflake's
    ``NUMBER(p, 0)`` columns beyond int64 resolve as ``decimal(p, 0)``.
    """
    dtype = value.type()
    if isinstance(dtype, (dt.SignedInteger, dt.UnsignedInteger)):
        lower, upper = dtype.bounds
        return lower, upper
    if not isinstance(dtype, dt.Decimal) or dtype.precision is None:
        raise TypeError(f"{dtype} is not an exact type with a known precision")
    if dtype.precision == 38 and dtype.scale == 0 and current_dialect(value).name == "duckdb":
        return -(1 << 127), (1 << 127) - 1
    maximum = 10**dtype.precision - 1
    return -maximum, maximum


@ibis.udf.scalar.builtin(name="ltrim", signature=((dt.string, dt.string), dt.string))
def _ltrim(value, _characters):
    """DuckDB and Snowflake ``ltrim`` removing the given characters."""


_SF_DECFLOAT = dt.Unknown(raw_type=sge.DataType.build("DECFLOAT", dialect="snowflake"))


@ibis.udf.scalar.builtin(name="to_decfloat", signature=((dt.float64,), _SF_DECFLOAT))
def _sf_decfloat(value):
    """A binary64 as a Snowflake DECFLOAT: its value correctly rounded to 18
    significant digits (measured), which always round-trips."""


@ibis.udf.scalar.builtin(
    name="try_to_number", signature=((dt.string, dt.int64, dt.int64), _SF_EXACT)
)
def _sf_try_number(text, precision, scale):
    """Snowflake NUMBER(precision, scale) from decimal text; NULL when it does not fit."""


@ibis.udf.scalar.builtin(name="try_to_double", signature=((dt.string,), dt.float64))
def _sf_try_double(text):
    """Snowflake binary64 from decimal text, correctly rounded (measured on halfway,
    subnormal and 19-digit near-halfway cases); NULL beyond binary64's range."""


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
    """A float widened to binary64 and its decimal text, NULL unless finite.

    DuckDB prints the shortest round-trip text. Snowflake's row-wise VARCHAR,
    VARIANT and JSON renderings keep at most 15 or 16 significant digits, so
    the text there is the DECFLOAT value: 18 significant digits, correctly
    rounded, within ``0.5 · 10**-17`` relative of the binary value.
    """
    widened = cast(ir.FloatingValue, value.cast(dt.float64))
    finite = float_is_finite(widened).ifelse(widened, ibis.null(dt.float64))
    if current_dialect(value).name == "snowflake":
        text = _sf_decfloat(finite).cast("string").lower()
    else:
        text = finite.cast("string")
    return widened, text


def _binary64(text: ir.StringValue, dialect: str) -> ir.FloatingValue:
    """Decimal text rounded once to binary64, as the backend's conversion does."""
    if dialect == "snowflake":
        return cast(ir.FloatingValue, _sf_try_double(text))
    return cast(ir.FloatingValue, text.cast(dt.float64))


def _not_converted(result: ir.FloatingValue) -> ir.BooleanValue:
    """Whether ``_binary64`` had no finite result: NULL (Snowflake beyond
    binary64's range) or an infinity (DuckDB)."""
    return result.isnull() | ~float_is_finite(result)


def _scale_text(scale: int | ir.IntegerValue) -> ir.StringValue:
    if isinstance(scale, int):
        return cast(ir.StringValue, ibis.literal(str(scale)))
    return scale.cast("string")


def float_scale(value: ir.Column, by: Sequence[ir.Column]) -> ir.IntegerValue:
    """The decimal scale at which every float in each ``by`` partition is an integer.

    DuckDB and Snowflake: the largest ``fraction_digits − exponent`` over the
    partition's non-null finite values' text (``_finite_text``), floored at
    0, so one tiny value widens only its own partition's coefficients.
    PostgreSQL and BigQuery hold float text as unscaled exact state, so their
    scale is 0.
    """
    if not has_scaled_float_state(value):
        return cast(ir.IntegerValue, ibis.literal(0, type=dt.int64))
    _, text = _finite_text(value)
    _, _, fraction_digits, exponent = _decimal_parts(text)
    need = cast(ir.IntegerColumn, fraction_digits - exponent)
    largest = need.max().over(ibis.window(group_by=list(by)))
    return cast(ir.IntegerValue, ibis.greatest(largest.fill_null(ibis.literal(0)), 0))


def exact_float(
    value: ir.Value, scale: int | ir.IntegerValue | None = None
) -> tuple[ir.Value, ir.BooleanValue, ir.BooleanValue]:
    """Exact state for a float from its decimal text (``_finite_text``).

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
    widened = value.cast(dt.float64)
    text = widened.cast("string")
    never = cast(ir.BooleanValue, ibis.literal(False))
    dialect = current_dialect(value).name
    if dialect == "postgres":
        return text.cast(_PG_EXACT), never, never
    floating = cast(ir.FloatingValue, widened)
    if dialect in _SCALED:
        if scale is None:
            raise ValueError(f"{dialect} exact float state needs the partition's scale")
        finite = float_is_finite(floating)
        sign, digits, fraction_digits, exponent = _decimal_parts(_finite_text(value)[1])
        padding = cast(ir.IntegerValue, scale - (fraction_digits - exponent))
        zeros = cast(ir.StringValue, ibis.literal("0")).repeat(padding)
        coefficient_text = sign + digits + zeros
        if dialect == "duckdb":
            coefficient = coefficient_text.cast(_DUCKDB_EXACT)
            fits = cast(ir.BooleanValue, ibis.literal(True))
        else:
            coefficient = _sf_try_number(coefficient_text, 38, 0)
            fits = coefficient.notnull()
        held = (padding >= ibis.literal(0)) & fits
        exact = (finite & held).ifelse(coefficient, ibis.null().cast(exact_type(value)))
        nonfinite = (~finite).fill_null(ibis.literal(False))
        unrepresentable = (finite & ~held).fill_null(ibis.literal(False))
        return exact, nonfinite, unrepresentable
    if dialect != "bigquery":
        raise unavailable("exact_float", dialect)
    nonfinite = (floating.isnan() | floating.isinf()).fill_null(ibis.literal(False))
    exact = nonfinite.ifelse(ibis.null().cast(_BQ_EXACT), text.try_cast(_BQ_EXACT))
    unrepresentable = ~nonfinite & widened.notnull() & exact.isnull()
    return exact, nonfinite, unrepresentable


def exact_ratio(numerator: ir.Value, denominator: ir.Value) -> ir.Value:
    """Exact-state ``numerator / denominator``; NULL for a zero denominator.

    Not for DuckDB or Snowflake, which divide normalized significands
    (``normalized_quotient``). PostgreSQL divides to at least 16 significant
    digits; BigQuery to 38 fractional digits.
    """
    dialect = current_dialect(numerator, denominator).name
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
    dialect = current_dialect(value).name
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
    DuckDB and Snowflake parse ``coefficient || 'e-' || scale`` text, which
    their string-to-double conversions round correctly (ties to even); a
    magnitude beyond binary64's range is an infinity on DuckDB and NULL from
    Snowflake's ``TRY_TO_DOUBLE``, flagged either way. PostgreSQL and BigQuery
    hold unscaled exact state, so ``scale`` is 0 there. Magnitudes at or
    below 2**-1075 become 0.0, as round-to-nearest gives.
    """
    false = ibis.literal(False)
    never = cast(ir.BooleanValue, false)
    dialect = current_dialect(value).name
    if dialect in _SCALED:
        text = value.cast("string") + "e-" + _scale_text(scale)
        result = _binary64(text, dialect)
        out_of_range = (value.notnull() & _not_converted(result)).fill_null(false)
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


_DUCKDB_HUGE = dt.Unknown(raw_type=sge.DataType.build("HUGEINT", dialect="duckdb"))
# A normalized value holds this many leading significant digits in a wide
# integer (DuckDB's 128-bit HUGEINT, Snowflake's NUMBER(38, 0)); a quotient of
# two such significands, shifted to as many digits, stays below 10**38 and so
# within either integer's range.
_SIGNIFICAND_DIGITS = 19


@ibis.udf.scalar.builtin(name="divide", signature=((_DUCKDB_HUGE, _DUCKDB_HUGE), _DUCKDB_HUGE))
def _duckdb_huge_quotient(value, divisor):
    """DuckDB 128-bit integer division truncating toward zero."""


@ibis.udf.scalar.builtin(name="abs", signature=((_DUCKDB_HUGE,), _DUCKDB_HUGE))
def _duckdb_huge_abs(value):
    """DuckDB 128-bit integer magnitude."""


def _wide_type(dialect: str) -> dt.DataType:
    """The wide integer type holding a normalized significand."""
    if dialect == "duckdb":
        return _DUCKDB_HUGE
    if dialect == "snowflake":
        return _SF_EXACT
    raise unavailable("normalized", dialect)


def _wide(text: str, dialect: str) -> ir.Value:
    return ibis.literal(text).cast(_wide_type(dialect))


def _wide_abs(value: ir.Value, dialect: str) -> ir.Value:
    if dialect == "duckdb":
        return _duckdb_huge_abs(value)
    return cast(ir.NumericValue, value).abs()


def _wide_quotient(value: ir.Value, divisor: ir.Value, dialect: str) -> ir.Value:
    """Wide integer division truncating toward zero.

    Snowflake divides NUMBER to six fractional digits and rounds, so the
    remainder (whose sign follows the dividend) is removed first; the exact
    quotient then divides without rounding.
    """
    if dialect == "duckdb":
        return _duckdb_huge_quotient(value, divisor)
    magnitude = cast(ir.NumericValue, value)
    exact = magnitude - magnitude % cast(ir.NumericValue, divisor)
    return (exact / cast(ir.NumericValue, divisor)).cast(_SF_EXACT)


def normalized(
    value: ir.Value, scale: int | ir.IntegerValue = 0
) -> tuple[ir.Value, ir.IntegerValue]:
    """``(significand, exponent)`` with ``value · 10**-scale ≈ significand · 10**(exponent − 18)``.

    DuckDB and Snowflake. The significand is a signed wide integer holding the
    value's leading 19 significant digits, so ``10**18 ≤ |significand| < 10**19``
    (0 with exponent 0 for zero); the exponent is that of the leading digit. An
    exact coefficient normalizes from its integer text, an already rounded
    binary64 from its ``_finite_text`` (at most 18 digits, held exactly), so
    it stands for that decimal rather than its binary expansion.
    Digits past the 19th are dropped, perturbing the value by less than
    ``10**-18`` relative. A NULL or non-finite float gives NULL.
    """
    dialect = current_dialect(value).name
    wide = _wide_type(dialect)
    text = _finite_text(value)[1] if value.type().is_floating() else value.cast("string")
    sign, digits, fraction_digits, exponent = _decimal_parts(text)
    significant = cast(ir.StringValue, _ltrim(digits, "0"))
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
    dialect = current_dialect(n_significand, d_significand).name
    wide = _wide_type(dialect)
    zero = _wide("0", dialect)
    numerator = _wide_abs(n_significand, dialect)
    denominator = (d_significand != zero).ifelse(
        _wide_abs(d_significand, dialect), ibis.null().cast(wide)
    )
    # |n / d| lies in (0.1, 10): shift so the integer quotient has 19 digits.
    shift = cast(
        ir.IntegerValue,
        (numerator >= denominator).ifelse(
            ibis.literal(_SIGNIFICAND_DIGITS - 1), ibis.literal(_SIGNIFICAND_DIGITS)
        ),
    )
    zeros = cast(ir.StringValue, ibis.literal("0")).repeat(shift)
    digits = _wide_quotient((numerator.cast("string") + zeros).cast(wide), denominator, dialect)
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
    result = _binary64(text, current_dialect(significand).name)
    overflow = (significand.notnull() & _not_converted(result)).fill_null(ibis.literal(False))
    return cast(ir.FloatingValue, overflow.ifelse(ibis.null().cast(dt.float64), result)), overflow


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
    dialect = current_dialect(value).name
    if dialect == "postgres":
        return _pg_exact_sum(value)
    if dialect == "duckdb":
        return _duckdb_exact_sum(value)
    if dialect == "bigquery":
        return _bq_exact_sum(value)
    if dialect == "snowflake":
        # NUMBER(38, 0) sums natively (overflow is a query error); the cast keeps
        # Ibis's type at NUMBER(38, 0), where Ibis would widen a decimal sum's
        # scale and type later fills and comparisons as NUMBER(38, 2).
        return cast(ir.NumericColumn, value).sum().cast(_SF_EXACT)
    raise unavailable("exact_state", dialect)


def exact_subtract(left, right):
    dialect = current_dialect(left, right).name
    if dialect == "postgres":
        return _pg_exact_subtract(left, right)
    if dialect == "duckdb":
        return _duckdb_exact_subtract(left, right)
    if dialect == "bigquery":
        return _bq_exact_subtract(left, right)
    if dialect == "snowflake":
        return cast(ir.NumericValue, left) - cast(ir.NumericValue, right)
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
    dialect = current_dialect(like, value).name
    if dialect in {"postgres", "bigquery"}:
        quotient = _div(value, divisor)
    elif dialect == "duckdb":
        quotient = _duckdb_quotient(value, divisor)
    elif dialect == "snowflake":
        # Snowflake's integer division rounds to six fractional digits; the
        # remainder (sign of the dividend) is removed so the division is exact.
        quotient = (value - value % divisor) / divisor
    else:
        raise unavailable("integer_quotient", dialect)
    return cast(ir.IntegerValue, cast(ir.IntegerValue, quotient).cast(dt.int64))


@ibis.udf.scalar.builtin(name="isfinite", signature=((dt.float64,), dt.boolean))
def _duckdb_isfinite(value):
    """Native finite-state predicate."""


def float_is_finite(value) -> ir.BooleanValue:
    if current_dialect(value).name == "duckdb":
        return cast(ir.BooleanValue, _duckdb_isfinite(value))
    # PostgreSQL has no isfinite(double precision), and NaN equals NaN there.
    nonfinite = [ibis.literal(item, type=dt.float64) for item in (math.inf, -math.inf, math.nan)]
    return cast(ir.FloatingValue, value.cast(dt.float64)).notin(nonfinite)
