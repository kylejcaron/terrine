"""Numeric expressions Ibis lacks or translates inexactly, per backend.

Exact state uses an arbitrary-precision native type: DuckDB ``BIGNUM`` or
PostgreSQL ``NUMERIC``. The IEEE binary64 primitives back DuckDB's certified
float reductions only; other backends validate floats against DuckDB within a
tolerance instead.
"""

from __future__ import annotations

import math
import sys
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import sqlglot.expressions as sge

from terrine.backends.dialect import dialect_of, unavailable
from terrine.errors import RefusalSpec

__all__ = [
    "EXACT_OVERFLOW",
    "exact_is_nonfinite",
    "exact_literal",
    "exact_ratio",
    "exact_subtract",
    "exact_sum",
    "exact_to_float",
    "exact_type",
    "float_is_finite",
    "ieee_add",
    "ieee_divide",
    "ieee_multiply",
    "ieee_subtract",
    "is_exact_type",
    "next_toward",
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
    value: ir.Value,
) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
    """Round an exact value once to binary64.

    Returns (float, nonfinite, out_of_range); the float is NULL for either flag.
    Magnitudes at or below 2**-1075 become 0.0, as round-to-nearest would give.
    """
    false = ibis.literal(False)
    if dialect_of(value) == "bigquery":
        # BIGNUMERIC is finite and far inside binary64's range.
        never = cast(ir.BooleanValue, false)
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
