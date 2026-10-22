"""Numeric expressions Ibis lacks or translates inexactly, per backend.

Exact state uses an arbitrary-precision native type: DuckDB ``BIGNUM`` or
PostgreSQL ``NUMERIC``. The IEEE binary64 primitives back DuckDB's certified
float reductions only; other backends validate floats against DuckDB within a
tolerance instead.
"""

from __future__ import annotations

import math
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import sqlglot.expressions as sge

from terrine.backends.dialect import dialect_of, unavailable

__all__ = [
    "exact_literal",
    "exact_subtract",
    "exact_sum",
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

_DUCKDB_EXACT = dt.Unknown(raw_type=sge.DataType.build("BIGNUM", dialect="duckdb"))
_PG_EXACT = dt.Unknown(raw_type=sge.DataType.build("NUMERIC", dialect="postgres"))


def exact_type(*like: ir.Value) -> dt.DataType:
    dialect = dialect_of(*like)
    if dialect == "postgres":
        return _PG_EXACT
    if dialect == "duckdb":
        return _DUCKDB_EXACT
    raise unavailable("exact_state", dialect)


def is_exact_type(dtype: dt.DataType) -> bool:
    """Whether ``dtype`` is an internal exact-state type on any backend."""
    return dtype in (_DUCKDB_EXACT, _PG_EXACT)


def exact_literal(value: int, *like: ir.Value) -> ir.Value:
    """An exact native integer, without an inferred fixed-width literal type."""
    return ibis.literal(str(value)).cast(exact_type(*like))


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


def exact_sum(value):
    dialect = dialect_of(value)
    if dialect == "postgres":
        return _pg_exact_sum(value)
    if dialect == "duckdb":
        return _duckdb_exact_sum(value)
    raise unavailable("exact_state", dialect)


def exact_subtract(left, right):
    dialect = dialect_of(left, right)
    if dialect == "postgres":
        return _pg_exact_subtract(left, right)
    if dialect == "duckdb":
        return _duckdb_exact_subtract(left, right)
    raise unavailable("exact_state", dialect)


@ibis.udf.scalar.builtin(name="divide", signature=((dt.int64, dt.int64), dt.int64))
def _duckdb_quotient(value, divisor):
    """DuckDB integer division truncating toward zero."""


@ibis.udf.scalar.builtin(name="div", signature=((dt.int64, dt.int64), dt.int64))
def _pg_quotient(value, divisor):
    """PostgreSQL integer division truncating toward zero."""


def truncating_quotient(
    value: ir.IntegerValue, divisor: int | ir.IntegerValue, *, like: ir.Value
) -> ir.IntegerValue:
    """Integer division truncating toward zero; Ibis ``//`` floors through doubles."""
    dialect = dialect_of(like, value)
    if dialect == "postgres":
        quotient = _pg_quotient(value, divisor)
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
