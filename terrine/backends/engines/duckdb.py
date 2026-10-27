"""DuckDB expression types and native identity admission."""

from typing import Any, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import sqlglot.expressions as sge

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE
from terrine.backends.dialect import Dialect
from terrine.backends.numeric import (
    _scaled_exact_to_float,
    _scaled_float_parts,
    _scaled_float_result,
)
from terrine.backends.registry import supported_engine_route

_EXACT = dt.Unknown(raw_type=sge.DataType.build("BIGNUM", dialect="duckdb"))
_HUGE = dt.Unknown(raw_type=sge.DataType.build("HUGEINT", dialect="duckdb"))


@ibis.udf.scalar.builtin(name="divide", signature=((_HUGE, _HUGE), _HUGE))
def _huge_quotient(value, divisor):
    """DuckDB 128-bit integer division truncating toward zero."""


@ibis.udf.scalar.builtin(name="abs", signature=((_HUGE,), _HUGE))
def _huge_abs(value):
    """DuckDB 128-bit integer magnitude."""


@ibis.udf.scalar.builtin(name="ltrim", signature=((dt.string, dt.string), dt.string))
def _ltrim(value, _characters):
    """Remove the given leading characters."""


@ibis.udf.agg.builtin(name="sum", signature=((_EXACT,), _EXACT))
def _exact_sum(value):
    """Native exact coefficient/count sum."""


@ibis.udf.scalar.builtin(name="subtract", signature=((_EXACT, _EXACT), _EXACT))
def _exact_subtract(left, right):
    """Native exact coefficient/count subtraction."""


@ibis.udf.scalar.builtin(name="divide", signature=((dt.int64, dt.int64), dt.int64))
def _quotient(value, divisor):
    """DuckDB integer division truncating toward zero."""


@ibis.udf.scalar.builtin(name="isfinite", signature=((dt.float64,), dt.boolean))
def _isfinite(value):
    """Native finite-state predicate."""


class DuckDBDialect(Dialect):
    __slots__ = ()
    has_scaled_float_state = True
    operations = frozenset(
        {
            "exact_state",
            "exact_float",
            "normalized",
            "integer_quotient",
            "timestamp_is_finite",
            "utc_wall_clock",
            "utc_instant",
            "timestamp_text",
        }
    )

    def exact_type(self) -> dt.DataType:
        return _EXACT

    def coefficient_bounds(self, value: ir.Value) -> tuple[int, int]:
        dtype = value.type()
        # DuckDB reports its physical HUGEINT as decimal(38, 0).
        if isinstance(dtype, dt.Decimal) and dtype.precision == 38 and dtype.scale == 0:
            return -(1 << 127), (1 << 127) - 1
        return super().coefficient_bounds(value)

    def exact_float(
        self, value: ir.Value, scale: int | ir.IntegerValue | None = None
    ) -> tuple[ir.Value, ir.BooleanValue, ir.BooleanValue]:
        text, finite, held = _scaled_float_parts(value, scale, self)
        return _scaled_float_result(text.cast(_EXACT), finite, held)

    def exact_to_float(
        self, value: ir.Value, scale: int | ir.IntegerValue = 0
    ) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
        return _scaled_exact_to_float(value, scale, self)

    def wide_type(self) -> dt.DataType:
        return _HUGE

    def wide_abs(self, value: ir.Value) -> ir.Value:
        return _huge_abs(value)

    def wide_quotient(self, value: ir.Value, divisor: ir.Value) -> ir.Value:
        return _huge_quotient(value, divisor)

    def trim_leading_zeros(self, digits: ir.StringValue) -> ir.StringValue:
        return cast(ir.StringValue, _ltrim(digits, "0"))

    def exact_sum(self, value: ir.Value) -> ir.Value:
        return _exact_sum(value)

    def exact_subtract(self, left: ir.Value, right: ir.Value) -> ir.Value:
        return _exact_subtract(left, right)

    def truncating_quotient(
        self, value: ir.IntegerValue, divisor: int | ir.IntegerValue
    ) -> ir.IntegerValue:
        quotient = _quotient(value, divisor)
        return cast(ir.IntegerValue, cast(ir.IntegerValue, quotient).cast(dt.int64))

    def float_is_finite(self, value: ir.Value) -> ir.BooleanValue:
        return cast(ir.BooleanValue, _isfinite(value))


def identify(con: Any) -> None:
    """Validate the native handle without reading or changing connection state."""
    try:
        import duckdb
    except ImportError:
        raw = None
    else:
        raw = next(
            (
                candidate
                for candidate in (getattr(con, "con", None), getattr(con, "_con", None), con)
                if isinstance(candidate, duckdb.DuckDBPyConnection)
            ),
            None,
        )
    if raw is None:
        raise _CAPABILITY_UNAVAILABLE.error(
            backend=str(getattr(con, "name", type(con).__name__)).casefold(),
            operation="query",
            status="unverified",
            route=supported_engine_route("query"),
        )
