"""Snowflake expression types and static module identity admission."""

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
from terrine.backends.registry import ENGINES, supported_engine_route

# Sums and differences beyond 38 digits remain exact-overflow query errors.
_EXACT = dt.Decimal(38, 0)
_DECFLOAT = dt.Unknown(raw_type=sge.DataType.build("DECFLOAT", dialect="snowflake"))


@ibis.udf.scalar.builtin(name="ltrim", signature=((dt.string, dt.string), dt.string))
def _ltrim(value, _characters):
    """Remove the given leading characters."""


@ibis.udf.scalar.builtin(name="to_decfloat", signature=((dt.float64,), _DECFLOAT))
def _decfloat(value):
    """A binary64 as a Snowflake DECFLOAT: its value correctly rounded to 18
    significant digits (measured), which always round-trips."""


@ibis.udf.scalar.builtin(name="try_to_number", signature=((dt.string, dt.int64, dt.int64), _EXACT))
def _try_number(text, precision, scale):
    """Snowflake NUMBER(precision, scale) from decimal text; NULL when it does not fit."""


@ibis.udf.scalar.builtin(name="try_to_double", signature=((dt.string,), dt.float64))
def _try_double(text):
    """Snowflake binary64 from decimal text, correctly rounded (measured on halfway,
    subnormal and 19-digit near-halfway cases); NULL beyond binary64's range."""


class SnowflakeDialect(Dialect):
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

    def finite_text(self, value: ir.Value) -> tuple[ir.FloatingValue, ir.StringValue]:
        # VARCHAR, VARIANT and JSON keep at most 15 or 16 significant digits;
        # DECFLOAT keeps 18, correctly rounded within 0.5 · 10**-17 relative.
        widened = cast(ir.FloatingValue, value.cast(dt.float64))
        finite = self.float_is_finite(widened).ifelse(widened, ibis.null(dt.float64))
        return widened, _decfloat(finite).cast("string").lower()

    def binary64(self, text: ir.StringValue) -> ir.FloatingValue:
        return cast(ir.FloatingValue, _try_double(text))

    def exact_float(
        self, value: ir.Value, scale: int | ir.IntegerValue | None = None
    ) -> tuple[ir.Value, ir.BooleanValue, ir.BooleanValue]:
        text, finite, held = _scaled_float_parts(value, scale, self)
        coefficient = _try_number(text, 38, 0)
        return _scaled_float_result(coefficient, finite, held & coefficient.notnull())

    def exact_to_float(
        self, value: ir.Value, scale: int | ir.IntegerValue = 0
    ) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
        return _scaled_exact_to_float(value, scale, self)

    def wide_type(self) -> dt.DataType:
        return _EXACT

    def wide_quotient(self, value: ir.Value, divisor: ir.Value) -> ir.Value:
        # Division rounds to six fractional digits: remove the remainder,
        # whose sign follows the dividend, so the quotient is exact.
        magnitude = cast(ir.NumericValue, value)
        exact = magnitude - magnitude % cast(ir.NumericValue, divisor)
        return (exact / cast(ir.NumericValue, divisor)).cast(_EXACT)

    def trim_leading_zeros(self, digits: ir.StringValue) -> ir.StringValue:
        return cast(ir.StringValue, _ltrim(digits, "0"))

    def exact_sum(self, value: ir.Value) -> ir.Value:
        # Keep NUMBER(38, 0); Ibis otherwise widens the scale and types later
        # fills and comparisons as NUMBER(38, 2).
        return cast(ir.NumericColumn, value).sum().cast(_EXACT)

    def exact_subtract(self, left: ir.Value, right: ir.Value) -> ir.Value:
        return cast(ir.NumericValue, left) - cast(ir.NumericValue, right)

    def truncating_quotient(
        self, value: ir.IntegerValue, divisor: int | ir.IntegerValue
    ) -> ir.IntegerValue:
        # Remove the signed remainder before Snowflake's rounded division.
        quotient = (value - value % divisor) / divisor
        return cast(ir.IntegerValue, cast(ir.IntegerValue, quotient).cast(dt.int64))


def identify(con: Any) -> None:
    """Module identity is the existing evidence; no server query is required."""
    if type(con).__module__ != ENGINES["snowflake"].ibis_module:
        raise _CAPABILITY_UNAVAILABLE.error(
            backend=str(getattr(con, "name", type(con).__name__)).casefold(),
            operation="query",
            status="unverified",
            route=supported_engine_route("query"),
        )
