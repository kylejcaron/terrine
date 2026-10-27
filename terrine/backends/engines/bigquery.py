"""BigQuery expression types and static module identity admission."""

from typing import Any, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import sqlglot.expressions as sge

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE
from terrine.backends.dialect import Dialect, current_dialect, unavailable
from terrine.backends.numeric import _scaled_text
from terrine.backends.registry import ENGINES, supported_engine_route
from terrine.backends.timestamps import _UNSUPPORTED, _epoch

# BIGNUMERIC holds integers to about 5.7e38 and 38 fractional digits. Overflow
# is an error (sums) or NULL (SAFE_ arithmetic), both surfaced as exact_overflow.
_EXACT = dt.Unknown(raw_type=sge.DataType.build("BIGNUMERIC", dialect="bigquery"))


@ibis.udf.scalar.builtin(name="safe_divide", signature=((_EXACT, _EXACT), _EXACT))
def _exact_divide(numerator, denominator):
    """BigQuery BIGNUMERIC division to 38 fractional digits; NULL for zero."""


@ibis.udf.scalar.builtin(name="safe_multiply", signature=((_EXACT, _EXACT), _EXACT))
def _exact_multiply(left, right):
    """BigQuery exact BIGNUMERIC product; NULL on overflow."""


@ibis.udf.agg.builtin(name="sum", signature=((_EXACT,), _EXACT))
def _exact_sum(value):
    """BigQuery BIGNUMERIC sum; overflow is a query error."""


@ibis.udf.scalar.builtin(name="safe_subtract", signature=((_EXACT, _EXACT), _EXACT))
def _exact_subtract(left, right):
    """BigQuery BIGNUMERIC subtraction; NULL on overflow."""


@ibis.udf.scalar.builtin(name="div", signature=((dt.int64, dt.int64), dt.int64))
def _quotient(value, divisor):
    """BigQuery integer division truncating toward zero."""


class BigQueryDialect(Dialect):
    __slots__ = ()
    bounds_accumulations = False
    operations = frozenset(
        {
            "exact_state",
            "exact_float",
            "exact_ratio",
            "exact_is_nonfinite",
            "integer_quotient",
            "timestamp_is_finite",
            "utc_wall_clock",
            "utc_instant",
            "timestamp_text",
            "asof_join",
            "least",
            "greatest",
            "integer_sequence",
        }
    )

    def exact_type(self) -> dt.DataType:
        return _EXACT

    def coefficient_type(self) -> dt.DataType:
        return _EXACT

    def decimal_coefficient(self, value: ir.Value, scale: int) -> ir.Value:
        exact = value.cast(_EXACT)
        return _exact_multiply(exact, ibis.literal(str(10**scale)).cast(_EXACT)) if scale else exact

    def exact_float(
        self, value: ir.Value, scale: int | ir.IntegerValue | None = None
    ) -> tuple[ir.Value, ir.BooleanValue, ir.BooleanValue]:
        widened = value.cast(dt.float64)
        text = widened.cast("string")
        floating = cast(ir.FloatingValue, widened)
        nonfinite = (floating.isnan() | floating.isinf()).fill_null(ibis.literal(False))
        exact = nonfinite.ifelse(ibis.null().cast(_EXACT), text.try_cast(_EXACT))
        unrepresentable = ~nonfinite & widened.notnull() & exact.isnull()
        return exact, nonfinite, unrepresentable

    def exact_ratio(self, numerator: ir.Value, denominator: ir.Value) -> ir.Value:
        return _exact_divide(numerator, denominator)

    def exact_is_nonfinite(self, value: ir.Value) -> ir.BooleanValue:
        return cast(
            ir.BooleanValue, value.isnull().ifelse(ibis.null(dt.boolean), ibis.literal(False))
        )

    def exact_to_float(
        self, value: ir.Value, scale: int | ir.IntegerValue = 0
    ) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
        if not isinstance(scale, int) or scale:
            raise ValueError(f"{self.name} exact state is unscaled; scale must be 0")
        # BIGNUMERIC is finite and far inside binary64's range.
        never = cast(ir.BooleanValue, ibis.literal(False))
        return cast(ir.FloatingValue, value.cast(dt.float64)), never, never

    def exact_sum(self, value: ir.Value) -> ir.Value:
        return _exact_sum(value)

    def exact_subtract(self, left: ir.Value, right: ir.Value) -> ir.Value:
        return _exact_subtract(left, right)

    def wide_type(self) -> dt.DataType:
        # Quotients divide in BIGNUMERIC; the normalized significand path is not native.
        raise unavailable("normalized", self.name)

    def wide_quotient(self, value: ir.Value, divisor: ir.Value) -> ir.Value:
        raise unavailable("normalized", self.name)

    def trim_leading_zeros(self, digits: ir.StringValue) -> ir.StringValue:
        raise unavailable("normalized", self.name)

    def truncating_quotient(
        self, value: ir.IntegerValue, divisor: int | ir.IntegerValue
    ) -> ir.IntegerValue:
        quotient = _quotient(value, divisor)
        return cast(ir.IntegerValue, cast(ir.IntegerValue, quotient).cast(dt.int64))

    def timestamp_is_finite(self, value: ir.TimestampValue) -> ir.BooleanValue:
        return cast(ir.BooleanValue, ibis.literal(True))

    def timestamp_extremes(
        self, dtype: dt.Timestamp
    ) -> tuple[ir.TimestampScalar, ir.TimestampScalar]:
        return (
            cast(ir.TimestampScalar, ibis.literal("0001-01-01 00:00:00").cast(dtype)),
            cast(ir.TimestampScalar, ibis.literal("9999-12-31 23:59:59.999999").cast(dtype)),
        )

    def utc_wall_clock(self, value: ir.Value) -> ir.TimestampValue:
        # TIMESTAMP -> DATETIME converts in UTC; terrine's session sets no time zone.
        return cast(ir.TimestampValue, value.cast(dt.Timestamp()))

    def utc_instant(self, value: ir.Value) -> ir.TimestampValue:
        return cast(ir.TimestampValue, value.cast(dt.Timestamp(timezone="UTC")))

    def timestamp_text(self, value: ir.TimestampValue) -> ir.StringValue:
        return value.cast(dt.string)

    def timestamp_parts(
        self, value: ir.TimestampValue
    ) -> tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue]:
        """Finite microsecond instants in years 1-9999 have BIGINT epoch microseconds."""
        dtype = value.type()
        if not isinstance(dtype, dt.Timestamp) or dtype.scale not in (None, 6):
            raise _UNSUPPORTED.error(dtype=str(dtype))
        micros = value.delta(_epoch(dtype), unit="microsecond")
        seconds = self.truncating_quotient(micros, 10**6)
        nanos = cast(ir.IntegerValue, (micros - seconds * 10**6) * 1000)
        rank = value.isnull().ifelse(ibis.null(dt.int64), ibis.literal(0, type=dt.int64))
        return cast(ir.IntegerValue, rank), seconds, nanos

    def timestamp_order_key(self, value: ir.TimestampValue) -> ir.Value:
        dtype = value.type()
        if not isinstance(dtype, dt.Timestamp) or dtype.scale not in (None, 6):
            raise _UNSUPPORTED.error(dtype=str(dtype))
        return value

    def narrow_exact(
        self, coefficient: ir.Value, scale: int, target: str | dt.DataType
    ) -> ir.Value:
        return _scaled_text(coefficient, scale).try_cast(target)

    def supports_exact_prefix(self, backend_name: str) -> bool:
        return backend_name == self.name and current_dialect().name == backend_name


def identify(con: Any) -> None:
    """Module identity is the existing evidence; no server query is required."""
    if type(con).__module__ != ENGINES["bigquery"].ibis_module:
        raise _CAPABILITY_UNAVAILABLE.error(
            backend=str(getattr(con, "name", type(con).__name__)).casefold(),
            operation="query",
            status="unverified",
            route=supported_engine_route("query"),
        )
