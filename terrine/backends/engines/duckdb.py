"""DuckDB expression types and native identity admission."""

from importlib.metadata import version
from typing import Any, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import sqlglot.expressions as sge

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE
from terrine.backends.dialect import Dialect, unavailable
from terrine.backends.numeric import (
    _scaled_exact_to_float,
    _scaled_float_parts,
    _scaled_float_result,
    _scaled_text,
)
from terrine.backends.registry import supported_engine_route
from terrine.backends.timestamps import _UNSUPPORTED, _epoch, native_day_start, native_local_day

_EXACT = dt.Unknown(raw_type=sge.DataType.build("BIGNUM", dialect="duckdb"))
_HUGE = dt.Unknown(raw_type=sge.DataType.build("HUGEINT", dialect="duckdb"))
_KEY = dt.Struct.from_tuples((("rank", dt.int8), ("seconds", dt.int64), ("nanos", dt.int64)))


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


@ibis.udf.scalar.builtin(
    name="timezone", signature=((dt.string, dt.Timestamp(timezone="UTC")), dt.timestamp)
)
def _utc_naive(zone, timestamp):
    """Remove a timestamp's zone explicitly in UTC, independent of session timezone."""


@ibis.udf.scalar.builtin(
    name="timezone", signature=((dt.string, dt.timestamp), dt.Timestamp(timezone="UTC"))
)
def _utc_aware(zone, timestamp):
    """Attach UTC explicitly to a naive UTC wall-clock value."""


@ibis.udf.scalar.builtin(name="isfinite")
def _timestamp_isfinite(value) -> bool:
    """Native timestamp finiteness predicate."""
    raise NotImplementedError


class DuckDBDialect(Dialect):
    __slots__ = ()
    bounds_accumulations = False
    has_scaled_float_state = True
    has_native_asof = True
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

    def exact_ratio(self, numerator: ir.Value, denominator: ir.Value) -> ir.Value:
        # Quotients divide normalized significands; the portable default is not native.
        raise unavailable("exact_ratio", self.name)

    def exact_is_nonfinite(self, value: ir.Value) -> ir.BooleanValue:
        raise unavailable("exact_is_nonfinite", self.name)

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

    def exact_sum(self, value: ir.Value, *, admitted: bool = False) -> ir.Value:
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

    def timestamp_is_finite(self, value: ir.TimestampValue) -> ir.BooleanValue:
        return cast(ir.BooleanValue, _timestamp_isfinite(value))

    def timestamp_extremes(
        self, dtype: dt.Timestamp
    ) -> tuple[ir.TimestampScalar, ir.TimestampScalar]:
        return (
            cast(ir.TimestampScalar, ibis.literal("-infinity").cast(dtype)),
            cast(ir.TimestampScalar, ibis.literal("infinity").cast(dtype)),
        )

    def utc_wall_clock(self, value: ir.Value) -> ir.TimestampValue:
        # A naive value already is its UTC wall clock; timezone('UTC', naive) would
        # attach the zone instead and leave later date extraction to the session.
        if cast(dt.Timestamp, value.type()).timezone is None:
            return cast(ir.TimestampValue, value)
        return cast(ir.TimestampValue, _utc_naive("UTC", value))

    def utc_instant(self, value: ir.Value) -> ir.TimestampValue:
        if cast(dt.Timestamp, value.type()).timezone == "UTC":
            return cast(ir.TimestampValue, value)
        return cast(ir.TimestampValue, _utc_aware("UTC", value))

    def local_day(
        self, timestamp: ir.TimestampValue, offset_minutes: int
    ) -> tuple[ir.DateValue, ir.TimestampValue]:
        return native_local_day(self, timestamp, offset_minutes)

    def day_start(self, timestamp: ir.TimestampValue, offset_minutes: int) -> ir.TimestampValue:
        return native_day_start(self, timestamp, offset_minutes)

    def timestamp_text(self, value: ir.TimestampValue) -> ir.StringValue:
        return value.cast(dt.string)

    def timestamp_parts(
        self, value: ir.TimestampValue
    ) -> tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue]:
        key = self.timestamp_order_key(value)
        return (
            cast(ir.IntegerValue, key["rank"]),
            cast(ir.IntegerValue, key["seconds"]),
            cast(ir.IntegerValue, key["nanos"]),
        )

    def timestamp_order_key(self, value: ir.TimestampValue) -> ir.StructValue:
        """Order signed seconds/remainders, with infinity ranks and a NULL struct."""
        dtype = value.type()
        if not isinstance(dtype, dt.Timestamp):
            raise _UNSUPPORTED.error(dtype=str(dtype))
        scale = dtype.scale if dtype.scale is not None else 6
        if scale == 9:
            unit, divisor, multiplier = "nanosecond", 10**9, 1
        elif scale == 6:
            unit, divisor, multiplier = "microsecond", 10**6, 1000
        else:
            raise _UNSUPPORTED.error(dtype=str(dtype))
        finite = self.timestamp_is_finite(value)
        epoch = _epoch(dtype)
        instant = cast(ir.TimestampValue, finite.ifelse(value, epoch))
        coefficient = instant.delta(epoch, unit=unit)
        seconds = self.truncating_quotient(coefficient, divisor)
        # Ibis renders a nanosecond delta as a bare subtraction, so `delta % n` would
        # bind as `a - (b % n)`; the remainder is taken by subtraction instead.
        nanos = (coefficient - seconds * divisor) * multiplier
        zero = ibis.literal(0)
        key = ibis.struct(
            {
                "rank": finite.ifelse(
                    zero, (value < epoch).ifelse(ibis.literal(-1), ibis.literal(1))
                ).cast("int8"),
                "seconds": finite.ifelse(seconds, zero).cast("int64"),
                "nanos": finite.ifelse(nanos, zero).cast("int64"),
            }
        )
        return cast(ir.StructValue, value.isnull().ifelse(ibis.null().cast(_KEY), key))

    def reverse_time_key(self, value: ir.TimestampValue) -> ir.Value:
        key = self.timestamp_order_key(value)
        return ibis.struct(
            {
                "rank": -cast(ir.IntegerValue, key["rank"]),
                "seconds": -cast(ir.IntegerValue, key["seconds"]),
                "nanos": -cast(ir.IntegerValue, key["nanos"]),
            }
        )

    def bounded_nanoseconds(self, scale: int | None) -> bool:
        return scale == 9

    def narrow_exact(
        self, coefficient: ir.Value, scale: int, target: str | dt.DataType
    ) -> ir.Value:
        return _scaled_text(coefficient, scale).try_cast(target)

    def asof_join(
        self,
        left: ibis.Table,
        right: ibis.Table,
        *,
        left_unit: str,
        left_key: str,
        right_unit: str,
        right_key: str,
        inclusive: bool,
    ) -> ibis.Table:
        on = left[left_key] >= right[right_key] if inclusive else left[left_key] > right[right_key]
        return left.asof_join(right, on=on, predicates=[left[left_unit] == right[right_unit]])

    def least(self, *values: Any) -> Any:
        return ibis.least(*values)

    def greatest(self, *values: Any) -> Any:
        return ibis.greatest(*values)

    def supports_exact_prefix(self, backend_name: str) -> bool:
        return backend_name == self.name and version("duckdb") == "1.5.6"


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
