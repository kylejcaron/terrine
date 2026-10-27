"""PostgreSQL expression types and qualified server identity admission."""

import re
import sys
from importlib.metadata import version
from typing import Any, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import sqlglot.expressions as sge

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE, _SNAPSHOT_UNAVAILABLE
from terrine.backends.dialect import Dialect, current_dialect, unavailable
from terrine.backends.numeric import _checked_narrow_exact
from terrine.backends.registry import supported_engine_route
from terrine.backends.timestamps import _UNSUPPORTED, _epoch
from terrine.errors import RefusalSpec

_EXACT = dt.Unknown(raw_type=sge.DataType.build("NUMERIC", dialect="postgres"))
_MIN_IBIS = (12, 0)
_VERIFIED_SERVER_MAJOR = 16
_DIALECT_ADMISSION = RefusalSpec(
    "terrine.backend.capability_unavailable",
    "The detected database server is not verified for PostgreSQL execution. {route}",
)

# Binary64's largest finite value, and 2**-1075: magnitudes at or below it round
# to zero, which PostgreSQL's float8 input rejects instead of returning.
_FLOAT64_MAX = str(int(sys.float_info.max))
_FLOAT64_ROUNDS_TO_ZERO = "0." + str(5**1075).rjust(1075, "0")
_NONFINITE = ("NaN", "Infinity", "-Infinity")


@ibis.udf.scalar.builtin(
    name="numeric_div", database="pg_catalog", signature=((_EXACT, _EXACT), _EXACT)
)
def _exact_divide(numerator, denominator):
    """PostgreSQL NUMERIC division to at least 16 significant digits."""


@ibis.udf.agg.builtin(name="sum", signature=((_EXACT,), _EXACT))
def _exact_sum(value):
    """PostgreSQL exact NUMERIC sum."""


@ibis.udf.scalar.builtin(
    name="numeric_sub", database="pg_catalog", signature=((_EXACT, _EXACT), _EXACT)
)
def _exact_subtract(left, right):
    """PostgreSQL exact NUMERIC subtraction."""


@ibis.udf.scalar.builtin(name="div", signature=((dt.int64, dt.int64), dt.int64))
def _quotient(value, divisor):
    """PostgreSQL integer division truncating toward zero."""


def _exact_text(text: str) -> ir.Value:
    return ibis.literal(text).cast(_EXACT)


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


@ibis.udf.scalar.builtin(
    name="extract",
    database="pg_catalog",
    signature=((dt.string, dt.Timestamp(timezone="UTC")), dt.Decimal(38, 6)),
)
def _pg_extract(field, value):
    """PostgreSQL's numeric field extraction; see ``_pg_epoch_seconds`` for epoch."""


# PostgreSQL's epoch extraction is exact only while the instant's Unix microseconds
# fit BIGINT (until about year 294247); later timestamps, valid through 294276,
# round. Those are shifted back by a whole number of hours, which needs no time
# zone, extracted exactly, and shifted forward again in NUMERIC.
_PG_LATE_YEAR = 290_000
_PG_SHIFT_HOURS = 100_000_000


def _pg_epoch_seconds(value: ir.TimestampValue) -> ir.DecimalValue:
    """Exact NUMERIC seconds since the Unix epoch for a finite PostgreSQL timestamp."""
    year = cast(ir.NumericColumn, _pg_extract("year", value))
    late = cast(ir.BooleanValue, year >= _PG_LATE_YEAR)
    shifted = cast(ir.TimestampValue, value - ibis.interval(hours=_PG_SHIFT_HOURS))
    return cast(
        ir.DecimalValue,
        late.ifelse(
            cast(ir.DecimalValue, _pg_extract("epoch", shifted)) + _PG_SHIFT_HOURS * 3600,
            _pg_extract("epoch", value),
        ),
    )


@ibis.udf.scalar.builtin(name="trunc", signature=((dt.Decimal(38, 6),), dt.Decimal(38, 0)))
def _pg_trunc(value):
    """PostgreSQL NUMERIC truncation toward zero."""


@ibis.udf.scalar.builtin(name="isfinite")
def _timestamp_isfinite(value) -> bool:
    """Native timestamp finiteness predicate."""
    raise NotImplementedError


class PostgresDialect(Dialect):
    __slots__ = ()
    bounds_accumulations = False
    has_native_asof = True
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

    def exact_float(
        self, value: ir.Value, scale: int | ir.IntegerValue | None = None
    ) -> tuple[ir.Value, ir.BooleanValue, ir.BooleanValue]:
        text = value.cast(dt.float64).cast("string")
        never = cast(ir.BooleanValue, ibis.literal(False))
        return text.cast(_EXACT), never, never

    def exact_ratio(self, numerator: ir.Value, denominator: ir.Value) -> ir.Value:
        zero = _exact_text("0")
        divisor = (denominator == zero).ifelse(ibis.null().cast(_EXACT), denominator)
        return _exact_divide(numerator, divisor)

    def exact_is_nonfinite(self, value: ir.Value) -> ir.BooleanValue:
        return value.isin([_exact_text(text) for text in _NONFINITE])

    def exact_to_float(
        self, value: ir.Value, scale: int | ir.IntegerValue = 0
    ) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
        if not isinstance(scale, int) or scale:
            raise ValueError(f"{self.name} exact state is unscaled; scale must be 0")
        # NUMERIC NaN equals itself and sorts above every number, so test it first.
        nonfinite = self.exact_is_nonfinite(value)
        limit, tiny = _exact_text(_FLOAT64_MAX), _exact_text(_FLOAT64_ROUNDS_TO_ZERO)
        negative_limit = _exact_text("-" + _FLOAT64_MAX)
        negative_tiny = _exact_text("-" + _FLOAT64_ROUNDS_TO_ZERO)
        out_of_range = ~nonfinite & ((value > limit) | (value < negative_limit))
        rounds_to_zero = (value <= tiny) & (value >= negative_tiny)
        missing = ibis.null().cast(dt.float64)
        result = ibis.cases(
            (nonfinite | out_of_range, missing),
            (rounds_to_zero, ibis.literal(0.0)),
            else_=value.cast(dt.float64),
        )
        false = ibis.literal(False)
        return (
            cast(ir.FloatingValue, result),
            nonfinite.fill_null(false),
            out_of_range.fill_null(false),
        )

    def exact_sum(self, value: ir.Value) -> ir.Value:
        return _exact_sum(value)

    def exact_subtract(self, left: ir.Value, right: ir.Value) -> ir.Value:
        return _exact_subtract(left, right)

    def wide_type(self) -> dt.DataType:
        # Quotients divide in NUMERIC; the normalized significand path is not native.
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
        return cast(ir.BooleanValue, _timestamp_isfinite(value))

    def timestamp_extremes(
        self, dtype: dt.Timestamp
    ) -> tuple[ir.TimestampScalar, ir.TimestampScalar]:
        return (
            cast(ir.TimestampScalar, ibis.literal("-infinity").cast(dtype)),
            cast(ir.TimestampScalar, ibis.literal("infinity").cast(dtype)),
        )

    def utc_wall_clock(self, value: ir.Value) -> ir.TimestampValue:
        return cast(ir.TimestampValue, _utc_naive("UTC", value))

    def utc_instant(self, value: ir.Value) -> ir.TimestampValue:
        return cast(ir.TimestampValue, _utc_aware("UTC", value))

    def timestamp_text(self, value: ir.TimestampValue) -> ir.StringValue:
        return value.cast(dt.string)

    def timestamp_parts(
        self, value: ir.TimestampValue
    ) -> tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue]:
        dtype = value.type()
        if not isinstance(dtype, dt.Timestamp) or dtype.scale not in (None, 6):
            raise _UNSUPPORTED.error(dtype=str(dtype))
        finite = self.timestamp_is_finite(value)
        epoch = _epoch(dtype)
        epoch_seconds = _pg_epoch_seconds(cast(ir.TimestampValue, finite.ifelse(value, epoch)))
        # Split before narrowing: whole seconds fit BIGINT for every PostgreSQL
        # timestamp, while the full microsecond coefficient does not.
        whole = cast(ir.DecimalValue, _pg_trunc(epoch_seconds))
        seconds = cast(ir.IntegerValue, whole.cast(dt.int64))
        nanos = cast(ir.IntegerValue, ((epoch_seconds - whole) * 10**6).cast(dt.int64)) * 1000
        zero = ibis.literal(0, type=dt.int64)
        rank = finite.ifelse(zero, (value < epoch).ifelse(ibis.literal(-1), ibis.literal(1)))
        missing = value.isnull()

        def part(expression: ir.Value) -> ir.IntegerValue:
            return cast(
                ir.IntegerValue, missing.ifelse(ibis.null(dt.int64), expression.cast(dt.int64))
            )

        return (
            part(rank),
            part(finite.ifelse(seconds, zero)),
            part(finite.ifelse(nanos, zero)),
        )

    def timestamp_order_key(self, value: ir.TimestampValue) -> ir.Value:
        dtype = value.type()
        if not isinstance(dtype, dt.Timestamp) or dtype.scale not in (None, 6):
            raise _UNSUPPORTED.error(dtype=str(dtype))
        return value

    def narrow_exact(
        self, coefficient: ir.Value, scale: int, target: str | dt.DataType
    ) -> ir.Value:
        return _checked_narrow_exact(coefficient, scale, target)

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
        return backend_name == self.name and current_dialect().name == backend_name


def raw_connection(con: Any) -> Any:
    raw = getattr(con, "con", None)
    if raw is None or type(raw).__module__.split(".")[0] != "psycopg":
        raise _SNAPSHOT_UNAVAILABLE.error(
            reason="connection is not a psycopg-backed Ibis PostgreSQL backend"
        )
    return raw


def _classify(version_string: str) -> tuple[str, str]:
    """Admit only PostgreSQL 16, the version measured by the compatibility suite."""
    lowered = version_string.casefold()
    if "redshift" in lowered:
        return "Redshift", "unsupported"
    if "greenplum" in lowered:
        return "Greenplum", "unsupported"
    if "cockroachdb" in lowered:
        return "CockroachDB", "unsupported"
    match = re.match(r"^PostgreSQL\s+(\d+)(?:\.\d+)?\b", version_string)
    if match is not None:
        major = int(match.group(1))
        return "PostgreSQL", "admitted" if major == _VERIFIED_SERVER_MAJOR else "unverified"
    server = version_string.split(maxsplit=1)[0] if version_string.strip() else "unknown"
    return server, "unverified"


def identify(con: Any) -> None:
    """Check the Ibis floor before reading the built-in server version function."""
    # Ibis 11 places PostgreSQL as-of unit filters after the nearest-row selection.
    parts = tuple(int(part) for part in re.findall(r"\d+", version("ibis-framework"))[:2])
    if parts < _MIN_IBIS:
        raise _CAPABILITY_UNAVAILABLE.error(
            backend="postgres",
            operation="dialect_admission",
            status="unsupported",
            route="Install terrine[postgres], which requires ibis-framework 12 or newer.",
        )
    with raw_connection(con).cursor() as cursor:
        cursor.execute("SELECT pg_catalog.version()")
        rows = cursor.fetchall()
    version_string = str(rows[0][0]) if rows and rows[0] else ""
    server, status = _classify(version_string)
    if status != "admitted":
        raise _DIALECT_ADMISSION.error(
            backend=server,
            operation="dialect_admission",
            status=status,
            route=supported_engine_route("dialect_admission"),
        )
