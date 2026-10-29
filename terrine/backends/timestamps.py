"""Lossless timestamp ordering, exact elapsed-offset comparisons and UTC conversion.

DuckDB keys finite instants by truncated seconds and a signed remainder taken from
Ibis's exact microsecond/nanosecond ``delta``, and compares a microsecond point
with a microsecond state natively; PostgreSQL timestamps are exact at
microseconds and are their own key; BigQuery and Snowflake instants are finite
within years 1 to 9999, their own key, and split into seconds and nanoseconds
from an exact epoch delta.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.dialect import Dialect, current_dialect, dialect_for_name
from terrine.backends.numeric import truncating_quotient
from terrine.errors import RefusalSpec

__all__ = [
    "compare_timestamps",
    "day_start",
    "has_submicrosecond",
    "local_day",
    "lookup_keys",
    "lossless_scale",
    "lossless_timestamp",
    "timestamp_extremes",
    "timestamp_is_finite",
    "timestamp_order_key",
    "timestamp_parts",
    "timestamp_text",
    "utc_instant",
    "utc_wall_clock",
]

_UNSUPPORTED = RefusalSpec(
    "terrine.temporal.timestamp_order_unsupported",
    "Event times of type {dtype!r} cannot be ordered without losing precision. "
    "Convert the times losslessly to a supported microsecond or nanosecond time type "
    "before adding the table to the model.",
)


def lossless_scale(scale: int | None) -> Literal[6, 9] | None:
    """The supported timestamp scale holding every instant of ``scale`` exactly:
    the native scale for ``None``, microseconds up to 6 and nanoseconds above."""
    if scale is None:
        return None
    return 6 if scale <= 6 else 9


def lossless_timestamp(value: ir.TimestampValue) -> ir.TimestampValue:
    """``value`` at the supported scale that holds it exactly."""
    dtype = cast(dt.Timestamp, value.type())
    scale = lossless_scale(dtype.scale)
    if scale == dtype.scale:
        return value
    promoted = dt.Timestamp(timezone=dtype.timezone, scale=scale, nullable=dtype.nullable)
    return cast(ir.TimestampValue, value.cast(promoted))


def timestamp_is_finite(value: ir.TimestampValue) -> ir.BooleanValue:
    return current_dialect(value).timestamp_is_finite(value)


def timestamp_extremes(dtype: dt.Timestamp) -> tuple[ir.TimestampScalar, ir.TimestampScalar]:
    """The lowest and highest values of ``dtype``: infinities where the backend has
    them (DuckDB, PostgreSQL), else the year 1 and year 9999 limits at the type's
    precision (BigQuery microseconds; Snowflake its declared scale, 9 by default)."""
    return current_dialect().timestamp_extremes(dtype)


def has_submicrosecond(value: ir.TimestampValue) -> ir.BooleanValue:
    """Whether a finite instant has nonzero nanoseconds below its microsecond."""
    dtype = cast(dt.Timestamp, value.type())
    finite = timestamp_is_finite(value)
    epoch = _epoch(dtype)
    instant = cast(ir.TimestampValue, finite.ifelse(value, epoch))
    coefficient = instant.delta(epoch, unit="nanosecond")
    # Subtraction, not `%`: Ibis renders this delta as a bare `a - b`.
    below = coefficient - truncating_quotient(coefficient, 1000, like=value) * 1000
    return value.notnull() & finite & (below != 0)


def utc_wall_clock(value: ir.Value) -> ir.TimestampValue:
    """An instant's naive UTC wall clock, independent of the session time zone."""
    return current_dialect(value).utc_wall_clock(value)


def utc_instant(value: ir.Value) -> ir.TimestampValue:
    """The UTC instant a naive UTC wall-clock value denotes."""
    return current_dialect(value).utc_instant(value)


def local_day(
    timestamp: ir.TimestampValue, offset_minutes: int
) -> tuple[ir.DateValue, ir.TimestampValue]:
    """The fixed-offset local calendar day of an instant and the UTC instant that
    starts it. A naive timestamp denotes UTC."""
    return current_dialect(timestamp).local_day(timestamp, offset_minutes)


def day_start(timestamp: ir.TimestampValue, offset_minutes: int) -> ir.TimestampValue:
    """The UTC instant starting the fixed-offset local day containing an instant."""
    return current_dialect(timestamp).day_start(timestamp, offset_minutes)


def native_local_day(
    dialect: Dialect, timestamp: ir.TimestampValue, offset_minutes: int
) -> tuple[ir.DateValue, ir.TimestampValue]:
    """``local_day`` through a registered engine's native UTC conversions.

    Takes the instant's UTC wall clock, truncates a nanosecond clock to the
    microsecond before it (every fixed-offset midnight is microsecond-aligned,
    so the day is unchanged), reads the date, moves it across the offset
    boundary where the clock falls in the previous or next local day, and
    converts the local midnight back to an instant.
    """
    aware = cast(dt.Timestamp, timestamp.type()).timezone is not None
    utc = dialect.utc_wall_clock(timestamp) if aware else timestamp
    dtype = utc.type()
    if isinstance(dtype, dt.Timestamp) and dtype.scale is not None and dtype.scale > 6:
        micros = cast(ir.TimestampValue, utc.cast("timestamp"))
        utc = cast(
            ir.TimestampValue,
            (utc < micros).ifelse(micros - ibis.interval(microseconds=1), micros),
        )
    day = utc.date()
    midnight = cast(ir.TimestampValue, day.cast("timestamp"))
    if offset_minutes < 0:
        day = (utc < midnight + ibis.interval(minutes=-offset_minutes)).ifelse(
            day - ibis.interval(days=1), day
        )
    elif offset_minutes > 0:
        day = (utc >= midnight + ibis.interval(minutes=1440 - offset_minutes)).ifelse(
            day + ibis.interval(days=1), day
        )
    day = cast(ir.DateValue, day.cast("date"))
    start = dialect.utc_instant(
        cast(ir.TimestampValue, day.cast("timestamp")) - ibis.interval(minutes=offset_minutes)
    )
    return day, start


def native_day_start(
    dialect: Dialect, timestamp: ir.TimestampValue, offset_minutes: int
) -> ir.TimestampValue:
    """``day_start`` through a registered engine's native UTC conversions: the
    local wall clock's date, back to UTC, as the engines have always compiled
    endpoint alignment."""
    utc = dialect.utc_wall_clock(timestamp)
    local = utc + ibis.interval(minutes=offset_minutes)
    midnight = cast(ir.TimestampValue, local.date().cast("timestamp")) - ibis.interval(
        minutes=offset_minutes
    )
    return dialect.utc_instant(midnight)


def timestamp_text(value: ir.TimestampValue) -> ir.StringValue:
    """A timestamp's text, distinct for distinct instants at the type's scale.

    DuckDB, PostgreSQL and BigQuery print every fractional digit through a plain
    cast; Snowflake's cast follows the session's output format, so it renders
    with an explicit full-precision format instead.
    """
    return current_dialect(value).timestamp_text(value)


def _epoch(dtype: dt.Timestamp) -> ir.TimestampScalar:
    """The Unix epoch typed like ``dtype``; an aware literal carries its UTC offset
    so its meaning does not depend on the session time zone."""
    instant = datetime(1970, 1, 1, tzinfo=UTC) if dtype.timezone else datetime(1970, 1, 1)
    return cast(ir.TimestampScalar, ibis.literal(instant, type=dtype))


def timestamp_parts(
    value: ir.TimestampValue, *, backend: str | None = None
) -> tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue]:
    """Return (rank, seconds, nanos): infinity rank, then truncated seconds and remainder.

    Every part is NULL when the value is NULL. Naive timestamps denote UTC. Pass
    ``backend`` when ``value`` may be an unbound literal compared with a bound column.
    """
    dialect = dialect_for_name(backend) if backend is not None else current_dialect(value)
    return dialect.timestamp_parts(value)


def timestamp_order_key(value: ir.TimestampValue) -> ir.Value:
    """Return a value whose ordering and equality are exactly the instants'."""
    return current_dialect(value).timestamp_order_key(value)


def lookup_keys(point: ir.TimestampValue, state: ir.TimestampValue) -> tuple[ir.Value, ir.Value]:
    """Keys whose order compares a point cutoff with a state instant exactly."""
    return current_dialect(point, state).lookup_keys(point, state)


def compare_timestamps(
    left: ir.TimestampValue,
    right: ir.TimestampValue,
    op: Literal["lt", "le", "gt", "ge", "eq"],
    *,
    right_offset_seconds: int = 0,
) -> ir.BooleanValue:
    """Compare instants with an optional exact whole-second shift of the right operand."""
    backend = current_dialect(left, right).name
    left_rank, left_seconds, left_nanos = timestamp_parts(left, backend=backend)
    right_rank, right_seconds, right_nanos = timestamp_parts(right, backend=backend)
    if right_offset_seconds:
        # Integer nanosecond coordinates preserve shifts without timestamp narrowing.
        wide = dt.Decimal(38, 0)
        left_seconds = cast(ir.DecimalValue, left_seconds.cast(wide)) * 10**9 + left_nanos
        shifted = (
            cast(ir.DecimalValue, right_seconds.cast(wide)) * 10**9
            + right_nanos
            + cast(ir.DecimalValue, ibis.literal(right_offset_seconds * 10**9, type=wide))
        )
        right_seconds = (right_rank == ibis.literal(0)).ifelse(shifted, right_seconds.cast(wide))
        left_nanos = right_nanos = ibis.literal(0)
    equal_rank = left_rank == right_rank
    equal_seconds = left_seconds == right_seconds
    equal = equal_rank & equal_seconds & (left_nanos == right_nanos)
    greater = (left_rank > right_rank) | (
        equal_rank & ((left_seconds > right_seconds) | (equal_seconds & (left_nanos > right_nanos)))
    )
    less = (left_rank < right_rank) | (
        equal_rank & ((left_seconds < right_seconds) | (equal_seconds & (left_nanos < right_nanos)))
    )
    if op == "eq":
        return equal
    if op == "gt":
        return greater
    if op == "ge":
        return greater | equal
    if op == "lt":
        return less
    return less | equal
