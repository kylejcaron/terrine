"""Lossless timestamp ordering, exact elapsed-offset comparisons and UTC conversion.

DuckDB keys finite instants by truncated seconds and a signed remainder taken from
Ibis's exact microsecond/nanosecond ``delta``; PostgreSQL timestamps are exact at
microseconds and are their own key.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.dialect import dialect_of, unavailable
from terrine.backends.numeric import truncating_quotient
from terrine.errors import RefusalSpec

__all__ = [
    "compare_timestamps",
    "timestamp_is_finite",
    "timestamp_order_key",
    "timestamp_parts",
    "utc_instant",
    "utc_wall_clock",
]

_UNSUPPORTED = RefusalSpec(
    "terrine.temporal.timestamp_order_unsupported",
    "Timestamp ordering cannot preserve {dtype}. Normalize to a losslessly representable "
    "native microsecond or nanosecond timestamp before binding.",
)
_KEY = dt.Struct.from_tuples((("rank", dt.int8), ("seconds", dt.int64), ("nanos", dt.int64)))
_NATIVE = frozenset({"duckdb", "postgres"})


@ibis.udf.scalar.builtin(name="isfinite")
def _isfinite(value) -> bool:
    """Native timestamp finiteness predicate (DuckDB and PostgreSQL)."""
    raise NotImplementedError


def timestamp_is_finite(value: ir.TimestampValue) -> ir.BooleanValue:
    dialect = dialect_of(value)
    if dialect not in _NATIVE:
        raise unavailable("timestamp_is_finite", dialect)
    return cast(ir.BooleanValue, _isfinite(value))


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


def utc_wall_clock(value: ir.Value) -> ir.TimestampValue:
    """An instant's naive UTC wall clock, independent of the session time zone."""
    dialect = dialect_of(value)
    if dialect not in _NATIVE:
        raise unavailable("utc_wall_clock", dialect)
    return cast(ir.TimestampValue, _utc_naive("UTC", value))


def utc_instant(value: ir.Value) -> ir.TimestampValue:
    """The UTC instant a naive UTC wall-clock value denotes."""
    dialect = dialect_of(value)
    if dialect not in _NATIVE:
        raise unavailable("utc_instant", dialect)
    return cast(ir.TimestampValue, _utc_aware("UTC", value))


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


def _epoch(dtype: dt.Timestamp) -> ir.TimestampScalar:
    """The Unix epoch typed like ``dtype``; an aware literal carries its UTC offset
    so its meaning does not depend on the session time zone."""
    instant = datetime(1970, 1, 1, tzinfo=UTC) if dtype.timezone else datetime(1970, 1, 1)
    return cast(ir.TimestampScalar, ibis.literal(instant, type=dtype))


def epoch_us_sql(dialect: str, column_sql: str) -> str:
    """Native SQL for a timestamp's exact signed microseconds since the Unix epoch.

    PostgreSQL returns exact NUMERIC: its late timestamps exceed the BIGINT
    microsecond range, so cast only a bounded result such as a difference.
    """
    if dialect.casefold() == "postgres":
        late = f"pg_catalog.extract('year', {column_sql}) >= {_PG_LATE_YEAR}"
        shifted = f"{column_sql} - INTERVAL '{_PG_SHIFT_HOURS} hours'"
        return (
            f"(CASE WHEN {late} THEN pg_catalog.extract('epoch', {shifted}) + "
            f"{_PG_SHIFT_HOURS * 3600} ELSE pg_catalog.extract('epoch', {column_sql}) END "
            "* 1000000)"
        )
    return f"epoch_us({column_sql})"


def timestamp_parts(
    value: ir.TimestampValue, *, backend: str | None = None
) -> tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue]:
    """Return (rank, seconds, nanos): infinity rank, then truncated seconds and remainder.

    Every part is NULL when the value is NULL. Naive timestamps denote UTC. Pass
    ``backend`` when ``value`` may be an unbound literal compared with a bound column.
    """
    if (backend or dialect_of(value)) != "postgres":
        key = _struct_key(value)
        return (
            cast(ir.IntegerValue, key["rank"]),
            cast(ir.IntegerValue, key["seconds"]),
            cast(ir.IntegerValue, key["nanos"]),
        )
    dtype = value.type()
    if not isinstance(dtype, dt.Timestamp) or dtype.scale not in (None, 6):
        raise _UNSUPPORTED.error(dtype=str(dtype))
    finite = timestamp_is_finite(value)
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
        return cast(ir.IntegerValue, missing.ifelse(ibis.null(dt.int64), expression.cast(dt.int64)))

    return (
        part(rank),
        part(finite.ifelse(seconds, zero)),
        part(finite.ifelse(nanos, zero)),
    )


def timestamp_order_key(value: ir.TimestampValue) -> ir.Value:
    """Return a value whose ordering and equality are exactly the instants'.

    PostgreSQL timestamps are exact at microseconds and order infinities natively, so
    they are their own key (Ibis's PostgreSQL struct lowering is not valid SQL).
    Other backends use the struct key from ``_struct_key``.
    """
    dtype = value.type()
    if not isinstance(dtype, dt.Timestamp):
        raise _UNSUPPORTED.error(dtype=str(dtype))
    if dialect_of(value) == "postgres":
        if dtype.scale not in (None, 6):
            raise _UNSUPPORTED.error(dtype=str(dtype))
        return value
    return _struct_key(value)


def _struct_key(value: ir.TimestampValue) -> ir.StructValue:
    """Order finite instants by signed seconds/remainder, with explicit infinity ranks.

    The remainder is smaller than one second, so only its conversion to nanoseconds
    multiplies a coefficient. NULL is a NULL struct, not a struct of NULL fields.
    Naive timestamps denote UTC, as in source normalization. DuckDB only.
    """
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
    finite = timestamp_is_finite(value)
    epoch = _epoch(dtype)
    instant = cast(ir.TimestampValue, finite.ifelse(value, epoch))
    coefficient = instant.delta(epoch, unit=unit)
    seconds = truncating_quotient(coefficient, divisor, like=value)
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


def compare_timestamps(
    left: ir.TimestampValue,
    right: ir.TimestampValue,
    op: Literal["lt", "le", "gt", "ge", "eq"],
    *,
    right_offset_seconds: int = 0,
) -> ir.BooleanValue:
    """Compare instants with an optional exact whole-second shift of the right operand."""
    backend = dialect_of(left, right)
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
