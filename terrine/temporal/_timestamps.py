"""Lossless native timestamp ordering and exact elapsed-offset comparisons."""

from __future__ import annotations

from typing import Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
from ibis.common.exceptions import IbisError

from terrine.errors import RefusalSpec

_UNSUPPORTED = RefusalSpec(
    "terrine.temporal.timestamp_order_unsupported",
    "Timestamp ordering cannot preserve {dtype}. Normalize to a losslessly representable "
    "native microsecond or nanosecond timestamp before binding.",
)
_KEY = dt.Struct.from_tuples((("rank", dt.int8), ("seconds", dt.int64), ("nanos", dt.int64)))


@ibis.udf.scalar.builtin(name="epoch_ns")
def _epoch_ns(value) -> int:
    """Native signed nanosecond coefficient; argument precision is unchanged."""
    raise NotImplementedError


@ibis.udf.scalar.builtin(name="epoch_us")
def _epoch_us(value) -> int:
    """Native signed microsecond coefficient; argument precision is unchanged."""
    raise NotImplementedError


@ibis.udf.scalar.builtin(name="isfinite")
def _finite(value) -> bool:
    """Native timestamp finiteness predicate."""
    raise NotImplementedError


@ibis.udf.scalar.builtin(name="divide", signature=((dt.int64, dt.int64), dt.int64))
def _quotient(value, divisor):
    """Native integer division truncating toward zero."""


@ibis.udf.scalar.builtin(name="mod", signature=((dt.int64, dt.int64), dt.int64))
def _remainder(value, divisor):
    """Native signed remainder, paired with truncating division."""


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


@ibis.udf.scalar.builtin(name="div", signature=((dt.int64, dt.int64), dt.int64))
def _pg_quotient(value, divisor):
    """PostgreSQL integer division truncating toward zero."""


@ibis.udf.scalar.builtin(name="trunc", signature=((dt.Decimal(38, 6),), dt.Decimal(38, 0)))
def _pg_trunc(value):
    """PostgreSQL NUMERIC truncation toward zero."""


def _backend(*values: ir.Value) -> str:
    for value in values:
        try:
            return value._find_backend(use_default=False).name
        except IbisError:
            continue
    return "duckdb"


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


def integer_quotient(
    value: ir.IntegerValue, divisor: int | ir.IntegerValue, *, like: ir.Value
) -> ir.IntegerValue:
    """Integer division truncating toward zero on ``like``'s backend."""
    if _backend(like) == "postgres":
        quotient = cast(ir.IntegerValue, _pg_quotient(value, divisor))
    else:
        quotient = cast(ir.IntegerValue, _quotient(value, divisor))
    return cast(ir.IntegerValue, quotient.cast(dt.int64))


def timestamp_parts(
    value: ir.TimestampValue, *, backend: str | None = None
) -> tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue]:
    """Return (rank, seconds, nanos): infinity rank, then truncated seconds and remainder.

    Every part is NULL when the value is NULL. Naive timestamps denote UTC. Pass
    ``backend`` when ``value`` may be an unbound literal compared with a bound column.
    """
    if (backend or _backend(value)) != "postgres":
        key = _struct_key(value)
        return (
            cast(ir.IntegerValue, key["rank"]),
            cast(ir.IntegerValue, key["seconds"]),
            cast(ir.IntegerValue, key["nanos"]),
        )
    dtype = value.type()
    if not isinstance(dtype, dt.Timestamp) or dtype.scale not in (None, 6):
        raise _UNSUPPORTED.error(dtype=str(dtype))
    finite = cast(ir.BooleanValue, _finite(value))
    epoch = cast(ir.TimestampScalar, ibis.literal("1970-01-01 00:00:00").cast(dtype))
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
    if _backend(value) == "postgres":
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
        extractor, divisor, multiplier = _epoch_ns, 10**9, 1
    elif scale == 6:
        extractor, divisor, multiplier = _epoch_us, 10**6, 1000
    else:
        raise _UNSUPPORTED.error(dtype=str(dtype))
    finite = cast(ir.BooleanValue, _finite(value))
    epoch = cast(ir.TimestampScalar, ibis.literal("1970-01-01 00:00:00").cast(dtype))
    coefficient = cast(ir.IntegerValue, extractor(finite.ifelse(value, epoch)))
    seconds = cast(ir.IntegerValue, _quotient(coefficient, divisor))
    nanos = cast(ir.IntegerValue, _remainder(coefficient, divisor)) * multiplier
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
    backend = _backend(left, right)
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
