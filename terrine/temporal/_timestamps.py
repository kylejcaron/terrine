"""Lossless native ordering of timestamp instants without widening coefficients."""

from __future__ import annotations

from typing import Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

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


def timestamp_order_key(value: ir.TimestampValue) -> ir.StructValue:
    """Order finite instants by signed seconds/remainder, with explicit infinity ranks.

    The remainder is smaller than one second, so only its conversion to nanoseconds
    multiplies a coefficient. NULL is a NULL struct, not a struct of NULL fields.
    Naive timestamps denote UTC, as in source normalization.
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
) -> ir.BooleanValue:
    left_key = timestamp_order_key(left)
    right_key = timestamp_order_key(right)
    left_rank, right_rank = left_key["rank"], right_key["rank"]
    left_seconds, right_seconds = left_key["seconds"], right_key["seconds"]
    left_nanos, right_nanos = left_key["nanos"], right_key["nanos"]
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
        return cast(ir.BooleanValue, equal)
    if op == "gt":
        return cast(ir.BooleanValue, greater)
    if op == "ge":
        return cast(ir.BooleanValue, greater | equal)
    if op == "lt":
        return cast(ir.BooleanValue, less)
    return cast(ir.BooleanValue, less | equal)
