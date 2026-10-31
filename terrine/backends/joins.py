"""As-of joins on backends with and without a native ASOF JOIN."""

from __future__ import annotations

from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.backends.dialect import current_dialect
from terrine.errors import RefusalSpec

__all__ = ["asof_join", "has_native_asof", "null_safe_equal", "union_unit_keys"]

_INCOMPATIBLE_UNIT_KEYS = RefusalSpec(
    "terrine.compile.incompatible_unit_keys",
    "Temporal lookup unit key types {left_type} and {right_type} are incompatible. {route}",
)


def has_native_asof(*values: ibis.Value) -> bool:
    """Whether the dialect selects Ibis's native ASOF lowering."""
    return current_dialect(*values).has_native_asof


def union_unit_keys(left: ir.Value, right: ir.Value) -> tuple[ir.Value, ir.Value]:
    """Align nullable unit keys, widening integers or refusing unequal types."""
    left_type = left.type().copy(nullable=True)
    right_type = right.type().copy(nullable=True)
    if left_type.is_integer() and right_type.is_integer():
        if left_type.is_signed_integer() == right_type.is_signed_integer():
            common = dt.higher_precedence(left_type, right_type)
        else:
            signed, unsigned = (
                cast(dt.Integer, left_type),
                cast(dt.Integer, right_type),
            )
            if not left_type.is_signed_integer():
                signed, unsigned = unsigned, signed
            common = next(
                (
                    dtype
                    for dtype in (dt.int16, dt.int32, dt.int64)
                    if dtype.nbytes >= signed.nbytes and dtype.nbytes > unsigned.nbytes
                ),
                dt.Decimal(20, 0),
            )
        left_type = right_type = common
    if left_type != right_type:
        raise _INCOMPATIBLE_UNIT_KEYS.error(
            left_type=str(left_type),
            right_type=str(right_type),
            route="Align the source key types before building the dataset.",
        )
    return left.cast(left_type), right.cast(right_type)


def null_safe_equal(left: ir.Value, right: ir.Value) -> ir.BooleanValue:
    """Equate keys including two NULLs, using the dialect's joinable representation."""
    return current_dialect(left, right).null_safe_equal(left, right)


def asof_join(
    left: ibis.Table,
    right: ibis.Table,
    *,
    left_unit: str,
    left_key: str,
    right_unit: str,
    right_key: str,
    inclusive: bool,
) -> ibis.Table:
    """Attach to each left row the right row with the greatest key below its own.

    ``inclusive`` admits an equal key (``>=``); otherwise only strictly smaller keys
    match (``>``). Right keys must be unique per unit; unmatched rows keep NULL right
    columns, as a native as-of join does. Column names must be disjoint.
    """
    return current_dialect(left[left_key], right[right_key]).asof_join(
        left,
        right,
        left_unit=left_unit,
        left_key=left_key,
        right_unit=right_unit,
        right_key=right_key,
        inclusive=inclusive,
    )
