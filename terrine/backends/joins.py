"""As-of joins on backends with and without a native ASOF JOIN."""

from __future__ import annotations

import ibis
import ibis.expr.types as ir

from terrine.backends.dialect import current_dialect

__all__ = ["asof_join", "has_native_asof", "null_safe_equal"]


def has_native_asof(*values: ibis.Value) -> bool:
    """Whether the dialect selects Ibis's native ASOF lowering."""
    return current_dialect(*values).has_native_asof


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
