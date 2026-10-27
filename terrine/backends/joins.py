"""As-of joins on backends with and without a native ASOF JOIN."""

from __future__ import annotations

import ibis

from terrine.backends.dialect import current_dialect

__all__ = ["asof_join", "has_native_asof"]


def has_native_asof(*values: ibis.Value) -> bool:
    """Whether Ibis lowers ``asof_join`` to valid SQL on this backend."""
    return current_dialect(*values).has_native_asof


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
