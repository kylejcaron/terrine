"""As-of joins on backends with and without a native ASOF JOIN."""

from __future__ import annotations

from typing import cast

import ibis
import ibis.expr.types as ir

from terrine.backends.dialect import current_dialect

__all__ = ["asof_join", "has_native_asof"]

_NATIVE_ASOF = frozenset({"duckdb", "postgres", "snowflake"})


def has_native_asof(*values: ibis.Value) -> bool:
    """Whether Ibis lowers ``asof_join`` to valid SQL on this backend."""
    return current_dialect(*values).name in _NATIVE_ASOF


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
    on = left[left_key] >= right[right_key] if inclusive else left[left_key] > right[right_key]
    if has_native_asof(left[left_key], right[right_key]):
        return left.asof_join(right, on=on, predicates=[left[left_unit] == right[right_unit]])
    used = {name.casefold() for name in (*left.columns, *right.columns)}

    def fresh(stem: str) -> str:
        name = stem
        while name.casefold() in used:
            name = f"_{name}"
        used.add(name.casefold())
        return name

    unit, key, kind, match = (
        fresh(f"__terrine_asof_{part}") for part in ("unit", "key", "kind", "match")
    )
    # At an equal key, the right row sorts first when it is admitted.
    left_kind, right_kind = (1, 0) if inclusive else (0, 1)
    requests = left.select(
        **{unit: left[left_unit], key: left[left_key], kind: ibis.literal(left_kind)}
    ).distinct()
    states = right.select(
        **{unit: right[right_unit], key: right[right_key], kind: ibis.literal(right_kind)}
    )
    timeline = requests.union(states, distinct=False)
    frame = ibis.window(
        group_by=timeline[unit], order_by=[timeline[key], timeline[kind]], rows=(None, 0)
    )
    admitted = cast(ir.Column, ibis.ifelse(timeline[kind] == right_kind, timeline[key], None))
    timeline = timeline.mutate(**{match: admitted.max().over(frame)})
    mapping = timeline.filter(timeline[kind] == left_kind).select(unit, key, match)
    mapped = left.left_join(
        mapping, [left[left_unit] == mapping[unit], left[left_key] == mapping[key]]
    )
    mapped = mapped.select(*left.columns, match)
    joined = mapped.left_join(
        right, [mapped[left_unit] == right[right_unit], mapped[match] == right[right_key]]
    )
    return joined.select(*left.columns, *right.columns)
