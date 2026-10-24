"""Stored view definitions admitted under the read-only SQL policy.

A snapshot pins rows, not evaluation: a view selecting ``RANDOM()`` or
filtering on ``CURRENT_TIMESTAMP()`` answers differently on each read while
its definition and the tables it reads are unchanged, and an unlisted or
user-defined function may read anything. Every provider therefore admits a
view's definition exactly as it admits a SQL relation: built-ins from the
read-only whitelist only.
"""

from __future__ import annotations

from typing import Any

from terrine.errors import TerrineError
from terrine.semantic.sql import _admit_stored_view_query_ast

__all__ = ["admitted_view_query"]

_ROUTE = "materialize its rows into a table, or bind a view using only admitted built-ins"


def admitted_view_query(definition: str, dialect: str, *, name: str) -> Any:
    """The query of a stored view definition, refused with
    ``terrine.execution.snapshot_unavailable`` unless every function it calls
    is an admitted read-only built-in."""
    from terrine.execution.inputs import _SNAPSHOT_UNAVAILABLE

    try:
        return _admit_stored_view_query_ast(definition, dialect)
    except TerrineError as exc:
        reason = exc.context.get("reason", "not an admitted read-only query")
        raise _SNAPSHOT_UNAVAILABLE.error(
            reason=f"view {name!r} is not an admitted read-only query ({reason}), so its "
            f"rows can change between pinned reads without any DDL; {_ROUTE}"
        ) from exc
