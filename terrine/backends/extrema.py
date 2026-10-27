"""NULL-ignoring LEAST and GREATEST on every backend."""

from __future__ import annotations

from typing import Any

import ibis

from terrine.backends.dialect import current_dialect

__all__ = ["greatest", "least"]

# DuckDB and PostgreSQL skip NULL arguments; BigQuery returns NULL for any.
_SKIPS_NULLS = frozenset({"duckdb", "postgres"})


def _non_null(values: tuple[Any, ...]) -> tuple[Any, ...]:
    """Replace each NULL with another argument; NULL only when all are NULL.

    A substitute is already one of the arguments, so the extremum is unchanged.
    """
    expressions = [
        value if isinstance(value, ibis.Value) else ibis.literal(value) for value in values
    ]
    if current_dialect(*expressions).name in _SKIPS_NULLS:
        return values
    return tuple(
        ibis.coalesce(value, *(other for other in expressions if other is not value))
        for value in expressions
    )


def least(*values: Any) -> Any:
    """The smallest non-NULL argument, as the DuckDB reference computes it."""
    return ibis.least(*_non_null(values))


def greatest(*values: Any) -> Any:
    """The largest non-NULL argument, as the DuckDB reference computes it."""
    return ibis.greatest(*_non_null(values))
