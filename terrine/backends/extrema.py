"""NULL-ignoring LEAST and GREATEST on every backend."""

from __future__ import annotations

from typing import Any

import ibis

from terrine.backends.dialect import current_dialect

__all__ = ["greatest", "least"]


def least(*values: Any) -> Any:
    """The smallest non-NULL argument, as the DuckDB reference computes it."""
    return current_dialect(*(value for value in values if isinstance(value, ibis.Value))).least(
        *values
    )


def greatest(*values: Any) -> Any:
    """The largest non-NULL argument, as the DuckDB reference computes it."""
    return current_dialect(*(value for value in values if isinstance(value, ibis.Value))).greatest(
        *values
    )
