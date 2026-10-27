"""Integer sequences on backends where Ibis cannot unnest a range."""

from __future__ import annotations

import ibis

from terrine.backends.dialect import dialect_for_name

__all__ = ["integer_sequence"]


def integer_sequence(stop: int, name: str, *, dialect: str) -> ibis.Table:
    """A table of the integers ``0 <= name < stop`` as ``int64``."""
    return dialect_for_name(dialect).integer_sequence(stop, name)
