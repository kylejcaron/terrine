"""Snowflake expression types and static module identity admission."""

from typing import Any

import ibis.expr.datatypes as dt

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE
from terrine.backends.dialect import Dialect
from terrine.backends.registry import ENGINES, supported_engine_route

# Sums and differences beyond 38 digits remain exact-overflow query errors.
_EXACT = dt.Decimal(38, 0)


class SnowflakeDialect(Dialect):
    __slots__ = ()
    operations = frozenset(
        {
            "exact_state",
            "exact_float",
            "normalized",
            "integer_quotient",
            "timestamp_is_finite",
            "utc_wall_clock",
            "utc_instant",
            "timestamp_text",
        }
    )

    def exact_type(self) -> dt.DataType:
        return _EXACT


def identify(con: Any) -> None:
    """Module identity is the existing evidence; no server query is required."""
    if type(con).__module__ != ENGINES["snowflake"].ibis_module:
        raise _CAPABILITY_UNAVAILABLE.error(
            backend=str(getattr(con, "name", type(con).__name__)).casefold(),
            operation="query",
            status="unverified",
            route=supported_engine_route("query"),
        )
