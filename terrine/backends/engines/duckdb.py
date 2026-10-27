"""DuckDB expression types and native identity admission."""

from typing import Any

import ibis.expr.datatypes as dt
import sqlglot.expressions as sge

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE
from terrine.backends.dialect import Dialect
from terrine.backends.registry import ENGINES, supported_engine_route

_EXACT = dt.Unknown(raw_type=sge.DataType.build("BIGNUM", dialect="duckdb"))


class DuckDBDialect(Dialect):
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
    """Validate the native handle without reading or changing connection state."""
    try:
        import duckdb
    except ImportError:
        raw = None
    else:
        raw = next(
            (
                candidate
                for candidate in (getattr(con, "con", None), getattr(con, "_con", None), con)
                if isinstance(candidate, duckdb.DuckDBPyConnection)
            ),
            None,
        )
    if raw is None:
        raise _CAPABILITY_UNAVAILABLE.error(
            backend=str(getattr(con, "name", type(con).__name__)).casefold(),
            operation="query",
            status="unverified",
            route=supported_engine_route("query"),
        )
    ENGINES["duckdb"].capabilities.require(
        "dialect_admission",
        backend="duckdb",
        route="Use a native backend with verified read-only dialect admission.",
    )
