"""BigQuery expression types and static module identity admission."""

from typing import Any

import ibis.expr.datatypes as dt
import sqlglot.expressions as sge

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE
from terrine.backends.dialect import Dialect
from terrine.backends.registry import ENGINES, supported_engine_route

# Overflow remains a query error or NULL from SAFE_ arithmetic.
_EXACT = dt.Unknown(raw_type=sge.DataType.build("BIGNUMERIC", dialect="bigquery"))


class BigQueryDialect(Dialect):
    __slots__ = ()
    operations = frozenset(
        {
            "exact_state",
            "exact_float",
            "exact_ratio",
            "exact_is_nonfinite",
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
    if type(con).__module__ != ENGINES["bigquery"].ibis_module:
        raise _CAPABILITY_UNAVAILABLE.error(
            backend=str(getattr(con, "name", type(con).__name__)).casefold(),
            operation="query",
            status="unverified",
            route=supported_engine_route("query"),
        )
