"""PostgreSQL expression types and qualified server identity admission."""

import re
from importlib.metadata import version
from typing import Any

import ibis.expr.datatypes as dt
import sqlglot.expressions as sge

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE, _SNAPSHOT_UNAVAILABLE
from terrine.backends.dialect import Dialect
from terrine.backends.registry import supported_engine_route
from terrine.errors import RefusalSpec

_EXACT = dt.Unknown(raw_type=sge.DataType.build("NUMERIC", dialect="postgres"))
_MIN_IBIS = (12, 0)
_VERIFIED_SERVER_MAJOR = 16
_DIALECT_ADMISSION = RefusalSpec(
    "terrine.backend.capability_unavailable",
    "The detected database server is not verified for PostgreSQL execution. {route}",
)


class PostgresDialect(Dialect):
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


def raw_connection(con: Any) -> Any:
    raw = getattr(con, "con", None)
    if raw is None or type(raw).__module__.split(".")[0] != "psycopg":
        raise _SNAPSHOT_UNAVAILABLE.error(
            reason="connection is not a psycopg-backed Ibis PostgreSQL backend"
        )
    return raw


def _classify(version_string: str) -> tuple[str, str]:
    """Admit only PostgreSQL 16, the version measured by the compatibility suite."""
    lowered = version_string.casefold()
    if "redshift" in lowered:
        return "Redshift", "unsupported"
    if "greenplum" in lowered:
        return "Greenplum", "unsupported"
    if "cockroachdb" in lowered:
        return "CockroachDB", "unsupported"
    match = re.match(r"^PostgreSQL\s+(\d+)(?:\.\d+)?\b", version_string)
    if match is not None:
        major = int(match.group(1))
        return "PostgreSQL", "admitted" if major == _VERIFIED_SERVER_MAJOR else "unverified"
    server = version_string.split(maxsplit=1)[0] if version_string.strip() else "unknown"
    return server, "unverified"


def identify(con: Any) -> None:
    """Check the Ibis floor before reading the built-in server version function."""
    # Ibis 11 places PostgreSQL as-of unit filters after the nearest-row selection.
    parts = tuple(int(part) for part in re.findall(r"\d+", version("ibis-framework"))[:2])
    if parts < _MIN_IBIS:
        raise _CAPABILITY_UNAVAILABLE.error(
            backend="postgres",
            operation="dialect_admission",
            status="unsupported",
            route="Install terrine[postgres], which requires ibis-framework 12 or newer.",
        )
    with raw_connection(con).cursor() as cursor:
        cursor.execute("SELECT pg_catalog.version()")
        rows = cursor.fetchall()
    version_string = str(rows[0][0]) if rows and rows[0] else ""
    server, status = _classify(version_string)
    if status != "admitted":
        raise _DIALECT_ADMISSION.error(
            backend=server,
            operation="dialect_admission",
            status=status,
            route=supported_engine_route("dialect_admission"),
        )
