"""Backend admission and caller-owned execution lifetimes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Protocol

from terrine.backends.capabilities import DUCKDB_CAPABILITIES, BackendCapabilities
from terrine.errors import RefusalSpec, TerrineError
from terrine.semantic.models import Catalog, RelationSpec, TableRef
from terrine.semantic.sql import admit_read_query

if TYPE_CHECKING:
    import ibis

    from terrine.execution.inputs import ExecutionContext, PhysicalRelationId, VerifiedInputBinding

__all__ = ["BackendProvider", "backend_provider"]

_CAPABILITY_UNAVAILABLE = RefusalSpec(
    "terrine.backend.capability_unavailable",
    "Backend {backend!r} cannot provide {operation!r}: {status}. {route}",
)


class BackendProvider(Protocol):
    """Admission authority, not an inference from Ibis compilation support.

    Execution retains the caller connection as ``owner_con`` unchanged, opens
    a private transaction on an owned session, and closes only that session.
    A transaction view is not a durable/replayable source snapshot.
    """

    capabilities: BackendCapabilities

    def matches(self, con: Any) -> bool: ...
    def dialect(self, con: Any) -> str: ...
    def resolve_relation(self, con: Any, relation: RelationSpec) -> ibis.Table: ...
    def required_relations(
        self, catalog: Catalog, con: Any, *, units: Mapping[str, Any] | None = None
    ) -> tuple[PhysicalRelationId, ...]: ...
    def bind_resolver(
        self, catalog: Catalog, con: Any, **kwargs: Any
    ) -> Callable[[RelationSpec], Any]: ...
    def sql(self, con: Any, sql: str) -> ibis.Table: ...
    def open_execution(
        self,
        catalog: Catalog,
        con: Any,
        *,
        units: Mapping[str, Any] | None = None,
        input_binding: VerifiedInputBinding | None = None,
    ) -> AbstractContextManager[ExecutionContext]: ...


class _DuckDBProvider:
    capabilities = DUCKDB_CAPABILITIES

    def matches(self, con: Any) -> bool:
        if type(con).__module__ not in {"ibis.backends.duckdb", "_duckdb"}:
            return False
        from terrine.execution._native import raw_connection

        try:
            raw_connection(con)
        except TerrineError:
            return False
        return True

    def dialect(self, con: Any) -> str:
        return "duckdb"

    def resolve_relation(self, con: Any, relation: RelationSpec) -> ibis.Table:
        from terrine.execution import _native

        raw = _native.raw_connection(con)
        with _native.connection_lock(raw):
            _native.admission_fingerprint(_native._dummy_catalog(), relation, raw)
            if isinstance(relation, TableRef):
                return con.table(relation.name)
            return con.sql(admit_read_query(relation.sql, "duckdb", "duckdb"))

    def required_relations(self, catalog, con, *, units=None):
        from terrine.execution.inputs import _required_native_relations

        return _required_native_relations(catalog, con, units=units)

    def bind_resolver(self, catalog, con, **kwargs):
        from terrine.execution.inputs import _bind_native_resolver

        return _bind_native_resolver(catalog, con, **kwargs)

    def sql(self, con: Any, sql: str) -> ibis.Table:
        from terrine.execution import _native

        raw = _native.raw_connection(con)
        with _native.connection_lock(raw):
            _native.check_native_functions(raw, sql, None)
            return con.sql(sql)

    def open_execution(self, catalog, con, *, units=None, input_binding=None):
        from terrine.execution.inputs import _open_native_execution

        return _open_native_execution(catalog, con, units=units, input_binding=input_binding)


_DUCKDB_PROVIDER = _DuckDBProvider()


def backend_provider(con: Any) -> BackendProvider:
    """Resolve verified native execution before schema or source access."""
    if _DUCKDB_PROVIDER.matches(con):
        return _DUCKDB_PROVIDER
    raise _CAPABILITY_UNAVAILABLE.error(
        backend=str(getattr(con, "name", type(con).__name__)).casefold(),
        operation="query",
        status="unverified",
        route="Bind a native local DuckDB connection; other backend execution is unverified.",
    )
