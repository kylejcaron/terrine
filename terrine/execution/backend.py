"""Backend admission and caller-owned execution lifetimes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Protocol

from terrine.backends.capabilities import (
    _CAPABILITY_UNAVAILABLE,
    BIGQUERY_CAPABILITIES,
    DUCKDB_CAPABILITIES,
    POSTGRES_CAPABILITIES,
    SNOWFLAKE_CAPABILITIES,
    UNVERIFIED_CAPABILITIES,
    BackendCapabilities,
)
from terrine.errors import TerrineError
from terrine.semantic.models import Catalog, RelationSpec, TableRef
from terrine.semantic.sql import admit_read_query

if TYPE_CHECKING:
    import ibis

    from terrine.execution.inputs import ExecutionContext, PhysicalRelationId, VerifiedInputBinding

__all__ = ["BackendProvider", "backend_provider", "backend_capabilities"]


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
        if type(con).__module__ != "ibis.backends.duckdb":
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

    def open_execution(self, catalog, con, *, units=None, input_binding=None):
        self.capabilities.require(
            "transaction_view",
            backend="duckdb",
            route="Use a backend with verified transaction-scoped input identity.",
        )
        from terrine.execution.inputs import _open_native_execution

        return _open_native_execution(catalog, con, units=units, input_binding=input_binding)


_DUCKDB_PROVIDER = _DuckDBProvider()


class _PostgresProvider:
    capabilities = POSTGRES_CAPABILITIES

    def matches(self, con: Any) -> bool:
        return type(con).__module__ == "ibis.backends.postgres"

    def dialect(self, con: Any) -> str:
        return "postgres"

    def resolve_relation(self, con: Any, relation: RelationSpec) -> ibis.Table:
        from terrine.execution import _postgres

        return _postgres.resolve(con, relation)

    def required_relations(self, catalog, con, *, units=None):
        from terrine.execution import _postgres

        return _postgres.required_relations(catalog, con, units=units)

    def bind_resolver(self, catalog, con, **kwargs):
        from terrine.execution import _postgres
        from terrine.execution.inputs import _INVALID

        units = kwargs.get("units")
        if kwargs.get("input_binding") is not None:
            raise _CAPABILITY_UNAVAILABLE.error(
                backend="postgres",
                operation="retained_snapshot",
                status="unverified",
                route="Retained input bindings are DuckDB-only; materialize directly.",
            )
        raw = _postgres.raw_connection(con)
        _postgres.required_relations(catalog, con, units=units)
        admitted: dict[tuple[str, str], Any] = {}
        fingerprints: dict[tuple[str, str], str] = {}

        def resolve(relation: RelationSpec) -> Any:
            key = (relation.kind, relation.name if isinstance(relation, TableRef) else relation.sql)
            fingerprint = _postgres.admission_fingerprint(relation, raw)
            if key in admitted:
                if fingerprints[key] != fingerprint:
                    raise _INVALID.error(
                        reason="relation definition or identity changed after Model binding"
                    )
                return admitted[key]
            admitted[key] = _postgres.resolve(con, relation, catalog.dialect)
            fingerprints[key] = fingerprint
            return admitted[key]

        return resolve

    def open_execution(self, catalog, con, *, units=None, input_binding=None):
        from terrine.execution.inputs import _open_postgres_execution

        return _open_postgres_execution(
            self, catalog, con, units=units, input_binding=input_binding
        )


_POSTGRES_PROVIDER = _PostgresProvider()


class _BigQueryProvider:
    capabilities = BIGQUERY_CAPABILITIES

    def matches(self, con: Any) -> bool:
        return type(con).__module__ == "ibis.backends.bigquery"

    def dialect(self, con: Any) -> str:
        return "bigquery"

    def resolve_relation(self, con: Any, relation: RelationSpec) -> ibis.Table:
        from terrine.execution import _bigquery

        return _bigquery.resolve(con, relation)

    def required_relations(self, catalog, con, *, units=None):
        from terrine.execution import _bigquery

        return _bigquery.required_relations(catalog, con, units=units)

    def bind_resolver(self, catalog, con, **kwargs):
        from terrine.execution import _bigquery
        from terrine.execution.inputs import _INVALID

        if kwargs.get("input_binding") is not None:
            raise _CAPABILITY_UNAVAILABLE.error(
                backend="bigquery",
                operation="retained_snapshot",
                status="unverified",
                route="Retained input bindings are DuckDB-only; materialize directly.",
            )
        _bigquery.required_relations(catalog, con, units=kwargs.get("units"))
        admitted: dict[tuple[str, str], Any] = {}
        fingerprints: dict[tuple[str, str], str] = {}

        def resolve(relation: RelationSpec) -> Any:
            key = (relation.kind, relation.name if isinstance(relation, TableRef) else relation.sql)
            fingerprint = _bigquery.admission_fingerprint(relation, con)
            if key in admitted:
                if fingerprints[key] != fingerprint:
                    raise _INVALID.error(
                        reason="relation definition or identity changed after Model binding"
                    )
                return admitted[key]
            admitted[key] = _bigquery.resolve(con, relation, catalog.dialect)
            fingerprints[key] = fingerprint
            return admitted[key]

        return resolve

    def open_execution(self, catalog, con, *, units=None, input_binding=None):
        from terrine.execution.inputs import _open_bigquery_execution

        return _open_bigquery_execution(
            self, catalog, con, units=units, input_binding=input_binding
        )


_BIGQUERY_PROVIDER = _BigQueryProvider()


class _SnowflakeProvider:
    capabilities = SNOWFLAKE_CAPABILITIES

    def matches(self, con: Any) -> bool:
        return type(con).__module__ == "ibis.backends.snowflake"

    def dialect(self, con: Any) -> str:
        return "snowflake"

    def resolve_relation(self, con: Any, relation: RelationSpec) -> ibis.Table:
        from terrine.execution import _snowflake

        return _snowflake.resolve(con, relation)

    def required_relations(self, catalog, con, *, units=None):
        from terrine.execution import _snowflake

        return _snowflake.required_relations(catalog, con, units=units)

    def bind_resolver(self, catalog, con, **kwargs):
        """Resolve relations; with ``session`` (the execution's Time Travel pin)
        every relation is admitted to it and re-verified after each read."""
        from terrine.execution import _snowflake
        from terrine.execution.inputs import _INVALID

        if kwargs.get("input_binding") is not None:
            raise _CAPABILITY_UNAVAILABLE.error(
                backend="snowflake",
                operation="retained_snapshot",
                status="unverified",
                route="Retained input bindings are DuckDB-only; materialize directly.",
            )
        session = kwargs.get("session")
        _snowflake.required_relations(catalog, con, units=kwargs.get("units"))
        admitted: dict[tuple[str, str], Any] = {}
        fingerprints: dict[tuple[str, str], str] = {}

        def resolve(relation: RelationSpec) -> Any:
            key = (relation.kind, relation.name if isinstance(relation, TableRef) else relation.sql)
            closure = _snowflake.closure_for(relation, con)
            if session is not None:
                session.admit(closure.values())
            fingerprint = _snowflake.fingerprint(closure)
            if key in admitted:
                if fingerprints[key] != fingerprint:
                    raise _INVALID.error(
                        reason="relation definition or identity changed after Model binding"
                    )
                return admitted[key]
            admitted[key] = _snowflake.resolve(con, relation, catalog.dialect, closure)
            fingerprints[key] = fingerprint
            return admitted[key]

        return resolve

    def open_execution(self, catalog, con, *, units=None, input_binding=None):
        from terrine.execution.inputs import _open_snowflake_execution

        return _open_snowflake_execution(
            self, catalog, con, units=units, input_binding=input_binding
        )


_SNOWFLAKE_PROVIDER = _SnowflakeProvider()
_PROVIDERS: tuple[BackendProvider, ...] = (
    _DUCKDB_PROVIDER,
    _POSTGRES_PROVIDER,
    _BIGQUERY_PROVIDER,
    _SNOWFLAKE_PROVIDER,
)


def backend_capabilities(con: Any) -> BackendCapabilities:
    """Inspect evidence without admitting a connection or reading source metadata."""
    for provider in _PROVIDERS:
        if provider.matches(con):
            return provider.capabilities
    return UNVERIFIED_CAPABILITIES


def backend_provider(con: Any) -> BackendProvider:
    """Resolve verified native execution before schema or source access."""
    if _DUCKDB_PROVIDER.matches(con):
        _DUCKDB_PROVIDER.capabilities.require(
            "dialect_admission",
            backend="duckdb",
            route="Use a native backend with verified read-only dialect admission.",
        )
        return _DUCKDB_PROVIDER
    if _POSTGRES_PROVIDER.matches(con):
        from terrine.execution._postgres import ibis_supported, verify_server_identity

        if not ibis_supported():
            raise _CAPABILITY_UNAVAILABLE.error(
                backend="postgres",
                operation="dialect_admission",
                status="unsupported",
                route="Install terrine[postgres], which requires ibis-framework 12 or newer.",
            )
        verify_server_identity(con)
        return _POSTGRES_PROVIDER
    if _BIGQUERY_PROVIDER.matches(con):
        return _BIGQUERY_PROVIDER
    if _SNOWFLAKE_PROVIDER.matches(con):
        return _SNOWFLAKE_PROVIDER
    raise _CAPABILITY_UNAVAILABLE.error(
        backend=str(getattr(con, "name", type(con).__name__)).casefold(),
        operation="query",
        status="unverified",
        route=(
            "Bind a local Ibis DuckDB, PostgreSQL, BigQuery or Snowflake backend; other "
            "providers are unverified."
        ),
    )
