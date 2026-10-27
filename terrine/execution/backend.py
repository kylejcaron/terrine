"""Backend admission and caller-owned execution lifetimes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from importlib import import_module
from typing import TYPE_CHECKING, Any, Protocol

from terrine.backends.capabilities import (
    _CAPABILITY_UNAVAILABLE,
    UNVERIFIED_CAPABILITIES,
    BackendCapabilities,
)
from terrine.backends.registry import ENGINES, GENERIC, Engine, backend_name, resolve_engine
from terrine.errors import TerrineError
from terrine.semantic.models import Catalog, RelationSpec

if TYPE_CHECKING:
    import ibis

    from terrine.execution.inputs import ExecutionContext, PhysicalRelationId, VerifiedInputBinding

__all__ = ["BackendProvider", "backend_provider", "backend_capabilities"]


class BackendProvider(Protocol):
    """Admission authority, not an inference from Ibis compilation support.

    Execution retains the caller connection as ``owner_con``. Verified providers
    own their consistent-view sessions; an unverified provider borrows the caller
    connection for independent statements without a transaction view. Ibis schema
    inspection on unverified backends may end an open caller transaction; use a
    connection with no pending writes or a dedicated read connection.
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


def backend_capabilities(con: Any) -> BackendCapabilities:
    """Inspect statically known evidence without querying the connection."""
    for engine in ENGINES.values():
        if engine.status != "verified" or type(con).__module__ != engine.ibis_module:
            continue
        if engine.identity_requires_query:
            return UNVERIFIED_CAPABILITIES
        try:
            engine.identify(con)
        except TerrineError:
            return UNVERIFIED_CAPABILITIES
        return import_module(engine.provider_module).PROVIDER.capabilities
    from ibis.backends import BaseBackend

    if isinstance(con, BaseBackend):
        return GENERIC.capabilities
    return UNVERIFIED_CAPABILITIES


def backend_provider(con: Any, *, engine: Engine | None = None) -> BackendProvider:
    """Resolve execution from explicit admission, validating the connection identity."""
    if engine is None:
        engine = resolve_engine(con)
    else:
        if engine is not GENERIC and (
            ENGINES.get(engine.name) is not engine or type(con).__module__ != engine.ibis_module
        ):
            raise _CAPABILITY_UNAVAILABLE.error(
                backend=backend_name(con),
                operation="query",
                status="unverified",
                route="Bind a Model to this connection before executing.",
            )
        engine.identify(con)
    provider = import_module(engine.provider_module).PROVIDER
    provider.capabilities.require(
        "dialect_admission",
        backend=backend_name(con),
        route="Use a native backend with verified read-only dialect admission.",
        allow_unverified=engine.status == "unverified",
    )
    return provider
