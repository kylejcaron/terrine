"""Backend admission and caller-owned execution lifetimes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from importlib import import_module
from typing import TYPE_CHECKING, Any, Protocol

from terrine.backends.capabilities import UNVERIFIED_CAPABILITIES, BackendCapabilities
from terrine.backends.registry import ENGINES, resolve_engine
from terrine.errors import TerrineError
from terrine.semantic.models import Catalog, RelationSpec

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
        return engine.capabilities
    return UNVERIFIED_CAPABILITIES


def backend_provider(con: Any) -> BackendProvider:
    """Resolve verified native execution before schema or source access."""
    engine = resolve_engine(con)
    return import_module(engine.provider_module).PROVIDER
