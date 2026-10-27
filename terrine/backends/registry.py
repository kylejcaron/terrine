"""Verified engine metadata and lazy expression/execution admission."""

from __future__ import annotations

import os
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from importlib import import_module
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE, BackendCapabilities
from terrine.errors import UnverifiedBackendWarning

if TYPE_CHECKING:
    from terrine.backends.dialect import Dialect

__all__ = [
    "ENGINES",
    "Engine",
    "SessionStrategy",
    "engine_supports",
    "get_engine",
    "resolve_engine",
    "supported_engine_route",
]


class SessionStrategy(StrEnum):
    NO_SNAPSHOT = "no_snapshot"
    TRANSACTION = "transaction"
    SESSION_TRANSACTION = "session_transaction"
    TIME_TRAVEL = "time_travel"


@dataclass(frozen=True, slots=True)
class Engine:
    """Admission evidence and deferred implementation paths for one engine."""

    name: str
    display_name: str
    ibis_module: str
    session_strategy: SessionStrategy
    status: Literal["verified", "unverified"]
    capabilities: BackendCapabilities
    features: frozenset[str]
    provider_module: str
    identity_requires_query: bool
    _dialect_path: str
    _identity_path: str

    @property
    def dialect(self) -> type[Dialect]:
        module, symbol = self._dialect_path.split(":")
        return getattr(import_module(module), symbol)

    def identify(self, con: Any) -> None:
        module, symbol = self._identity_path.split(":")
        getattr(import_module(module), symbol)(con)


ENGINES: Mapping[str, Engine] = MappingProxyType(
    {
        "duckdb": Engine(
            name="duckdb",
            display_name="DuckDB",
            ibis_module="ibis.backends.duckdb",
            session_strategy=SessionStrategy.TRANSACTION,
            status="verified",
            capabilities=BackendCapabilities(
                snapshot=False,
                change_evidence=frozenset(),
                atomic_manifest_publish=True,
                scan_estimate=None,
                cancellation=None,
                dialect_admission=True,
                exact_timestamps=True,
                exact_decimals=True,
                transaction_view=True,
                retained_snapshot=True,
                table_publication=True,
                arrow_batches=True,
                runtime_reader=True,
            ),
            features=frozenset(
                {
                    "naive_anchor_utc",
                    "first_occurrence",
                    "event_resolution",
                    "quantile",
                    "approx_distinct",
                    "warehouse_preflight",
                }
            ),
            provider_module="terrine.execution._native",
            identity_requires_query=False,
            _dialect_path="terrine.backends.engines.duckdb:DuckDBDialect",
            _identity_path="terrine.backends.engines.duckdb:identify",
        ),
        # A private REPEATABLE READ READ ONLY transaction; no retained snapshot or publication.
        "postgres": Engine(
            name="postgres",
            display_name="PostgreSQL",
            ibis_module="ibis.backends.postgres",
            session_strategy=SessionStrategy.TRANSACTION,
            status="verified",
            capabilities=BackendCapabilities(
                snapshot=False,
                change_evidence=frozenset(),
                scan_estimate=None,
                cancellation=None,
                dialect_admission=True,
                exact_timestamps=True,
                exact_decimals=True,
                transaction_view=True,
                arrow_batches=True,
            ),
            features=frozenset({"naive_anchor_utc", "first_occurrence", "event_resolution"}),
            provider_module="terrine.execution._postgres",
            identity_requires_query=True,
            _dialect_path="terrine.backends.engines.postgres:PostgresDialect",
            _identity_path="terrine.backends.engines.postgres:identify",
        ),
        # One snapshot transaction in a private session on a new client.
        "bigquery": Engine(
            name="bigquery",
            display_name="BigQuery",
            ibis_module="ibis.backends.bigquery",
            session_strategy=SessionStrategy.SESSION_TRANSACTION,
            status="verified",
            capabilities=BackendCapabilities(
                snapshot=False,
                change_evidence=frozenset(),
                scan_estimate=None,
                cancellation=None,
                dialect_admission=True,
                exact_timestamps=True,
                exact_decimals=True,
                transaction_view=True,
                arrow_batches=True,
            ),
            features=frozenset({"naive_anchor_utc", "first_occurrence", "event_resolution"}),
            provider_module="terrine.execution._bigquery",
            identity_requires_query=False,
            _dialect_path="terrine.backends.engines.bigquery:BigQueryDialect",
            _identity_path="terrine.backends.engines.bigquery:identify",
        ),
        # Caller-owned Time Travel reads, with relation identity verified after every read.
        "snowflake": Engine(
            name="snowflake",
            display_name="Snowflake",
            ibis_module="ibis.backends.snowflake",
            session_strategy=SessionStrategy.TIME_TRAVEL,
            status="verified",
            capabilities=BackendCapabilities(
                snapshot=False,
                change_evidence=frozenset(),
                scan_estimate=None,
                cancellation=None,
                dialect_admission=True,
                exact_timestamps=True,
                exact_decimals=True,
                transaction_view=True,
                arrow_batches=True,
            ),
            features=frozenset({"naive_anchor_utc", "first_occurrence", "event_resolution"}),
            provider_module="terrine.execution._snowflake",
            identity_requires_query=False,
            _dialect_path="terrine.backends.engines.snowflake:SnowflakeDialect",
            _identity_path="terrine.backends.engines.snowflake:identify",
        ),
    }
)


GENERIC = Engine(
    name="generic",
    display_name="Unverified Ibis",
    ibis_module="",
    session_strategy=SessionStrategy.NO_SNAPSHOT,
    status="unverified",
    capabilities=BackendCapabilities(
        snapshot=False,
        transaction_view=False,
        retained_snapshot=False,
        table_publication=False,
        atomic_manifest_publish=False,
        runtime_reader=False,
        native_parquet_unload=False,
        change_evidence=frozenset(),
    ),
    features=frozenset({"naive_anchor_utc", "first_occurrence", "event_resolution"}),
    provider_module="terrine.execution._generic",
    identity_requires_query=False,
    _dialect_path="terrine.backends.dialect:Dialect",
    _identity_path="terrine.backends.registry:_identify_generic",
)


def backend_name(con: Any) -> str:
    """Return the actual connection name, independently of expression policy."""
    return str(getattr(con, "name", type(con).__name__)).casefold()


def expression_dialect(engine: Engine) -> str:
    """Return the expression policy selected by an admitted engine."""
    return engine.name


def _identify_generic(con: Any) -> None:
    from ibis.backends import BaseBackend

    if not isinstance(con, BaseBackend) or any(
        type(con).__module__ == engine.ibis_module for engine in ENGINES.values()
    ):
        raise _CAPABILITY_UNAVAILABLE.error(
            backend=backend_name(con),
            operation="query",
            status="unverified",
            route=supported_engine_route("query"),
        )


def get_engine(name: str) -> Engine | None:
    """Look up metadata without importing an engine implementation."""
    return GENERIC if name.casefold() == GENERIC.name else ENGINES.get(name.casefold())


def engine_supports(name: str, feature: str) -> bool:
    engine = get_engine(name)
    return engine is not None and feature in engine.features


def supported_engine_route(operation: str) -> str:
    """Offer only verified implementations of the requested operation."""
    destinations = []
    for engine in ENGINES.values():
        if engine.status != "verified":
            continue
        if operation in {"query", "dialect_admission"}:
            supported = engine.capabilities.dialect_admission is True
        elif operation in engine.capabilities.__dataclass_fields__:
            supported = getattr(engine.capabilities, operation) is True
        elif operation in engine.features:
            supported = True
        else:
            supported = operation in engine.dialect.operations
        if supported:
            destinations.append(engine.display_name)
    if not destinations:
        return "No registered verified engine provides this operation."
    return "Use a verified Ibis backend providing this operation: " + ", ".join(destinations) + "."


def resolve_engine(con: Any, *, allow_unverified: bool = False) -> Engine:
    """Admit an exact Ibis backend identity before schema or source access.

    With ``allow_unverified=True``, Ibis schema inspection may end an open
    caller transaction. Use a connection with no pending writes or a dedicated
    read connection.
    """
    for engine in ENGINES.values():
        if engine.status == "verified" and type(con).__module__ == engine.ibis_module:
            engine.identify(con)
            return engine
    _identify_generic(con)
    if allow_unverified:
        warnings.warn(
            f"Backend {backend_name(con)!r} is unverified. Reads are per statement, "
            "without a consistent multi-statement view, retained inputs or publication.",
            UnverifiedBackendWarning,
            stacklevel=2,
            skip_file_prefixes=(os.path.dirname(os.path.dirname(__file__)),),
        )
        return GENERIC
    raise _CAPABILITY_UNAVAILABLE.error(
        backend=backend_name(con),
        operation="query",
        status="unverified",
        route="Pass allow_unverified=True for unverified read-only use. "
        + supported_engine_route("query"),
    )
