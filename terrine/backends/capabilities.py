"""Operation evidence shared by execution and publication backends."""

from __future__ import annotations

from dataclasses import dataclass

from terrine.errors import RefusalSpec

__all__ = [
    "BIGQUERY_CAPABILITIES",
    "BackendCapabilities",
    "DUCKDB_CAPABILITIES",
    "POSTGRES_CAPABILITIES",
    "SNOWFLAKE_CAPABILITIES",
    "UNVERIFIED_CAPABILITIES",
]

_CAPABILITY_UNAVAILABLE = RefusalSpec(
    "terrine.backend.capability_unavailable",
    "Backend {backend!r} cannot provide {operation!r}: {status}. {route}",
)


@dataclass(frozen=True, slots=True)
class BackendCapabilities:
    """True is verified, False is unsupported, and None is unverified."""

    snapshot: bool | None = None
    change_evidence: frozenset[str] | None = None
    atomic_manifest_publish: bool | None = None
    scan_estimate: bool | None = None
    cancellation: bool | None = None
    dialect_admission: bool | None = None
    exact_timestamps: bool | None = None
    exact_decimals: bool | None = None
    transaction_view: bool | None = None
    retained_snapshot: bool | None = None
    table_publication: bool | None = None
    arrow_batches: bool | None = None
    native_parquet_unload: bool | None = None
    scan_limit: bool | None = None
    # Reading owner-published generations and manifests (Dataset, open_generation).
    runtime_reader: bool | None = None

    def require(self, operation: str, *, backend: str, route: str) -> None:
        """Refuse only the requested operation when its evidence is absent."""
        value = getattr(self, operation)
        if value is not True:
            raise _CAPABILITY_UNAVAILABLE.error(
                backend=backend,
                operation=operation,
                status="unsupported" if value is False else "unverified",
                route=route,
            )


DUCKDB_CAPABILITIES = BackendCapabilities(
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
)
# Materialization reads one REPEATABLE READ READ ONLY transaction on a private
# connection. Timestamps are exact at microseconds; runtime staging, retained
# snapshots and publication stay on DuckDB.
POSTGRES_CAPABILITIES = BackendCapabilities(
    snapshot=False,
    change_evidence=frozenset(),
    scan_estimate=None,
    cancellation=None,
    dialect_admission=True,
    exact_timestamps=True,
    exact_decimals=True,
    transaction_view=True,
    arrow_batches=True,
)
# Materialization reads one snapshot transaction in a private session on a new
# client. Timestamps are exact at microseconds; staging, retained snapshots and
# publication stay on DuckDB.
BIGQUERY_CAPABILITIES = BackendCapabilities(
    snapshot=False,
    change_evidence=frozenset(),
    scan_estimate=None,
    cancellation=None,
    dialect_admission=True,
    exact_timestamps=True,
    exact_decimals=True,
    transaction_view=True,
    arrow_batches=True,
)
# Materialization pins every read to one Time Travel instant on the caller's
# connection (Snowflake isolates per statement, so a transaction would not).
# Timestamps are exact at nanoseconds; staging, retained snapshots and
# publication stay on DuckDB. Unverified until the live tier runs.
SNOWFLAKE_CAPABILITIES = BackendCapabilities(
    snapshot=False,
    change_evidence=frozenset(),
    scan_estimate=None,
    cancellation=None,
    dialect_admission=True,
    exact_timestamps=True,
    exact_decimals=True,
    transaction_view=True,
    arrow_batches=True,
)
UNVERIFIED_CAPABILITIES = BackendCapabilities()
