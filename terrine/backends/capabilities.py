"""Operation evidence shared by execution and publication backends."""

from __future__ import annotations

from dataclasses import dataclass

from terrine.errors import RefusalSpec

__all__ = ["BackendCapabilities", "UNVERIFIED_CAPABILITIES"]

_CAPABILITY_UNAVAILABLE = RefusalSpec(
    "terrine.backend.capability_unavailable",
    "Backend {backend!r} cannot provide {operation!r}: {status}. {route}",
)

_SNAPSHOT_UNAVAILABLE = RefusalSpec(
    "terrine.execution.snapshot_unavailable",
    "A coherent native execution snapshot is unavailable: {reason}.",
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

    def require(
        self, operation: str, *, backend: str, route: str, allow_unverified: bool = False
    ) -> None:
        """Admit unverified read primitives only with explicit permission."""
        value = getattr(self, operation)
        if (
            value is None
            and allow_unverified
            and operation
            in {"dialect_admission", "exact_timestamps", "exact_decimals", "arrow_batches"}
        ):
            return
        if value is not True:
            raise _CAPABILITY_UNAVAILABLE.error(
                backend=backend,
                operation=operation,
                status="unsupported" if value is False else "unverified",
                route=route,
            )


UNVERIFIED_CAPABILITIES = BackendCapabilities()
