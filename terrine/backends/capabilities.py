"""Operation evidence shared by execution and publication backends."""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["BackendCapabilities", "DUCKDB_CAPABILITIES", "UNVERIFIED_CAPABILITIES"]


@dataclass(frozen=True, slots=True)
class BackendCapabilities:
    """True is verified, False is unsupported, and None is unverified."""

    snapshot: bool | None = None
    change_evidence: frozenset[str] = frozenset()
    atomic_manifest_publish: bool | None = None
    scan_estimate: bool | None = None
    cancellation: bool | None = None


DUCKDB_CAPABILITIES = BackendCapabilities(
    snapshot=False,
    change_evidence=frozenset(),
    atomic_manifest_publish=True,
    scan_estimate=None,
    cancellation=None,
)
UNVERIFIED_CAPABILITIES = BackendCapabilities()
