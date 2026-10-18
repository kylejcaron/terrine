from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

import pyarrow as pa

from terrine.runtime.leases import Lease

__all__ = [
    "BackendCapabilities",
    "PublicationReceipt",
    "Reconciliation",
    "RuntimeBackend",
    "StagedGeneration",
]


@dataclass(frozen=True, slots=True)
class BackendCapabilities:
    snapshot: bool | None
    change_evidence: frozenset[str]
    atomic_manifest_publish: bool | None
    scan_estimate: bool | None
    cancellation: bool | None


@dataclass(frozen=True, slots=True)
class StagedGeneration:
    dataset: str
    generation: str
    partitions: Mapping[str, pa.Table]
    run_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "partitions", MappingProxyType(dict(self.partitions)))


@dataclass(frozen=True, slots=True)
class PublicationReceipt:
    dataset: str
    generation: str
    manifest_version: int
    partitions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Reconciliation:
    run_id: str
    published_generation: str | None
    manifest_version: int | None


class RuntimeBackend(Protocol):
    capabilities: BackendCapabilities

    def publish_generation(
        self, stage: StagedGeneration, *, expected_manifest: str | None, lease: Lease
    ) -> PublicationReceipt: ...

    def reconcile(self, run_id: str) -> Reconciliation: ...
