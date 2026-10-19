from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

from terrine.backends.capabilities import BackendCapabilities
from terrine.runtime.leases import Lease

if TYPE_CHECKING:
    from terrine.runtime.planner import TaskSpec

__all__ = [
    "BackendCapabilities",
    "CostProvider",
    "PublicationReceipt",
    "Reconciliation",
    "RuntimeBackend",
    "StagedGeneration",
]


@dataclass(frozen=True, slots=True)
class StagedGeneration:
    dataset: str
    generation: str
    partitions: Mapping[str, pa.Table | pa.RecordBatchReader]
    run_id: str | None = None
    snapshot_id: str | None = None

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


class CostProvider(Protocol):
    """Owner metadata only: no source-row reads while estimating a task."""

    capabilities: BackendCapabilities

    def estimate_task(
        self, task: TaskSpec, *, definition_hash: str, snapshot_id: str
    ) -> TaskSpec: ...


class RuntimeBackend(Protocol):
    capabilities: BackendCapabilities

    def publish_generation(
        self, stage: StagedGeneration, *, expected_manifest: str | None, lease: Lease
    ) -> PublicationReceipt: ...

    def reconcile(self, run_id: str) -> Reconciliation: ...
