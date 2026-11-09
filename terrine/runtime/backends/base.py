from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

from terrine.backends.capabilities import BackendCapabilities
from terrine.errors import RefusalSpec
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
    execution_revision: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "partitions", MappingProxyType(dict(self.partitions)))
        if self.execution_revision is not None and (
            type(self.execution_revision) is not int or self.execution_revision <= 0
        ):
            raise _EXECUTION_IDENTITY.error(reason="invalid_execution_revision")


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

    def generation_execution_revision(self, dataset: str, generation: str) -> int: ...


_EXECUTION_KEY = b"terrine.execution"
_FRAME_KEY = b"terrine.materialized_frame.v2"
_EXECUTION_IDENTITY = RefusalSpec(
    "terrine.runtime.saved_plan_mismatch",
    "Execution identity failed verification ({reason}); rebuild the plan and refresh.",
)


def _schema_execution_revision(schema: pa.Schema) -> int:
    raw = (schema.metadata or {}).get(_EXECUTION_KEY)
    if raw is None:
        return 0
    try:
        marker = json.loads(raw)
        if (
            not isinstance(marker, dict)
            or set(marker) != {"version", "execution_revision"}
            or type(marker["version"]) is not int
            or marker["version"] != 1
            or type(marker["execution_revision"]) is not int
            or marker["execution_revision"] <= 0
        ):
            raise ValueError("invalid marker")
        revision = marker["execution_revision"]
        frame = (schema.metadata or {}).get(_FRAME_KEY)
        if frame is not None:
            provenance = json.loads(frame)["provenance"]
            value = provenance.get("execution_revision")
            if type(value) is not int or value != revision:
                raise ValueError("provenance differs")
        return revision
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        raise _EXECUTION_IDENTITY.error(reason="invalid_execution_marker") from error


def _execution_payload(
    payload: pa.Table | pa.RecordBatchReader, revision: int | None
) -> pa.Table | pa.RecordBatchReader:
    """Stamp execution context without changing historical schemas or provenance."""
    if not isinstance(payload, (pa.Table, pa.RecordBatchReader)):
        raise TypeError("partition data must be a PyArrow Table or RecordBatchReader")
    schema = payload.schema
    existing = _schema_execution_revision(schema)
    if revision is None:
        if existing:
            raise _EXECUTION_IDENTITY.error(reason="execution_context_required")
        return payload
    if existing not in {0, revision}:
        raise _EXECUTION_IDENTITY.error(reason="execution_marker_mismatch")
    metadata = dict(schema.metadata or {})
    frame = metadata.get(_FRAME_KEY)
    if frame is not None:
        try:
            provenance_revision = json.loads(frame)["provenance"].get("execution_revision")
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            raise _EXECUTION_IDENTITY.error(reason="invalid_frame_provenance") from error
        if provenance_revision != revision or type(provenance_revision) is not int:
            raise _EXECUTION_IDENTITY.error(reason="frame_execution_revision_mismatch")
    metadata[_EXECUTION_KEY] = json.dumps(
        {"version": 1, "execution_revision": revision}, sort_keys=True, separators=(",", ":")
    ).encode()
    schema = schema.with_metadata(metadata)
    if isinstance(payload, pa.Table):
        return payload.replace_schema_metadata(metadata)

    def batches():
        try:
            for batch in payload:
                yield batch.replace_schema_metadata(metadata)
        finally:
            payload.close()

    return pa.RecordBatchReader.from_batches(schema, batches())
