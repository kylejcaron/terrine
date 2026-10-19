"""Estimator-side batch-source contracts, free of query and runtime dependencies."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Protocol

import pyarrow as pa

from terrine.data import ColumnMeta, MaterializedFrame, Provenance, Roles


class BatchSource(Protocol):
    """Replayable typed rows pinned to one immutable published generation."""

    schema: pa.Schema
    roles: Roles
    columns: Mapping[str, ColumnMeta]
    provenance: Provenance
    snapshot_id: str
    generation_id: str

    def reopen(self) -> BatchSource: ...

    def iter_batches(self, batch_size: int) -> Iterator[pa.RecordBatch]: ...

    def close(self) -> None: ...

    def cancel(self) -> None: ...


class MaterializedBatchSource:
    """Test/local adapter exposing an existing Arrow frame through bounded batches."""

    __slots__ = (
        "_frame",
        "schema",
        "roles",
        "columns",
        "provenance",
        "snapshot_id",
        "generation_id",
        "_closed",
    )

    def __init__(self, frame: MaterializedFrame, *, snapshot_id: str, generation_id: str) -> None:
        if not isinstance(frame, MaterializedFrame):
            raise TypeError("frame must be a MaterializedFrame")
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise ValueError("snapshot_id must be a non-empty string")
        if not isinstance(generation_id, str) or not generation_id:
            raise ValueError("generation_id must be a non-empty string")
        self._frame = frame
        self.schema = frame.table.schema
        self.roles = frame.roles
        self.columns = frame.columns
        self.provenance = frame.provenance
        self.snapshot_id = snapshot_id
        self.generation_id = generation_id
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def reopen(self) -> MaterializedBatchSource:
        if self._closed:
            raise RuntimeError("cannot reopen a closed batch source")
        return type(self)(
            self._frame, snapshot_id=self.snapshot_id, generation_id=self.generation_id
        )

    def iter_batches(self, batch_size: int) -> Iterator[pa.RecordBatch]:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        if self._closed:
            raise RuntimeError("batch source is closed")
        for batch in self._frame.table.to_batches(max_chunksize=batch_size):
            if self._closed:
                raise RuntimeError("batch source is closed")
            yield batch

    def close(self) -> None:
        self._closed = True

    def cancel(self) -> None:
        self.close()


__all__ = ["BatchSource", "MaterializedBatchSource"]
