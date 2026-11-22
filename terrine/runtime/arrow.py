"""Durable bounded Arrow task outputs retained by the runtime owner."""

from __future__ import annotations

import hashlib
import io
import json
import os
import uuid
from collections.abc import Buffer
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from terrine.data import _OUTPUT_BYTES_EXCEEDED, _ROWS_EXCEEDED, Budget

if TYPE_CHECKING:
    from terrine.execution.inputs import ArrowReader

_PREFIX = b"terrine.arrow.output.v1\n"
_BOUND_PREFIX = b"terrine.arrow.output.v2\n"
_OUTPUT_PREFIXES = (_PREFIX, _BOUND_PREFIX)
_BINDING_SIZE = hashlib.sha256().digest_size
_OUTPUT_RUN: ContextVar[str | None] = ContextVar("terrine_output_run", default=None)


def _output_binding(owner: str, namespace: str, run_id: str, task_id: str) -> bytes:
    identity = json.dumps((owner, namespace, run_id, task_id), separators=(",", ":")).encode()
    return hashlib.sha256(_BOUND_PREFIX + identity).digest()


def _owner_output_directory(owner_reference: str, namespace: str, fingerprint: str) -> Path:
    owner_key = hashlib.sha256(owner_reference.encode()).hexdigest()
    return Path(namespace, "outputs", owner_key, fingerprint)


def _task_key(task_id: str) -> str:
    return hashlib.sha256(task_id.encode()).hexdigest()


def _checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


class _HashedWriter(io.BufferedWriter):
    def __init__(self, path: Path) -> None:
        super().__init__(path.open("wb", buffering=0))
        self.digest = hashlib.sha256()

    def write(self, buffer: Buffer, /) -> int:
        size = super().write(buffer)
        self.digest.update(buffer)
        return size


class _OutputBindingError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ArrowOutput:
    path: Path
    checksum: str
    num_rows: int
    nbytes: int
    bound: bool = False

    @classmethod
    def write(
        cls,
        reader: ArrowReader,
        *,
        root: Path,
        budget: Budget | None = None,
        binding: bytes | None = None,
    ):
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{uuid.uuid4().hex}.arrow"
        temporary = path.with_suffix(".tmp")
        rows = size = 0
        try:
            with _HashedWriter(temporary) as stream:
                if binding is not None:
                    stream.write(binding)
                with pa.ipc.new_stream(stream, reader.schema) as writer:
                    for batch in reader:
                        if not batch.schema.equals(reader.schema, check_metadata=True):
                            batch = batch.cast(reader.schema)
                        rows += batch.num_rows
                        size += batch.nbytes
                        if budget is not None:
                            if budget.max_rows is not None and rows > budget.max_rows:
                                raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
                            if (
                                budget.max_output_bytes is not None
                                and size > budget.max_output_bytes
                            ):
                                raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
                        writer.write_batch(batch)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)
            return cls(path, stream.digest.hexdigest(), rows, size, binding is not None)
        finally:
            reader.close()
            temporary.unlink(missing_ok=True)

    def open(self) -> pa.RecordBatchReader:
        if _checksum(self.path) != self.checksum:
            raise ValueError("retained Arrow output checksum differs")
        return self._open_verified(self.path)

    def _open_verified(self, path: Path, *, binding: bytes | None = None) -> pa.RecordBatchReader:
        if not self.bound:
            return pa.ipc.open_stream(str(path))
        stream = pa.OSFile(str(path), "rb")
        try:
            recorded = stream.read(_BINDING_SIZE)
            if len(recorded) != _BINDING_SIZE or (binding is not None and recorded != binding):
                raise _OutputBindingError("retained Arrow output binding differs")
            return pa.ipc.open_stream(stream)
        except BaseException:
            stream.close()
            raise

    @property
    def schema(self) -> pa.Schema:
        with self.open() as reader:
            return reader.schema

    def to_bytes(self) -> bytes:
        return (
            (_BOUND_PREFIX if self.bound else _PREFIX)
            + json.dumps(
                {
                    "path": str(self.path),
                    "checksum": self.checksum,
                    "num_rows": self.num_rows,
                    "nbytes": self.nbytes,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )

    @classmethod
    def from_bytes(cls, payload: bytes):
        prefix = next((prefix for prefix in _OUTPUT_PREFIXES if payload.startswith(prefix)), None)
        if prefix is None:
            raise ValueError("unsupported retained Arrow output version")
        data = json.loads(payload.removeprefix(prefix))
        return cls(
            Path(data["path"]),
            data["checksum"],
            data["num_rows"],
            data["nbytes"],
            prefix == _BOUND_PREFIX,
        )
