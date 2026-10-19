"""Durable bounded Arrow task outputs retained by the runtime owner."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from terrine.data import _OUTPUT_BYTES_EXCEEDED, _ROWS_EXCEEDED, Budget

_PREFIX = b"terrine.arrow.output.v1\n"


def _checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class ArrowOutput:
    path: Path
    checksum: str
    num_rows: int
    nbytes: int

    @classmethod
    def write(cls, reader: pa.RecordBatchReader, *, root: Path, budget: Budget | None = None):
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{uuid.uuid4().hex}.arrow"
        temporary = path.with_suffix(".tmp")
        rows = size = 0
        try:
            with temporary.open("wb") as stream:
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
            return cls(path, _checksum(path), rows, size)
        finally:
            reader.close()
            temporary.unlink(missing_ok=True)

    def open(self) -> pa.RecordBatchReader:
        if _checksum(self.path) != self.checksum:
            raise ValueError("retained Arrow output checksum differs")
        stream = self.path.open("rb")
        try:
            return pa.ipc.open_stream(stream)
        except Exception:
            stream.close()
            raise

    @property
    def schema(self) -> pa.Schema:
        with self.open() as reader:
            return reader.schema

    def to_bytes(self) -> bytes:
        return (
            _PREFIX
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
        data = json.loads(payload.removeprefix(_PREFIX))
        return cls(Path(data["path"]), data["checksum"], data["num_rows"], data["nbytes"])
