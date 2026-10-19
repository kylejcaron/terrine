"""Bounded, generation-scoped Parquet export."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq

from terrine.data import Budget
from terrine.errors import RefusalSpec

__all__ = ["ExportManifest", "ExportShard", "export_dataset"]

_INVALID = RefusalSpec("terrine.dataset.invalid_export", "Invalid Parquet export: {reason}.")
_CONFLICT = RefusalSpec(
    "terrine.dataset.export_conflict",
    "Export destination already exists and will not be overwritten.",
)
_LIMIT = RefusalSpec(
    "terrine.budget.output_bytes_exceeded",
    "Parquet export exceeded the configured byte budget of {limit}.",
)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class ExportShard:
    path: str
    checksum: str
    row_count: int
    byte_count: int


@dataclass(frozen=True, slots=True)
class ExportManifest:
    dataset_id: str
    generation_id: str
    shards: tuple[ExportShard, ...]
    row_count: int
    byte_count: int
    schema: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "shards", tuple(self.shards))
        object.__setattr__(self, "schema", tuple(self.schema))

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "schema_version": 1,
                "dataset_id": self.dataset_id,
                "generation_id": self.generation_id,
                "shards": [
                    {
                        "path": shard.path,
                        "checksum": shard.checksum,
                        "row_count": shard.row_count,
                        "byte_count": shard.byte_count,
                    }
                    for shard in self.shards
                ],
                "row_count": self.row_count,
                "byte_count": self.byte_count,
                "schema": self.schema,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()

    @classmethod
    def from_json(cls, content: bytes | str) -> ExportManifest:
        payload = json.loads(content)
        if (
            not isinstance(payload, dict)
            or set(payload)
            != {
                "schema_version",
                "dataset_id",
                "generation_id",
                "shards",
                "row_count",
                "byte_count",
                "schema",
            }
            or payload["schema_version"] != 1
        ):
            raise ValueError("invalid export manifest")
        return cls(
            payload["dataset_id"],
            payload["generation_id"],
            tuple(ExportShard(**item) for item in payload["shards"]),
            payload["row_count"],
            payload["byte_count"],
            tuple(tuple(field) for field in payload["schema"]),
        )


def _existing_export(path: Path, dataset: Any) -> ExportManifest | None:
    try:
        manifest = ExportManifest.from_json((path / "export-manifest.json").read_bytes())
        generation = dataset.manifest.generation
        if (
            manifest.dataset_id != generation.dataset_id
            or manifest.generation_id != generation.generation_id
            or len({shard.path for shard in manifest.shards}) != len(manifest.shards)
        ):
            return None
        rows = bytes_ = 0
        root = path.resolve()
        for shard in manifest.shards:
            file_path = (root / shard.path).resolve()
            if not file_path.is_relative_to(root) or not file_path.is_file():
                return None
            size = file_path.stat().st_size
            if size != shard.byte_count or _hash_file(file_path) != shard.checksum:
                return None
            parquet = pq.ParquetFile(file_path)
            schema = tuple((field.name, str(field.type)) for field in parquet.schema_arrow)
            if schema != manifest.schema or parquet.metadata.num_rows != shard.row_count:
                return None
            rows += shard.row_count
            bytes_ += size
        if rows != manifest.row_count or bytes_ != manifest.byte_count:
            return None
        return manifest
    except (OSError, ValueError, TypeError, KeyError):
        return None


def _partition_rows(batch: pa.RecordBatch, columns: tuple[str, ...]):
    if not columns:
        yield (), pa.Table.from_batches([batch])
        return
    table = pa.Table.from_batches([batch])
    key_rows = zip(*(table[name].to_pylist() for name in columns), strict=True)
    groups: dict[tuple[Any, ...], list[int]] = {}
    for index, key in enumerate(key_rows):
        groups.setdefault(tuple(key), []).append(index)
    for key, indices in groups.items():
        yield key, table.take(pa.array(indices, type=pa.int64()))


def _shard_path(root: Path, key: tuple[Any, ...], columns: tuple[str, ...], index: int) -> Path:
    if key:
        directory = root
        for name, value in zip(columns, key, strict=True):
            encoded = "__null__" if value is None else quote(str(value), safe="")
            directory = directory / f"{quote(name, safe='')}={encoded}"
        directory.mkdir(parents=True, exist_ok=True)
    else:
        directory = root
    return directory / f"part-{index:08d}.parquet"


def export_dataset(
    dataset: Any,
    *,
    destination: str,
    partition_by: tuple[str, ...] = (),
    target_file_bytes: int = 134_217_728,
    budget: Budget | None = None,
) -> ExportManifest:
    if (
        isinstance(target_file_bytes, bool)
        or not isinstance(target_file_bytes, int)
        or target_file_bytes < 1
    ):
        raise _INVALID.error(reason="target_file_bytes must be positive")
    if not isinstance(partition_by, tuple) or any(
        not isinstance(item, str) for item in partition_by
    ):
        raise _INVALID.error(reason="partition_by must be a tuple of column names")
    names = {column.name for column in dataset.manifest.columns}
    if len(set(partition_by)) != len(partition_by) or not set(partition_by) <= names:
        raise _INVALID.error(reason="partition_by contains duplicate or unknown columns")
    destination_path = Path(destination).expanduser()
    if destination_path.exists():
        existing = _existing_export(destination_path, dataset)
        if existing is not None:
            return existing
        raise _CONFLICT.error()
    parent = destination_path.parent.resolve()
    if not parent.is_dir():
        raise _INVALID.error(reason="destination parent directory must exist")
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination_path.name}-", dir=parent))
    schema: pa.Schema | None = None
    row_count = byte_count = 0
    shard_records: list[ExportShard] = []
    shard_index = 0
    active_path: Path | None = None
    active_key: tuple[Any, ...] | None = None
    writer: pq.ParquetWriter | None = None
    current_estimate = 0
    current_rows = 0

    def close_writer() -> None:
        nonlocal writer, active_path, active_key, current_estimate, current_rows, byte_count
        if writer is None or active_path is None:
            return
        writer.close()
        size = active_path.stat().st_size
        relative = str(active_path.relative_to(temporary))
        checksum = _hash_file(active_path)
        shard_records.append(ExportShard(relative, checksum, current_rows, size))
        byte_count += size
        writer = None
        active_path = None
        active_key = None
        current_estimate = current_rows = 0

    try:
        for batch in dataset.to_arrow_batches():
            if schema is None:
                schema = batch.schema
            elif batch.schema != schema:
                raise _INVALID.error(reason="Arrow batch schema changed during export")
            row_count += batch.num_rows
            for key, table in _partition_rows(batch, partition_by):
                estimated = table.nbytes
                if writer is not None and (
                    key != active_key
                    or (current_estimate and current_estimate + estimated > target_file_bytes)
                ):
                    close_writer()
                if writer is None:
                    active_path = _shard_path(temporary, key, partition_by, shard_index)
                    writer = pq.ParquetWriter(active_path, schema, compression="zstd")
                    active_key = key
                    shard_index += 1
                assert writer is not None
                writer.write_table(table)
                current_estimate += estimated
                current_rows += table.num_rows
        close_writer()
        if (
            budget is not None
            and budget.max_output_bytes is not None
            and byte_count > budget.max_output_bytes
        ):
            raise _LIMIT.error(limit=budget.max_output_bytes)
        if schema is None:
            schema = pa.schema(
                [
                    (column.name, pa.type_for_alias(column.data_type))
                    for column in dataset.manifest.columns
                ]
            )
        manifest = ExportManifest(
            dataset.manifest.generation.dataset_id,
            dataset.manifest.generation.generation_id,
            tuple(shard_records),
            row_count,
            byte_count,
            tuple((field.name, str(field.type)) for field in schema),
        )
        manifest_path = temporary / "export-manifest.json"
        with manifest_path.open("xb") as stream:
            stream.write(manifest.to_json())
            stream.flush()
            os.fsync(stream.fileno())
        reserved = False
        try:
            os.mkdir(destination_path)
            reserved = True
        except FileExistsError as error:
            raise _CONFLICT.error() from error
        children = sorted(
            temporary.iterdir(), key=lambda child: child.name == "export-manifest.json"
        )
        for child in children:
            child.rename(destination_path / child.name)
        temporary.rmdir()
        return manifest
    except Exception:
        if writer is not None:
            writer.close()
        shutil.rmtree(temporary, ignore_errors=True)
        if "reserved" in locals() and reserved:
            shutil.rmtree(destination_path, ignore_errors=True)
        raise
