"""Bounded, generation-scoped Parquet export."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq

from terrine.data import _OUTPUT_BYTES_EXCEEDED, _ROWS_EXCEEDED, Budget
from terrine.errors import RefusalSpec

__all__ = ["ExportManifest", "ExportShard", "export_dataset"]

_INVALID = RefusalSpec("terrine.dataset.invalid_export", "Invalid Parquet export: {reason}.")
_CONFLICT = RefusalSpec(
    "terrine.dataset.export_conflict",
    "Export destination conflicts with an existing export or partial export.",
)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1_048_576), b""):
            digest.update(chunk)
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
    partition_by: tuple[str, ...] = ()
    target_file_bytes: int = 134_217_728

    def __post_init__(self) -> None:
        object.__setattr__(self, "shards", tuple(self.shards))
        object.__setattr__(self, "schema", tuple(self.schema))
        object.__setattr__(self, "partition_by", tuple(self.partition_by))

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "schema_version": 2,
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
                "partition_by": self.partition_by,
                "target_file_bytes": self.target_file_bytes,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()

    @classmethod
    def from_json(cls, content: bytes | str) -> ExportManifest:
        payload = json.loads(content)
        expected = {
            "schema_version",
            "dataset_id",
            "generation_id",
            "shards",
            "row_count",
            "byte_count",
            "schema",
            "partition_by",
            "target_file_bytes",
        }
        if (
            not isinstance(payload, dict)
            or set(payload) != expected
            or payload["schema_version"] != 2
        ):
            raise ValueError("invalid export manifest")
        return cls(
            payload["dataset_id"],
            payload["generation_id"],
            tuple(ExportShard(**item) for item in payload["shards"]),
            payload["row_count"],
            payload["byte_count"],
            tuple(tuple(field) for field in payload["schema"]),
            tuple(payload["partition_by"]),
            payload["target_file_bytes"],
        )


def _verify_shards(root: Path, manifest: ExportManifest) -> bool:
    try:
        root = root.resolve()
        rows = byte_count = 0
        seen: set[str] = set()
        for shard in manifest.shards:
            if shard.path in seen:
                return False
            seen.add(shard.path)
            file_path = (root / shard.path).resolve()
            if not file_path.is_relative_to(root) or not file_path.is_file():
                return False
            size = file_path.stat().st_size
            if size != shard.byte_count or _hash_file(file_path) != shard.checksum:
                return False
            parquet = pq.ParquetFile(file_path)
            schema = tuple((field.name, str(field.type)) for field in parquet.schema_arrow)
            if schema != manifest.schema or parquet.metadata.num_rows != shard.row_count:
                return False
            rows += shard.row_count
            byte_count += size
        return rows == manifest.row_count and byte_count == manifest.byte_count
    except (OSError, ValueError, TypeError, KeyError):
        return False


def _existing_export(
    path: Path,
    dataset: Any,
    *,
    partition_by: tuple[str, ...],
    target_file_bytes: int,
    budget: Budget | None,
) -> ExportManifest | None:
    try:
        manifest = ExportManifest.from_json((path / "export-manifest.json").read_bytes())
    except (OSError, ValueError, TypeError, KeyError):
        return None
    generation = dataset.manifest.generation
    if (
        manifest.dataset_id != generation.dataset_id
        or manifest.generation_id != generation.generation_id
        or manifest.partition_by != partition_by
        or manifest.target_file_bytes != target_file_bytes
        or not _verify_shards(path, manifest)
    ):
        return None
    if budget is not None:
        if budget.max_rows is not None and manifest.row_count > budget.max_rows:
            raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
        if budget.max_output_bytes is not None and manifest.byte_count > budget.max_output_bytes:
            raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
    return manifest


def _partition_rows(batch: pa.RecordBatch, columns: tuple[str, ...]):
    if not columns:
        yield (), pa.Table.from_batches([batch])
        return
    table = pa.Table.from_batches([batch])
    groups: dict[tuple[Any, ...], list[int]] = {}
    for index, key in enumerate(zip(*(table[name].to_pylist() for name in columns), strict=True)):
        groups.setdefault(tuple(key), []).append(index)
    for key, indices in groups.items():
        yield key, table.take(pa.array(indices, type=pa.int64()))


def _shard_path(root: Path, key: tuple[Any, ...], columns: tuple[str, ...], index: int) -> Path:
    directory = root
    for name, value in zip(columns, key, strict=True):
        encoded = "__null__" if value is None else quote(str(value), safe="")
        directory = directory / f"{quote(name, safe='')}={encoded}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"part-{index:08d}.parquet"


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_state(root: Path, manifest: ExportManifest) -> None:
    temporary = root / ".export-state.tmp"
    with temporary.open("wb") as stream:
        stream.write(manifest.to_json())
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(root / "export-state.json")
    _sync_directory(root)


def _schema(dataset: Any) -> pa.Schema:
    return dataset._arrow_schema()


def export_dataset(
    dataset: Any,
    *,
    destination: str,
    partition_by: tuple[str, ...] = (),
    target_file_bytes: int = 134_217_728,
    budget: Budget | None = None,
) -> ExportManifest:
    destination_path = Path(destination).expanduser()
    if not destination_path.parent.is_dir():
        raise _INVALID.error(reason="destination parent directory must exist")
    lock_path = destination_path.parent / f".{destination_path.name}.lock"
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise _CONFLICT.error() from None
        return _export_dataset_locked(
            dataset,
            destination=destination,
            partition_by=partition_by,
            target_file_bytes=target_file_bytes,
            budget=budget,
        )
    finally:
        os.close(descriptor)


def _export_dataset_locked(
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
    partial = destination_path.parent / f".{destination_path.name}.partial"
    if destination_path.exists():
        existing = _existing_export(
            destination_path,
            dataset,
            partition_by=partition_by,
            target_file_bytes=target_file_bytes,
            budget=budget,
        )
        if existing is not None:
            _sync_directory(destination_path.parent)
            return existing
        raise _CONFLICT.error()
    parent = destination_path.parent.resolve()
    if not parent.is_dir():
        raise _INVALID.error(reason="destination parent directory must exist")
    if partial.exists():
        try:
            state = ExportManifest.from_json((partial / "export-state.json").read_bytes())
        except (OSError, ValueError, TypeError, KeyError):
            raise _CONFLICT.error() from None
        generation = dataset.manifest.generation
        if (
            state.dataset_id != generation.dataset_id
            or state.generation_id != generation.generation_id
            or state.partition_by != partition_by
            or state.target_file_bytes != target_file_bytes
            or not _verify_shards(partial, state)
        ):
            raise _CONFLICT.error()
        records = list(state.shards)
        schema = _schema(dataset)
    else:
        schema = _schema(dataset)
        partial.mkdir()
        records = []
    by_path = {record.path: record for record in records}
    row_count = byte_count = 0
    shard_index = 0
    active_path: Path | None = None
    active_key: tuple[Any, ...] | None = None
    writer: pq.ParquetWriter | None = None
    skip_active = False
    current_estimate = current_rows = 0

    def checkpoint() -> None:
        manifest = ExportManifest(
            dataset.manifest.generation.dataset_id,
            dataset.manifest.generation.generation_id,
            tuple(records),
            sum(shard.row_count for shard in records),
            sum(shard.byte_count for shard in records),
            tuple((field.name, str(field.type)) for field in schema),
            partition_by,
            target_file_bytes,
        )
        _write_state(partial, manifest)

    if not records:
        try:
            _sync_directory(parent)
            checkpoint()
        except Exception:
            import shutil

            shutil.rmtree(partial, ignore_errors=True)
            raise

    def close_writer() -> None:
        nonlocal writer, active_path, active_key, skip_active, current_estimate, current_rows
        if active_path is None:
            return
        if writer is not None:
            writer.close()
            with active_path.open("rb") as stream:
                os.fsync(stream.fileno())
            directory = active_path.parent
            while True:
                _sync_directory(directory)
                if directory == partial:
                    break
                directory = directory.parent
            size = active_path.stat().st_size
            relative = str(active_path.relative_to(partial))
            record = ExportShard(relative, _hash_file(active_path), current_rows, size)
            records.append(record)
            by_path[relative] = record
        elif skip_active:
            record = by_path[str(active_path.relative_to(partial))]
            if current_rows != record.row_count:
                raise _CONFLICT.error()
        writer = None
        active_path = None
        active_key = None
        skip_active = False
        current_estimate = current_rows = 0
        checkpoint()

    batches = dataset.to_arrow_batches()
    try:
        for batch in batches:
            if batch.schema != schema:
                raise _INVALID.error(reason="Arrow batch schema changed during export")
            row_count += batch.num_rows
            if budget is not None and budget.max_rows is not None and row_count > budget.max_rows:
                raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
            for key, table in _partition_rows(batch, partition_by):
                estimated = table.nbytes
                if active_path is not None and (
                    key != active_key
                    or (current_estimate and current_estimate + estimated > target_file_bytes)
                ):
                    close_writer()
                if active_path is None:
                    shard_path = _shard_path(partial, key, partition_by, shard_index)
                    active_path = shard_path
                    relative = str(shard_path.relative_to(partial))
                    active_key = key
                    shard_index += 1
                    known = by_path.get(relative)
                    if known is not None and _verify_shards(
                        partial,
                        ExportManifest(
                            dataset.manifest.generation.dataset_id,
                            dataset.manifest.generation.generation_id,
                            (known,),
                            known.row_count,
                            known.byte_count,
                            tuple((field.name, str(field.type)) for field in schema),
                            partition_by,
                            target_file_bytes,
                        ),
                    ):
                        skip_active = True
                    else:
                        shard_path.unlink(missing_ok=True)
                        writer = pq.ParquetWriter(shard_path, schema, compression="zstd")
                if writer is not None:
                    writer.write_table(table)
                current_estimate += estimated
                current_rows += table.num_rows
        if row_count == 0 and not records and active_path is None:
            active_path = _shard_path(partial, (), (), 0)
            writer = pq.ParquetWriter(active_path, schema, compression="zstd")
        close_writer()
        byte_count = sum(shard.byte_count for shard in records)
        if (
            budget is not None
            and budget.max_output_bytes is not None
            and byte_count > budget.max_output_bytes
        ):
            raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
        manifest = ExportManifest(
            dataset.manifest.generation.dataset_id,
            dataset.manifest.generation.generation_id,
            tuple(records),
            row_count,
            byte_count,
            tuple((field.name, str(field.type)) for field in schema),
            partition_by,
            target_file_bytes,
        )
        with (partial / "export-manifest.json").open("wb") as stream:
            stream.write(manifest.to_json())
            stream.flush()
            os.fsync(stream.fileno())
        _sync_directory(partial)
        os.rename(partial, destination_path)
        _sync_directory(parent)
        return manifest
    except Exception:
        if active_path is not None:
            if writer is not None:
                try:
                    writer.close()
                finally:
                    active_path.unlink(missing_ok=True)
            writer = None
            active_path = None
            active_key = None
            skip_active = False
            current_estimate = current_rows = 0
            checkpoint()
        raise
    finally:
        batches.close()
