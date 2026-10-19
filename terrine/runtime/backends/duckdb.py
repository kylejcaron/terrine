from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Generator
from typing import Any

import ibis
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from ibis.backends.duckdb import Backend as IbisDuckDBBackend

from terrine.errors import RefusalSpec
from terrine.runtime.backends.base import (
    BackendCapabilities,
    PublicationReceipt,
    Reconciliation,
    StagedGeneration,
)
from terrine.runtime.leases import LeaseStore
from terrine.runtime.state import RuntimeStore, snapshot_manifest_reference

__all__ = ["DuckDBBackend"]

_LAZY_DURATION_UNSUPPORTED = RefusalSpec(
    "terrine.runtime.lazy_duration_unsupported",
    "DuckDB lazy reads cannot represent duration unit {unit!r} in column {column!r}; use {route}.",
)


class DuckDBBackend:
    """DuckDB publication via one transaction containing immutable tables and manifest switch."""

    capabilities = BackendCapabilities(
        snapshot=False,
        change_evidence=frozenset(),
        atomic_manifest_publish=True,
        scan_estimate=None,
        cancellation=None,
    )

    def __init__(self, con: Any, *, namespace: str) -> None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
            raise ValueError("namespace must be a simple SQL identifier")
        self.con = con
        self.namespace = namespace
        self._schema = f'"{namespace}"'
        self.fault_injector: Callable[[str], None] | None = None
        self._ibis_backend: Any | None = None
        self.initialize()

    def initialize(self) -> None:
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_manifests ("
            "dataset VARCHAR PRIMARY KEY, generation VARCHAR NOT NULL, version BIGINT NOT NULL, "
            "snapshot_id VARCHAR)"
        )
        self.con.execute(
            f"ALTER TABLE {self._schema}.runtime_manifests "
            "ADD COLUMN IF NOT EXISTS snapshot_id VARCHAR"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_publications ("
            "dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, version BIGINT NOT NULL, "
            "PRIMARY KEY(dataset, generation))"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_run_publications ("
            "run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, "
            "version BIGINT NOT NULL)"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_partitions ("
            "dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, "
            "partition VARCHAR NOT NULL, table_name VARCHAR NOT NULL, "
            "digest VARCHAR NOT NULL, PRIMARY KEY(dataset, generation, partition))"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_partition_metadata ("
            "dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, partition VARCHAR NOT NULL, "
            "schema_metadata BLOB NOT NULL, PRIMARY KEY(dataset, generation, partition))"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_snapshot_references ("
            "snapshot_id VARCHAR NOT NULL, kind VARCHAR NOT NULL, reference VARCHAR NOT NULL, "
            "PRIMARY KEY(snapshot_id, kind, reference))"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_snapshot_gc ("
            "snapshot_id VARCHAR PRIMARY KEY)"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_snapshot_guards ("
            "snapshot_id VARCHAR PRIMARY KEY, guard_version BIGINT NOT NULL)"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_snapshot_manifests ("
            "dataset VARCHAR PRIMARY KEY, generation VARCHAR NOT NULL, snapshot_id VARCHAR)"
        )
        LeaseStore(self.con, namespace=self.namespace).initialize()

    def publish_generation(
        self,
        stage: StagedGeneration,
        *,
        expected_manifest: str | None,
        lease,
    ) -> PublicationReceipt:
        if not stage.partitions:
            raise ValueError("generation must contain at least one partition")
        if lease.resource != f"dataset:{stage.dataset}":
            raise ValueError("lease resource does not match staged dataset")
        lease_store = LeaseStore(self.con, namespace=self.namespace)
        lease_store.assert_current(lease)
        current = self.current_generation(stage.dataset)
        if current == stage.generation:
            manifest_snapshot = self.con.execute(
                f"SELECT snapshot_id FROM {self._schema}.runtime_manifests WHERE dataset=?",
                [stage.dataset],
            ).fetchone()
            if manifest_snapshot is None or manifest_snapshot[0] != stage.snapshot_id:
                raise ValueError("retry snapshot identity does not match published generation")
            receipt = self._matching_receipt(stage)
            if stage.run_id is not None:
                self.con.execute("BEGIN TRANSACTION")
                try:
                    self._record_run_publication(stage, receipt.manifest_version)
                    self.con.execute("COMMIT")
                except Exception:
                    self.con.execute("ROLLBACK")
                    raise
            return receipt
        if current != expected_manifest:
            raise ValueError("expected manifest does not match current publication")

        self.con.execute("BEGIN TRANSACTION")
        try:
            locked_current = self._current_generation(stage.dataset)
            if locked_current != expected_manifest:
                raise ValueError("expected manifest does not match current publication")
            published_before = self.con.execute(
                f"SELECT 1 FROM {self._schema}.runtime_publications "
                "WHERE dataset=? AND generation=?",
                [stage.dataset, stage.generation],
            ).fetchone()
            if published_before is not None:
                raise ValueError("generation was already published and cannot be reactivated")
            previous = self.con.execute(
                f"SELECT version, generation, snapshot_id FROM {self._schema}.runtime_manifests "
                "WHERE dataset=?",
                [stage.dataset],
            ).fetchone()
            tracked_previous = self.con.execute(
                f"SELECT generation, snapshot_id FROM {self._schema}.runtime_snapshot_manifests "
                "WHERE dataset=?",
                [stage.dataset],
            ).fetchone()
            previous_snapshot_id = (
                tracked_previous[1]
                if (
                    previous is not None
                    and tracked_previous is not None
                    and tracked_previous[0] == previous[1]
                )
                else previous[2]
                if previous is not None
                else None
            )
            version = (previous[0] if previous else 0) + 1
            stored = []
            for partition, payload in sorted(stage.partitions.items()):
                if not isinstance(payload, (pa.Table, pa.RecordBatchReader)):
                    raise TypeError("partition data must be a PyArrow Table or RecordBatchReader")
                table_name = _table_name(stage.dataset, stage.generation, partition)
                existing = self.con.execute(
                    f"SELECT digest, table_name FROM {self._schema}.runtime_partitions "
                    "WHERE dataset=? AND generation=? AND partition=?",
                    [stage.dataset, stage.generation, partition],
                ).fetchone()
                if existing is not None:
                    digest = _digest_relation(
                        self.con, self._schema, existing[1], version=_digest_version(existing[0])
                    )
                    if existing[0] != digest:
                        raise ValueError("retry partition content does not match staged generation")
                    table_name = existing[1]
                else:
                    self.con.register("__terrine_partition", _storage_payload(payload))
                    try:
                        self.con.execute(
                            f"CREATE TABLE {self._schema}.{_quote(table_name)} AS "
                            "SELECT * FROM __terrine_partition"
                        )
                    finally:
                        self.con.unregister("__terrine_partition")
                    digest = _digest_relation(self.con, self._schema, table_name)
                    self.con.execute(
                        f"INSERT INTO {self._schema}.runtime_partitions VALUES (?, ?, ?, ?, ?)",
                        [stage.dataset, stage.generation, partition, table_name, digest],
                    )
                metadata = payload.schema.serialize().to_pybytes()
                saved_metadata = self.con.execute(
                    f"SELECT schema_metadata FROM {self._schema}.runtime_partition_metadata "
                    "WHERE dataset=? AND generation=? AND partition=?",
                    [stage.dataset, stage.generation, partition],
                ).fetchone()
                if saved_metadata is None:
                    self.con.execute(
                        f"INSERT INTO {self._schema}.runtime_partition_metadata "
                        "VALUES (?, ?, ?, ?)",
                        [stage.dataset, stage.generation, partition, metadata],
                    )
                elif bytes(saved_metadata[0]) != metadata:
                    raise ValueError(
                        "retry partition Arrow schema does not match staged generation"
                    )
                stored.append(partition)
            self.con.execute(
                f"INSERT INTO {self._schema}.runtime_publications VALUES (?, ?, ?) "
                "ON CONFLICT(dataset, generation) DO NOTHING",
                [stage.dataset, stage.generation, version],
            )
            if stage.run_id is not None:
                self._record_run_publication(stage, version)
            lease_store.assert_current(lease)
            lease_store.guard_publication(lease)
            self.con.execute(
                f"INSERT INTO {self._schema}.runtime_manifests "
                "(dataset, generation, version, snapshot_id) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(dataset) DO UPDATE SET generation=excluded.generation, "
                "version=excluded.version, snapshot_id=excluded.snapshot_id",
                [stage.dataset, stage.generation, version, stage.snapshot_id],
            )
            snapshot_store = RuntimeStore(self.con, namespace=self.namespace)
            if previous is not None and previous[2] is not None:
                snapshot_store.advance_snapshot_guard(previous[2])
            if stage.snapshot_id is not None:
                if not stage.snapshot_id:
                    raise ValueError("snapshot_id must not be empty")
                snapshot_store.advance_snapshot_guard(stage.snapshot_id)
                if snapshot_store.snapshot_marked_for_gc(stage.snapshot_id):
                    raise ValueError("collected snapshots cannot be published")
                self.con.execute(
                    f"INSERT INTO {self._schema}.runtime_snapshot_references VALUES "
                    "(?, 'published_manifest', ?) ON CONFLICT DO NOTHING",
                    [
                        stage.snapshot_id,
                        snapshot_manifest_reference(stage.dataset, stage.generation),
                    ],
                )
                self.con.execute(
                    f"INSERT INTO {self._schema}.runtime_snapshot_manifests "
                    "VALUES (?, ?, ?) ON CONFLICT(dataset) DO UPDATE SET "
                    "generation=excluded.generation, snapshot_id=excluded.snapshot_id",
                    [stage.dataset, stage.generation, stage.snapshot_id],
                )
            else:
                self.con.execute(
                    f"DELETE FROM {self._schema}.runtime_snapshot_manifests WHERE dataset=?",
                    [stage.dataset],
                )
            if previous is not None and previous_snapshot_id is not None:
                self.con.execute(
                    f"DELETE FROM {self._schema}.runtime_snapshot_references "
                    "WHERE snapshot_id=? AND kind='published_manifest' AND reference=?",
                    [
                        previous_snapshot_id,
                        snapshot_manifest_reference(stage.dataset, previous[1]),
                    ],
                )
            if self.fault_injector is not None:
                self.fault_injector("before_commit")
            self.con.execute("COMMIT")
        except Exception:
            try:
                self.con.unregister("__terrine_partition")
            except Exception:
                pass
            self.con.execute("ROLLBACK")
            raise
        if self.fault_injector is not None:
            self.fault_injector("after_commit")
        return PublicationReceipt(stage.dataset, stage.generation, version, tuple(stored))

    def reuse_generation(
        self, dataset: str, *, expected_manifest: str | None, lease: Any, run_id: str
    ) -> PublicationReceipt:
        """Record a no-change run against the current immutable publication."""
        if lease.resource != f"dataset:{dataset}":
            raise ValueError("lease resource does not match dataset")
        leases = LeaseStore(self.con, namespace=self.namespace)
        self.con.execute("BEGIN TRANSACTION")
        try:
            leases.guard_publication(lease)
            current = self.con.execute(
                f"SELECT generation,version,snapshot_id FROM {self._schema}.runtime_manifests "
                "WHERE dataset=?",
                [dataset],
            ).fetchone()
            if current is None or current[0] != expected_manifest:
                raise ValueError("no-change refresh requires the current base publication")
            generation, version, snapshot_id = current
            self._record_run_publication(
                StagedGeneration(dataset, generation, {}, run_id=run_id, snapshot_id=snapshot_id),
                version,
            )
            partitions = self.con.execute(
                f"SELECT partition FROM {self._schema}.runtime_partitions "
                "WHERE dataset=? AND generation=? ORDER BY partition",
                [dataset, generation],
            ).fetchall()
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return PublicationReceipt(dataset, generation, version, tuple(row[0] for row in partitions))

    def current_generation(self, dataset: str) -> str | None:
        return self._current_generation(dataset)

    def _current_generation(self, dataset: str) -> str | None:
        row = self.con.execute(
            f"SELECT generation FROM {self._schema}.runtime_manifests WHERE dataset=?", [dataset]
        ).fetchone()
        return row[0] if row else None

    def _matching_receipt(self, stage: StagedGeneration) -> PublicationReceipt:
        rows = self.con.execute(
            f"SELECT partition, digest, table_name FROM {self._schema}.runtime_partitions "
            "WHERE dataset=? AND generation=? ORDER BY partition",
            [stage.dataset, stage.generation],
        ).fetchall()
        stored = {name: (digest, table_name) for name, digest, table_name in rows}
        for partition, payload in stage.partitions.items():
            schema = self._partition_schema(
                PublicationReceipt(stage.dataset, stage.generation, 0, tuple(stored)), partition
            )
            if not payload.schema.equals(schema, check_metadata=True):
                raise ValueError("retry partition Arrow schema does not match staged generation")
        if set(stored) != set(stage.partitions):
            raise ValueError("retry generation content does not match published generation")
        for name, (expected, table_name) in stored.items():
            version = _digest_version(expected)
            if expected != _digest_relation(self.con, self._schema, table_name, version=version):
                raise ValueError("retry generation content does not match published generation")
            canonical = (
                expected if version == 2 else _digest_relation(self.con, self._schema, table_name)
            )
            if canonical != _digest_payload(self.con, stage.partitions[name]):
                raise ValueError("retry generation content does not match published generation")
        row = self.con.execute(
            f"SELECT version FROM {self._schema}.runtime_publications "
            "WHERE dataset=? AND generation=?",
            [stage.dataset, stage.generation],
        ).fetchone()
        if row is None:
            raise ValueError("published manifest has no generation receipt")
        return PublicationReceipt(stage.dataset, stage.generation, row[0], tuple(sorted(stored)))

    def read_partition(self, receipt: PublicationReceipt, partition: str) -> pa.Table:
        row = self.con.execute(
            f"SELECT table_name FROM {self._schema}.runtime_partitions "
            "WHERE dataset=? AND generation=? AND partition=?",
            [receipt.dataset, receipt.generation, partition],
        ).fetchone()
        if row is None:
            raise KeyError(partition)
        table = self.con.execute(f"SELECT * FROM {self._schema}.{_quote(row[0])}").to_arrow_table()
        schema = self._partition_schema(receipt, partition)
        for index, field in enumerate(schema):
            if pa.types.is_null(field.type):
                if table.column(index).null_count != table.num_rows:
                    raise ValueError("published null column contains non-null values")
                table = table.set_column(index, field, pa.nulls(table.num_rows))
        return table.cast(schema)

    def read_batches(
        self, receipt: PublicationReceipt, partition: str, *, batch_size: int = 65_536
    ) -> Generator[pa.RecordBatch]:
        """Stream exact-generation data with its retained logical schema and metadata."""
        row = self.con.execute(
            f"SELECT table_name FROM {self._schema}.runtime_partitions "
            "WHERE dataset=? AND generation=? AND partition=?",
            [receipt.dataset, receipt.generation, partition],
        ).fetchone()
        if row is None or partition not in receipt.partitions:
            raise KeyError(partition)
        schema = self._partition_schema(receipt, partition)

        def batches():
            cursor = self.con.cursor()
            reader = None
            try:
                reader = cursor.execute(
                    f"SELECT * FROM {self._schema}.{_quote(row[0])}"
                ).to_arrow_reader(batch_size=batch_size)
                for batch in reader:
                    for index, field in enumerate(schema):
                        if pa.types.is_null(field.type):
                            if batch.column(index).null_count != batch.num_rows:
                                raise ValueError("published null column contains non-null values")
                            batch = batch.set_column(index, field, pa.nulls(batch.num_rows))
                    yield batch.cast(schema)
            finally:
                if reader is not None:
                    reader.close()
                cursor.close()

        return batches()

    def _partition_schema(self, receipt: PublicationReceipt, partition: str) -> pa.Schema:
        metadata = self.con.execute(
            f"SELECT schema_metadata FROM {self._schema}.runtime_partition_metadata "
            "WHERE dataset=? AND generation=? AND partition=?",
            [receipt.dataset, receipt.generation, partition],
        ).fetchone()
        if metadata is None:
            raise ValueError("published partition has no retained Arrow schema")
        return pa.ipc.read_schema(pa.BufferReader(bytes(metadata[0])))

    def read_relation(
        self, receipt: PublicationReceipt, partition: str, *, ibis_backend: Any
    ) -> ibis.Table:
        """Resolve a committed partition with its logical types as a lazy Ibis relation."""
        row = self.con.execute(
            f"SELECT table_name FROM {self._schema}.runtime_partitions "
            "WHERE dataset=? AND generation=? AND partition=?",
            [receipt.dataset, receipt.generation, partition],
        ).fetchone()
        if row is None:
            raise KeyError(partition)
        schema = self._partition_schema(receipt, partition)
        for field in schema:
            if pa.types.is_duration(field.type) and field.type.unit == "ns":
                raise _LAZY_DURATION_UNSUPPORTED.error(
                    column=field.name,
                    unit=field.type.unit,
                    route="DuckDBBackend.read_partition or open_published_frame",
                )
        model_connection = getattr(ibis_backend, "con", None)
        if model_connection is self.con:
            backend = ibis_backend
        else:
            if self._ibis_backend is None:
                timezone = self.con.execute("SELECT current_setting('TimeZone')").fetchone()[0]
                try:
                    self._ibis_backend = IbisDuckDBBackend.from_connection(self.con)
                finally:
                    escaped_timezone = timezone.replace("'", "''")
                    self.con.execute(f"SET TimeZone = '{escaped_timezone}'")
            backend = self._ibis_backend
        relation = backend.table(row[0], database=self.namespace)
        restored = {}
        for field in schema:
            if pa.types.is_duration(field.type):
                restored[field.name] = relation[field.name].as_interval(field.type.unit)
            elif pa.types.is_null(field.type):
                restored[field.name] = ibis.null()
        return relation.mutate(**restored) if restored else relation

    def reconcile(self, run_id: str) -> Reconciliation:
        row = self.con.execute(
            f"SELECT generation, version FROM {self._schema}.runtime_run_publications "
            "WHERE run_id=?",
            [run_id],
        ).fetchone()
        return Reconciliation(run_id, row[0], row[1]) if row else Reconciliation(run_id, None, None)

    def _record_run_publication(self, stage: StagedGeneration, version: int) -> None:
        self.con.execute(
            f"INSERT INTO {self._schema}.runtime_run_publications "
            "VALUES (?, ?, ?, ?) ON CONFLICT(run_id) DO NOTHING",
            [stage.run_id, stage.dataset, stage.generation, version],
        )
        row = self.con.execute(
            f"SELECT dataset, generation, version FROM {self._schema}.runtime_run_publications "
            "WHERE run_id=?",
            [stage.run_id],
        ).fetchone()
        if row != (stage.dataset, stage.generation, version):
            raise ValueError("run_id is already bound to a different publication")


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _table_name(dataset: str, generation: str, partition: str) -> str:
    raw = json.dumps([dataset, generation, partition], separators=(",", ":"))
    return "generation_" + hashlib.sha256(raw.encode()).hexdigest()[:32]


def _digest_version(checksum: str) -> int:
    if re.fullmatch(r"[0-9a-f]{64}", checksum):
        return 1
    if re.fullmatch(r"arrow-v2:[0-9a-f]{64}", checksum):
        return 2
    raise ValueError("unsupported partition checksum format")


def _digest_relation(con: Any, schema: str, table_name: str, *, version: int = 2) -> str:
    """Hash canonical logical buffers; version one retains the original physical encoding."""
    if version not in (1, 2):
        raise ValueError("unsupported partition checksum version")
    reader = con.execute(f"SELECT * FROM {schema}.{_quote(table_name)}").to_arrow_reader(
        batch_size=65_536
    )
    canonical_schema = (
        pa.schema(
            [field.with_type(_digest_type(field.type)) for field in reader.schema],
            metadata=reader.schema.metadata,
        )
        if version == 2
        else reader.schema
    )
    digest = hashlib.sha256(canonical_schema.serialize().to_pybytes())
    normalize = canonical_schema != reader.schema
    indices = None
    try:
        for batch in reader:
            if normalize:
                batch = batch.cast(canonical_schema)
            if version == 2:
                if indices is None or len(indices) < batch.num_rows:
                    indices = pa.array(np.arange(batch.num_rows, dtype=np.int32))
                batch = pc.take(batch, indices.slice(0, batch.num_rows))
            digest.update(batch.serialize().to_pybytes())
    finally:
        reader.close()
    return ("arrow-v2:" if version == 2 else "") + digest.hexdigest()


def _digest_type(dtype: pa.DataType) -> pa.DataType:
    if pa.types.is_timestamp(dtype) and dtype.tz is not None:
        return pa.timestamp(dtype.unit, tz="UTC")
    if pa.types.is_struct(dtype):
        return pa.struct([field.with_type(_digest_type(field.type)) for field in dtype])
    if pa.types.is_list(dtype):
        return pa.list_(dtype.value_field.with_type(_digest_type(dtype.value_type)))
    if pa.types.is_large_list(dtype):
        return pa.large_list(dtype.value_field.with_type(_digest_type(dtype.value_type)))
    if pa.types.is_fixed_size_list(dtype):
        return pa.list_(
            dtype.value_field.with_type(_digest_type(dtype.value_type)), dtype.list_size
        )
    if pa.types.is_map(dtype):
        return pa.map_(
            dtype.key_field.with_type(_digest_type(dtype.key_type)),
            dtype.item_field.with_type(_digest_type(dtype.item_type)),
            keys_sorted=dtype.keys_sorted,
        )
    return dtype


def _digest_payload(con: Any, payload: pa.Table | pa.RecordBatchReader) -> str:
    con.register("__terrine_retry", _storage_payload(payload))
    try:
        return _digest_relation(con, "temp", "__terrine_retry")
    finally:
        con.unregister("__terrine_retry")


def _storage_payload(payload: pa.Table | pa.RecordBatchReader):
    """Keep duration ticks exact instead of converting them to calendar intervals."""
    schema = pa.schema(
        [
            pa.field(field.name, pa.int64() if pa.types.is_duration(field.type) else field.type)
            for field in payload.schema
        ]
    )
    if isinstance(payload, pa.Table):
        return payload.cast(schema)

    def batches():
        try:
            for batch in payload:
                yield batch.cast(schema)
        finally:
            payload.close()

    return pa.RecordBatchReader.from_batches(schema, batches())
