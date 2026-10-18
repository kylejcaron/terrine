from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from typing import Any

import pyarrow as pa

from terrine.runtime.backends.base import (
    BackendCapabilities,
    PublicationReceipt,
    Reconciliation,
    StagedGeneration,
)
from terrine.runtime.leases import LeaseStore

__all__ = ["DuckDBBackend"]


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
        self.initialize()

    def initialize(self) -> None:
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_manifests ("
            "dataset VARCHAR PRIMARY KEY, generation VARCHAR NOT NULL, version BIGINT NOT NULL)"
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
                f"SELECT version FROM {self._schema}.runtime_manifests WHERE dataset=?",
                [stage.dataset],
            ).fetchone()
            version = (previous[0] if previous else 0) + 1
            stored = []
            for partition, table in sorted(stage.partitions.items()):
                if not isinstance(table, pa.Table):
                    raise TypeError("partition data must be a PyArrow Table")
                digest = _digest(table)
                table_name = _table_name(stage.dataset, stage.generation, partition)
                existing = self.con.execute(
                    f"SELECT digest, table_name FROM {self._schema}.runtime_partitions "
                    "WHERE dataset=? AND generation=? AND partition=?",
                    [stage.dataset, stage.generation, partition],
                ).fetchone()
                if existing is not None:
                    if existing[0] != digest:
                        raise ValueError("retry partition content does not match staged generation")
                    table_name = existing[1]
                else:
                    self.con.register("__terrine_partition", table)
                    self.con.execute(
                        f"CREATE TABLE {self._schema}.{_quote(table_name)} AS "
                        "SELECT * FROM __terrine_partition"
                    )
                    self.con.unregister("__terrine_partition")
                    self.con.execute(
                        f"INSERT INTO {self._schema}.runtime_partitions VALUES (?, ?, ?, ?, ?)",
                        [stage.dataset, stage.generation, partition, table_name, digest],
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
                f"INSERT INTO {self._schema}.runtime_manifests VALUES (?, ?, ?) "
                "ON CONFLICT(dataset) DO UPDATE SET generation=excluded.generation, "
                "version=excluded.version",
                [stage.dataset, stage.generation, version],
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

    def current_generation(self, dataset: str) -> str | None:
        return self._current_generation(dataset)

    def _current_generation(self, dataset: str) -> str | None:
        row = self.con.execute(
            f"SELECT generation FROM {self._schema}.runtime_manifests WHERE dataset=?", [dataset]
        ).fetchone()
        return row[0] if row else None

    def _matching_receipt(self, stage: StagedGeneration) -> PublicationReceipt:
        rows = self.con.execute(
            f"SELECT partition, digest FROM {self._schema}.runtime_partitions "
            "WHERE dataset=? AND generation=? ORDER BY partition",
            [stage.dataset, stage.generation],
        ).fetchall()
        stored = {name: digest for name, digest in rows}
        if set(stored) != set(stage.partitions) or any(
            stored[name] != _digest(table) for name, table in stage.partitions.items()
        ):
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
        return self.con.execute(f"SELECT * FROM {self._schema}.{_quote(row[0])}").to_arrow_table()

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


def _digest(table: pa.Table) -> str:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()
