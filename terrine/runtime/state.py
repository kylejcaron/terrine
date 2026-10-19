from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

from terrine.data import BoundCoverage
from terrine.runtime.leases import Lease, LeaseStore

__all__ = [
    "DefinitionVersion",
    "GenerationRecord",
    "GenerationRef",
    "LayerNode",
    "RuntimeStore",
    "SourceSnapshot",
    "snapshot_manifest_reference",
]


@dataclass(frozen=True, slots=True)
class DefinitionVersion:
    definition_hash: str
    schema_hash: str
    codec_version: str


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    snapshot_id: str
    source_revisions: Mapping[str, str | None]
    readiness: Mapping[str, BoundCoverage]
    captured_at: datetime
    owner_reference: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_revisions", MappingProxyType(dict(self.source_revisions)))
        object.__setattr__(self, "readiness", MappingProxyType(dict(self.readiness)))
        if self.captured_at.tzinfo is None:
            raise ValueError("captured_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class GenerationRef:
    namespace: str
    dataset: str
    generation: str


@dataclass(frozen=True, slots=True)
class LayerNode:
    layer: str
    source: str
    partition: str
    definition_hash: str
    generation: str


@dataclass(frozen=True, slots=True)
class GenerationRecord:
    ref: GenerationRef
    definition: DefinitionVersion
    snapshot: SourceSnapshot
    created_at: datetime
    supersedes: GenerationRef | None = None
    superseded_by: GenerationRef | None = None


class RuntimeStore:
    """Durable metadata store in an owner-selected DuckDB schema."""

    def __init__(self, con: Any, *, namespace: str) -> None:
        if not isinstance(namespace, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
            raise ValueError("namespace must be a simple SQL identifier")
        self.con = con
        self.namespace = namespace
        self._schema = '"' + namespace.replace('"', '""') + '"'

    def initialize(self) -> None:
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_generations ("
            "dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, "
            "definition_json VARCHAR NOT NULL, snapshot_json VARCHAR NOT NULL, "
            "created_at TIMESTAMP NOT NULL, supersedes VARCHAR, "
            "superseded_by VARCHAR, PRIMARY KEY(dataset, generation))"
        )
        column_type = self.con.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE lower(table_schema)=lower(?) "
            "AND table_name='runtime_generations' AND column_name='created_at'",
            [self.namespace],
        ).fetchone()[0]
        if column_type != "TIMESTAMP":
            self.con.execute(
                f"ALTER TABLE {self._schema}.runtime_generations "
                "ALTER COLUMN created_at SET DATA TYPE TIMESTAMP USING "
                "CAST(CAST(created_at AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP)"
            )
        self.con.execute(
            f"ALTER TABLE {self._schema}.runtime_generations "
            "ADD COLUMN IF NOT EXISTS guard_version BIGINT DEFAULT 0"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_generation_pins ("
            "dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, reader VARCHAR NOT NULL, "
            "PRIMARY KEY(dataset, generation, reader))"
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
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_snapshot_captures ("
            "snapshot_id VARCHAR PRIMARY KEY, cost_json VARCHAR NOT NULL, "
            "readiness_json VARCHAR NOT NULL)"
        )

    def write_generation(
        self,
        ref: GenerationRef,
        *,
        definition: DefinitionVersion,
        snapshot: SourceSnapshot,
        supersedes: GenerationRef | None = None,
    ) -> GenerationRecord:
        self._check_ref(ref)
        if supersedes is not None:
            self._check_ref(supersedes)
            if supersedes == ref:
                raise ValueError("a generation cannot supersede itself")
            if supersedes.dataset != ref.dataset:
                raise ValueError("superseded generation must belong to the same dataset")
        now = datetime.now(UTC)
        definition_json = json.dumps(
            {
                "definition_hash": definition.definition_hash,
                "schema_hash": definition.schema_hash,
                "codec_version": definition.codec_version,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        snapshot_json = json.dumps(_snapshot_json(snapshot), sort_keys=True, separators=(",", ":"))
        self.con.execute("BEGIN TRANSACTION")
        inserted = False
        try:
            self.con.execute(
                f"INSERT INTO {self._schema}.runtime_generations "
                "(dataset, generation, definition_json, snapshot_json, created_at, supersedes, "
                "superseded_by) VALUES (?, ?, ?, ?, ?, ?, NULL)",
                [
                    ref.dataset,
                    ref.generation,
                    definition_json,
                    snapshot_json,
                    now.replace(tzinfo=None),
                    supersedes.generation if supersedes else None,
                ],
            )
            inserted = True
            if supersedes is not None:
                result = self.con.execute(
                    f"UPDATE {self._schema}.runtime_generations SET superseded_by=? "
                    "WHERE dataset=? AND generation=? AND superseded_by IS NULL "
                    "RETURNING generation",
                    [ref.generation, ref.dataset, supersedes.generation],
                ).fetchone()
                if result is None:
                    raise ValueError(
                        "superseded generation does not exist or is already superseded"
                    )
            self.con.execute("COMMIT")
        except Exception as exc:
            self.con.execute("ROLLBACK")
            if not inserted and not isinstance(exc, ValueError):
                raise ValueError(
                    "generation identity already exists or supersession is invalid"
                ) from exc
            raise
        record = self.read_generation(ref)
        if record is None:
            raise RuntimeError("inserted generation is not readable")
        return record

    def read_generation(self, ref: GenerationRef) -> GenerationRecord | None:
        self._check_ref(ref)
        row = self.con.execute(
            f"SELECT definition_json, snapshot_json, created_at, supersedes, superseded_by "
            f"FROM {self._schema}.runtime_generations WHERE dataset=? AND generation=?",
            [ref.dataset, ref.generation],
        ).fetchone()
        if row is None:
            return None
        definition_data, snapshot_data, created_at, supersedes, superseded_by = row
        definition = DefinitionVersion(**json.loads(definition_data))
        snapshot = _snapshot_from_json(json.loads(snapshot_data))
        return GenerationRecord(
            ref,
            definition,
            snapshot,
            created_at.replace(tzinfo=UTC),
            GenerationRef(self.namespace, ref.dataset, supersedes) if supersedes else None,
            GenerationRef(self.namespace, ref.dataset, superseded_by) if superseded_by else None,
        )

    def generations(self) -> tuple[GenerationRecord, ...]:
        rows = self.con.execute(
            f"SELECT dataset, generation FROM {self._schema}.runtime_generations "
            "ORDER BY dataset, generation"
        ).fetchall()
        records = tuple(
            self.read_generation(GenerationRef(self.namespace, dataset, generation))
            for dataset, generation in rows
        )
        return tuple(record for record in records if record is not None)

    def pin_generation(self, ref: GenerationRef, reader: str) -> None:
        self._check_ref(ref)
        if not reader:
            raise ValueError("reader must not be empty")
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.execute(
                f"UPDATE {self._schema}.runtime_generations "
                "SET guard_version=guard_version+1 WHERE dataset=? AND generation=?",
                [ref.dataset, ref.generation],
            )
            exists = self.con.execute(
                f"SELECT 1 FROM {self._schema}.runtime_generations "
                "WHERE dataset=? AND generation=?",
                [ref.dataset, ref.generation],
            ).fetchone()
            if exists is None:
                raise ValueError("cannot pin an absent generation")
            self.con.execute(
                f"INSERT INTO {self._schema}.runtime_generation_pins VALUES (?, ?, ?) "
                "ON CONFLICT DO NOTHING",
                [ref.dataset, ref.generation, reader],
            )
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise

    def unpin_generation(self, ref: GenerationRef, reader: str) -> None:
        self._check_ref(ref)
        self.con.execute(
            f"DELETE FROM {self._schema}.runtime_generation_pins "
            "WHERE dataset=? AND generation=? AND reader=?",
            [ref.dataset, ref.generation, reader],
        )

    def delete_generation(self, ref: GenerationRef) -> None:
        self._check_ref(ref)
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.execute(
                f"UPDATE {self._schema}.runtime_generations "
                "SET guard_version=guard_version+1 WHERE dataset=? AND generation=?",
                [ref.dataset, ref.generation],
            )
            row = self.con.execute(
                f"SELECT superseded_by FROM {self._schema}.runtime_generations "
                "WHERE dataset=? AND generation=?",
                [ref.dataset, ref.generation],
            ).fetchone()
            if row is None or row[0] is None:
                raise ValueError("only explicitly superseded generations can be collected")
            manifest_catalog = self.con.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema=? AND table_name='runtime_manifests'",
                [self.namespace],
            ).fetchone()
            if manifest_catalog is not None:
                current = self.con.execute(
                    f"SELECT 1 FROM {self._schema}.runtime_manifests "
                    "WHERE dataset=? AND generation=?",
                    [ref.dataset, ref.generation],
                ).fetchone()
                if current is not None:
                    raise ValueError("currently published generations cannot be collected")
            pin = self.con.execute(
                f"SELECT 1 FROM {self._schema}.runtime_generation_pins "
                "WHERE dataset=? AND generation=? LIMIT 1",
                [ref.dataset, ref.generation],
            ).fetchone()
            if pin is not None:
                raise ValueError("pinned generations cannot be collected")
            partition_catalog = self.con.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema=? AND table_name='runtime_partitions'",
                [self.namespace],
            ).fetchone()
            if partition_catalog is not None:
                metadata_catalog = self.con.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema=? AND table_name='runtime_partition_metadata'",
                    [self.namespace],
                ).fetchone()
                if metadata_catalog is not None:
                    self.con.execute(
                        f"DELETE FROM {self._schema}.runtime_partition_metadata "
                        "WHERE dataset=? AND generation=?",
                        [ref.dataset, ref.generation],
                    )
                partitions = self.con.execute(
                    f"SELECT table_name FROM {self._schema}.runtime_partitions "
                    "WHERE dataset=? AND generation=?",
                    [ref.dataset, ref.generation],
                ).fetchall()
                for (table_name,) in partitions:
                    quoted_table = '"' + table_name.replace('"', '""') + '"'
                    self.con.execute(f"DROP TABLE IF EXISTS {self._schema}.{quoted_table}")
                self.con.execute(
                    f"DELETE FROM {self._schema}.runtime_partitions "
                    "WHERE dataset=? AND generation=?",
                    [ref.dataset, ref.generation],
                )
            self.con.execute(
                f"DELETE FROM {self._schema}.runtime_generations WHERE dataset=? AND generation=?",
                [ref.dataset, ref.generation],
            )
            self.con.execute("COMMIT")
        except Exception as exc:
            self.con.execute("ROLLBACK")
            if type(exc).__name__ == "TransactionException":
                raise ValueError("generation changed concurrently during collection") from exc
            raise

    def advance_snapshot_guard(self, snapshot_id: str) -> None:
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise ValueError("snapshot_id must not be empty")
        self.con.execute(
            f"INSERT INTO {self._schema}.runtime_snapshot_guards VALUES (?, 0) "
            "ON CONFLICT DO NOTHING",
            [snapshot_id],
        )
        self.con.execute(
            f"UPDATE {self._schema}.runtime_snapshot_guards "
            "SET guard_version=guard_version+1 WHERE snapshot_id=?",
            [snapshot_id],
        )

    def pin_snapshot(
        self, snapshot_id: str, *, kind: str, reference: str, _in_transaction: bool = False
    ) -> None:
        _check_snapshot_reference(snapshot_id, kind, reference)
        if not _in_transaction:
            self.con.execute("BEGIN TRANSACTION")
        try:
            self.advance_snapshot_guard(snapshot_id)
            if self.snapshot_marked_for_gc(snapshot_id):
                raise ValueError("collected snapshots cannot be pinned")
            self.con.execute(
                f"INSERT INTO {self._schema}.runtime_snapshot_references VALUES (?, ?, ?) "
                "ON CONFLICT DO NOTHING",
                [snapshot_id, kind, reference],
            )
            if not _in_transaction:
                self.con.execute("COMMIT")
        except Exception:
            if not _in_transaction:
                self.con.execute("ROLLBACK")
            raise

    def unpin_snapshot(self, snapshot_id: str, *, kind: str, reference: str) -> None:
        _check_snapshot_reference(snapshot_id, kind, reference)
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.advance_snapshot_guard(snapshot_id)
            self.con.execute(
                f"DELETE FROM {self._schema}.runtime_snapshot_references "
                "WHERE snapshot_id=? AND kind=? AND reference=?",
                [snapshot_id, kind, reference],
            )
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise

    def snapshot_references(self, snapshot_id: str) -> tuple[tuple[str, str], ...]:
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise ValueError("snapshot_id must not be empty")
        rows = self.con.execute(
            f"SELECT kind, reference FROM {self._schema}.runtime_snapshot_references "
            "WHERE snapshot_id=? ORDER BY kind, reference",
            [snapshot_id],
        ).fetchall()
        return tuple((kind, reference) for kind, reference in rows)

    def release_terminal_run_snapshot_references(self) -> None:
        """Reconcile active-run pins left by terminal cleanup conflicts."""
        table = self.con.execute(
            "SELECT 1 FROM information_schema.tables WHERE lower(table_schema)=lower(?) "
            "AND table_name='runtime_runs'",
            [self.namespace],
        ).fetchone()
        if table is None:
            return
        terminal = {
            run_id
            for (run_id,) in self.con.execute(
                f"SELECT run_id FROM {self._schema}.runtime_runs "
                "WHERE state IN ('succeeded', 'cancelled')"
            ).fetchall()
        }
        if not terminal:
            return
        references = self.con.execute(
            f"SELECT snapshot_id, reference FROM {self._schema}.runtime_snapshot_references "
            "WHERE kind='active_run'"
        ).fetchall()
        for snapshot_id, reference in references:
            try:
                run_id, _fence = json.loads(reference)
            except (TypeError, ValueError):
                run_id = reference
            if run_id in terminal:
                try:
                    self.unpin_snapshot(snapshot_id, kind="active_run", reference=reference)
                except Exception as exc:
                    if type(exc).__name__ != "TransactionException":
                        raise

    def replace_published_snapshot(
        self,
        dataset: str,
        generation: str,
        snapshot_id: str | None,
        *,
        lease: Lease | None = None,
    ) -> None:
        if not dataset or not generation:
            raise ValueError("dataset and generation must not be empty")
        self.con.execute("BEGIN TRANSACTION")
        try:
            if lease is not None:
                LeaseStore(self.con, namespace=self.namespace).guard_publication(lease)
            previous = self.con.execute(
                f"SELECT generation, snapshot_id FROM {self._schema}.runtime_snapshot_manifests "
                "WHERE dataset=?",
                [dataset],
            ).fetchone()
            reference = snapshot_manifest_reference(dataset, generation)
            previous_reference = (
                snapshot_manifest_reference(dataset, previous[0]) if previous is not None else None
            )
            if previous is not None and previous[0] == generation and previous[1] != snapshot_id:
                raise ValueError("retry snapshot identity does not match published generation")
            for guarded in (
                previous[1] if previous is not None else None,
                snapshot_id,
            ):
                if guarded is not None:
                    self.advance_snapshot_guard(guarded)
            if snapshot_id is not None and self.snapshot_marked_for_gc(snapshot_id):
                raise ValueError("collected snapshots cannot be published")
            if previous is not None and previous[1] is not None:
                self.con.execute(
                    f"DELETE FROM {self._schema}.runtime_snapshot_references "
                    "WHERE snapshot_id=? AND kind='published_manifest' AND reference=?",
                    [previous[1], previous_reference],
                )
            if snapshot_id is None:
                self.con.execute(
                    f"DELETE FROM {self._schema}.runtime_snapshot_manifests WHERE dataset=?",
                    [dataset],
                )
            else:
                self.con.execute(
                    f"INSERT INTO {self._schema}.runtime_snapshot_manifests "
                    "VALUES (?, ?, ?) ON CONFLICT(dataset) DO UPDATE SET "
                    "generation=excluded.generation, snapshot_id=excluded.snapshot_id",
                    [dataset, generation, snapshot_id],
                )
                self.con.execute(
                    f"INSERT INTO {self._schema}.runtime_snapshot_references VALUES "
                    "(?, 'published_manifest', ?) ON CONFLICT DO NOTHING",
                    [snapshot_id, reference],
                )
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise

    def mark_snapshot_for_gc(self, snapshot_id: str) -> bool:
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise ValueError("snapshot_id must not be empty")
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.advance_snapshot_guard(snapshot_id)
            refs = self.con.execute(
                f"SELECT 1 FROM {self._schema}.runtime_snapshot_references "
                "WHERE snapshot_id=? LIMIT 1",
                [snapshot_id],
            ).fetchone()
            if refs is not None:
                self.con.execute("ROLLBACK")
                return False
            self.con.execute(
                f"INSERT INTO {self._schema}.runtime_snapshot_gc VALUES (?) ON CONFLICT DO NOTHING",
                [snapshot_id],
            )
            self.con.execute("COMMIT")
            return True
        except Exception:
            self.con.execute("ROLLBACK")
            raise

    def snapshot_marked_for_gc(self, snapshot_id: str) -> bool:
        return (
            self.con.execute(
                f"SELECT 1 FROM {self._schema}.runtime_snapshot_gc WHERE snapshot_id=?",
                [snapshot_id],
            ).fetchone()
            is not None
        )

    def _check_ref(self, ref: GenerationRef) -> None:
        if ref.namespace != self.namespace:
            raise ValueError("generation reference namespace does not match store")


def _check_snapshot_reference(snapshot_id: str, kind: str, reference: str) -> None:
    for name, value in (
        ("snapshot_id", snapshot_id),
        ("kind", kind),
        ("reference", reference),
    ):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must not be empty")


def snapshot_manifest_reference(dataset: str, generation: str) -> str:
    return json.dumps((dataset, generation), separators=(",", ":"))


def _snapshot_json(snapshot: SourceSnapshot) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot.snapshot_id,
        "source_revisions": dict(snapshot.source_revisions),
        "readiness": {
            name: {
                "start": coverage.start.isoformat(),
                "end_exclusive": coverage.end_exclusive.isoformat(),
            }
            for name, coverage in snapshot.readiness.items()
        },
        "captured_at": snapshot.captured_at.isoformat(),
        "owner_reference": snapshot.owner_reference,
    }


def _snapshot_from_json(data: dict[str, Any]) -> SourceSnapshot:
    return SourceSnapshot(
        data["snapshot_id"],
        data["source_revisions"],
        {
            name: BoundCoverage(
                datetime.fromisoformat(value["start"]),
                datetime.fromisoformat(value["end_exclusive"]),
            )
            for name, value in data["readiness"].items()
        },
        datetime.fromisoformat(data["captured_at"]),
        data["owner_reference"],
    )
