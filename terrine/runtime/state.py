from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any

from terrine.data import BoundCoverage

__all__ = [
    "DefinitionVersion",
    "GenerationRecord",
    "GenerationRef",
    "LayerNode",
    "RuntimeStore",
    "SourceSnapshot",
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
            "created_at VARCHAR NOT NULL, supersedes VARCHAR, "
            "superseded_by VARCHAR, PRIMARY KEY(dataset, generation))"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_generation_pins ("
            "dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, reader VARCHAR NOT NULL, "
            "PRIMARY KEY(dataset, generation, reader))"
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
        now = datetime.now().astimezone()
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
                f"INSERT INTO {self._schema}.runtime_generations VALUES (?, ?, ?, ?, ?, ?, NULL)",
                [
                    ref.dataset,
                    ref.generation,
                    definition_json,
                    snapshot_json,
                    now.isoformat(),
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
            datetime.fromisoformat(created_at),
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
        if self.read_generation(ref) is None:
            raise ValueError("cannot pin an absent generation")
        self.con.execute(
            f"INSERT INTO {self._schema}.runtime_generation_pins VALUES (?, ?, ?) "
            "ON CONFLICT DO NOTHING",
            [ref.dataset, ref.generation, reader],
        )

    def unpin_generation(self, ref: GenerationRef, reader: str) -> None:
        self._check_ref(ref)
        self.con.execute(
            f"DELETE FROM {self._schema}.runtime_generation_pins "
            "WHERE dataset=? AND generation=? AND reader=?",
            [ref.dataset, ref.generation, reader],
        )

    def delete_generation(self, ref: GenerationRef) -> None:
        self._check_ref(ref)
        row = self.con.execute(
            f"SELECT superseded_by FROM {self._schema}.runtime_generations "
            "WHERE dataset=? AND generation=?",
            [ref.dataset, ref.generation],
        ).fetchone()
        if row is None or row[0] is None:
            raise ValueError("only explicitly superseded generations can be collected")
        pin = self.con.execute(
            f"SELECT 1 FROM {self._schema}.runtime_generation_pins "
            "WHERE dataset=? AND generation=? LIMIT 1",
            [ref.dataset, ref.generation],
        ).fetchone()
        if pin is not None:
            raise ValueError("pinned generations cannot be collected")
        self.con.execute("BEGIN TRANSACTION")
        try:
            partition_catalog = self.con.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema=? AND table_name='runtime_partitions'",
                [self.namespace],
            ).fetchone()
            if partition_catalog is not None:
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
        except Exception:
            self.con.execute("ROLLBACK")
            raise

    def _check_ref(self, ref: GenerationRef) -> None:
        if ref.namespace != self.namespace:
            raise ValueError("generation reference namespace does not match store")


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
