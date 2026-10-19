"""Pinned references to committed runtime generations."""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa

from terrine.data import Budget, MaterializedFrame
from terrine.dataset.manifest import DatasetManifest, ManifestColumn, ManifestGeneration
from terrine.errors import RefusalSpec
from terrine.runtime.api import run_serialized_plan
from terrine.runtime.backends.base import PublicationReceipt
from terrine.runtime.backends.duckdb import DuckDBBackend, _digest_relation
from terrine.runtime.graph import dependency_graph
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.spec.dataset import DatasetRecipe

__all__ = ["Dataset", "stage_frame"]

_INVALID_DESTINATION = RefusalSpec(
    "terrine.dataset.invalid_destination", "Dataset destination is invalid: {reason}."
)
_INVALID_BATCH = RefusalSpec(
    "terrine.dataset.invalid_batch_size", "Arrow batch size must be a positive integer."
)
_GENERATION_INVALID = RefusalSpec(
    "terrine.dataset.invalid_generation",
    "Dataset manifest does not identify a verified committed runtime generation.",
)


def _owner_for(con: Any) -> RuntimeOwner:
    try:
        raw = con.con
        rows = raw.execute("PRAGMA database_list").fetchall()
        current = raw.execute("SELECT current_database()").fetchone()[0]
        database = next(Path(path).resolve() for _, name, path in rows if name == current and path)
    except Exception as error:
        raise _INVALID_DESTINATION.error(
            reason="staging requires a file-backed DuckDB connection"
        ) from error
    root = os.environ.get("TERRINE_RUNTIME_SNAPSHOT_ROOT")
    snapshot_root = Path(root).expanduser() if root else database.parent / ".terrine" / "snapshots"
    return RuntimeOwner(database, snapshot_root)


def _column_role(name: str, roles: Any) -> str:
    for role in ("unit", "anchor", "t0", "treatment"):
        if name == getattr(roles, role):
            return role
    if name in roles.features:
        return "feature"
    if name in roles.outcomes:
        return "outcome"
    return "support"


def _recipe_for(frame: Any) -> DatasetRecipe:
    view = None
    if frame._training_view is not None:
        training = frame._training_view
        view = {
            "kind": "training",
            "features": list(training.features),
            "labels": list(training.labels),
            "incomplete_labels": training.incomplete_labels,
            "split": None if training.split is None else training.split.to_record(),
        }
    elif frame._scoring_view is not None:
        from terrine.spec.io import canonical_json

        scoring = frame._scoring_view
        design_record = json.loads(canonical_json(frame._design))["design"]
        view = {
            "kind": "scoring",
            "features": list(scoring.features),
            "as_of": scoring.resolved_as_of.isoformat(),
            "eligibility": design_record["eligibility"],
            "parent_design_hash": scoring.parent_design_hash,
        }
    return DatasetRecipe(schema_version=1, base_design=frame._design, view=view)


class _ModelResolver:
    def __init__(self, model: Any) -> None:
        self.model = model

    def resolve(self, reference: ModelReference):
        return self.model


class Dataset:
    """An immutable reference to one committed output generation."""

    __slots__ = ("_con", "_raw", "_namespace", "_manifest", "_receipt")

    def __init__(self, manifest: DatasetManifest, *, con: Any, namespace: str) -> None:
        if not isinstance(manifest, DatasetManifest):
            raise TypeError("manifest must be a DatasetManifest")
        self._con, self._raw = con, con.con
        self._namespace, self._manifest = namespace, manifest
        self._verify_publication()

    @property
    def manifest(self) -> DatasetManifest:
        return self._manifest

    @classmethod
    def from_manifest(cls, manifest: DatasetManifest, *, con: Any, namespace: str | None = None):
        resolved = namespace or os.environ.get("TERRINE_RUNTIME_NAMESPACE", "terrine_runtime")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", resolved):
            raise _GENERATION_INVALID.error()
        return cls(manifest, con=con, namespace=resolved)

    def _verify_publication(self) -> None:
        generation, namespace = self._manifest.generation, self._namespace
        if self._manifest.source_snapshots.get("owner") != _owner_for(self._con).reference:
            raise _GENERATION_INVALID.error()
        backend = DuckDBBackend(self._raw, namespace=namespace)
        record = self._raw.execute(
            "SELECT d.manifest_json,p.version,r.state "
            f'FROM "{namespace}".runtime_dataset_recipes d '
            f'JOIN "{namespace}".runtime_runs r ON r.request_key=d.request_key '
            f'JOIN "{namespace}".runtime_run_publications p ON p.run_id=r.run_id '
            "WHERE d.dataset=? AND d.generation=? AND p.dataset=d.dataset "
            "AND p.generation=d.generation AND r.published_generation=d.generation",
            [generation.dataset_id, generation.generation_id],
        ).fetchone()
        if (
            record is None
            or record[2] != "succeeded"
            or record[0].encode() != self._manifest.to_json()
        ):
            raise _GENERATION_INVALID.error()
        self._receipt = PublicationReceipt(
            generation.dataset_id, generation.generation_id, record[1], generation.output_refs
        )
        expected_schema = tuple(
            (column.name, column.data_type) for column in self._manifest.columns
        )
        rows = 0
        for partition, checksum in zip(generation.output_refs, generation.checksums, strict=True):
            stored = self._raw.execute(
                f'SELECT digest,table_name FROM "{namespace}".runtime_partitions '
                "WHERE dataset=? AND generation=? AND partition=?",
                [generation.dataset_id, generation.generation_id, partition],
            ).fetchone()
            if (
                stored is None
                or stored[0] != checksum
                or _digest_relation(self._raw, f'"{namespace}"', stored[1]) != checksum
            ):
                raise _GENERATION_INVALID.error()
            schema = backend._partition_schema(self._receipt, partition)
            if tuple((field.name, str(field.type)) for field in schema) != expected_schema:
                raise _GENERATION_INVALID.error()
            rows += self._raw.execute(
                f'SELECT count(*) FROM "{namespace}"."{stored[1]}"'
            ).fetchone()[0]
        if rows != generation.row_count:
            raise _GENERATION_INVALID.error()

    def _arrow_schema(self) -> pa.Schema:
        return DuckDBBackend(self._raw, namespace=self._namespace)._partition_schema(
            self._receipt, self._receipt.partitions[0]
        )

    def to_arrow_batches(self, *, batch_size: int = 65_536) -> Iterator[pa.RecordBatch]:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise _INVALID_BATCH.error()
        return self._iter_arrow_batches(batch_size)

    def _iter_arrow_batches(self, batch_size: int) -> Iterator[pa.RecordBatch]:
        backend = DuckDBBackend(self._raw, namespace=self._namespace)
        for partition in self._receipt.partitions:
            reader = backend.read_batches(self._receipt, partition, batch_size=batch_size)
            try:
                yield from reader
            finally:
                reader.close()

    def export_parquet(
        self,
        *,
        destination: str,
        partition_by: tuple[str, ...] = (),
        target_file_bytes: int = 134_217_728,
        budget: Budget | None = None,
    ):
        from terrine.dataset.export import export_dataset

        return export_dataset(
            self,
            destination=destination,
            partition_by=partition_by,
            target_file_bytes=target_file_bytes,
            budget=budget,
        )


def stage_frame(frame: Any, table: str, *, budget: Budget | None = None) -> Dataset:
    if not isinstance(table, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise _INVALID_DESTINATION.error(reason="table must be a simple SQL identifier")
    from terrine.runtime.api import preflight_runtime_shape
    from terrine.spec.validate import validate_design

    model = frame._model
    budget = budget if budget is not None else Budget(strict_scan=False)
    preflight_runtime_shape(frame._design, budget)
    namespace = os.environ.get("TERRINE_RUNTIME_NAMESPACE", "terrine_runtime")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
        raise _INVALID_DESTINATION.error(reason="runtime namespace must be a simple SQL identifier")
    owner = _owner_for(model.con)
    authority = SnapshotAuthority(owner)
    request_key = uuid.uuid4().hex
    snapshot = authority.capture(
        model,
        design=frame._design,
        con=model.con,
        namespace=namespace,
        run_id=request_key,
        budget=budget,
    )
    raw = model.con.con
    store = RuntimeStore(raw, namespace=namespace)
    store.initialize()
    backend = DuckDBBackend(raw, namespace=namespace)
    recipe = _recipe_for(frame)
    required_sources = validate_design(frame._design, model).required_sources
    plan = plan_refresh(
        namespace=namespace,
        dataset=table,
        definition_hash=frame._design.spec_hash,
        snapshot_id=snapshot.snapshot_id,
        budget=budget,
        graph=dependency_graph(
            model, frame._design, sources=required_sources, definition_hash=recipe.digest
        ),
    )
    saved = SavedPlanRecipe.create(
        plan_json=plan.serialize(),
        design=frame._design,
        model=ModelReference.from_model(model, owner_key=owner.reference),
        destination=DestinationRef(str(owner.state_path), namespace, table),
        namespace=namespace,
        expected_base_manifest=backend.current_generation(table),
        dataset_recipe=recipe,
    )
    saved.pin_snapshot(store)
    raw.execute(
        f'CREATE TABLE IF NOT EXISTS "{namespace}".runtime_dataset_requests '
        "(request_key VARCHAR PRIMARY KEY,recipe_json VARCHAR NOT NULL)"
    )
    raw.execute(
        f'INSERT INTO "{namespace}".runtime_dataset_requests VALUES (?,?)',
        [request_key, saved.to_json()],
    )
    receipt = run_serialized_plan(
        saved.to_json(),
        con=raw,
        namespace=namespace,
        request_key=request_key,
        resolver=_ModelResolver(model),
        snapshot_authority=authority,
    )
    if receipt.state != "succeeded" or receipt.published_generation is None:
        raise _GENERATION_INVALID.error()
    partitions = raw.execute(
        f'SELECT p.partition,p.digest,p.table_name FROM "{namespace}".runtime_partitions p '
        f'JOIN "{namespace}".runtime_run_results r ON r.partition=p.partition '
        "WHERE r.run_id=? AND p.dataset=? AND p.generation=? ORDER BY p.partition",
        [receipt.run_id, table, receipt.published_generation],
    ).fetchall()
    published = PublicationReceipt(
        table, receipt.published_generation, 0, tuple(item[0] for item in partitions)
    )
    schema = backend._partition_schema(published, partitions[0][0])
    metadata = MaterializedFrame.from_arrow(pa.Table.from_batches([], schema=schema))
    row_count = sum(
        raw.execute(f'SELECT count(*) FROM "{namespace}"."{item[2]}"').fetchone()[0]
        for item in partitions
    )
    coverage = {
        name: [
            model.readiness.sources[name].start.isoformat(),
            model.readiness.sources[name].end_exclusive.isoformat(),
        ]
        for name in required_sources
        if name in model.readiness.sources
    }
    manifest = DatasetManifest(
        recipe,
        ManifestGeneration(
            table,
            receipt.published_generation,
            published.partitions,
            tuple(item[1] for item in partitions),
            row_count,
        ),
        tuple(
            ManifestColumn(
                field.name,
                str(field.type),
                _column_role(field.name, metadata.roles),
                metadata.to_metadata()["columns"].get(field.name, {}),
            )
            for field in schema
        ),
        frame._design.spec_hash,
        model.model_hash,
        coverage,
        {
            "snapshot_id": snapshot.snapshot_id,
            "receipt_hash": snapshot.receipt_hash,
            "owner": owner.reference,
            "revisions": {name: None for name in required_sources},
        },
        coverage,
        None if recipe.view is None else recipe.view.get("split"),
    )
    raw.execute(
        f'CREATE TABLE IF NOT EXISTS "{namespace}".runtime_dataset_recipes '
        "(dataset VARCHAR,generation VARCHAR,recipe_json VARCHAR,request_key VARCHAR,"
        "manifest_json VARCHAR,PRIMARY KEY(dataset,generation))"
    )
    raw.execute(
        f'INSERT INTO "{namespace}".runtime_dataset_recipes VALUES (?,?,?,?,?)',
        [
            table,
            receipt.published_generation,
            saved.to_json(),
            request_key,
            manifest.to_json().decode(),
        ],
    )
    return Dataset(manifest, con=model.con, namespace=namespace)
