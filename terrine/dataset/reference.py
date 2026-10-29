"""Pinned references to committed runtime generations."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from collections.abc import Generator
from dataclasses import replace
from typing import Any

import pyarrow as pa

from terrine.data import Budget, MaterializedFrame
from terrine.dataset.manifest import DatasetManifest, ManifestColumn, ManifestGeneration
from terrine.errors import RefusalSpec
from terrine.execution.backend import backend_capabilities, backend_provider
from terrine.runtime.api import RunReceipt, run_serialized_plan
from terrine.runtime.backends.base import PublicationReceipt
from terrine.runtime.backends.duckdb import DuckDBBackend, _digest_relation, _digest_version
from terrine.runtime.changes import ChangeSet
from terrine.runtime.graph import dependency_graph
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import RefreshPlan, plan_refresh
from terrine.runtime.saved_plan import (
    DestinationRef,
    ModelReference,
    SavedPlan,
    _bind_saved_plan,
)
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.spec.dataset import SCHEMA_VERSION, DatasetDefinition

__all__ = ["Dataset", "stage_frame", "dataset_from_run"]

_INVALID_DESTINATION = RefusalSpec(
    "terrine.dataset.invalid_destination", "Dataset destination is invalid: {reason}."
)
_FILE_BACKED_CONNECTION_REQUIRED = RefusalSpec(
    "terrine.dataset.invalid_destination",
    "Staging, refreshing and reopening a dataset need a file-backed DuckDB connection; "
    "an in-memory model materializes only. Bind the same tables with {route}.",
)
_INVALID_BATCH = RefusalSpec(
    "terrine.dataset.invalid_batch_size", "Arrow batch size must be a positive integer."
)
_GENERATION_INVALID = RefusalSpec(
    "terrine.dataset.invalid_generation",
    "Dataset manifest does not identify a verified committed runtime generation.",
)
_STALE_BASE = RefusalSpec(
    "terrine.dataset.stale_generation",
    "Refresh requires the pinned generation to remain the current publication.",
)
_INVALID_MANIFEST = RefusalSpec(
    "terrine.dataset.invalid_manifest", "Dataset manifest input {field} is invalid: {reason}."
)
_INVALID_REQUEST = RefusalSpec(
    "terrine.runtime.plan_request_invalid", "Owner plan request input {field} is invalid: {reason}."
)


def _owner_for(con: Any) -> RuntimeOwner:
    try:
        return RuntimeOwner.from_connection(con)
    except Exception as error:
        raise _FILE_BACKED_CONNECTION_REQUIRED.error(
            reason="file_backed_connection_required",
            route="Model.from_tables(ibis.duckdb.connect('<file>.duckdb'), ...)",
        ) from error


def _column_role(name: str, roles: Any) -> str:
    for role in ("unit", "anchor", "t0", "treatment"):
        if name == getattr(roles, role):
            return role
    if name in roles.features:
        return "feature"
    if name in roles.outcomes:
        return "outcome"
    return "support"


def _definition_for(frame: Any, *, include_transforms: bool = True) -> DatasetDefinition:
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
    return DatasetDefinition(
        schema_version=SCHEMA_VERSION,
        base_design=frame._design,
        view=view,
        transforms=frame._winsorizations if include_transforms else (),
    )


class _ModelResolver:
    def __init__(self, model: Any) -> None:
        self.model = model

    def resolve(self, reference: ModelReference):
        return self.model


def _require_runtime_reader(con: Any) -> None:
    """Refuse before database access when published generations cannot be read."""
    backend_capabilities(con).require(
        "runtime_reader",
        backend=str(getattr(con, "name", type(con).__name__)).casefold(),
        route="Published datasets live in the DuckDB runtime; reopen them with a DuckDB "
        "connection, or materialize this backend's Frame directly.",
    )


class Dataset:
    """Reference one immutable published dataset generation.

    Example: frame.stage("customer_spend")
    """

    __slots__ = ("_con", "_raw", "_namespace", "_manifest", "_receipt")

    def __init__(self, manifest: DatasetManifest, *, con: Any, namespace: str) -> None:
        if not isinstance(manifest, DatasetManifest):
            raise _INVALID_MANIFEST.error(reason="wrong_type", field="manifest")
        _require_runtime_reader(con)
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

    @classmethod
    def open_generation(cls, dataset: str, generation: str, *, con: Any, namespace: str) -> Dataset:
        """Open an owner-persisted publication without rescanning its output."""
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
            raise _GENERATION_INVALID.error()
        _require_runtime_reader(con)
        record = con.con.execute(
            f'SELECT manifest_json FROM "{namespace}".runtime_dataset_recipes '
            "WHERE dataset=? AND generation=?",
            [dataset, generation],
        ).fetchone()
        if record is None:
            raise _GENERATION_INVALID.error()
        instance = cls.__new__(cls)
        instance._con, instance._raw = con, con.con
        instance._namespace, instance._manifest = namespace, DatasetManifest.from_json(record[0])
        if (
            instance.manifest.generation.dataset_id != dataset
            or instance.manifest.generation.generation_id != generation
        ):
            raise _GENERATION_INVALID.error()
        instance._load_publication()
        for partition, checksum in zip(
            instance.manifest.generation.output_refs,
            instance.manifest.generation.checksums,
            strict=True,
        ):
            instance._partition_table(partition, checksum)
        return instance

    def _partition_table(self, partition: str, checksum: str) -> str:
        generation = self._manifest.generation
        stored = self._raw.execute(
            f'SELECT digest,table_name FROM "{self._namespace}".runtime_partitions '
            "WHERE dataset=? AND generation=? AND partition=?",
            [generation.dataset_id, generation.generation_id, partition],
        ).fetchone()
        if stored is None or stored[0] != checksum:
            raise _GENERATION_INVALID.error()
        return stored[1]

    def _load_publication(self) -> None:
        generation, namespace = self._manifest.generation, self._namespace
        if self._manifest.source_snapshots.get("owner") != _owner_for(self._con).reference:
            raise _GENERATION_INVALID.error()
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
            or record[0].encode() != replace(self._manifest, definition_reference=None).to_json()
        ):
            raise _GENERATION_INVALID.error()
        self._receipt = PublicationReceipt(
            generation.dataset_id, generation.generation_id, record[1], generation.output_refs
        )

    def _verify_publication(self) -> None:
        self._load_publication()
        generation, namespace = self._manifest.generation, self._namespace
        backend = DuckDBBackend(self._raw, namespace=namespace)
        expected_schema = tuple(
            (column.name, column.data_type) for column in self._manifest.columns
        )
        rows = 0
        for partition, checksum in zip(generation.output_refs, generation.checksums, strict=True):
            table_name = self._partition_table(partition, checksum)
            if (
                _digest_relation(
                    self._raw, f'"{namespace}"', table_name, version=_digest_version(checksum)
                )
                != checksum
            ):
                raise _GENERATION_INVALID.error()
            schema = backend._partition_schema(self._receipt, partition)
            if tuple((field.name, str(field.type)) for field in schema) != expected_schema:
                raise _GENERATION_INVALID.error()
            rows += self._raw.execute(
                f'SELECT count(*) FROM "{namespace}"."{table_name}"'
            ).fetchone()[0]
        if rows != generation.row_count:
            raise _GENERATION_INVALID.error()

    def _arrow_schema(self) -> pa.Schema:
        return DuckDBBackend(self._raw, namespace=self._namespace)._partition_schema(
            self._receipt, self._receipt.partitions[0]
        )

    def _require_arrow_batches(self) -> None:
        provider = backend_provider(self._con)
        provider.capabilities.require(
            "arrow_batches",
            backend=provider.dialect(self._con),
            route="Read or export the Dataset with a backend supporting bounded Arrow batches.",
        )

    def to_arrow_batches(self, *, batch_size: int = 65_536) -> Generator[pa.RecordBatch]:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise _INVALID_BATCH.error()
        self._require_arrow_batches()
        return self._iter_arrow_batches(batch_size)

    def _iter_arrow_batches(self, batch_size: int) -> Generator[pa.RecordBatch]:
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

    def plan_refresh(
        self, model: Any, *, changes: ChangeSet | None = None, budget: Budget | None = None
    ) -> RefreshPlan:
        """Plan from the saved definition without reading or staging source rows."""
        if _owner_for(model.con).reference != self.manifest.source_snapshots["owner"]:
            raise _GENERATION_INVALID.error()
        if changes is not None and not isinstance(changes, ChangeSet):
            raise _INVALID_REQUEST.error(reason="wrong_type", field="changes")
        if model.model_hash != self.manifest.model_hash:
            changes = ChangeSet((), "conservative_rebuild")
        frame = self.manifest.definition.build(model)
        return _plan_frame(
            frame,
            namespace=self._namespace,
            table=self.manifest.generation.dataset_id,
            snapshot_id=self.manifest.source_snapshots["snapshot_id"],
            budget=budget if budget is not None else Budget(strict_scan=False),
            changes=changes,
        )

    def refresh(
        self, model: Any, *, changes: ChangeSet | None = None, budget: Budget | None = None
    ) -> Dataset:
        """Publish a definition reconstruction while leaving this generation pinned."""
        plan = self.plan_refresh(model, changes=changes, budget=budget)
        if (
            DuckDBBackend(model.con.con, namespace=self._namespace).current_generation(
                self.manifest.generation.dataset_id
            )
            != self.manifest.generation.generation_id
        ):
            raise _STALE_BASE.error()
        if not plan.tasks:
            return Dataset.from_manifest(self.manifest, con=model.con, namespace=self._namespace)
        return stage_frame(
            self.manifest.definition.build(model),
            self.manifest.generation.dataset_id,
            budget=budget,
            _base=self,
            _changes=(
                ChangeSet((), "conservative_rebuild")
                if model.model_hash != self.manifest.model_hash
                else changes
            ),
        )


def _plan_frame(frame, *, namespace, table, snapshot_id, budget, changes=None):
    from terrine.spec.validate import validate_design

    definition = _definition_for(frame)
    definition_hash = hashlib.sha256(
        (definition.digest + frame._model.model_hash).encode()
    ).hexdigest()
    return plan_refresh(
        namespace=namespace,
        dataset=table,
        definition_hash=frame._design.spec_hash,
        snapshot_id=snapshot_id,
        budget=budget,
        changes=changes,
        graph=dependency_graph(
            frame._model,
            frame._design,
            sources=validate_design(frame._design, frame._model).required_sources,
            definition_hash=definition_hash,
        ),
    )


def stage_frame(
    frame: Any,
    table: str,
    *,
    budget: Budget | None = None,
    _base: Dataset | None = None,
    _changes: ChangeSet | None = None,
) -> Dataset:
    if not isinstance(table, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise _INVALID_DESTINATION.error(reason="table must be a simple SQL identifier")
    from terrine.runtime.api import preflight_runtime_shape

    model = frame._model
    provider = backend_provider(model.con, engine=model._engine)
    for operation in ("table_publication", "atomic_manifest_publish", "retained_snapshot"):
        provider.capabilities.require(
            operation,
            backend=model.catalog.dialect,
            route="Materialize the Frame directly, or use verified local DuckDB staging.",
        )
    budget = budget if budget is not None else Budget(strict_scan=False)
    preflight_runtime_shape(frame._design, budget)
    namespace = (
        _base._namespace
        if _base is not None
        else os.environ.get("TERRINE_RUNTIME_NAMESPACE", "terrine_runtime")
    )
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
        raise _INVALID_DESTINATION.error(reason="runtime namespace must be a simple SQL identifier")
    owner = _owner_for(model.con)
    raw = model.con.con
    backend = DuckDBBackend(raw, namespace=namespace)
    expected_base = (
        backend.current_generation(table)
        if _base is None
        else _base.manifest.generation.generation_id
    )
    if _base is not None and backend.current_generation(table) != expected_base:
        raise _STALE_BASE.error()
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
    definition = _definition_for(frame)
    plan = _plan_frame(
        frame,
        namespace=namespace,
        table=table,
        snapshot_id=snapshot.snapshot_id,
        budget=budget,
        changes=_changes,
    )
    saved = SavedPlan.create(
        plan_json=plan.serialize(),
        design=frame._design,
        model=ModelReference.from_model(model, owner_key=owner.reference),
        destination=DestinationRef(str(owner.state_path), namespace, table),
        namespace=namespace,
        expected_base_manifest=expected_base,
        dataset_definition=definition,
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
    return dataset_from_run(
        saved,
        receipt,
        model=model,
        namespace=namespace,
        request_key=request_key,
        snapshot_authority=authority,
    )


def dataset_from_run(
    saved: SavedPlan,
    receipt: RunReceipt,
    *,
    model: Any,
    namespace: str,
    request_key: str,
    snapshot_authority: SnapshotAuthority,
) -> Dataset:
    """Record and reopen the exact committed generation of a saved dataset run."""
    from terrine.spec.validate import validate_design

    if (
        namespace != saved.namespace
        or receipt.state != "succeeded"
        or receipt.published_generation is None
    ):
        raise _GENERATION_INVALID.error()
    if snapshot_authority.owner.reference != _owner_for(model.con).reference:
        raise _GENERATION_INVALID.error()
    table, raw = saved.destination.dataset, model.con.con
    plan = _bind_saved_plan(saved, namespace=namespace)
    record = raw.execute(
        f"SELECT dataset,state,published_generation,request_key,fingerprint,destination_json "
        f'FROM "{namespace}".runtime_runs WHERE run_id=?',
        [receipt.run_id],
    ).fetchone()
    if record is None or (
        record[:5]
        != (
            table,
            "succeeded",
            receipt.published_generation,
            request_key,
            plan.definition.fingerprint,
        )
        or json.loads(record[5]) != saved.destination.to_dict()
        or ModelReference.from_model(model, owner_key=saved.model.owner_key) != saved.model
    ):
        raise _GENERATION_INVALID.error()
    exists = raw.execute(
        "SELECT 1 FROM information_schema.tables WHERE lower(table_schema)=lower(?) "
        "AND table_name='runtime_dataset_recipes'",
        [namespace],
    ).fetchone()
    if exists is not None:
        prior = raw.execute(
            f'SELECT manifest_json FROM "{namespace}".runtime_dataset_recipes '
            "WHERE dataset=? AND generation=?",
            [table, receipt.published_generation],
        ).fetchone()
        if prior is not None:
            return Dataset(DatasetManifest.from_json(prior[0]), con=model.con, namespace=namespace)
    definition = saved.restore_dataset_definition()
    if definition is None:
        raise _GENERATION_INVALID.error()
    snapshot = snapshot_authority.find_retained(plan.snapshot_id, namespace=namespace)
    owner = snapshot_authority.owner
    backend = DuckDBBackend(raw, namespace=namespace)
    required_sources = validate_design(definition.base_design, model).required_sources
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
        definition,
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
        definition.base_design.spec_hash,
        model.model_hash,
        coverage,
        {
            "snapshot_id": snapshot.snapshot_id,
            "receipt_hash": snapshot.receipt_hash,
            "owner": owner.reference,
            "revisions": {name: None for name in required_sources},
        },
        coverage,
        None if definition.view is None else definition.view.get("split"),
        frame_metadata=metadata.to_metadata(),
    )
    raw.execute(
        f'CREATE TABLE IF NOT EXISTS "{namespace}".runtime_dataset_recipes '
        "(dataset VARCHAR,generation VARCHAR,recipe_json VARCHAR,request_key VARCHAR,"
        "manifest_json VARCHAR,PRIMARY KEY(dataset,generation))"
    )
    raw.execute(
        f'INSERT INTO "{namespace}".runtime_dataset_recipes VALUES (?,?,?,?,?) '
        "ON CONFLICT DO NOTHING",
        [
            table,
            receipt.published_generation,
            saved.to_json(),
            request_key,
            manifest.to_json().decode(),
        ],
    )
    return Dataset(manifest, con=model.con, namespace=namespace)
