"""Pinned references to committed runtime generations."""

from __future__ import annotations

import os
import re
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa

from terrine.data import Budget
from terrine.dataset.manifest import DatasetManifest, ManifestColumn, ManifestGeneration
from terrine.errors import RefusalSpec
from terrine.runtime.backends.base import PublicationReceipt, StagedGeneration
from terrine.runtime.backends.duckdb import DuckDBBackend, _digest_relation
from terrine.runtime.leases import LeaseStore
from terrine.runtime.owner import RuntimeOwner
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
_ROWS_EXCEEDED = RefusalSpec(
    "terrine.budget.rows_exceeded", "Output exceeded the configured row budget of {limit}."
)
_OUTPUT_EXCEEDED = RefusalSpec(
    "terrine.budget.output_bytes_exceeded",
    "Output exceeded the configured byte budget of {limit}.",
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
    if name == roles.unit:
        return "unit"
    if name == roles.anchor:
        return "anchor"
    if name == roles.t0:
        return "t0"
    if name == roles.treatment:
        return "treatment"
    if name in roles.features:
        return "feature"
    if name in roles.outcomes:
        return "outcome"
    return "support"


class Dataset:
    """An immutable reference to one committed output generation."""

    __slots__ = ("_con", "_raw", "_namespace", "_manifest")

    def __init__(self, manifest: DatasetManifest, *, con: Any, namespace: str) -> None:
        if not isinstance(manifest, DatasetManifest):
            raise TypeError("manifest must be a DatasetManifest")
        self._con = con
        self._raw = con.con
        self._namespace = namespace
        self._manifest = manifest
        self._verify_publication()

    @property
    def manifest(self) -> DatasetManifest:
        return self._manifest

    @classmethod
    def from_manifest(
        cls, manifest: DatasetManifest, *, con: Any, namespace: str | None = None
    ) -> Dataset:
        resolved = namespace or os.environ.get("TERRINE_RUNTIME_NAMESPACE", "terrine_runtime")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", resolved):
            raise _GENERATION_INVALID.error()
        return cls(manifest, con=con, namespace=resolved)

    def _verify_publication(self) -> None:
        generation = self._manifest.generation
        namespace = self._namespace
        backend = DuckDBBackend(self._raw, namespace=namespace)
        publication = self._raw.execute(
            f'SELECT version FROM "{namespace}".runtime_publications '
            "WHERE dataset=? AND generation=?",
            [generation.dataset_id, generation.generation_id],
        ).fetchone()
        rows = self._raw.execute(
            f'SELECT partition, digest, table_name FROM "{namespace}".runtime_partitions '
            "WHERE dataset=? AND generation=? ORDER BY partition",
            [generation.dataset_id, generation.generation_id],
        ).fetchall()
        expected = dict(zip(generation.output_refs, generation.checksums, strict=True))
        actual = {partition: digest for partition, digest, _ in rows}
        if publication is None or actual != expected:
            raise _GENERATION_INVALID.error()
        for _partition, digest, table_name in rows:
            if _digest_relation(self._raw, f'"{namespace}"', table_name) != digest:
                raise _GENERATION_INVALID.error()
        receipt = PublicationReceipt(
            generation.dataset_id,
            generation.generation_id,
            publication[0],
            tuple(sorted(expected)),
        )
        measured_rows = 0
        expected_schema = tuple(
            (column.name, column.data_type) for column in self._manifest.columns
        )
        for partition in receipt.partitions:
            relation = backend.read_relation(receipt, partition, ibis_backend=self._con)
            reader = self._con.to_pyarrow_batches(relation, chunk_size=65_536)
            try:
                schema = tuple((field.name, str(field.type)) for field in reader.schema)
                if schema != expected_schema:
                    raise _GENERATION_INVALID.error()
                for batch in reader:
                    measured_rows += batch.num_rows
            finally:
                reader.close()
        if measured_rows != generation.row_count:
            raise _GENERATION_INVALID.error()

    def _arrow_schema(self) -> pa.Schema:
        generation = self._manifest.generation
        backend = DuckDBBackend(self._raw, namespace=self._namespace)
        publication = self._raw.execute(
            f'SELECT version FROM "{self._namespace}".runtime_publications '
            "WHERE dataset=? AND generation=?",
            [generation.dataset_id, generation.generation_id],
        ).fetchone()
        if publication is None or not generation.output_refs:
            raise _GENERATION_INVALID.error()
        receipt = PublicationReceipt(
            generation.dataset_id,
            generation.generation_id,
            publication[0],
            generation.output_refs,
        )
        relation = backend.read_relation(receipt, generation.output_refs[0], ibis_backend=self._con)
        reader = self._con.to_pyarrow_batches(relation, chunk_size=1)
        try:
            return reader.schema
        finally:
            reader.close()

    def to_arrow_batches(self, *, batch_size: int = 65_536) -> Iterator[pa.RecordBatch]:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise _INVALID_BATCH.error()
        return self._iter_arrow_batches(batch_size)

    def _iter_arrow_batches(self, batch_size: int) -> Iterator[pa.RecordBatch]:
        generation = self._manifest.generation
        backend = DuckDBBackend(self._raw, namespace=self._namespace)
        publication = self._raw.execute(
            f'SELECT version FROM "{self._namespace}".runtime_publications '
            "WHERE dataset=? AND generation=?",
            [generation.dataset_id, generation.generation_id],
        ).fetchone()
        if publication is None:
            raise _GENERATION_INVALID.error()
        receipt = PublicationReceipt(
            generation.dataset_id,
            generation.generation_id,
            publication[0],
            generation.output_refs,
        )
        for partition in generation.output_refs:
            relation = backend.read_relation(receipt, partition, ibis_backend=self._con)
            reader = self._con.to_pyarrow_batches(relation, chunk_size=batch_size)
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
    from terrine.compile.dispatch import compile_design
    from terrine.execution.gates import enforce_shape_budget, validate_compiled_output
    from terrine.spec.validate import validate_design

    model = frame._model
    budget = budget if budget is not None else Budget(strict_scan=False)
    enforce_shape_budget(frame._design.shape, budget)
    namespace = os.environ.get("TERRINE_RUNTIME_NAMESPACE", "terrine_runtime")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
        raise _INVALID_DESTINATION.error(reason="runtime namespace must be a simple SQL identifier")
    owner = _owner_for(model.con)
    authority = SnapshotAuthority(owner)
    run_id = uuid.uuid4().hex
    snapshot = authority.capture(
        model,
        design=frame._design,
        con=model.con,
        namespace=namespace,
        run_id=run_id,
        budget=budget,
    )
    raw = model.con.con
    RuntimeStore(raw, namespace=namespace).initialize()
    lease_store = LeaseStore(raw, namespace=namespace)
    lease_store.initialize()
    backend = DuckDBBackend(raw, namespace=namespace)
    expected_manifest = backend.current_generation(table)
    lease = lease_store.acquire(
        f"dataset:{table}", run_id, ttl=timedelta(minutes=5), now=datetime.now(UTC)
    )
    generation_id = uuid.uuid4().hex
    try:
        with authority.reopen(snapshot, con=model.con, namespace=namespace) as context:
            execution_model = model._for_execution(
                context,
                source_names=set(validate_design(frame._design, model).required_sources),
                unit_name=frame._design.unit,
            )
            compiled = compile_design(frame._design, execution_model)
            compiled.validate(execute=context.execute)
            compiled = frame._active_compiled(compiled)
            validate_compiled_output(compiled, context.execute)
            query = compiled.table
            context._check_query(query)
            source_reader = context.con.to_pyarrow_batches(query, chunk_size=65_536)
            schema = source_reader.schema
            row_count = 0
            output_bytes = 0

            def batches():
                nonlocal row_count, output_bytes
                try:
                    for batch in source_reader:
                        row_count += batch.num_rows
                        output_bytes += batch.nbytes
                        if budget.max_rows is not None and row_count > budget.max_rows:
                            raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
                        if (
                            budget.max_output_bytes is not None
                            and output_bytes > budget.max_output_bytes
                        ):
                            raise _OUTPUT_EXCEEDED.error(limit=budget.max_output_bytes)
                        yield batch
                finally:
                    source_reader.close()

            stream = pa.RecordBatchReader.from_batches(schema, batches())
            receipt = backend.publish_generation(
                StagedGeneration(
                    table,
                    generation_id,
                    {"data": stream},
                    run_id=run_id,
                    snapshot_id=snapshot.snapshot_id,
                ),
                expected_manifest=expected_manifest,
                lease=lease,
            )
            columns = tuple(
                ManifestColumn(
                    field.name,
                    str(field.type),
                    _column_role(field.name, compiled.roles),
                    {"nullable": field.nullable},
                )
                for field in schema
            )
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
            digests = raw.execute(
                f'SELECT digest FROM "{namespace}".runtime_partitions '
                "WHERE dataset=? AND generation=? ORDER BY partition",
                [table, generation_id],
            ).fetchall()
            coverage = {
                name: [item.start.isoformat(), item.end_exclusive.isoformat()]
                for name, item in model.readiness.sources.items()
            }
            manifest = DatasetManifest(
                DatasetRecipe(schema_version=1, base_design=frame._design, view=view),
                ManifestGeneration(
                    table,
                    generation_id,
                    receipt.partitions,
                    tuple(row[0] for row in digests),
                    row_count,
                ),
                columns,
                frame._design.spec_hash,
                model.model_hash,
                coverage,
                {"snapshot_id": snapshot.snapshot_id, "receipt_hash": snapshot.receipt_hash},
                coverage,
            )
            return Dataset(manifest, con=model.con, namespace=namespace)
    finally:
        lease_store.release(lease)
