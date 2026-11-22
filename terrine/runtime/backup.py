"""Offline inventory and atomic recovery for a single DuckDB owner."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import hmac
import json
import os
import re
import sys
from collections import defaultdict
from collections.abc import Callable, Generator
from contextlib import closing, contextmanager
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO

import duckdb
import pyarrow as pa

from terrine.dataset.manifest import DatasetManifest
from terrine.errors import TerrineError
from terrine.runtime._relocation import (
    _IMPORT,
    _INVALID,
    _INVENTORY,
    _ROOT,
    _STATE,
    _VERSION,
    _hash,
    _Imports,
    _json,
    _key,
    _open_regular,
    _Record,
    _relative,
)
from terrine.runtime.arrow import (
    _OUTPUT_PREFIXES,
    _PREFIX,
    ArrowOutput,
    _output_binding,
    _OutputBindingError,
    _owner_output_directory,
    _task_key,
)
from terrine.runtime.backends.base import PublicationReceipt
from terrine.runtime.backends.duckdb import (
    DuckDBBackend,
    _digest_relation,
    _digest_version,
    _quote,
    _table_name,
)
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import RefreshPlan
from terrine.runtime.planning import PlanInvocation
from terrine.runtime.saved_plan import SavedPlan, _bind_saved_plan
from terrine.runtime.schema import _METADATA_TABLES, check_runtime_schema
from terrine.runtime.snapshot import (
    ReopenableSnapshot,
    _open_snapshot_context,
    _retained_declarations,
)
from terrine.runtime.state import DefinitionVersion, _snapshot_from_json


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise _INVALID.error(reason=reason)


def _digest(value: str) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


@contextmanager
def _read_only(path: Path) -> Generator[duckdb.DuckDBPyConnection]:
    # DuckDB's native read-only lock excludes writers, including through another
    # alias. Never connect for recovery or allow catalog objects to read other files.
    _require(not os.path.lexists(f"{path}.wal"), "owner_not_checkpointed")
    with closing(
        duckdb.connect(str(path), read_only=True, config={"enable_external_access": False})
    ) as con:
        _require(not os.path.lexists(f"{path}.wal"), "owner_not_checkpointed")
        yield con


def _file(stream: BinaryIO, target: Path) -> dict[str, Any]:
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    digest = hashlib.sha256()
    size = 0
    with target.open("xb") as dest:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            dest.write(block)
            digest.update(block)
            size += len(block)
        dest.flush()
        os.fsync(dest.fileno())
    return {"size": size, "sha256": digest.hexdigest()}


def _write(path: Path, data: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _sync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _install(stage: Path, target: Path) -> None:
    # Rename without replacement, even if an empty directory appears during verification.
    for directory, _, _ in os.walk(stage, topdown=False):
        _sync_dir(Path(directory))
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        status = libc.renamex_np(os.fsencode(stage), os.fsencode(target), 0x00000004)
    elif sys.platform == "linux":
        status = libc.renameat2(-100, os.fsencode(stage), -100, os.fsencode(target), 1)
    else:
        raise _INVALID.error(reason="unsupported_platform")
    if status != 0:
        error = ctypes.get_errno()
        if error in {errno.EEXIST, errno.ENOTEMPTY}:
            raise _INVALID.error(reason="destination_exists")
        raise OSError(error, os.strerror(error))
    _sync_dir(target.parent)


def _destination(path: Path, *protected: Path) -> Path:
    supplied = Path(os.path.abspath(path.expanduser()))
    parent = supplied.parent.resolve(strict=True)
    _require(parent == supplied.parent, "unsafe_path")
    target = parent / supplied.name
    _require(not os.path.lexists(target), "destination_exists")
    _require(
        all(
            not target.is_relative_to(item) and not item.is_relative_to(target)
            for item in protected
        ),
        "overlapping_paths",
    )
    return target


def _owner_parts(reference: str) -> tuple[Path, Path]:
    state, root = map(Path, reference.split("\n"))
    _require(state.is_absolute() and root.is_absolute(), "invalid_owner_identity")
    return state, root


class _Inventory:
    def __init__(
        self,
        origin: str,
        imported: _Imports | None,
        materialize: Callable[[Path, str | None, Callable[[bytes], None] | None], Path],
        *,
        restoring: bool = False,
    ) -> None:
        self.origin = origin
        self.imported = imported
        self.materialize = materialize
        self.restoring = restoring
        self.records: dict[str, _Record] = {}
        self.artifacts: set[Path] = set()

    def record(self, key: str, origin: str, digest: str, path: Path | None = None) -> None:
        record = _Record(origin, digest, None if path is None else path.as_posix())
        if origin != self.origin:
            _require(
                self.imported is not None and self.imported.records.get(key) == record,
                "foreign_owner_record",
            )
        _require(self.records.setdefault(key, record) == record, "record_collision")

    def artifact(
        self, path: Path, digest: str | None = None, check: Callable[[bytes], None] | None = None
    ) -> Path:
        self.artifacts.add(path)
        return self.materialize(path, digest, check)

    def receipt(self, namespace: str, snapshot_id: str, location: Path) -> ReopenableSnapshot:
        key = _key("snapshot", namespace, snapshot_id)
        prior = None if self.imported is None else self.imported.records.get(key)
        if prior is not None and prior.path is not None:
            relative = _relative(prior.path)
        else:
            # Legacy state records have a snapshot ID but no filename. Match
            # only that ID under DuckDB aliases of this schema.
            candidates = []
            for directory in location.iterdir():
                if directory.name.lower() != namespace.lower():
                    continue
                _require(not directory.is_symlink(), "unsafe_path")
                candidates.extend(
                    Path(directory.name, item.name, "receipt.json")
                    for item in directory.iterdir()
                    if item.name.endswith(f"-{snapshot_id}")
                    and re.fullmatch(r"[A-Za-z0-9_.-]+", item.name)
                )
            _require(len(candidates) == 1, "missing_or_ambiguous_snapshot")
            with _open_regular(location, candidates[0]) as stream:
                stored_namespace = ReopenableSnapshot.from_json(stream.read()).namespace
            _require(stored_namespace.lower() == namespace.lower(), "invalid_snapshot_binding")
            relative = _ROOT / stored_namespace / candidates[0].parent.name / "receipt.json"

        def verify(encoded: bytes) -> None:
            receipt = ReopenableSnapshot.from_json(encoded)
            _require(
                type(receipt.schema_version) is int
                and set(json.loads(encoded)) == set(ReopenableSnapshot.__dataclass_fields__),
                "unsupported_snapshot_format",
            )
            _, original_root = _owner_parts(receipt.owner_reference)
            expected = Path(receipt.namespace, f"{receipt.run_id}-{snapshot_id}")
            _require(
                receipt.namespace.lower() == namespace.lower()
                and receipt.snapshot_id == snapshot_id
                and re.fullmatch(r"[A-Za-z0-9_.-]+", receipt.run_id) is not None
                and relative == _ROOT / expected / "receipt.json"
                and receipt.receipt_path == str(original_root / expected / "receipt.json")
                and receipt.artifact_path == str(original_root / expected / "inputs.duckdb"),
                "invalid_snapshot_binding",
            )
            self.record(key, receipt.owner_reference, receipt.receipt_hash, relative)

        receipt_path = self.artifact(relative, check=verify)
        receipt = ReopenableSnapshot.from_json(receipt_path.read_bytes())
        artifact = self.artifact(relative.with_name("inputs.duckdb"), receipt.artifact_hash)
        catalog, units = _retained_declarations(receipt)
        with _read_only(artifact) as raw:
            # Snapshot roots are resolved against the verified copy, not live source paths.
            with closing(_open_snapshot_context(receipt, catalog, units, raw.cursor())):
                pass
        return receipt

    def arrow(self, namespace: str, task: tuple, run: tuple, plan: RefreshPlan) -> None:
        run_id, task_id, payload, output_rows, output_bytes = task
        output = ArrowOutput.from_bytes(payload)
        _require((output_rows, output_bytes) == (output.num_rows, output.nbytes), "arrow_counts")
        key = _key("arrow", namespace, run_id, task_id)
        prior = None if self.imported is None else self.imported.records.get(key)
        origin = self.origin if prior is None else prior.origin
        _, root = _owner_parts(origin)
        expected = _owner_output_directory(origin, namespace, run[1]) / _task_key(task_id)
        _require(
            output.path.parent == root / expected
            and re.fullmatch(r"[0-9a-f]{32}\.arrow", output.path.name) is not None,
            "unbound_arrow_output",
        )
        relative = _ROOT / expected / output.path.name
        self.record(key, origin, _hash(payload), relative)
        artifact = self.artifact(relative, output.checksum)
        specification = next((item for item in plan.tasks if item.task_id == task_id), None)
        _require(specification is not None, "missing_task_definition")
        rows = 0
        binding = _output_binding(origin, namespace, run_id, task_id)
        try:
            with output._open_verified(artifact, binding=binding) as reader:
                if specification is not None and specification.layer == "frame":
                    from terrine.data import MaterializedFrame

                    MaterializedFrame.from_arrow(pa.Table.from_batches([], schema=reader.schema))
                for batch in reader:
                    batch.validate(full=True)
                    rows += batch.num_rows
        except _OutputBindingError as error:
            raise _INVALID.error(reason="arrow_owner_binding") from error
        _require(rows == output.num_rows, "arrow_counts")

    def saved(self, value: str, namespace: str) -> tuple[SavedPlan, RefreshPlan]:
        saved = SavedPlan.from_json(value)
        _require(saved.namespace.lower() == namespace.lower(), "plan_scope_mismatch")
        plan = _bind_saved_plan(saved, namespace=saved.namespace)
        self.plan_record(namespace, saved.plan_hash, saved.destination.database)
        return saved, plan

    def plan_record(self, namespace: str, plan_hash: str, database: str) -> None:
        key = _key("plan", namespace, plan_hash)
        prior = None if self.imported is None else self.imported.records.get(key)
        origin = self.origin if prior is None else prior.origin
        # Persist the exact destination binding so restore never follows a
        # historical alias, which may no longer exist or name the same file.
        digest = _hash(_json((plan_hash, database)))
        if prior is None:
            _require(not self.restoring, "foreign_plan_destination")
            source_state, _ = _owner_parts(origin)
            path = Path(database)
            if path.is_absolute():
                _require(
                    path == source_state or path.resolve() == source_state,
                    "foreign_plan_destination",
                )
        else:
            _require(prior.digest == digest, "foreign_plan_destination")
        self.record(key, origin, digest)

    def scan(self, con: Any, location: Path) -> list[str]:
        namespaces = [
            row[0]
            for row in con.execute(
                "SELECT DISTINCT table_schema FROM information_schema.tables "
                "WHERE table_catalog=current_database() "
                "AND lower(table_name) IN (SELECT unnest(?)) "
                "ORDER BY table_schema",
                [sorted(_METADATA_TABLES)],
            ).fetchall()
        ]
        for namespace in namespaces:
            _require(
                re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace) is not None, "invalid_namespace"
            )
            check_runtime_schema(con, namespace)
            self.namespace(con, namespace, location)
        return namespaces

    def namespace(self, con: Any, namespace: str, location: Path) -> None:
        schema = _quote(namespace)

        def query(table: str, columns: str = "*") -> list[tuple]:
            return con.execute(f"SELECT {columns} FROM {schema}.runtime_{table}").fetchall()

        captures = {row[0] for row in query("snapshot_captures", "snapshot_id")}
        collected = {row[0] for row in query("snapshot_gc", "snapshot_id")}
        pinned = {row[0] for row in query("snapshot_references", "snapshot_id")}
        _require(not pinned.intersection(collected), "collected_snapshot_is_pinned")
        snapshots = {}
        for snapshot_id in sorted(captures - collected):
            _require(
                re.fullmatch(r"[0-9a-f]{32}", snapshot_id) is not None, "invalid_snapshot_identity"
            )
            snapshots[snapshot_id] = self.receipt(namespace, snapshot_id, location)

        plans: dict[str, RefreshPlan] = {}
        requests: dict[tuple[str, str], tuple[SavedPlan, RefreshPlan]] = {}
        executions: dict[str, RefreshPlan] = {}
        for request_key, value in query("dataset_requests"):
            saved, plan = self.saved(value, namespace)
            requests[request_key, _json(saved.destination.to_dict()).decode()] = saved, plan
            plans[plan.definition.fingerprint] = plan
        for invocation_id, digest, snapshot_id, value in query("plan_invocations"):
            if invocation_id.startswith('["execution",'):
                plan = RefreshPlan.deserialize(value)
                _require(plan.definition.fingerprint == digest, "execution_plan_digest")
                prefix, run_id = json.loads(invocation_id)
                _require(prefix == "execution" and bool(run_id), "execution_plan_binding")
                executions[run_id] = plan
            else:
                invocation = PlanInvocation.from_dict(json.loads(value))
                saved, plan = self.saved(invocation.saved_plan_json, namespace)
                _require(
                    _digest(digest)
                    and invocation.invocation_id == invocation_id
                    and invocation.request_key
                    == _hash(
                        _json((plan.namespace, invocation_id, invocation.destination.to_dict()))
                    )
                    and (
                        invocation.serialized_plan,
                        invocation.destination,
                        invocation.expected_base_manifest,
                    )
                    == (saved.plan_json, saved.destination, saved.expected_base_manifest),
                    "invalid_plan_invocation",
                )
                key = invocation.request_key, _json(saved.destination.to_dict()).decode()
                requests[key] = saved, plan
            _require(
                plan.namespace.lower() == namespace.lower() and plan.snapshot_id == snapshot_id,
                "plan_scope_mismatch",
            )
            plans[plan.definition.fingerprint] = plan

        for (request_key, destination), (saved, plan) in requests.items():
            if plan.snapshot_id in snapshots:
                self.record(
                    _key("planned_request", namespace, request_key, saved.plan_hash),
                    self.origin,
                    _hash(_json((plan.definition.fingerprint, destination))),
                )

        runs = {
            run[0]: run[1:]
            for run in query(
                "runs",
                "run_id,dataset,fingerprint,state,published_generation,request_key,destination_json,snapshot_id",
            )
        }
        for run_id, plan in executions.items():
            _require(
                run_id in runs and runs[run_id][1] == plan.definition.fingerprint,
                "execution_plan_binding",
            )
        for run_id, run in runs.items():
            dataset, fingerprint, _, _, request_key, destination, snapshot_id = run
            if fingerprint in plans:
                plan = plans[fingerprint]
                _require(
                    (plan.dataset, plan.snapshot_id) == (dataset, snapshot_id), "run_plan_binding"
                )
            request = requests.get((request_key, destination))
            if request is not None:
                _require(fingerprint == request[1].definition.fingerprint, "run_request_binding")
            reconstruction = executions.get(run_id)
            if request_key is not None and (
                request is not None
                or (
                    reconstruction is not None
                    and reconstruction.definition.reconstruction_hash is not None
                )
            ):
                plan = plans[fingerprint]
                destination_data = json.loads(destination)
                database = destination_data["database"]
                _require(destination_data["dataset"] == dataset, "run_destination_binding")
                plan_hash = plan.definition.reconstruction_hash
                if plan_hash is None or not _digest(plan_hash):
                    raise _INVALID.error(reason="missing_saved_plan")
                self.plan_record(namespace, plan_hash, database)
                self.record(
                    _key("request", namespace, request_key, run_id),
                    self.origin,
                    _hash(_json((fingerprint, destination))),
                )
        # Custom executors can use logical snapshot IDs with no external artifact.
        saved_snapshots = {saved.plan_hash: plan.snapshot_id for saved, plan in requests.values()}
        for snapshot_id, kind, reference in query("snapshot_references"):
            if kind == "saved_plan" and reference in saved_snapshots:
                _require(
                    snapshot_id == saved_snapshots[reference] and snapshot_id in captures,
                    "saved_snapshot_binding",
                )
            if snapshot_id not in captures:
                _require(not re.fullmatch(r"[0-9a-f]{32}", snapshot_id), "missing_capture_record")
        task_cursor = con.cursor()
        try:
            task_cursor.execute(
                f"SELECT run_id,task_id,output_blob,output_rows,output_bytes "
                f"FROM {schema}.runtime_tasks WHERE output_blob IS NOT NULL"
            )
            while (task := task_cursor.fetchone()) is not None:
                self.task(task, runs, plans)
        finally:
            task_cursor.close()

        self.publications(con, namespace, query, runs, requests, snapshots, collected)

    def task(self, task, runs, plans) -> None:
        run_id, _, payload, _, _ = task
        _require(run_id in runs, "orphan_task_output")
        run = runs[run_id]
        if payload.startswith(_PREFIX):
            # Terminal replay verifies published partitions or cancellation,
            # never the completed task stream. Keep its pointer as history only.
            if run[2] in {"succeeded", "cancelled"}:
                return
            raise _INVALID.error(reason="legacy_retry_artifact_unbound")
        if payload.startswith(_OUTPUT_PREFIXES):
            plan = plans.get(run[1])
            if plan is None:
                raise _INVALID.error(reason="missing_arrow_plan")
            self.arrow(plan.namespace, task, run, plan)
        else:
            with pa.ipc.open_stream(payload) as reader:
                for batch in reader:
                    batch.validate(full=True)

    def publications(self, con, namespace, query, runs, requests, snapshots, collected) -> None:
        schema = _quote(namespace)
        backend = DuckDBBackend(con, namespace=namespace)
        published = {(d, g): v for d, g, v in query("publications")}
        links = {run: (d, g, v) for run, d, g, v in query("run_publications")}
        generations = set()
        for dataset, generation, definition, encoded in query(
            "generations", "dataset,generation,definition_json,snapshot_json"
        ):
            DefinitionVersion(**json.loads(definition))
            snapshot = _snapshot_from_json(json.loads(encoded))
            self.record(
                _key("generation", namespace, dataset, generation),
                snapshot.owner_reference,
                _hash(encoded.encode()),
            )
            if snapshot.snapshot_id in snapshots:
                _require(
                    snapshots[snapshot.snapshot_id].owner_reference == snapshot.owner_reference,
                    "generation_snapshot_binding",
                )
            generations.add((dataset, generation))
        partitions = defaultdict(dict)
        for dataset, generation, name, table, digest in query("partitions"):
            key = dataset, generation
            _require(
                key in published and table == _table_name(dataset, generation, name),
                "unpublished_partition",
            )
            _require(
                _digest_relation(con, schema, table, version=_digest_version(digest)) == digest,
                "partition_content_mismatch",
            )
            receipt = PublicationReceipt(dataset, generation, published[key], (name,))
            arrow = backend._partition_schema(receipt, name)
            count = con.execute(f"SELECT count(*) FROM {schema}.{_quote(table)}").fetchone()[0]
            partitions[key][name] = digest, arrow, count
        snapshot_manifests = {
            dataset: (generation, snapshot_id)
            for dataset, generation, snapshot_id in query("snapshot_manifests")
        }
        for dataset, generation, version, snapshot_id in query("manifests"):
            _require(
                published.get((dataset, generation)) == version
                and (dataset, generation) in partitions,
                "current_publication_mismatch",
            )
            retained = snapshot_manifests.pop(dataset, None)
            _require(
                (retained is None and snapshot_id is None)
                or (
                    retained is not None
                    and retained[0] == generation
                    and (snapshot_id is None or retained[1] == snapshot_id)
                ),
                "current_snapshot_binding",
            )
        _require(not snapshot_manifests, "orphan_snapshot_manifest")
        for dataset, generation, _ in query("generation_pins"):
            _require(
                (dataset, generation) in partitions or (dataset, generation) in generations,
                "missing_pinned_generation",
            )
        for run_id, (dataset, generation, version) in links.items():
            _require(
                run_id in runs and published.get((dataset, generation)) == version,
                "run_publication_mismatch",
            )
            run = runs[run_id]
            _require(
                run[0] == dataset and (run[2] != "succeeded" or run[3] == generation),
                "run_publication_mismatch",
            )
        results = defaultdict(set)
        for run_id, partition in query("run_results"):
            _require(run_id in runs, "orphan_run_result")
            if runs[run_id][2] == "succeeded":
                _require(
                    run_id in links and partition in partitions[links[run_id][:2]],
                    "missing_run_result",
                )
            # The raw generation can include intermediate partitions that aren't
            # part of the dataset's published result.
            results[run_id].add(partition)
        for dataset, generation, recipe, request_key, manifest_json in query("dataset_recipes"):
            if (dataset, generation) not in partitions:
                raise _INVALID.error(reason="missing_dataset_generation")
            saved, plan = self.saved(recipe, namespace)
            manifest = DatasetManifest.from_json(manifest_json)
            gen = manifest.generation
            _require(
                (gen.dataset_id, gen.generation_id, plan.dataset) == (dataset, generation, dataset),
                "manifest_binding",
            )
            _require(
                manifest.design_hash == saved.design_hash
                and manifest.model_hash == saved.model.model_hash,
                "manifest_plan_binding",
            )
            destination = _json(saved.destination.to_dict()).decode()
            binding = (request_key, destination)
            writer_ids = [
                run_id
                for run_id, run in runs.items()
                if run[4] == request_key
                and run[5] == destination
                and run[2] == "succeeded"
                and run[3] == generation
                and links.get(run_id) == (dataset, generation, published[dataset, generation])
            ]
            _require(len(writer_ids) == 1, "manifest_publication_binding")
            writer = runs[writer_ids[0]]
            _require(
                writer[1] == plan.definition.fingerprint
                and (binding not in requests or requests[binding] == (saved, plan)),
                "manifest_plan_binding",
            )
            snapshot_id = manifest.source_snapshots["snapshot_id"]
            origin = manifest.source_snapshots["owner"]
            _require(
                snapshot_id == plan.snapshot_id == writer[6],
                "manifest_snapshot_binding",
            )
            if snapshot_id not in collected:
                receipt = snapshots.get(snapshot_id)
                receipt_hash = manifest.source_snapshots["receipt_hash"]
                _require(
                    receipt is not None and receipt.receipt_hash == receipt_hash,
                    "manifest_snapshot_binding",
                )
                if receipt is not None and receipt.owner_reference != origin:
                    # A new generation may publish from an imported snapshot. Verify
                    # the exact origin plan and snapshot rather than equating owners.
                    plan_record = self.records.get(_key("plan", namespace, saved.plan_hash))
                    imported_snapshot = (
                        None
                        if self.imported is None
                        else self.imported.match(
                            _key("snapshot", namespace, snapshot_id),
                            origin=receipt.owner_reference,
                            digest=receipt_hash,
                        )
                    )
                    _require(
                        plan_record is not None
                        and plan_record.origin == receipt.owner_reference
                        and imported_snapshot is not None,
                        "manifest_snapshot_binding",
                    )
            stored = partitions[dataset, generation]
            _require(
                len(gen.output_refs) == len(set(gen.output_refs))
                and set(gen.output_refs) == results[writer_ids[0]],
                "manifest_partitions",
            )
            expected = tuple((column.name, column.data_type) for column in manifest.columns)
            for name, digest in zip(gen.output_refs, gen.checksums, strict=True):
                found, arrow, _ = stored[name]
                _require(
                    found == digest and tuple((f.name, str(f.type)) for f in arrow) == expected,
                    "manifest_partition_metadata",
                )
            _require(
                sum(stored[name][2] for name in gen.output_refs) == gen.row_count,
                "manifest_row_count",
            )
            self.record(
                _key("manifest", namespace, dataset, generation),
                origin,
                _hash(manifest_json.encode()),
            )


def backup_owner(state_path: Path, snapshot_root: Path, bundle: Path) -> dict[str, Any]:
    """Copy one closed owner's verified records.

    Args:
        state_path: Source DuckDB file.
        snapshot_root: Configured source snapshot root.
        bundle: New directory for the verified backup.

    Returns:
        The inventory, including its SHA-256 digest.
    """
    owner = RuntimeOwner(state_path, snapshot_root)
    target = _destination(bundle, owner.state_path, owner.snapshot_root)
    files: dict[str, dict] = {}
    try:
        with TemporaryDirectory(prefix=".terrine-backup-", dir=target.parent) as temporary:
            stage = Path(temporary) / "bundle"
            stage.mkdir(mode=0o700)

            def materialize(
                relative: Path, digest: str | None, check: Callable[[bytes], None] | None
            ) -> Path:
                destination = stage / relative
                name = relative.as_posix()
                if name not in files:
                    with _open_regular(owner.snapshot_root, relative.relative_to(_ROOT)) as stream:
                        if check is None:
                            files[name] = _file(stream, destination)
                        else:
                            data = stream.read()
                            check(data)
                            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                            _write(destination, data)
                            files[name] = {"size": len(data), "sha256": _hash(data)}
                if digest is not None:
                    _require(files[name]["sha256"] == digest, "artifact_checksum_mismatch")
                return destination

            with _open_regular(owner.state_path.parent, Path(owner.state_path.name)) as stream:
                original = os.fstat(stream.fileno())
                with _read_only(owner.state_path):
                    _require(os.path.samestat(original, owner.state_path.stat()), "owner_changed")
                    files[_STATE.as_posix()] = _file(stream, stage / _STATE)
                    inventory = _Inventory(owner.reference, _Imports.read(owner), materialize)
                    with _read_only(stage / _STATE) as con:
                        namespaces = inventory.scan(con, owner.snapshot_root)
                    _require(os.path.samestat(original, owner.state_path.stat()), "owner_changed")
            payload = {
                "version": _VERSION,
                "origin": owner.reference,
                "namespaces": namespaces,
                "files": [{"path": name, **info} for name, info in sorted(files.items())],
                "records": {key: asdict(record) for key, record in inventory.records.items()},
            }
            encoded = _json(payload)
            _write(stage / _INVENTORY, encoded)
            _install(stage, target)
    except TerrineError:
        raise
    except (duckdb.Error, OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        raise _INVALID.error(reason="source_invalid_or_in_use") from exc
    return {**payload, "digest": _hash(encoded)}


def restore_owner(bundle: Path, expected_digest: str, destination: Path) -> dict[str, str]:
    """Verify a bundle and atomically install its owner into a new directory.

    Args:
        bundle: Directory produced by backup.
        expected_digest: Trusted SHA-256 digest of the inventory.
        destination: New owner directory.

    Returns:
        The installed state and snapshot locations.
    """
    source = Path(os.path.abspath(bundle.expanduser()))
    _require(_digest(expected_digest), "invalid_expected_digest")
    target = _destination(destination, source)
    try:
        with _open_regular(source, _INVENTORY) as stream:
            encoded = stream.read()
        _require(
            hmac.compare_digest(_hash(encoded), expected_digest), "inventory_checksum_mismatch"
        )
        payload = json.loads(encoded)
        _require(_json(payload) == encoded, "noncanonical_inventory")
        _require(
            type(payload["version"]) is int and payload["version"] == _VERSION,
            "unsupported_inventory_version",
        )
        origin = payload["origin"]
        origin_state, origin_root = _owner_parts(origin)
        _require(
            not any(
                target.is_relative_to(item) or item.is_relative_to(target)
                for item in (origin_state, origin_root)
            ),
            "overlapping_paths",
        )
        # Origin paths are identity strings; never resolve, stat or open them.
        records = {key: _Record(**value) for key, value in payload["records"].items()}
        with TemporaryDirectory(prefix=".terrine-restore-", dir=target.parent) as temporary:
            stage = Path(temporary) / "owner"
            stage.mkdir(mode=0o700)
            files: dict[Path, dict] = {}
            for entry in payload["files"]:
                relative = _relative(entry["path"])
                _require(
                    relative not in files
                    and (relative == _STATE or relative.is_relative_to(_ROOT)),
                    "inventory_path_collision",
                )
                with _open_regular(source, relative) as stream:
                    got = _file(stream, stage / relative)
                _require(
                    got == {"size": entry["size"], "sha256": entry["sha256"]},
                    "file_checksum_mismatch",
                )
                files[relative] = got
            _require(_STATE in files, "state_not_in_inventory")

            def materialize(
                relative: Path, digest: str | None, check: Callable[[bytes], None] | None
            ) -> Path:
                _require(
                    relative in files and (digest is None or files[relative]["sha256"] == digest),
                    "artifact_checksum_mismatch",
                )
                artifact = stage / relative
                if check is not None:
                    check(artifact.read_bytes())
                return artifact

            inventory = _Inventory(origin, _Imports(records), materialize, restoring=True)
            with _read_only(stage / _STATE) as con:
                namespaces = inventory.scan(con, stage / _ROOT)
            _require(
                inventory.artifacts | {_STATE} == files.keys()
                and records == inventory.records
                and namespaces == payload["namespaces"],
                "inventory_scope_mismatch",
            )
            owner = RuntimeOwner(target / _STATE, target / _ROOT)
            imported = {
                "version": _VERSION,
                "destination": owner.reference,
                "records": payload["records"],
            }
            _write(stage / _IMPORT, _json({"payload": imported, "digest": _hash(_json(imported))}))
            _install(stage, target)
    except TerrineError:
        raise
    except (duckdb.Error, OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        raise _INVALID.error(reason="bundle_invalid") from exc
    return {"state_path": str(owner.state_path), "snapshot_root": str(owner.snapshot_root)}
