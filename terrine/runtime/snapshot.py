from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import shutil
import uuid
from collections.abc import Callable, Generator
from contextlib import ExitStack, closing, contextmanager
from dataclasses import asdict, dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO

from terrine.data import Budget, UnitSource
from terrine.errors import RefusalSpec, TerrineError
from terrine.execution._metadata import metadata_operation
from terrine.execution.inputs import (
    ExecutionContext,
    PublishedInputs,
    _bind_native_inputs,
    _capture_native_inputs,
    _catalog_hash,
    _native,
    open_execution,
    verify_immutable_inputs,
)
from terrine.model import Model
from terrine.runtime._relocation import _ROOT, _Imports, _key, _open_regular, _relative
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.schema import check_runtime_schema
from terrine.runtime.state import RuntimeStore
from terrine.semantic.io import catalog_payload, validate_catalog
from terrine.spec._capture import _design_root_names as _design_root_names
from terrine.spec.design import Design

__all__ = ["ReopenableSnapshot", "SnapshotAuthority"]

_INVALID = RefusalSpec(
    "terrine.runtime.snapshot_invalid",
    "Runtime snapshot receipt is invalid: {reason}.",
)
_INVALID_CONFIGURATION = RefusalSpec(
    "terrine.runtime.invalid_configuration",
    "Owner runtime configuration {field} is invalid: {reason}.",
)

_BUDGET_UNKNOWN = RefusalSpec(
    "terrine.budget.scan_unknown",
    "A strict scan-byte budget requires a verified scan bound; this backend has none. "
    "Explicitly choose non-strict scan policy after reviewing upstream work.",
)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _file_hash(path: Path, *, copy_to: BinaryIO | None = None) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            if copy_to is not None:
                copy_to.write(chunk)
            digest.update(chunk)
    return digest.hexdigest()


def _assert_owner_connection(owner: RuntimeOwner, con: Any) -> None:
    try:
        raw = _native.raw_connection(con)
        with _native.connection_lock(raw):
            rows = raw.execute("PRAGMA database_list").fetchall()
            current_catalog = raw.execute("SELECT current_database()").fetchone()[0]
        database_path = next(
            location for _sequence, name, location in rows if name == current_catalog and location
        )
    except Exception as exc:
        raise _INVALID.error(reason="owner database identity is unavailable") from exc
    if Path(database_path).resolve() != owner.state_path:
        raise _INVALID.error(reason="connection does not belong to configured RuntimeOwner")


@dataclass(frozen=True, slots=True)
class ReopenableSnapshot:
    schema_version: int
    snapshot_id: str
    namespace: str
    run_id: str
    owner_reference: str
    artifact_path: str
    receipt_path: str
    catalog_json: str
    units_json: str
    event_ids_json: str
    model_hash: str
    roots: tuple[dict[str, Any], ...]
    artifact_hash: str
    receipt_hash: str

    def to_json(self) -> str:
        return json.dumps(self._data(include_hash=True), sort_keys=True, separators=(",", ":"))

    def _data(self, *, include_hash: bool) -> dict[str, Any]:
        data = {
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "namespace": self.namespace,
            "run_id": self.run_id,
            "owner_reference": self.owner_reference,
            "artifact_path": self.artifact_path,
            "receipt_path": self.receipt_path,
            "catalog_json": self.catalog_json,
            "units_json": self.units_json,
            "event_ids_json": self.event_ids_json,
            "model_hash": self.model_hash,
            "roots": list(self.roots),
            "artifact_hash": self.artifact_hash,
        }
        if include_hash:
            data["receipt_hash"] = self.receipt_hash
        return data

    @classmethod
    def from_json(cls, value: str | bytes) -> ReopenableSnapshot:
        try:
            data = json.loads(value)
            fields = {name: data[name] for name in cls.__dataclass_fields__}
            snapshot = cls(**fields)
        except (ValueError, TypeError, KeyError) as exc:
            raise _INVALID.error(reason="receipt is malformed") from exc
        if snapshot.schema_version != 1 or not snapshot.snapshot_id or not snapshot.namespace:
            raise _INVALID.error(reason="receipt version or identity is invalid")
        expected = hashlib.sha256(_canonical(snapshot._data(include_hash=False))).hexdigest()
        if not isinstance(snapshot.receipt_hash, str) or not hmac.compare_digest(
            snapshot.receipt_hash, expected
        ):
            raise _INVALID.error(reason="receipt checksum does not match")
        return snapshot


class SnapshotAuthority:
    """Capture immutable execution inputs in an owner-retained local artifact."""

    def __init__(self, owner: RuntimeOwner) -> None:
        if not isinstance(owner, RuntimeOwner):
            raise _INVALID_CONFIGURATION.error(reason="wrong_type", field="owner")
        self.owner = owner

    def find_retained(self, snapshot_id: str, *, namespace: str) -> ReopenableSnapshot:
        if not snapshot_id or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
            raise ValueError("snapshot_id and a simple namespace are required")
        imported = self._imported_snapshot(snapshot_id, namespace)
        if imported is not None:
            return imported[0]
        root = self.owner.snapshot_root / namespace
        if root.exists():
            for directory in root.iterdir():
                receipt_path = directory / "receipt.json"
                if not directory.is_dir() or not receipt_path.is_file():
                    continue
                try:
                    receipt = ReopenableSnapshot.from_json(receipt_path.read_bytes())
                except (OSError, ValueError):
                    continue
                if (
                    receipt.snapshot_id == snapshot_id
                    and receipt.namespace == namespace
                    and receipt.owner_reference == self.owner.reference
                    and Path(receipt.receipt_path).resolve() == receipt_path.resolve()
                    and Path(receipt.artifact_path).resolve().parent == directory.resolve()
                ):
                    return receipt
        raise _INVALID.error(reason="retained receipt is absent or does not match owner")

    def collect_unreferenced(
        self,
        store: RuntimeStore,
        *,
        namespace: str,
        retain: timedelta,
        now: datetime | None = None,
    ) -> tuple[str, ...]:
        """Delete only verified owner snapshots beyond retention with no live references."""
        if store.namespace != namespace:
            raise ValueError("snapshot namespace does not match runtime store")
        _assert_owner_connection(self.owner, store.con)
        store.release_terminal_run_snapshot_references()
        if retain < timedelta(0):
            raise ValueError("snapshot retention must not be negative")
        root = self.owner.snapshot_root / namespace
        if not root.exists():
            return ()
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("collection time must be timezone-aware")
        removed = []
        for directory in root.iterdir():
            if directory.is_symlink() or not directory.is_dir():
                continue
            receipt_path = directory / "receipt.json"
            if not receipt_path.is_file():
                continue
            receipt = ReopenableSnapshot.from_json(receipt_path.read_bytes())
            if receipt.namespace != namespace:
                continue
            marked = store.snapshot_marked_for_gc(receipt.snapshot_id)
            if receipt.owner_reference == self.owner.reference:
                if Path(receipt.artifact_path).resolve().parent != directory.resolve():
                    continue
                if Path(receipt.receipt_path).resolve() != receipt_path.resolve():
                    continue
            else:
                imported = self._imported_snapshot(
                    receipt.snapshot_id, namespace, allow_missing_input=marked
                )
                if imported is None or (
                    imported[0].receipt_hash != receipt.receipt_hash
                    or imported[1].parent != directory
                ):
                    continue
            modified = datetime.fromtimestamp(directory.stat().st_mtime, UTC)
            if not marked and current - modified < retain:
                continue
            if not marked and not store.mark_snapshot_for_gc(receipt.snapshot_id):
                continue
            shutil.rmtree(directory)
            removed.append(receipt.snapshot_id)
        return tuple(sorted(removed))

    def release(
        self,
        snapshot: ReopenableSnapshot,
        store: RuntimeStore,
        *,
        kind: str,
        reference: str,
    ) -> None:
        """Release one verified owner reference; physical deletion remains GC-only."""
        self._retained_artifact(snapshot, store.namespace)
        verified = snapshot
        _assert_owner_connection(self.owner, store.con)
        store.unpin_snapshot(verified.snapshot_id, kind=kind, reference=reference)

    def capture(
        self,
        model: Model,
        *,
        design: Design,
        con: Any,
        namespace: str,
        run_id: str,
        budget: Budget,
    ) -> ReopenableSnapshot:
        if not isinstance(model, Model):
            raise TypeError("model must be Model")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
            raise ValueError("namespace must be a simple identifier")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", run_id):
            raise ValueError("run_id must be a safe owner-local identifier")
        if not isinstance(budget, Budget):
            raise TypeError("budget must be Budget")
        if budget.max_scan_bytes is not None and budget.strict_scan:
            raise _BUDGET_UNKNOWN.error()
        if not isinstance(design, Design):
            raise TypeError("design must be Design")
        from terrine.execution.backend import backend_provider

        backend_provider(model.con, engine=model._engine).capabilities.require(
            "retained_snapshot",
            backend=model.catalog.dialect,
            route="Materialize a Frame without saving it, or use verified retained local inputs.",
        )
        _assert_owner_connection(self.owner, con)
        check_runtime_schema(_native.raw_connection(con), namespace)
        snapshot_id = uuid.uuid4().hex
        directory = self.owner.snapshot_root / namespace / f"{run_id}-{snapshot_id}"
        source_names, dimension_names, unit_names = _design_root_names(model, design)
        published = _capture_native_inputs(
            model.catalog,
            con=con,
            units=model.units,
            budget=budget,
            retained_path=directory,
            source_names=source_names,
            dimension_names=dimension_names,
            unit_names=unit_names,
        )
        try:
            entries = []
            issuer = published._issuer
            for (kind, relation_key), storage_name in sorted(issuer.root_names.items()):
                identity = issuer.root_identities[(kind, relation_key)]
                entries.append(
                    {
                        "kind": kind,
                        "relation_key": relation_key,
                        "storage_name": storage_name,
                        "identity": {
                            "kind": identity.kind,
                            "catalog": identity.catalog,
                            "schema": identity.schema,
                            "name": identity.name,
                            "definition_hash": identity.definition_hash,
                        },
                    }
                )
            catalog_json = json.dumps(
                catalog_payload(model.catalog), sort_keys=True, separators=(",", ":")
            )
            units_json = json.dumps(
                {name: asdict(model.units[name]) for name in sorted(unit_names)},
                sort_keys=True,
                separators=(",", ":"),
            )
            event_ids_json = json.dumps(
                {name: column for name, column in model.event_ids.items() if name in source_names},
                sort_keys=True,
                separators=(",", ":"),
            )
            artifact = directory / "inputs.duckdb"
            receipt_path = directory / "receipt.json"
            provisional = ReopenableSnapshot(
                1,
                snapshot_id,
                namespace,
                run_id,
                self.owner.reference,
                str(artifact),
                str(receipt_path),
                catalog_json,
                units_json,
                event_ids_json,
                model.model_hash,
                tuple(entries),
                _file_hash(artifact),
                "",
            )
            receipt_hash = hashlib.sha256(
                _canonical(provisional._data(include_hash=False))
            ).hexdigest()
            receipt = replace(provisional, receipt_hash=receipt_hash)
            temporary_receipt = receipt_path.with_suffix(".json.tmp")
            temporary_receipt.write_text(receipt.to_json(), encoding="utf-8")
            temporary_receipt.replace(receipt_path)
            store = RuntimeStore(_native.raw_connection(con), namespace=namespace)
            store.initialize()
            store.con.execute(
                f"INSERT INTO {store._schema}.runtime_snapshot_captures VALUES (?, ?, ?)",
                [
                    snapshot_id,
                    json.dumps(
                        {
                            "capture": {
                                "snapshot_id": snapshot_id,
                                "artifact_bytes": artifact.stat().st_size,
                                "root_count": len(entries),
                                "scan_bytes": None,
                            }
                        },
                        sort_keys=True,
                    ),
                    json.dumps(
                        {
                            name: {
                                "start": model.readiness.for_fact(name).start.isoformat(),
                                "end_exclusive": model.readiness.for_fact(
                                    name
                                ).end_exclusive.isoformat(),
                            }
                            for name in sorted(source_names)
                        },
                        sort_keys=True,
                    ),
                ],
            )
            return receipt
        finally:
            published.close()

    @metadata_operation()
    def reopen(self, snapshot: ReopenableSnapshot, *, con: Any, namespace: str) -> _SnapshotContext:
        import duckdb

        restored, artifact = self._retained_artifact(snapshot, namespace)
        if restored.owner_reference != self.owner.reference:
            _assert_owner_connection(self.owner, con)
        if not artifact.is_file() or _file_hash(artifact) != restored.artifact_hash:
            raise _INVALID.error(reason="retained artifact is absent or changed")
        catalog, units = _retained_declarations(restored)
        raw = duckdb.connect(str(artifact), read_only=True)
        return _open_snapshot_context(restored, catalog, units, raw)

    @contextmanager
    def _run_image(
        self, snapshot: ReopenableSnapshot, *, con: Any, namespace: str
    ) -> Generator[Callable[[], _SnapshotContext]]:
        """Retain one detached, read-only database backing for this attempt."""
        import duckdb

        if os.name == "posix":
            restored, artifact = self._retained_artifact(snapshot, namespace)
            if restored.owner_reference != self.owner.reference:
                _assert_owner_connection(self.owner, con)
            with ExitStack() as resources:
                try:
                    private = resources.enter_context(TemporaryDirectory(prefix="terrine-run-"))
                    image = Path(private) / "inputs.duckdb"
                    with image.open("xb") as stream:
                        digest = _file_hash(artifact, copy_to=stream)
                except OSError:
                    # No private image: keep checking the retained artifact at every open.
                    pass
                else:
                    if digest != restored.artifact_hash:
                        raise _INVALID.error(reason="retained artifact is absent or changed")
                    catalog, units = _retained_declarations(restored)
                    raw = resources.enter_context(
                        closing(duckdb.connect(str(image), read_only=True))
                    )
                    try:
                        image.unlink()
                    except OSError:
                        pass
                    else:
                        # The hashed bytes were written to a private copy. Its only
                        # remaining handle is read-only, and cursors have separate temp state.
                        yield lambda: _open_snapshot_context(restored, catalog, units, raw.cursor())
                        return
        yield partial(self.reopen, snapshot, con=con, namespace=namespace)

    def _retained_artifact(
        self, snapshot: ReopenableSnapshot, namespace: str
    ) -> tuple[ReopenableSnapshot, Path]:
        if not isinstance(snapshot, ReopenableSnapshot):
            raise TypeError("snapshot must be ReopenableSnapshot")
        restored = ReopenableSnapshot.from_json(snapshot.to_json())
        if namespace != restored.namespace:
            raise _INVALID.error(reason="owner namespace or identity changed")
        if self.owner.reference != restored.owner_reference:
            imported = self._imported_snapshot(restored.snapshot_id, namespace)
            if imported is None or imported[0].receipt_hash != restored.receipt_hash:
                raise _INVALID.error(reason="owner namespace or identity changed")
            return imported
        receipt_path = Path(restored.receipt_path).resolve()
        try:
            receipt_path.relative_to(self.owner.snapshot_root)
            on_disk = ReopenableSnapshot.from_json(receipt_path.read_bytes())
        except (OSError, ValueError) as exc:
            raise _INVALID.error(reason="retained receipt is absent or invalid") from exc
        if on_disk.receipt_hash != restored.receipt_hash:
            raise _INVALID.error(reason="retained receipt differs from supplied identity")
        artifact = Path(restored.artifact_path).resolve()
        try:
            artifact.relative_to(self.owner.snapshot_root)
        except ValueError as exc:
            raise _INVALID.error(reason="snapshot artifact is outside the owner root") from exc
        return restored, artifact

    def _imported_snapshot(
        self, snapshot_id: str, namespace: str, *, allow_missing_input: bool = False
    ) -> tuple[ReopenableSnapshot, Path] | None:
        imports = _Imports.read(self.owner)
        record = imports.records.get(_key("snapshot", namespace, snapshot_id)) if imports else None
        if record is None or record.path is None:
            return None
        relative = _relative(record.path).relative_to(_ROOT)
        try:
            with _open_regular(self.owner.snapshot_root, relative) as stream:
                receipt = ReopenableSnapshot.from_json(stream.read())
            artifact = relative.with_name("inputs.duckdb")
            try:
                with _open_regular(self.owner.snapshot_root, artifact):
                    pass
            except FileNotFoundError:
                if not allow_missing_input:
                    raise
        except OSError as exc:
            raise _INVALID.error(reason="imported snapshot files are absent or invalid") from exc
        if (
            receipt.snapshot_id != snapshot_id
            or receipt.namespace != namespace
            or receipt.owner_reference != record.origin
            or receipt.receipt_hash != record.digest
        ):
            raise _INVALID.error(reason="imported snapshot identity changed")
        return receipt, self.owner.snapshot_root / artifact


def _retained_declarations(restored: ReopenableSnapshot) -> tuple[Any, dict[str, UnitSource]]:
    try:
        catalog = validate_catalog(json.loads(restored.catalog_json))
    except ValueError as exc:
        raise _INVALID.error(reason="source declarations cannot be reconstructed") from exc
    try:
        units = {name: UnitSource(**unit) for name, unit in json.loads(restored.units_json).items()}
        event_ids = json.loads(restored.event_ids_json)
        if not isinstance(event_ids, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in event_ids.items()
        ):
            raise ValueError("event ID declarations are malformed")
    except TerrineError:
        raise
    except (ValueError, TypeError, KeyError) as exc:
        raise _INVALID.error(reason="source declarations cannot be reconstructed") from exc
    if (
        _catalog_hash(catalog)
        != hashlib.sha256(_canonical(json.loads(restored.catalog_json))).hexdigest()
    ):
        raise _INVALID.error(reason="Catalog declaration is not canonical")
    return catalog, units


@metadata_operation()
def _open_snapshot_context(
    restored: ReopenableSnapshot, catalog: Any, units: dict[str, UnitSource], raw: Any
) -> _SnapshotContext:
    published = _bind_native_inputs(catalog, units=units, raw=raw, roots=restored.roots)
    try:
        binding = verify_immutable_inputs(
            catalog, published.con, units=units, relations=published.relations
        )
        manager = open_execution(catalog, published.con, units=units, input_binding=binding)
        context = manager.__enter__()
    except BaseException:
        published.close()
        raise
    context.snapshot = replace(
        context.snapshot,
        snapshot_id=restored.snapshot_id,
        input_binding_id=restored.snapshot_id,
        source_generations={
            key: hashlib.sha256(f"{restored.artifact_hash}:{key}".encode()).hexdigest()
            for key in context.snapshot.source_generations
        },
        replay_equivalence="verified",
    )
    return _SnapshotContext(context, manager, published)


class _SnapshotContext(ExecutionContext):
    __slots__ = ("_manager", "_published")

    def __init__(self, context: ExecutionContext, manager: Any, published: PublishedInputs) -> None:
        # Every field carries over, admission and preflight included, so a
        # retained read is admitted exactly as a direct one.
        super().__init__(**{item.name: getattr(context, item.name) for item in fields(context)})
        self._manager = manager
        self._published = published

    def __enter__(self) -> _SnapshotContext:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        manager, self._manager = self._manager, None
        try:
            if manager is not None:
                manager.__exit__(None, None, None)
        finally:
            published, self._published = self._published, None
            if published is not None:
                published.close()
