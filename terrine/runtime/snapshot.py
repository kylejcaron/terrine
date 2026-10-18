from __future__ import annotations

import hashlib
import hmac
import json
import re
import uuid
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

from terrine.data import Budget, UnitSource
from terrine.errors import RefusalSpec
from terrine.execution.inputs import (
    ExecutionContext,
    PublishedInputs,
    _capture_native_inputs,
    _catalog_hash,
    _native,
    _reopen_native_inputs,
    open_execution,
    verify_immutable_inputs,
)
from terrine.model import Model
from terrine.runtime.owner import RuntimeOwner
from terrine.semantic.models import Catalog

__all__ = ["ReopenableSnapshot", "SnapshotAuthority"]

_INVALID = RefusalSpec(
    "terrine.runtime.snapshot_invalid",
    "Runtime snapshot receipt is invalid: {reason}.",
)

_BUDGET_UNKNOWN = RefusalSpec(
    "terrine.budget.scan_unknown",
    "A strict scan-byte budget requires a verified scan bound; this backend has none. "
    "Explicitly choose non-strict scan policy after reviewing upstream work.",
)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assert_owner_connection(owner: RuntimeOwner, con: Any) -> None:
    try:
        raw = _native.raw_connection(con)
        with _native.connection_lock(raw):
            rows = raw.execute("PRAGMA database_list").fetchall()
        database_path = next(
            location
            for _sequence, _name, location in rows
            if location and Path(location).resolve() == owner.state_path
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
            raise TypeError("owner must be RuntimeOwner")
        self.owner = owner

    def capture(
        self,
        model: Model,
        *,
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
        _assert_owner_connection(self.owner, con)
        snapshot_id = uuid.uuid4().hex
        directory = self.owner.snapshot_root / namespace / f"{run_id}-{snapshot_id}"
        published = _capture_native_inputs(
            model.catalog,
            con=con,
            units=model.units,
            budget=budget,
            retained_path=directory,
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
                model.catalog.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
            )
            units_json = json.dumps(
                {name: asdict(unit) for name, unit in sorted(model.units.items())},
                sort_keys=True,
                separators=(",", ":"),
            )
            event_ids_json = json.dumps(
                dict(model.event_ids), sort_keys=True, separators=(",", ":")
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
            return receipt
        finally:
            published.close()

    def reopen(self, snapshot: ReopenableSnapshot, *, con: Any, namespace: str) -> ExecutionContext:
        if not isinstance(snapshot, ReopenableSnapshot):
            raise TypeError("snapshot must be ReopenableSnapshot")
        restored = ReopenableSnapshot.from_json(snapshot.to_json())
        if namespace != restored.namespace or self.owner.reference != restored.owner_reference:
            raise _INVALID.error(reason="owner namespace or identity changed")
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
        if not artifact.is_file() or _file_hash(artifact) != restored.artifact_hash:
            raise _INVALID.error(reason="retained artifact is absent or changed")
        try:
            catalog = Catalog.model_validate(json.loads(restored.catalog_json))
            units = {
                name: UnitSource(**unit) for name, unit in json.loads(restored.units_json).items()
            }
            event_ids = json.loads(restored.event_ids_json)
            if not isinstance(event_ids, dict) or not all(
                isinstance(key, str) and isinstance(value, str) for key, value in event_ids.items()
            ):
                raise ValueError("event ID declarations are malformed")
        except (ValueError, TypeError, KeyError) as exc:
            raise _INVALID.error(reason="source declarations cannot be reconstructed") from exc
        if (
            _catalog_hash(catalog)
            != hashlib.sha256(_canonical(json.loads(restored.catalog_json))).hexdigest()
        ):
            raise _INVALID.error(reason="Catalog declaration is not canonical")
        published = _reopen_native_inputs(
            catalog,
            units=units,
            path=artifact,
            roots=restored.roots,
        )
        binding = verify_immutable_inputs(
            catalog, published.con, units=units, relations=published.relations
        )
        manager = open_execution(catalog, published.con, units=units, input_binding=binding)
        try:
            context = manager.__enter__()
        except Exception:
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
        super().__init__(
            context.con, context.snapshot, context._resolver, context._raw, context._revalidate
        )
        self._manager = manager
        self._published = published

    def close(self) -> None:
        manager, self._manager = self._manager, None
        if manager is not None:
            manager.__exit__(None, None, None)
        published, self._published = self._published, None
        if published is not None:
            published.close()
