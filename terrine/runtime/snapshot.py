from __future__ import annotations

import hashlib
import hmac
import json
import re
import shutil
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields, is_dataclass, replace
from datetime import UTC, datetime, timedelta
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
from terrine.runtime.state import RuntimeStore
from terrine.semantic.models import Catalog
from terrine.spec.design import Design
from terrine.spec.expressions import AttributeAsOf, EventTime, MetricAgg, MetricQuantile

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
            raise TypeError("owner must be RuntimeOwner")
        self.owner = owner

    def find_retained(self, snapshot_id: str, *, namespace: str) -> ReopenableSnapshot:
        if not snapshot_id or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
            raise ValueError("snapshot_id and a simple namespace are required")
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
            if not directory.is_dir():
                continue
            receipt_path = directory / "receipt.json"
            if not receipt_path.is_file():
                continue
            receipt = ReopenableSnapshot.from_json(receipt_path.read_bytes())
            if receipt.namespace != namespace or receipt.owner_reference != self.owner.reference:
                continue
            if Path(receipt.artifact_path).resolve().parent != directory.resolve():
                continue
            if Path(receipt.receipt_path).resolve() != receipt_path.resolve():
                continue
            marked = store.snapshot_marked_for_gc(receipt.snapshot_id)
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
        verified = ReopenableSnapshot.from_json(snapshot.to_json())
        if (
            verified.owner_reference != self.owner.reference
            or verified.namespace != store.namespace
            or not Path(verified.receipt_path).is_file()
        ):
            raise _INVALID.error(reason="snapshot release identity does not match owner")
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
        _assert_owner_connection(self.owner, con)
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
                model.catalog.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
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


def _design_root_names(
    model: Model, design: Design
) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    from terrine.spec.validate import validate_design

    sources: set[str] = (
        set(validate_design(design, model).required_sources)
        if design.unit in model.units
        else set()
    )
    properties: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, EventTime):
            fact = model.catalog.facts.get(value.fact)
            if fact is not None:
                sources.add(fact.source)
            if value.label_property is not None:
                properties.add(value.label_property)
            if value.relative_to is not None:
                visit(value.relative_to)
        elif isinstance(value, AttributeAsOf):
            properties.add(value.attribute)
            if value.relative_to is not None:
                visit(value.relative_to)
        elif isinstance(value, (MetricAgg, MetricQuantile)):
            metric = model.catalog.metrics.get(value.metric)
            if metric is not None:
                for measure_name in (metric.measure, metric.numerator, metric.denominator):
                    if measure_name is None:
                        continue
                    measure = model.catalog.measures.get(measure_name)
                    if measure is not None:
                        fact = model.catalog.facts.get(measure.fact)
                        if fact is not None:
                            sources.add(fact.source)
        elif isinstance(value, Mapping):
            for child in value.values():
                visit(child)
        elif isinstance(value, (tuple, list)):
            for child in value:
                visit(child)
        elif is_dataclass(value) and not isinstance(value, type):
            for item in fields(value):
                visit(getattr(value, item.name))

    for role in ("features", "outcomes", "eligibility"):
        visit(getattr(design, role, {}))
    visit(design.anchor)
    visit(design.treatment)
    visit(design.shape)
    dimension_names: set[str] = set()
    for property_name in properties:
        prop = model.catalog.properties.get(property_name)
        if prop is None:
            continue
        sources.add(prop.source)
        if prop.dimension is not None:
            dimension_names.add(prop.dimension)
    for source_name in sources:
        source = model.catalog.sources.get(source_name)
        if source is not None:
            dimension_names.update(source.dimensions)
    unit_names = {design.unit} if design.unit in model.units else set()
    return frozenset(sources), frozenset(dimension_names), frozenset(unit_names)
