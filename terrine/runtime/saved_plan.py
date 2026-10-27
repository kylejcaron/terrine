from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

from terrine.errors import RefusalSpec, TerrineError
from terrine.model import Model
from terrine.runtime.materialize import ensure_runtime_expressions_supported
from terrine.runtime.planner import RefreshPlan
from terrine.runtime.state import RuntimeStore
from terrine.spec.dataset import DatasetDefinition
from terrine.spec.design import Design
from terrine.spec.io import canonical_json, from_json, to_json

__all__ = [
    "DestinationRef",
    "ModelReference",
    "ModelResolver",
    "SavedPlan",
    "resolve_plan",
]

_INVALID = RefusalSpec(
    "terrine.runtime.saved_plan_mismatch",
    "Saved plan reconstruction failed verification ({reason}).",
)


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


@dataclass(frozen=True, slots=True)
class DestinationRef:
    database: str
    schema: str
    dataset: str

    def __post_init__(self) -> None:
        if not isinstance(self.database, str) or not self.database.strip():
            raise ValueError("destination database must be non-empty")
        for field in ("schema", "dataset"):
            value = getattr(self, field)
            if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
                raise ValueError(f"destination {field} must be a simple identifier")

    def to_dict(self) -> dict[str, str]:
        return {"database": self.database, "schema": self.schema, "dataset": self.dataset}


@dataclass(frozen=True, slots=True)
class ModelReference:
    owner_key: str
    catalog_hash: str
    model_hash: str
    units_hash: str
    readiness_hash: str
    event_ids_hash: str

    def __post_init__(self) -> None:
        if not all(getattr(self, name) for name in self.__dataclass_fields__):
            raise ValueError("model reference fields must be non-empty")

    @classmethod
    def from_model(cls, model: Model, *, owner_key: str) -> ModelReference:
        if not isinstance(model, Model):
            raise TypeError("model must be Model")
        readiness = {
            name: [coverage.start.isoformat(), coverage.end_exclusive.isoformat()]
            for name, coverage in sorted(model.readiness.sources.items())
        }
        return cls(
            owner_key,
            _digest(model.catalog.model_dump(mode="json")),
            model.model_hash,
            _digest(
                {
                    name: vars(unit)
                    if hasattr(unit, "__dict__")
                    else [
                        unit.table,
                        unit.key,
                        unit.entered_at,
                        unit.exited_at,
                        unit.observed_from,
                        unit.observed_until,
                    ]
                    for name, unit in sorted(model.units.items())
                }
            ),
            _digest(readiness),
            _digest(dict(model.event_ids)),
        )

    def to_dict(self) -> dict[str, str]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


class ModelResolver(Protocol):
    def resolve(self, reference: ModelReference) -> Model: ...


@dataclass(frozen=True, slots=True)
class SavedPlan:
    schema_version: int
    plan_json: str
    design_json: str
    design_hash: str
    model: ModelReference
    destination: DestinationRef
    namespace: str
    expected_base_manifest: str | None
    plan_hash: str
    dataset_definition_json: str | None = None

    @classmethod
    def create(
        cls,
        *,
        plan_json: bytes | str,
        design: Design,
        model: ModelReference,
        destination: DestinationRef,
        namespace: str,
        expected_base_manifest: str | None,
        dataset_definition: DatasetDefinition | None = None,
    ) -> SavedPlan:
        ensure_runtime_expressions_supported(design)
        if not isinstance(model, ModelReference) or not isinstance(destination, DestinationRef):
            raise TypeError("model and destination must be verified references")
        if not namespace:
            raise ValueError("namespace must be non-empty")
        try:
            parsed_plan = json.loads(plan_json)
        except (TypeError, ValueError) as exc:
            raise ValueError("plan_json must contain a JSON plan") from exc
        canonical_plan = json.dumps(parsed_plan, sort_keys=True, separators=(",", ":"))
        design_json = to_json(design)
        design_hash = hashlib.sha256(canonical_json(design)).hexdigest()
        if dataset_definition is not None and (
            not isinstance(dataset_definition, DatasetDefinition)
            or canonical_json(dataset_definition.base_design) != canonical_json(design)
        ):
            raise TypeError("dataset_definition must describe the saved Design")
        dataset_definition_json = (
            dataset_definition.canonical_json().decode() if dataset_definition is not None else None
        )
        provisional = cls(
            1,
            canonical_plan,
            design_json,
            design_hash,
            model,
            destination,
            namespace,
            expected_base_manifest,
            "",
            dataset_definition_json,
        )
        plan_hash = hashlib.sha256(_canonical(provisional._data(include_hash=False))).hexdigest()
        return cls(
            provisional.schema_version,
            provisional.plan_json,
            provisional.design_json,
            provisional.design_hash,
            provisional.model,
            provisional.destination,
            provisional.namespace,
            provisional.expected_base_manifest,
            plan_hash,
            dataset_definition_json,
        )

    def _data(self, *, include_hash: bool) -> dict[str, Any]:
        data = {
            "schema_version": self.schema_version,
            "plan_json": self.plan_json,
            "design_json": self.design_json,
            "design_hash": self.design_hash,
            "model": self.model.to_dict(),
            "destination": self.destination.to_dict(),
            "namespace": self.namespace,
            "expected_base_manifest": self.expected_base_manifest,
        }
        if self.dataset_definition_json is not None:
            data["dataset_definition_json"] = self.dataset_definition_json
        if include_hash:
            data["plan_hash"] = self.plan_hash
        return data

    def to_json(self) -> str:
        return json.dumps(self._data(include_hash=True), sort_keys=True, separators=(",", ":"))

    def restore_dataset_definition(self) -> DatasetDefinition | None:
        """Return the validated complete dataset construction definition, if present."""
        if self.dataset_definition_json is None:
            return None
        return DatasetDefinition.from_json(self.dataset_definition_json)

    def pin_snapshot(
        self,
        store: RuntimeStore,
        *,
        reference: str | None = None,
        _in_transaction: bool = False,
    ) -> str:
        """Retain this verified saved plan's exact input snapshot."""
        verified = SavedPlan.from_json(self.to_json())
        plan = RefreshPlan.deserialize(verified.plan_json)
        if (
            verified.namespace != store.namespace
            or plan.namespace != verified.namespace
            or plan.dataset != verified.destination.dataset
        ):
            raise _INVALID.error(reason="plan identity or store namespace differs")
        store.pin_snapshot(
            plan.snapshot_id,
            kind="saved_plan",
            reference=reference or verified.plan_hash,
            _in_transaction=_in_transaction,
        )
        return plan.snapshot_id

    def release_snapshot(self, store: RuntimeStore, *, reference: str | None = None) -> str:
        """Release this saved plan's input snapshot reference."""
        verified = SavedPlan.from_json(self.to_json())
        plan = RefreshPlan.deserialize(verified.plan_json)
        if verified.namespace != store.namespace or plan.namespace != verified.namespace:
            raise _INVALID.error(reason="plan identity or store namespace differs")
        store.unpin_snapshot(
            plan.snapshot_id,
            kind="saved_plan",
            reference=reference or verified.plan_hash,
        )
        return plan.snapshot_id

    @classmethod
    def from_json(cls, value: str | bytes) -> SavedPlan:
        try:
            data = json.loads(value)
            required = {
                "schema_version",
                "plan_json",
                "design_json",
                "design_hash",
                "model",
                "destination",
                "namespace",
                "expected_base_manifest",
                "plan_hash",
            }
            if not isinstance(data, dict) or set(data) - {"dataset_definition_json"} != required:
                raise _INVALID.error(reason="saved plan fields differ")
            model = ModelReference(**data["model"])
            destination = DestinationRef(**data["destination"])
            saved_plan = cls(
                data["schema_version"],
                data["plan_json"],
                data["design_json"],
                data["design_hash"],
                model,
                destination,
                data["namespace"],
                data["expected_base_manifest"],
                data["plan_hash"],
                data.get("dataset_definition_json"),
            )
        except (ValueError, TypeError, KeyError) as exc:
            raise _INVALID.error(reason="saved plan is malformed") from exc
        if saved_plan.schema_version != 1:
            raise _INVALID.error(reason="unsupported schema version")
        expected = hashlib.sha256(_canonical(saved_plan._data(include_hash=False))).hexdigest()
        if not isinstance(saved_plan.plan_hash, str) or not hmac.compare_digest(
            saved_plan.plan_hash, expected
        ):
            raise _INVALID.error(reason="saved plan checksum mismatch")
        try:
            RefreshPlan.deserialize(saved_plan.plan_json)
            design = from_json(saved_plan.design_json)
        except TerrineError:
            raise
        except (ValueError, TypeError) as exc:
            raise _INVALID.error(reason="plan or Design payload is invalid") from exc
        if hashlib.sha256(canonical_json(design)).hexdigest() != saved_plan.design_hash:
            raise _INVALID.error(reason="Design hash mismatch")
        if saved_plan.dataset_definition_json is not None:
            try:
                dataset_definition = DatasetDefinition.from_json(saved_plan.dataset_definition_json)
            except (ValueError, TypeError) as exc:
                raise _INVALID.error(reason="dataset definition is invalid") from exc
            if canonical_json(dataset_definition.base_design) != canonical_json(design):
                raise _INVALID.error(reason="dataset definition Design differs")
        return saved_plan


def _bind_saved_plan(saved_plan: SavedPlan, *, namespace: str) -> RefreshPlan:
    restored = SavedPlan.from_json(saved_plan.to_json())
    if namespace != restored.namespace:
        raise _INVALID.error(reason="owner namespace mismatch")
    plan = RefreshPlan.deserialize(restored.plan_json)
    if (
        plan.namespace != restored.namespace
        or plan.definition_hash != restored.design_hash
        or plan.dataset != restored.destination.dataset
        or not plan.snapshot_id
    ):
        raise _INVALID.error(reason="plan identity differs from owner-bound references")
    return plan.bind_base_manifest(
        restored.expected_base_manifest, reconstruction_hash=restored.plan_hash
    )


def resolve_plan(
    saved_plan: SavedPlan, *, resolver: ModelResolver, namespace: str
) -> tuple[RefreshPlan, Model, Design | DatasetDefinition, DestinationRef]:
    restored = SavedPlan.from_json(saved_plan.to_json())
    if namespace != restored.namespace:
        raise _INVALID.error(reason="owner namespace mismatch")
    model = resolver.resolve(restored.model)
    if (
        not isinstance(model, Model)
        or ModelReference.from_model(model, owner_key=restored.model.owner_key) != restored.model
    ):
        raise _INVALID.error(reason="owner Model declaration does not match reference")
    plan = _bind_saved_plan(restored, namespace=namespace)
    design = from_json(restored.design_json)
    return plan, model, restored.restore_dataset_definition() or design, restored.destination
