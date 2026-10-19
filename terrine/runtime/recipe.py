from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass, replace
from typing import Any, Protocol

from terrine.errors import RefusalSpec, TerrineError
from terrine.model import Model
from terrine.runtime.planner import RefreshPlan
from terrine.runtime.state import RuntimeStore
from terrine.spec.dataset import DatasetRecipe
from terrine.spec.design import Design
from terrine.spec.io import canonical_json, from_json, to_json

__all__ = [
    "DestinationRef",
    "ModelReference",
    "ModelResolver",
    "SavedPlanRecipe",
    "resolve_plan",
]

_INVALID = RefusalSpec(
    "terrine.runtime.plan_recipe_mismatch",
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
class SavedPlanRecipe:
    schema_version: int
    plan_json: str
    design_json: str
    design_hash: str
    model: ModelReference
    destination: DestinationRef
    namespace: str
    expected_base_manifest: str | None
    recipe_hash: str
    dataset_recipe_json: str | None = None

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
        dataset_recipe: DatasetRecipe | None = None,
    ) -> SavedPlanRecipe:
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
        if dataset_recipe is not None and (
            not isinstance(dataset_recipe, DatasetRecipe)
            or canonical_json(dataset_recipe.base_design) != canonical_json(design)
        ):
            raise TypeError("dataset_recipe must describe the saved Design")
        dataset_recipe_json = (
            dataset_recipe.canonical_json().decode() if dataset_recipe is not None else None
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
            dataset_recipe_json,
        )
        recipe_hash = hashlib.sha256(_canonical(provisional._data(include_hash=False))).hexdigest()
        return cls(
            provisional.schema_version,
            provisional.plan_json,
            provisional.design_json,
            provisional.design_hash,
            provisional.model,
            provisional.destination,
            provisional.namespace,
            provisional.expected_base_manifest,
            recipe_hash,
            dataset_recipe_json,
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
        if self.dataset_recipe_json is not None:
            data["dataset_recipe_json"] = self.dataset_recipe_json
        if include_hash:
            data["recipe_hash"] = self.recipe_hash
        return data

    def to_json(self) -> str:
        return json.dumps(self._data(include_hash=True), sort_keys=True, separators=(",", ":"))

    def restore_dataset_recipe(self) -> DatasetRecipe | None:
        """Return the validated complete dataset construction recipe, if present."""
        if self.dataset_recipe_json is None:
            return None
        return DatasetRecipe.from_json(self.dataset_recipe_json)

    def pin_snapshot(self, store: RuntimeStore, *, reference: str | None = None) -> str:
        """Retain this verified recipe's exact input snapshot."""
        verified = SavedPlanRecipe.from_json(self.to_json())
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
            reference=reference or verified.recipe_hash,
        )
        return plan.snapshot_id

    def release_snapshot(self, store: RuntimeStore, *, reference: str | None = None) -> str:
        """Release this recipe's input snapshot reference."""
        verified = SavedPlanRecipe.from_json(self.to_json())
        plan = RefreshPlan.deserialize(verified.plan_json)
        if verified.namespace != store.namespace or plan.namespace != verified.namespace:
            raise _INVALID.error(reason="plan identity or store namespace differs")
        store.unpin_snapshot(
            plan.snapshot_id,
            kind="saved_plan",
            reference=reference or verified.recipe_hash,
        )
        return plan.snapshot_id

    @classmethod
    def from_json(cls, value: str | bytes) -> SavedPlanRecipe:
        try:
            data = json.loads(value)
            model = ModelReference(**data["model"])
            destination = DestinationRef(**data["destination"])
            recipe = cls(
                data["schema_version"],
                data["plan_json"],
                data["design_json"],
                data["design_hash"],
                model,
                destination,
                data["namespace"],
                data["expected_base_manifest"],
                data["recipe_hash"],
                data.get("dataset_recipe_json"),
            )
        except (ValueError, TypeError, KeyError) as exc:
            raise _INVALID.error(reason="recipe is malformed") from exc
        if recipe.schema_version != 1:
            raise _INVALID.error(reason="unsupported schema version")
        expected = hashlib.sha256(_canonical(recipe._data(include_hash=False))).hexdigest()
        if not isinstance(recipe.recipe_hash, str) or not hmac.compare_digest(
            recipe.recipe_hash, expected
        ):
            raise _INVALID.error(reason="recipe checksum mismatch")
        try:
            RefreshPlan.deserialize(recipe.plan_json)
            design = from_json(recipe.design_json)
        except TerrineError:
            raise
        except (ValueError, TypeError) as exc:
            raise _INVALID.error(reason="plan or Design payload is invalid") from exc
        if hashlib.sha256(canonical_json(design)).hexdigest() != recipe.design_hash:
            raise _INVALID.error(reason="Design hash mismatch")
        if recipe.dataset_recipe_json is not None:
            try:
                dataset_recipe = DatasetRecipe.from_json(recipe.dataset_recipe_json)
            except (ValueError, TypeError) as exc:
                raise _INVALID.error(reason="dataset recipe is invalid") from exc
            if canonical_json(dataset_recipe.base_design) != canonical_json(design):
                raise _INVALID.error(reason="dataset recipe Design differs")
        return recipe


def _bind_recipe_plan(recipe: SavedPlanRecipe, *, namespace: str) -> RefreshPlan:
    restored = SavedPlanRecipe.from_json(recipe.to_json())
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
    bound_payload = {
        "namespace": plan.namespace,
        "dataset": plan.dataset,
        "definition_hash": plan.definition_hash,
        "snapshot_id": plan.snapshot_id,
        "tasks": plan.recipe.tasks,
        "change_evidence": plan.recipe.change_evidence,
        "budget": plan.recipe.budget,
        "expected_base_manifest": restored.expected_base_manifest,
        "expected_base_manifest_bound": True,
    }
    fingerprint = hashlib.sha256(
        json.dumps(bound_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    plan_recipe = replace(
        plan.recipe,
        fingerprint=fingerprint,
        expected_base_manifest=restored.expected_base_manifest,
        expected_base_manifest_bound=True,
    )
    return replace(
        plan,
        recipe=plan_recipe,
        expected_base_manifest=restored.expected_base_manifest,
        expected_base_manifest_bound=True,
    )


def resolve_plan(
    recipe: SavedPlanRecipe, *, resolver: ModelResolver, namespace: str
) -> tuple[RefreshPlan, Model, Design, DestinationRef]:
    restored = SavedPlanRecipe.from_json(recipe.to_json())
    if namespace != restored.namespace:
        raise _INVALID.error(reason="owner namespace mismatch")
    model = resolver.resolve(restored.model)
    if (
        not isinstance(model, Model)
        or ModelReference.from_model(model, owner_key=restored.model.owner_key) != restored.model
    ):
        raise _INVALID.error(reason="owner Model declaration does not match reference")
    plan = _bind_recipe_plan(restored, namespace=namespace)
    design = from_json(restored.design_json)
    return plan, model, design, restored.destination
