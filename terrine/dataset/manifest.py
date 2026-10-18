"""Versioned metadata binding staged outputs to their recipe and provenance."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from terrine.errors import freeze, thaw
from terrine.spec.dataset import (
    INVALID_RECIPE,
    DatasetRecipe,
    DatasetRecipeReference,
    DatasetRecipeStore,
)

__all__ = ["DatasetManifest", "ManifestColumn", "ManifestGeneration"]


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


@dataclass(frozen=True, slots=True)
class ManifestColumn:
    name: str
    data_type: str
    role: str
    metadata: Any = None

    def __post_init__(self) -> None:
        if not self.name or not self.data_type or not self.role:
            raise ValueError("column name, data_type and role must be non-empty")
        object.__setattr__(self, "metadata", freeze({} if self.metadata is None else self.metadata))


@dataclass(frozen=True, slots=True)
class ManifestGeneration:
    dataset_id: str
    generation_id: str
    output_refs: tuple[str, ...]
    checksums: tuple[str, ...]
    row_count: int

    def __post_init__(self) -> None:
        if (
            not self.dataset_id
            or not self.generation_id
            or self.row_count < 0
            or isinstance(self.row_count, bool)
        ):
            raise ValueError("invalid generation identity or row count")
        object.__setattr__(self, "output_refs", tuple(self.output_refs))
        object.__setattr__(self, "checksums", tuple(self.checksums))
        if len(self.output_refs) != len(self.checksums):
            raise ValueError("each output reference requires a checksum")


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    recipe: DatasetRecipe
    generation: ManifestGeneration
    columns: tuple[ManifestColumn, ...]
    design_hash: str
    model_hash: str
    source_coverage: Any
    source_snapshots: Any
    readiness: Any = None
    split_policy: Any = None
    schema_version: int = 1
    compiler_version: str = "1"
    recipe_reference: DatasetRecipeReference | None = None

    def __post_init__(self) -> None:
        if self.schema_version != 1 or not isinstance(self.recipe, DatasetRecipe):
            raise ValueError("unsupported manifest schema or recipe")
        if self.recipe_reference is not None:
            if (
                not isinstance(self.recipe_reference, DatasetRecipeReference)
                or self.recipe_reference.digest != self.recipe.digest
            ):
                raise INVALID_RECIPE.error(reason="recipe reference does not match manifest recipe")
        if not isinstance(self.generation, ManifestGeneration):
            raise TypeError("generation must be ManifestGeneration")
        if not self.design_hash or not self.model_hash:
            raise ValueError("design_hash and model_hash are required")
        for name in ("source_coverage", "source_snapshots", "readiness", "split_policy"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, freeze(value))
        object.__setattr__(self, "columns", tuple(self.columns))

    def to_json(self) -> bytes:
        payload = {
            "schema_version": 1,
            "recipe": json.loads(self.recipe.canonical_json()),
            "recipe_hash": self.recipe.digest,
            "generation": {
                "dataset_id": self.generation.dataset_id,
                "generation_id": self.generation.generation_id,
                "output_refs": self.generation.output_refs,
                "checksums": self.generation.checksums,
                "row_count": self.generation.row_count,
            },
            "columns": [
                {
                    "name": column.name,
                    "data_type": column.data_type,
                    "role": column.role,
                    "metadata": thaw(column.metadata),
                }
                for column in self.columns
            ],
            "design_hash": self.design_hash,
            "model_hash": self.model_hash,
            "source_coverage": thaw(self.source_coverage),
            "source_snapshots": thaw(self.source_snapshots),
            "readiness": thaw(self.readiness),
            "split_policy": thaw(self.split_policy),
            "compiler_version": self.compiler_version,
        }
        if self.recipe_reference is not None:
            del payload["recipe"]
            payload["recipe_reference"] = {
                "owner": self.recipe_reference.owner,
                "digest": self.recipe_reference.digest,
            }
        return _canonical(payload)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.to_json()).hexdigest()

    @classmethod
    def from_json(
        cls,
        value: str | bytes,
        *,
        recipe_store: DatasetRecipeStore | None = None,
    ) -> DatasetManifest:
        payload = json.loads(value)
        common = {
            "schema_version",
            "recipe_hash",
            "generation",
            "columns",
            "design_hash",
            "model_hash",
            "source_coverage",
            "source_snapshots",
            "readiness",
            "split_policy",
            "compiler_version",
        }
        is_reference = "recipe_reference" in payload if isinstance(payload, dict) else False
        expected = common | ({"recipe_reference"} if is_reference else {"recipe"})
        if (
            not isinstance(payload, dict)
            or set(payload) != expected
            or payload["schema_version"] != 1
        ):
            raise ValueError("invalid dataset manifest")
        reference = None
        if is_reference:
            if recipe_store is None:
                raise INVALID_RECIPE.error(reason="owner-configured recipe storage is required")
            record = payload["recipe_reference"]
            if not isinstance(record, dict) or set(record) != {"owner", "digest"}:
                raise INVALID_RECIPE.error(reason="invalid recipe reference")
            reference = DatasetRecipeReference(**record)
            recipe = recipe_store.resolve(reference)
        else:
            recipe = DatasetRecipe.from_json(_canonical(payload["recipe"]))
        if recipe.digest != payload["recipe_hash"]:
            raise INVALID_RECIPE.error(reason="recipe digest mismatch")
        gen = payload["generation"]
        if set(gen) != {"dataset_id", "generation_id", "output_refs", "checksums", "row_count"}:
            raise ValueError("invalid generation record")
        columns = tuple(ManifestColumn(**col) for col in payload["columns"])
        return cls(
            recipe,
            ManifestGeneration(**gen),
            columns,
            payload["design_hash"],
            payload["model_hash"],
            payload["source_coverage"],
            payload["source_snapshots"],
            payload["readiness"],
            payload["split_policy"],
            1,
            payload["compiler_version"],
            reference,
        )
