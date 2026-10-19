"""Canonical data-only recipes for reproducible staged datasets."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from terrine.errors import RefusalSpec, freeze, thaw
from terrine.spec.design import Design
from terrine.spec.io import canonical_json as design_json
from terrine.spec.io import from_json as design_from_json

__all__ = [
    "DatasetRecipe",
    "DatasetRecipeReference",
    "DatasetRecipeStore",
    "INVALID_RECIPE",
    "INVALID_VIEW",
    "UNSUPPORTED_VIEW",
]

INVALID_RECIPE = RefusalSpec("terrine.dataset.invalid_recipe", "Invalid dataset recipe: {reason}")
INVALID_VIEW = RefusalSpec("terrine.dataset.invalid_view", "Invalid dataset view: {reason}.")
UNSUPPORTED_VIEW = RefusalSpec(
    "terrine.dataset.unsupported_view",
    "Unsupported dataset view: {reason}.",
)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


@dataclass(frozen=True, slots=True, init=False)
class DatasetRecipe:
    schema_version: int
    base_design: Design
    view: Any

    def __init__(self, *, schema_version: int, base_design: Design, view: Any = None) -> None:
        if schema_version != 1 or isinstance(schema_version, bool):
            raise INVALID_RECIPE.error(reason="unsupported schema version")
        if not isinstance(base_design, Design):
            raise TypeError("base_design must be Design")
        if view is not None:
            if not isinstance(view, dict) or not isinstance(view.get("kind"), str):
                raise INVALID_RECIPE.error(reason="view must be a tagged data mapping")
            kind = view["kind"]
            fields = {
                "training": {"kind", "features", "labels", "incomplete_labels", "split"},
                "scoring": {"kind", "features", "as_of", "eligibility", "parent_design_hash"},
            }
            if kind not in fields or set(view) - fields[kind]:
                raise INVALID_RECIPE.error(reason="unknown view tag or field")
            try:
                view = freeze(view)
            except (TypeError, ValueError) as error:
                raise INVALID_RECIPE.error(reason="view contains a runtime value") from error
        object.__setattr__(self, "schema_version", 1)
        object.__setattr__(self, "base_design", base_design)
        object.__setattr__(self, "view", view)

    def canonical_json(self) -> bytes:
        payload = {
            "schema_version": 1,
            "base_design": json.loads(design_json(self.base_design))["design"],
            "view": thaw(self.view),
        }
        return _canonical(payload)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json()).hexdigest()

    @classmethod
    def from_json(cls, value: str | bytes) -> DatasetRecipe:
        try:
            payload = json.loads(
                value, object_pairs_hook=_unique_pairs, parse_constant=_bad_constant
            )
            if not isinstance(payload, dict) or set(payload) != {
                "schema_version",
                "base_design",
                "view",
            }:
                raise ValueError("unknown recipe fields")
            if payload["schema_version"] != 1 or isinstance(payload["schema_version"], bool):
                raise ValueError("unsupported recipe version")
            design = design_from_json(
                _canonical({"schema_version": 1, "design": payload["base_design"]})
            )
            return cls(schema_version=1, base_design=design, view=payload["view"])
        except Exception as error:
            if getattr(error, "code", None) == INVALID_RECIPE.code:
                raise
            raise INVALID_RECIPE.error(reason="malformed or unsupported content") from error


@dataclass(frozen=True, slots=True)
class DatasetRecipeReference:
    """Content-addressed recipe identity scoped to a configured owner."""

    owner: str
    digest: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.owner, str)
            or not self.owner
            or not isinstance(self.digest, str)
            or len(self.digest) != 64
            or any(char not in "0123456789abcdef" for char in self.digest)
        ):
            raise INVALID_RECIPE.error(reason="invalid owner-bound recipe reference")


@dataclass(frozen=True, slots=True)
class DatasetRecipeStore:
    """Immutable local recipe storage configured for one data owner."""

    root: Path
    owner: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root).expanduser().resolve())
        if not isinstance(self.owner, str) or not self.owner:
            raise ValueError("owner must be a non-empty string")

    def put(self, recipe: DatasetRecipe) -> DatasetRecipeReference:
        if not isinstance(recipe, DatasetRecipe):
            raise TypeError("recipe must be DatasetRecipe")
        content = recipe.canonical_json()
        path = self.root / f"{recipe.digest}.json"
        self.root.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.root, prefix=".recipe-", delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError as error:
                try:
                    existing = path.read_bytes()
                except OSError as read_error:
                    raise INVALID_RECIPE.error(
                        reason="immutable recipe object cannot be read"
                    ) from read_error
                if existing != content:
                    raise INVALID_RECIPE.error(
                        reason="immutable recipe content conflicts"
                    ) from error
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return DatasetRecipeReference(self.owner, recipe.digest)

    def resolve(self, reference: DatasetRecipeReference) -> DatasetRecipe:
        if not isinstance(reference, DatasetRecipeReference) or reference.owner != self.owner:
            raise INVALID_RECIPE.error(reason="recipe reference belongs to another owner")
        try:
            content = (self.root / f"{reference.digest}.json").read_bytes()
        except OSError as error:
            raise INVALID_RECIPE.error(reason="recipe reference object is unavailable") from error
        if hashlib.sha256(content).hexdigest() != reference.digest:
            raise INVALID_RECIPE.error(reason="recipe reference digest mismatch")
        recipe = DatasetRecipe.from_json(content)
        if recipe.digest != reference.digest:
            raise INVALID_RECIPE.error(reason="recipe reference digest mismatch")
        return recipe


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _bad_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")
