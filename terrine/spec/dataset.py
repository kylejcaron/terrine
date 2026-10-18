"""Canonical data-only recipes for reproducible staged datasets."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from terrine.errors import RefusalSpec, freeze, thaw
from terrine.spec.design import Design
from terrine.spec.io import canonical_json as design_json
from terrine.spec.io import from_json as design_from_json

__all__ = ["DatasetRecipe", "INVALID_RECIPE"]

INVALID_RECIPE = RefusalSpec("terrine.dataset.invalid_recipe", "Invalid dataset recipe: {reason}")


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
                "scoring": {"kind", "features", "as_of", "eligibility"},
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


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _bad_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")
