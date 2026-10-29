"""Canonical data-only definitions for reproducible staged datasets."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from terrine.errors import RefusalSpec, freeze, parameter_names, thaw
from terrine.spec.design import Design
from terrine.spec.io import SCHEMA_VERSION as DESIGN_SCHEMA_VERSION
from terrine.spec.io import canonical_json as design_json
from terrine.spec.io import from_json as design_from_json
from terrine.spec.transforms import Winsorization

__all__ = [
    "SCHEMA_VERSION",
    "DatasetDefinition",
    "DatasetDefinitionReference",
    "DatasetDefinitionStore",
    "INVALID_DEFINITION",
    "INVALID_VIEW",
    "UNSUPPORTED_VIEW",
]

SCHEMA_VERSION = 2

INVALID_DEFINITION = RefusalSpec(
    "terrine.dataset.invalid_definition", "Invalid dataset definition: {reason}"
)
_INVALID_INPUT = RefusalSpec(
    "terrine.dataset.invalid_definition", "Dataset definition input {field} is invalid: {reason}."
)
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
class DatasetDefinition:
    """Save a versioned design with its selected view and fixed transforms.

    Example: tr.DatasetDefinition(schema_version=2, base_design=design)
    """

    schema_version: int
    base_design: Design
    view: Any
    transforms: tuple[Winsorization, ...]

    def __init__(
        self,
        *,
        schema_version: int,
        base_design: Design,
        view: Any = None,
        transforms: tuple[Winsorization, ...] = (),
    ) -> None:
        if schema_version != SCHEMA_VERSION or isinstance(schema_version, bool):
            raise INVALID_DEFINITION.error(reason="unsupported schema version")
        if not isinstance(base_design, Design):
            raise _INVALID_INPUT.error(reason="wrong_type", field="base_design")
        if view is not None:
            if not isinstance(view, dict) or not isinstance(view.get("kind"), str):
                raise INVALID_DEFINITION.error(reason="view must be a tagged data mapping")
            kind = view["kind"]
            fields = {
                "training": {"kind", "features", "labels", "incomplete_labels", "split"},
                "scoring": {"kind", "features", "as_of", "eligibility", "parent_design_hash"},
            }
            if kind not in fields or set(view) - fields[kind]:
                raise INVALID_DEFINITION.error(reason="unknown view tag or field")
            try:
                view = freeze(view)
            except (TypeError, ValueError) as error:
                raise INVALID_DEFINITION.error(reason="view contains a runtime value") from error
        if not isinstance(transforms, Iterable):
            raise INVALID_DEFINITION.error(
                reason="transforms must be typed frozen winsorization records"
            )
        transforms = tuple(transforms)
        if any(
            not isinstance(item, Winsorization) or item.source_type is None for item in transforms
        ):
            raise INVALID_DEFINITION.error(
                reason="transforms must be typed frozen winsorization records"
            )
        if len({item.column for item in transforms}) != len(transforms):
            raise INVALID_DEFINITION.error(reason="duplicate transformed columns")
        object.__setattr__(self, "schema_version", SCHEMA_VERSION)
        object.__setattr__(self, "base_design", base_design)
        object.__setattr__(self, "view", view)
        object.__setattr__(self, "transforms", transforms)

    def canonical_json(self) -> bytes:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "base_design": json.loads(design_json(self.base_design))["design"],
            "view": thaw(self.view),
        }
        if self.transforms:
            payload["transforms"] = [item.to_record() for item in self.transforms]
        return _canonical(payload)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json()).hexdigest()

    def resolve_unit(self, model: Any) -> DatasetDefinition:
        """Return this definition on its base design with the unit resolved against ``model``."""
        from terrine.spec.validate import resolve_unit

        design = resolve_unit(self.base_design, model)
        if design is self.base_design:
            return self
        return DatasetDefinition(
            schema_version=SCHEMA_VERSION,
            base_design=design,
            view=thaw(self.view),
            transforms=self.transforms,
        )

    def build(self, model: Any):
        """Reconstruct the recorded view and frozen transforms without refitting."""
        frame = self.base_design.build(model)
        view = thaw(self.view)
        if view is not None and view["kind"] == "training":
            split = view.get("split")
            if split is not None:
                from importlib import import_module

                with parameter_names(record="split"):
                    split = import_module("terrine.dataset.splits").policy_from_record(split)
            frame = frame.training_set(
                label=tuple(view["labels"]),
                features=tuple(view["features"]),
                incomplete_labels=view["incomplete_labels"],
                split=split,
            )
        elif view is not None:
            from datetime import datetime

            from terrine.frame import _ScoringView

            object.__setattr__(
                frame,
                "_scoring_view",
                _ScoringView(
                    view["parent_design_hash"],
                    tuple(view["features"]),
                    datetime.fromisoformat(view["as_of"]),
                ),
            )
        for transform in self.transforms:
            frame = frame.winsorize(transform)
        return frame

    @classmethod
    def from_json(cls, value: str | bytes) -> DatasetDefinition:
        try:
            payload = json.loads(
                value, object_pairs_hook=_unique_pairs, parse_constant=_bad_constant
            )
            if not isinstance(payload, dict) or set(payload) - {"transforms"} != {
                "schema_version",
                "base_design",
                "view",
            }:
                raise ValueError("unknown definition fields")
            version = payload["schema_version"]
            if version != SCHEMA_VERSION or isinstance(version, bool):
                raise ValueError("unsupported definition version")
            design = design_from_json(
                _canonical(
                    {"schema_version": DESIGN_SCHEMA_VERSION, "design": payload["base_design"]}
                )
            )
            return cls(
                schema_version=SCHEMA_VERSION,
                base_design=design,
                view=payload["view"],
                transforms=tuple(
                    Winsorization.from_record(item) for item in payload.get("transforms", [])
                ),
            )
        except Exception as error:
            if getattr(error, "code", None) == INVALID_DEFINITION.code:
                raise
            raise INVALID_DEFINITION.error(reason="malformed or unsupported content") from error


@dataclass(frozen=True, slots=True)
class DatasetDefinitionReference:
    """Identify an immutable dataset definition by owner and content digest.

    Example: spec.DatasetDefinitionReference("analytics", definition.digest)
    """

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
            raise INVALID_DEFINITION.error(reason="invalid owner-bound definition reference")


@dataclass(frozen=True, slots=True)
class DatasetDefinitionStore:
    """Store immutable dataset definitions locally for one data owner.

    Example: spec.DatasetDefinitionStore(Path("definitions"), owner="analytics")
    """

    root: Path
    owner: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root).expanduser().resolve())
        if not isinstance(self.owner, str) or not self.owner:
            raise _INVALID_INPUT.error(reason="empty_name", field="owner")

    def put(self, definition: DatasetDefinition) -> DatasetDefinitionReference:
        if not isinstance(definition, DatasetDefinition):
            raise _INVALID_INPUT.error(reason="wrong_type", field="definition")
        content = definition.canonical_json()
        path = self.root / f"{definition.digest}.json"
        self.root.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.root, prefix=".definition-", delete=False
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
                    raise INVALID_DEFINITION.error(
                        reason="immutable definition object cannot be read"
                    ) from read_error
                if existing != content:
                    raise INVALID_DEFINITION.error(
                        reason="immutable definition content conflicts"
                    ) from error
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return DatasetDefinitionReference(self.owner, definition.digest)

    def resolve(self, reference: DatasetDefinitionReference) -> DatasetDefinition:
        if not isinstance(reference, DatasetDefinitionReference) or reference.owner != self.owner:
            raise INVALID_DEFINITION.error(reason="definition reference belongs to another owner")
        try:
            content = (self.root / f"{reference.digest}.json").read_bytes()
        except OSError as error:
            raise INVALID_DEFINITION.error(
                reason="definition reference object is unavailable"
            ) from error
        if hashlib.sha256(content).hexdigest() != reference.digest:
            raise INVALID_DEFINITION.error(reason="definition reference digest mismatch")
        definition = DatasetDefinition.from_json(content)
        if definition.digest != reference.digest:
            raise INVALID_DEFINITION.error(reason="definition reference digest mismatch")
        return definition


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _bad_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")
