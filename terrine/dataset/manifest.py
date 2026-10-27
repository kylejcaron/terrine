"""Versioned metadata binding staged outputs to their definition and provenance."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from terrine.errors import RefusalSpec, freeze, thaw
from terrine.spec.dataset import (
    INVALID_DEFINITION,
    DatasetDefinition,
    DatasetDefinitionReference,
    DatasetDefinitionStore,
    _bad_constant,
    _unique_pairs,
)

__all__ = ["DatasetManifest", "ManifestColumn", "ManifestGeneration"]

_INVALID = RefusalSpec(
    "terrine.dataset.invalid_manifest",
    "Invalid dataset manifest: {reason}. Stage its dataset definition to create a new manifest.",
)


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
        if any(
            not isinstance(value, str) or not value
            for value in (self.name, self.data_type, self.role)
        ) or (self.metadata is not None and not isinstance(self.metadata, Mapping)):
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
            any(
                not isinstance(value, str) or not value
                for value in (self.dataset_id, self.generation_id)
            )
            or not isinstance(self.row_count, int)
            or isinstance(self.row_count, bool)
            or self.row_count < 0
            or any(
                not isinstance(values, (list, tuple))
                or any(not isinstance(value, str) or not value for value in values)
                for values in (self.output_refs, self.checksums)
            )
        ):
            raise ValueError("invalid generation identity or row count")
        object.__setattr__(self, "output_refs", tuple(self.output_refs))
        object.__setattr__(self, "checksums", tuple(self.checksums))
        if len(self.output_refs) != len(self.checksums):
            raise ValueError("each output reference requires a checksum")


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    definition: DatasetDefinition
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
    definition_reference: DatasetDefinitionReference | None = None
    frame_metadata: Any = None

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or type(self.schema_version) is not int
            or not isinstance(self.definition, DatasetDefinition)
        ):
            raise ValueError("unsupported manifest schema or definition")
        if self.definition_reference is not None:
            if (
                not isinstance(self.definition_reference, DatasetDefinitionReference)
                or self.definition_reference.digest != self.definition.digest
            ):
                raise INVALID_DEFINITION.error(
                    reason="definition reference does not match manifest definition"
                )
        if not isinstance(self.generation, ManifestGeneration):
            raise TypeError("generation must be ManifestGeneration")
        if any(
            not isinstance(value, str) or not value
            for value in (self.design_hash, self.model_hash, self.compiler_version)
        ):
            raise ValueError("design_hash, model_hash and compiler_version are required")
        for name in (
            "source_coverage",
            "source_snapshots",
            "readiness",
            "split_policy",
            "frame_metadata",
        ):
            value = getattr(self, name)
            if (value is None and name in ("source_coverage", "source_snapshots")) or (
                value is not None and not isinstance(value, Mapping)
            ):
                raise TypeError(f"{name} must be a metadata mapping")
            if value is not None:
                object.__setattr__(self, name, freeze(value))
        object.__setattr__(self, "columns", tuple(self.columns))
        if any(not isinstance(column, ManifestColumn) for column in self.columns):
            raise TypeError("columns must be ManifestColumn records")

    def to_json(self) -> bytes:
        payload = {
            "schema_version": 1,
            "definition": json.loads(self.definition.canonical_json()),
            "definition_hash": self.definition.digest,
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
        if self.frame_metadata is not None:
            payload["frame_metadata"] = thaw(self.frame_metadata)
        if self.definition_reference is not None:
            del payload["definition"]
            payload["definition_reference"] = {
                "owner": self.definition_reference.owner,
                "digest": self.definition_reference.digest,
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
        definition_store: DatasetDefinitionStore | None = None,
    ) -> DatasetManifest:
        try:
            payload = json.loads(
                value, object_pairs_hook=_unique_pairs, parse_constant=_bad_constant
            )
            common = {
                "schema_version",
                "definition_hash",
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
            is_reference = "definition_reference" in payload if isinstance(payload, dict) else False
            expected = common | ({"definition_reference"} if is_reference else {"definition"})
            if (
                not isinstance(payload, dict)
                or set(payload) - {"frame_metadata"} != expected
                or payload["schema_version"] != 1
                or type(payload["schema_version"]) is not int
            ):
                raise ValueError("invalid dataset manifest")
            reference = None
            if is_reference:
                if definition_store is None:
                    raise ValueError("owner-configured definition storage is required")
                record = payload["definition_reference"]
                if not isinstance(record, dict) or set(record) != {"owner", "digest"}:
                    raise ValueError("invalid definition reference")
                reference = DatasetDefinitionReference(**record)
                definition = definition_store.resolve(reference)
            else:
                definition = DatasetDefinition.from_json(_canonical(payload["definition"]))
            if definition.digest != payload["definition_hash"]:
                raise ValueError("definition digest mismatch")
            gen = payload["generation"]
            if not isinstance(gen, dict) or set(gen) != {
                "dataset_id",
                "generation_id",
                "output_refs",
                "checksums",
                "row_count",
            }:
                raise ValueError("invalid generation record")
            if not isinstance(payload["columns"], list):
                raise ValueError("columns must be a list of records")
            columns = tuple(ManifestColumn(**col) for col in payload["columns"])
            return cls(
                definition,
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
                payload.get("frame_metadata"),
            )
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            raise _INVALID.error(reason="malformed or unsupported content") from error
