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
_INVALID_INPUT = RefusalSpec(
    "terrine.dataset.invalid_manifest", "Dataset manifest input {field} is invalid: {reason}."
)


def _name(value: object, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise _INVALID_INPUT.error(reason="empty_name", field=field)


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
        for field in ("name", "data_type", "role"):
            _name(getattr(self, field), field)
        if self.metadata is not None and not isinstance(self.metadata, Mapping):
            raise _INVALID_INPUT.error(reason="wrong_type", field="metadata")
        object.__setattr__(self, "metadata", freeze({} if self.metadata is None else self.metadata))


@dataclass(frozen=True, slots=True)
class ManifestGeneration:
    dataset_id: str
    generation_id: str
    output_refs: tuple[str, ...]
    checksums: tuple[str, ...]
    row_count: int

    def __post_init__(self) -> None:
        for field in ("dataset_id", "generation_id"):
            _name(getattr(self, field), field)
        if (
            not isinstance(self.row_count, int)
            or isinstance(self.row_count, bool)
            or self.row_count < 0
        ):
            raise _INVALID_INPUT.error(reason="not_nonnegative_integer", field="row_count")
        for field in ("output_refs", "checksums"):
            values = getattr(self, field)
            if not isinstance(values, (list, tuple)):
                raise _INVALID_INPUT.error(reason="wrong_type", field=field)
            for value in values:
                _name(value, field)
        object.__setattr__(self, "output_refs", tuple(self.output_refs))
        object.__setattr__(self, "checksums", tuple(self.checksums))
        if len(self.output_refs) != len(self.checksums):
            raise _INVALID_INPUT.error(reason="checksum_count_mismatch", field="checksums")


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
        if self.schema_version != 1 or type(self.schema_version) is not int:
            raise _INVALID_INPUT.error(reason="unsupported_schema_version", field="schema_version")
        if not isinstance(self.definition, DatasetDefinition):
            raise _INVALID_INPUT.error(reason="wrong_type", field="definition")
        if self.definition_reference is not None:
            if (
                not isinstance(self.definition_reference, DatasetDefinitionReference)
                or self.definition_reference.digest != self.definition.digest
            ):
                raise INVALID_DEFINITION.error(
                    reason="definition reference does not match manifest definition"
                )
        if not isinstance(self.generation, ManifestGeneration):
            raise _INVALID_INPUT.error(reason="wrong_type", field="generation")
        for field in ("design_hash", "model_hash", "compiler_version"):
            _name(getattr(self, field), field)
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
                raise _INVALID_INPUT.error(reason="wrong_type", field=name)
            if value is not None:
                object.__setattr__(self, name, freeze(value))
        if not isinstance(self.columns, (list, tuple)) or any(
            not isinstance(column, ManifestColumn) for column in self.columns
        ):
            raise _INVALID_INPUT.error(reason="wrong_type", field="columns")
        object.__setattr__(self, "columns", tuple(self.columns))

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
                raise _INVALID.error(reason="malformed_manifest")
            reference = None
            if is_reference:
                if definition_store is None:
                    raise _INVALID.error(reason="definition_storage_required")
                record = payload["definition_reference"]
                if not isinstance(record, dict) or set(record) != {"owner", "digest"}:
                    raise _INVALID.error(reason="invalid_definition_reference")
                reference = DatasetDefinitionReference(**record)
                definition = definition_store.resolve(reference)
            else:
                definition = DatasetDefinition.from_json(_canonical(payload["definition"]))
            if definition.digest != payload["definition_hash"]:
                raise _INVALID.error(reason="definition_digest_mismatch")
            gen = payload["generation"]
            if not isinstance(gen, dict) or set(gen) != {
                "dataset_id",
                "generation_id",
                "output_refs",
                "checksums",
                "row_count",
            }:
                raise _INVALID.error(reason="invalid_generation_record")
            if not isinstance(payload["columns"], list):
                raise _INVALID.error(reason="invalid_columns")
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
            if getattr(error, "code", None) == _INVALID.code:
                raise
            raise _INVALID.error(reason="malformed or unsupported content") from error
