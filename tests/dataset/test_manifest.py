import json
from datetime import date
from typing import Any, cast

import pytest

from terrine.dataset.export import ExportManifest
from terrine.dataset.manifest import DatasetManifest, ManifestColumn, ManifestGeneration
from terrine.dataset.reference import Dataset
from terrine.spec import Design, FixedDate
from terrine.spec.dataset import DatasetDefinition, DatasetDefinitionStore


def test_manifest_definition_reference_requires_owner_storage_and_verifies_digest(tmp_path) -> None:
    import pytest

    from terrine.errors import TerrineError

    definition = DatasetDefinition(
        schema_version=2, base_design=Design("unit", FixedDate(date(2025, 1, 1)))
    )
    store = DatasetDefinitionStore(tmp_path / "owner-a", owner="owner-a")
    reference = store.put(definition)
    manifest = DatasetManifest(
        definition,
        ManifestGeneration("ds", "g1", ("table:g1",), ("sha256:x",), 0),
        (),
        "design",
        "model",
        {},
        {},
        definition_reference=reference,
    )
    with pytest.raises(TerrineError):
        DatasetManifest.from_json(manifest.to_json())
    reopened = DatasetManifest.from_json(manifest.to_json(), definition_store=store)
    assert reopened.definition == definition
    assert reopened.definition_reference == reference
    other = DatasetDefinitionStore(tmp_path / "owner-b", owner="owner-b")
    with pytest.raises(TerrineError):
        DatasetManifest.from_json(manifest.to_json(), definition_store=other)


def test_dataset_manifest_round_trip_preserves_generation_schema_roles_and_metadata() -> None:
    definition = DatasetDefinition(
        schema_version=2, base_design=Design("unit", FixedDate(date(2025, 1, 1)))
    )
    manifest = DatasetManifest(
        definition,
        ManifestGeneration("ds", "g1", ("table:g1",), ("sha256:x",), 2),
        (ManifestColumn("y", "int64", "outcome", {"numeric_policy": "exact"}),),
        "design",
        "model",
        {"source": "coverage"},
        {"source": {"status": "unknown", "snapshot_id": "snapshot"}},
    )
    reopened = DatasetManifest.from_json(manifest.to_json())
    assert reopened.generation == manifest.generation
    assert reopened.columns == manifest.columns
    assert reopened.source_snapshots["source"]["status"] == "unknown"


@pytest.mark.parametrize(
    "damage",
    (
        "syntax",
        "envelope",
        "unknown",
        "generation",
        "columns",
        "definition",
        "digest",
        "version",
        "duplicate",
        "nonfinite",
        "row_count",
        "references",
    ),
)
def test_malformed_manifest_payload_has_coded_refusal(damage) -> None:
    from terrine.errors import TerrineError

    definition = DatasetDefinition(
        schema_version=2, base_design=Design("unit", FixedDate(date(2025, 1, 1)))
    )
    manifest = DatasetManifest(
        definition,
        ManifestGeneration("ds", "g1", ("table:g1",), ("sha256:x",), 0),
        (ManifestColumn("unit", "string", "unit"),),
        "design",
        "model",
        {},
        {},
    )
    payload = json.loads(manifest.to_json())
    if damage == "syntax":
        serialized = "{"
    elif damage == "envelope":
        serialized = "null"
    elif damage == "duplicate":
        serialized = (
            manifest.to_json()
            .decode()
            .replace('"schema_version":1', '"schema_version":1,"schema_version":1', 1)
        )
    elif damage == "nonfinite":
        payload["source_coverage"]["start"] = float("nan")
        serialized = json.dumps(payload)
    else:
        if damage == "unknown":
            payload["unknown"] = None
        elif damage == "generation":
            payload["generation"]["unknown"] = None
        elif damage == "columns":
            payload["columns"][0]["unknown"] = None
        elif damage == "definition":
            payload["definition"]["unknown"] = None
        elif damage == "digest":
            payload["definition_hash"] = "corrupt"
        elif damage == "version":
            payload["schema_version"] = True
        elif damage == "row_count":
            payload["generation"]["row_count"] = 0.5
        elif damage == "references":
            payload["generation"]["output_refs"] = "table:g1"
            payload["generation"]["checksums"] = "sha256:x"
        serialized = json.dumps(payload)
    with pytest.raises(TerrineError) as error:
        DatasetManifest.from_json(serialized)
    assert error.value.code == "terrine.dataset.invalid_manifest"


_DEFINITION = DatasetDefinition(
    schema_version=2, base_design=Design("unit", FixedDate(date(2025, 1, 1)))
)
_GENERATION = ManifestGeneration("ds", "g1", ("table:g1",), ("sha256:x",), 0)


@pytest.mark.parametrize(
    ("code", "build", "context"),
    [
        (
            "terrine.dataset.invalid_manifest",
            lambda: ManifestColumn("", "int64", "feature"),
            {"reason": "empty_name", "field": "name"},
        ),
        (
            "terrine.dataset.invalid_manifest",
            lambda: ManifestGeneration("ds", "g1", ("a",), ("x",), -1),
            {"reason": "not_nonnegative_integer", "field": "row_count"},
        ),
        (
            "terrine.dataset.invalid_manifest",
            lambda: ManifestGeneration("ds", "g1", ("a", "b"), ("x",), 0),
            {"reason": "checksum_count_mismatch", "field": "checksums"},
        ),
        (
            "terrine.dataset.invalid_manifest",
            lambda: DatasetManifest(_DEFINITION, _GENERATION, (), "d", "m", {}, {}, None, None, 2),
            {"reason": "unsupported_schema_version", "field": "schema_version"},
        ),
        (
            "terrine.dataset.invalid_manifest",
            lambda: DatasetManifest(_DEFINITION, _GENERATION, cast(Any, 5), "d", "m", {}, {}),
            {"reason": "wrong_type", "field": "columns"},
        ),
        (
            "terrine.dataset.invalid_manifest",
            lambda: DatasetManifest.from_json("{}"),
            {"reason": "malformed_manifest"},
        ),
        (
            "terrine.dataset.invalid_export",
            lambda: ExportManifest.from_json("{}"),
            {"reason": "unsupported_export_manifest"},
        ),
        (
            "terrine.dataset.invalid_export",
            lambda: ExportManifest.from_json("["),
            {"reason": "malformed_export_manifest"},
        ),
        (
            "terrine.dataset.invalid_manifest",
            lambda: Dataset(cast(Any, None), con=None, namespace="owner"),
            {"reason": "wrong_type", "field": "manifest"},
        ),
    ],
)
def test_manifest_inputs_refuse_with_codes(code, build, context):
    from terrine.errors import TerrineError

    with pytest.raises(TerrineError) as error:
        build()
    assert error.value.code == code
    assert {key: error.value.context.get(key) for key in context} == context
