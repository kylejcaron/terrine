from datetime import date

from terrine.dataset.manifest import DatasetManifest, ManifestColumn, ManifestGeneration
from terrine.spec import Design, FixedDate
from terrine.spec.dataset import DatasetDefinition, DatasetDefinitionStore


def test_manifest_definition_reference_requires_owner_storage_and_verifies_digest(tmp_path) -> None:
    import pytest

    from terrine.errors import TerrineError

    definition = DatasetDefinition(
        schema_version=1, base_design=Design("unit", FixedDate(date(2025, 1, 1)))
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
        schema_version=1, base_design=Design("unit", FixedDate(date(2025, 1, 1)))
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


def test_corrupt_manifest_definition_hash_uses_invalid_definition_refusal() -> None:
    import json

    import pytest

    from terrine.errors import TerrineError

    definition = DatasetDefinition(
        schema_version=1, base_design=Design("unit", FixedDate(date(2025, 1, 1)))
    )
    manifest = DatasetManifest(
        definition,
        ManifestGeneration("ds", "g1", ("table:g1",), ("sha256:x",), 0),
        (),
        "design",
        "model",
        {},
        {},
    )
    payload = json.loads(manifest.to_json())
    payload["definition_hash"] = "corrupt"
    with pytest.raises(TerrineError) as error:
        DatasetManifest.from_json(json.dumps(payload))
    assert error.value.code == "terrine.dataset.invalid_definition"
