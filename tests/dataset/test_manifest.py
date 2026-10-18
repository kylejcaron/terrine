from datetime import date

from terrine.dataset.manifest import DatasetManifest, ManifestColumn, ManifestGeneration
from terrine.spec import Design, FixedDate
from terrine.spec.dataset import DatasetRecipe


def test_dataset_manifest_round_trip_preserves_generation_schema_roles_and_certificates() -> None:
    recipe = DatasetRecipe(
        schema_version=1, base_design=Design("unit", FixedDate(date(2025, 1, 1)))
    )
    manifest = DatasetManifest(
        recipe,
        ManifestGeneration("ds", "g1", ("table:g1",), ("sha256:x",), 2),
        (ManifestColumn("y", "int64", "outcome", {"certificate": "cert-1"}),),
        "design",
        "model",
        {"source": "coverage"},
        {"source": {"status": "unknown", "snapshot_id": "snapshot"}},
    )
    reopened = DatasetManifest.from_json(manifest.to_json())
    assert reopened.generation == manifest.generation
    assert reopened.columns == manifest.columns
    assert reopened.source_snapshots["source"]["status"] == "unknown"


def test_corrupt_manifest_recipe_hash_uses_invalid_recipe_refusal() -> None:
    import json

    import pytest

    from terrine.errors import TerrineError

    recipe = DatasetRecipe(
        schema_version=1, base_design=Design("unit", FixedDate(date(2025, 1, 1)))
    )
    manifest = DatasetManifest(
        recipe,
        ManifestGeneration("ds", "g1", ("table:g1",), ("sha256:x",), 0),
        (),
        "design",
        "model",
        {},
        {},
    )
    payload = json.loads(manifest.to_json())
    payload["recipe_hash"] = "corrupt"
    with pytest.raises(TerrineError) as error:
        DatasetManifest.from_json(json.dumps(payload))
    assert error.value.code == "terrine.dataset.invalid_recipe"
