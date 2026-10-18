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
