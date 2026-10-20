from __future__ import annotations

import pytest

from terrine.data import Budget
from terrine.dataset.reference import Dataset
from terrine.spec import Design, FixedDate, agg
from tests._worlds import bind_model


def _durable_frame(events_world, tmp_path):
    model = bind_model(events_world, tmp_path)
    con = model.con
    frame = Design(
        "user",
        FixedDate("2025-01-05"),
        features={"sessions": agg("sessions", window=("-7d", "0s"))},
        outcomes={"churn": agg("churned", window=("0s", "7d"))},
    ).build(model)
    return frame, con


def test_frame_stage_publishes_and_reads_its_pinned_generation(events_world, tmp_path, monkeypatch):
    frame, con = _durable_frame(events_world, tmp_path)

    def collect_forbidden(*args, **kwargs):
        raise AssertionError("staging must stream the compiled output")

    monkeypatch.setattr(type(frame), "materialize", collect_forbidden)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    pinned = Dataset.from_manifest(dataset.manifest, con=con)
    original_generation = dataset.manifest.generation.generation_id

    assert dataset.manifest.generation.dataset_id == "training"
    assert sum(batch.num_rows for batch in dataset.to_arrow_batches(batch_size=1)) == (
        dataset.manifest.generation.row_count
    )
    assert all(batch.num_rows <= 1 for batch in pinned.to_arrow_batches(batch_size=1))

    newer = frame.stage("training", budget=Budget(strict_scan=False))
    assert newer.manifest.generation.generation_id != original_generation
    assert sum(batch.num_rows for batch in pinned.to_arrow_batches(batch_size=2)) == (
        dataset.manifest.generation.row_count
    )
    con.disconnect()


def test_manifest_reopen_rejects_tampered_generation_row_count(events_world, tmp_path):
    from dataclasses import replace

    from terrine.errors import TerrineError

    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    altered_generation = replace(
        dataset.manifest.generation,
        row_count=dataset.manifest.generation.row_count + 1,
    )
    altered_manifest = replace(dataset.manifest, generation=altered_generation)
    with pytest.raises(TerrineError) as error:
        Dataset.from_manifest(altered_manifest, con=con)
    assert error.value.code == "terrine.dataset.invalid_generation"
    con.disconnect()


def test_invalid_arrow_batch_size_refuses_before_read(events_world, tmp_path):
    from terrine.errors import TerrineError

    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    with pytest.raises(TerrineError) as error:
        dataset.to_arrow_batches(batch_size=0)
    assert error.value.code == "terrine.dataset.invalid_batch_size"
    con.disconnect()


def test_roster_only_stage_uses_executor_default_durable_outputs(
    events_world, tmp_path, monkeypatch
):
    from terrine.runtime.api import DesignExecutor

    frame, con = _durable_frame(events_world, tmp_path)
    original = DesignExecutor.__init__

    def initialize(self, *args, **kwargs):
        kwargs.pop("output_root", None)
        original(self, *args, **kwargs)

    monkeypatch.setattr(DesignExecutor, "__init__", initialize)
    con.drop_table("labels")
    roster = Design("user", FixedDate("2025-01-05")).build(frame._model)
    dataset = roster.stage("roster", budget=Budget(strict_scan=False))
    assert dataset.manifest.generation.row_count == 2
    assert set(dataset.manifest.source_coverage) == set()
    assert sorted(
        key for batch in dataset.to_arrow_batches() for key in batch["unit"].to_pylist()
    ) == ["a", "b"]
    con.disconnect()


def test_reopen_accepts_resolved_owner_scoped_recipe_reference(events_world, tmp_path):
    from dataclasses import replace

    from terrine.dataset.manifest import DatasetManifest
    from terrine.spec.dataset import DatasetRecipeStore

    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    recipes = DatasetRecipeStore(tmp_path / "recipes", owner="customer")
    reference = recipes.put(dataset.manifest.recipe)
    manifest = DatasetManifest.from_json(
        replace(dataset.manifest, recipe_reference=reference).to_json(), recipe_store=recipes
    )
    reopened = Dataset.from_manifest(manifest, con=con)
    assert reopened.manifest.recipe_reference == reference
    assert sum(batch.num_rows for batch in reopened.to_arrow_batches()) == 2
    con.disconnect()


def test_reopen_preserves_version_one_manifest_without_frame_metadata(events_world, tmp_path):
    import json
    from dataclasses import replace

    from terrine.dataset.manifest import DatasetManifest
    from terrine.runtime.backends.duckdb import _digest_relation

    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    checksums = []
    for partition in dataset.manifest.generation.output_refs:
        physical = con.con.execute(
            "SELECT table_name FROM terrine_runtime.runtime_partitions WHERE partition=?",
            [partition],
        ).fetchone()
        assert physical is not None
        checksum = _digest_relation(con.con, "terrine_runtime", physical[0], version=1)
        con.con.execute(
            "UPDATE terrine_runtime.runtime_partitions SET digest=? WHERE partition=?",
            [checksum, partition],
        )
        checksums.append(checksum)
    old = replace(
        dataset.manifest,
        frame_metadata=None,
        generation=replace(dataset.manifest.generation, checksums=tuple(checksums)),
    ).to_json()
    assert "frame_metadata" not in json.loads(old)
    con.con.execute(
        "UPDATE terrine_runtime.runtime_dataset_recipes SET manifest_json=? "
        "WHERE dataset=? AND generation=?",
        [old.decode(), "training", dataset.manifest.generation.generation_id],
    )
    restored = DatasetManifest.from_json(old)
    assert restored.to_json() == old
    reopened = Dataset.from_manifest(restored, con=con)
    assert sum(batch.num_rows for batch in reopened.to_arrow_batches()) == 2
    con.disconnect()
