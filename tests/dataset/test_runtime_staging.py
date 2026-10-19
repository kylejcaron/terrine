from __future__ import annotations

import json
from dataclasses import replace

import pyarrow as pa
import pytest

from terrine.data import Budget, MaterializedFrame
from terrine.dataset.reference import Dataset
from terrine.errors import TerrineError
from terrine.runtime.api import run_serialized_plan
from terrine.runtime.recipe import SavedPlanRecipe
from tests._timing import HANG_GUARD_SECONDS
from tests.dataset.test_staging import _durable_frame


def test_saved_stage_replays_exact_generation_and_refuses_changed_view(native_inputs, tmp_path):
    frame, con = _durable_frame(native_inputs, tmp_path)
    training = frame.training_set(label="churn", incomplete_labels="keep")
    dataset = training.stage("training", budget=Budget(strict_scan=False))
    saved, request_key = con.con.execute(
        "SELECT recipe_json,request_key FROM terrine_runtime.runtime_dataset_recipes "
        "WHERE dataset=? AND generation=?",
        ["training", dataset.manifest.generation.generation_id],
    ).fetchone()
    recipe = SavedPlanRecipe.from_json(saved)
    restored_recipe = recipe.restore_dataset_recipe()
    assert restored_recipe is not None
    assert restored_recipe == dataset.manifest.recipe
    assert recipe.dataset_recipe_json is not None
    con.raw_sql("DELETE FROM events")
    replay = run_serialized_plan(
        saved, con=con.con, namespace="terrine_runtime", request_key=request_key
    )
    assert replay.published_generation == dataset.manifest.generation.generation_id
    altered = json.loads(recipe.dataset_recipe_json)
    altered["view"]["incomplete_labels"] = "drop"
    changed = SavedPlanRecipe.create(
        plan_json=recipe.plan_json,
        design=restored_recipe.base_design,
        model=recipe.model,
        destination=recipe.destination,
        namespace=recipe.namespace,
        expected_base_manifest=recipe.expected_base_manifest,
        dataset_recipe=type(dataset.manifest.recipe).from_json(json.dumps(altered)),
    )
    with pytest.raises(TerrineError) as error:
        run_serialized_plan(
            changed.to_json(), con=con.con, namespace="terrine_runtime", request_key=request_key
        )
    assert error.value.code == "terrine.runtime.request_key_conflict"
    con.disconnect()


def test_staged_batches_preserve_frame_metadata_and_verify_recipe_ownership(
    native_inputs, tmp_path
):
    frame, con = _durable_frame(native_inputs, tmp_path)
    training = frame.training_set(label="churn", incomplete_labels="keep")
    expected = training.materialize(Budget(strict_scan=False))
    dataset = training.stage("training", budget=Budget(strict_scan=False))
    batches = list(dataset.to_arrow_batches(batch_size=1))
    actual = MaterializedFrame.from_arrow(pa.Table.from_batches(batches))
    assert actual.roles == expected.roles
    assert actual.columns == expected.columns
    assert actual.provenance.dataset_view == expected.provenance.dataset_view
    assert all(batch.num_rows <= 1 for batch in batches)
    wrong = replace(dataset.manifest, recipe=frame_recipe(frame))
    with pytest.raises(TerrineError) as error:
        Dataset.from_manifest(wrong, con=con)
    assert error.value.code == "terrine.dataset.invalid_generation"
    con.disconnect()


def frame_recipe(frame):
    from terrine.spec.dataset import DatasetRecipe

    return DatasetRecipe(schema_version=1, base_design=frame._design)


def test_stage_renews_lease_during_snapshot_tasks(native_inputs, tmp_path, monkeypatch):
    import threading

    from terrine.runtime.api import DesignExecutor
    from terrine.runtime.leases import LeaseStore

    renewed = threading.Event()
    original_renew = LeaseStore.renew
    original_task = DesignExecutor.run_snapshot_task

    def renew(store, lease, *, ttl, now):
        result = original_renew(store, lease, ttl=ttl, now=now)
        renewed.set()
        return result

    def run_task(executor, context, task, dependencies):
        assert renewed.wait(timeout=HANG_GUARD_SECONDS)
        return original_task(executor, context, task, dependencies)

    monkeypatch.setattr(LeaseStore, "renew", renew)
    monkeypatch.setattr(DesignExecutor, "run_snapshot_task", run_task)
    frame, con = _durable_frame(native_inputs, tmp_path)
    dataset = frame.stage("long_stage", budget=Budget(strict_scan=False))
    assert renewed.is_set()
    assert dataset.manifest.generation.row_count > 0
    con.disconnect()
