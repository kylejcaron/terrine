from __future__ import annotations

import pyarrow as pa

from terrine.data import Budget
from terrine.dataset.reference import Dataset
from terrine.runtime.changes import Change, ChangeSet
from terrine.spec import Design, FixedDate, agg, gt
from tests.dataset.test_staging import _durable_frame


def test_refresh_reconstructs_recipe_without_mutating_pinned_reader(events_world, tmp_path):
    initial, con = _durable_frame(events_world, tmp_path)
    model = initial._model
    frame = (
        Design(
            "user",
            FixedDate("2025-01-04"),
            features={"sessions": agg("sessions", window=("-1d", "0s"))},
            outcomes={"churn": agg("churned", window=("0s", "1d"))},
        )
        .build(model)
        .training_set(label="churn", incomplete_labels="keep")
    )
    budget = Budget(strict_scan=False)
    original = frame.stage("training", budget=budget)
    reopened = Dataset.from_manifest(original.manifest, con=con)
    old_reader = reopened.to_arrow_batches(batch_size=1)
    first = next(old_reader)
    con.raw_sql("DELETE FROM events WHERE user_id='a'")
    changes = ChangeSet((Change("delete", "events"),), "operator")
    before = con.con.execute("SELECT count(*) FROM terrine_runtime.runtime_runs").fetchone()[0]
    plan = reopened.plan_refresh(model, changes=changes, budget=budget)
    assert plan.tasks
    assert (
        con.con.execute("SELECT count(*) FROM terrine_runtime.runtime_runs").fetchone()[0] == before
    )
    refreshed = reopened.refresh(model, changes=changes, budget=budget)
    assert refreshed.manifest.generation.generation_id != original.manifest.generation.generation_id
    old = pa.Table.from_batches([first, *old_reader])
    new = pa.Table.from_batches(list(refreshed.to_arrow_batches()))
    assert sorted(old["sessions"].to_pylist()) == [1, 1]
    assert sorted(new["sessions"].to_pylist()) == [0, 1]
    clean = refreshed.manifest.recipe.build(model).materialize(budget)
    order = [("unit", "ascending"), ("anchor_id", "ascending")]
    assert (
        new.select(clean.table.column_names)
        .replace_schema_metadata(None)
        .sort_by(order)
        .equals(clean.table.replace_schema_metadata(None).sort_by(order))
    )
    assert original.manifest.recipe.digest == refreshed.manifest.recipe.digest
    con.disconnect()


def test_scoring_refresh_preserves_as_of_and_never_reads_unused_labels(events_world, tmp_path):
    initial, con = _durable_frame(events_world, tmp_path)
    model = initial._model
    design = Design(
        "user",
        FixedDate("2025-01-04"),
        features={"sessions": agg("sessions", window=("-1d", "0s"))},
        outcomes={"churn": agg("churned", window=("0s", "1d"))},
    )
    scoring = design.snapshot(
        model,
        as_of="2025-01-04",
        eligibility={"active": gt(agg("sessions", window=("-1d", "0s")), 0)},
    )
    con.drop_table("labels")
    dataset = scoring.stage("scoring", budget=Budget(strict_scan=False))
    refreshed = dataset.refresh(model, budget=Budget(strict_scan=False))
    assert refreshed.manifest.recipe.view["as_of"] == dataset.manifest.recipe.view["as_of"]
    assert all("churn" not in batch.schema.names for batch in refreshed.to_arrow_batches())
    assert set(refreshed.manifest.source_coverage) == {"events"}
    con.disconnect()


def test_refresh_nochange_reuses_only_same_definition_and_rejects_stale_base(
    events_world, tmp_path
):
    import pytest

    from terrine.errors import TerrineError

    frame, con = _durable_frame(events_world, tmp_path)
    budget = Budget(strict_scan=False)
    dataset = frame.stage("training", budget=budget)
    empty = ChangeSet((), "operator")
    assert not dataset.plan_refresh(frame._model, changes=empty, budget=budget).tasks
    assert dataset.refresh(frame._model, changes=empty, budget=budget).manifest == dataset.manifest
    newer = dataset.refresh(frame._model, budget=budget)
    assert newer.manifest.generation != dataset.manifest.generation
    with pytest.raises(TerrineError) as error:
        dataset.refresh(frame._model, changes=empty, budget=budget)
    assert error.value.code == "terrine.dataset.stale_generation"
    con.disconnect()
