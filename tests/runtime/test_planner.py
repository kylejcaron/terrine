import json

import pytest

from terrine.data import Budget
from terrine.errors import TerrineError
from terrine.runtime.changes import Change, ChangeSet
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh


def test_refresh_plan_serialization_is_stable_and_costs_remain_unknown():
    graph = dependency_graph(
        sources=("events",), definition_hash="definition", partitions=("2025-01",)
    )
    first = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=graph,
    )
    second = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=graph,
    )
    assert first.serialize() == second.serialize()
    data = json.loads(first.serialize())
    assert data["schema_version"] == 1
    assert all(task.estimated_scan_bytes is None for task in first.tasks)


def test_unknown_scan_estimate_fails_strict_budget_before_execution():
    graph = dependency_graph(sources=("events",), definition_hash="definition")
    with pytest.raises(TerrineError) as error:
        plan_refresh(
            namespace="owner",
            dataset="study",
            definition_hash="definition",
            snapshot_id="snap",
            graph=graph,
            budget=Budget(max_scan_bytes=1),
        )
    assert error.value.code == "terrine.budget.scan_unknown"


def test_plan_fingerprint_includes_change_evidence():
    graph = dependency_graph(sources=("events",), definition_hash="definition")
    initial = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="definition",
        snapshot_id="snap",
        graph=graph,
    )
    changed = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="definition",
        snapshot_id="snap",
        graph=graph,
        changes=ChangeSet((Change("insert", "events", revision="r2"),), "change_feed"),
    )
    assert initial.recipe.fingerprint != changed.recipe.fingerprint
