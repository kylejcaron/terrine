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


def test_plan_fingerprint_includes_change_support_not_only_revision():
    graph = dependency_graph(sources=("events",), definition_hash="definition")
    earlier = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="definition",
        snapshot_id="snap",
        graph=graph,
        changes=ChangeSet(
            (Change("insert", "events", after_key_range=("a", "a"), revision="r1"),),
            "change_feed",
        ),
    )
    later = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="definition",
        snapshot_id="snap",
        graph=graph,
        changes=ChangeSet(
            (Change("insert", "events", after_key_range=("z", "z"), revision="r1"),),
            "change_feed",
        ),
    )
    assert earlier.recipe.fingerprint != later.recipe.fingerprint


def test_planner_limits_work_to_changed_sources_and_skips_readiness_only_advance():
    graph = dependency_graph(
        sources=("numerator", "denominator"),
        definition_hash="definition",
        partitions=("2025-01", "2025-02"),
    )
    changed = plan_refresh(
        namespace="owner",
        dataset="ratio",
        definition_hash="definition",
        snapshot_id="snap",
        graph=graph,
        changes=ChangeSet(
            (Change("insert", "numerator", revision="r2"),),
            "change_feed",
        ),
    )
    assert {task.source for task in changed.tasks} == {"numerator"}
    no_change = plan_refresh(
        namespace="owner",
        dataset="ratio",
        definition_hash="definition",
        snapshot_id="snap",
        graph=graph,
        changes=ChangeSet((), "revision"),
    )
    assert no_change.tasks == ()


def test_source_invalidation_includes_cross_source_dependents():
    from terrine.runtime.graph import DependencyGraph
    from terrine.runtime.state import LayerNode

    source = LayerNode("daily", "events", "p1", "d", "g")
    dependent = LayerNode("frame", "study", "p1", "d", "g")
    graph = DependencyGraph((source, dependent), ((source, dependent),))
    plan = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="d",
        snapshot_id="snap",
        graph=graph,
        changes=ChangeSet((Change("insert", "events", revision="r1"),), "change_feed"),
    )
    assert {task.source for task in plan.tasks} == {"events", "study"}


def test_max_tasks_applies_after_change_selection():
    graph = dependency_graph(
        sources=("events", "other"),
        definition_hash="definition",
        partitions=("p1",),
    )
    plan = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="definition",
        snapshot_id="snap",
        graph=graph,
        changes=ChangeSet((Change("insert", "events", revision="r1"),), "change_feed"),
        max_tasks=4,
    )
    assert len(plan.tasks) == 4


def test_readiness_only_noop_plan_satisfies_strict_scan_budget():
    graph = dependency_graph(sources=("events",), definition_hash="definition")
    plan = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="definition",
        snapshot_id="snap",
        graph=graph,
        changes=ChangeSet((), "revision"),
        budget=Budget(max_scan_bytes=1),
    )
    assert plan.tasks == ()


def test_definition_change_starts_new_fingerprint():
    first_graph = dependency_graph(sources=("events",), definition_hash="v1")
    next_graph = dependency_graph(sources=("events",), definition_hash="v2")
    first = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="v1",
        snapshot_id="snap",
        graph=first_graph,
    )
    next_plan = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="v2",
        snapshot_id="snap",
        graph=next_graph,
        changes=ChangeSet((Change("definition", "events"),), "revision"),
    )
    assert first.recipe.fingerprint != next_plan.recipe.fingerprint


def test_event_anchor_identity_change_rebuilds_all_source_partitions():
    graph = dependency_graph(
        sources=("events",),
        definition_hash="d",
        partitions=("2025-01", "2025-02"),
    )
    plan = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="d",
        snapshot_id="snap",
        graph=graph,
        changes=ChangeSet(
            (Change("insert", "events", after_key_range=("earlier-event", "earlier-event")),),
            "change_feed",
        ),
    )
    assert {task.partition for task in plan.tasks if task.layer == "frame"} == {
        "2025-01",
        "2025-02",
    }


def test_delimiter_fields_do_not_collide_in_task_identities():
    graph_a = dependency_graph(sources=("a:b",), definition_hash="def", partitions=("c",))
    graph_b = dependency_graph(sources=("a",), definition_hash="def", partitions=("b:c",))
    task_a = plan_refresh(
        namespace="owner", dataset="study", definition_hash="def", snapshot_id="s", graph=graph_a
    ).tasks[0]
    task_b = plan_refresh(
        namespace="owner", dataset="study", definition_hash="def", snapshot_id="s", graph=graph_b
    ).tasks[0]
    assert task_a.task_id != task_b.task_id


def test_plan_serialization_preserves_execution_budgets():
    graph = dependency_graph(sources=("events",), definition_hash="definition")
    plan = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="definition",
        snapshot_id="snap",
        graph=graph,
        budget=Budget(max_rows=7, max_output_bytes=701),
    )
    recipe = json.loads(plan.serialize())
    assert recipe["budget"] == {
        "max_rows": 7,
        "max_output_bytes": 701,
        "max_scan_bytes": None,
        "strict_scan": True,
    }
    assert (
        plan.recipe.fingerprint
        != plan_refresh(
            namespace="owner",
            dataset="study",
            definition_hash="definition",
            snapshot_id="snap",
            graph=graph,
        ).recipe.fingerprint
    )


def test_state_panel_correction_rebuilds_adjacent_terminal_and_baseline_layers():
    from types import SimpleNamespace

    from terrine.spec.expressions import AttributeAsOf
    from terrine.spec.state_panel import StateObservations, StatePanel

    design = SimpleNamespace(
        spec_hash="d",
        shape=StatePanel(
            StateObservations("status", ("new", "old")),
            max_periods=3,
            baseline_features={"base": AttributeAsOf("tier", 0)},
        ),
    )
    graph = dependency_graph(
        design=design,
        sources=("events",),
        definition_hash="d",
        partitions=("p1", "p2", "p3"),
    )
    plan = plan_refresh(
        namespace="owner",
        dataset="panel",
        definition_hash="d",
        snapshot_id="s2",
        graph=graph,
        changes=ChangeSet(
            (Change("update", "events", before_key_range=("u", "u"), revision="r3"),),
            "change_feed",
        ),
    )
    assert {
        (task.layer, task.partition)
        for task in plan.tasks
        if task.layer in {"state_point", "state_period", "baseline", "frame"}
    } == {
        (layer, partition)
        for layer in {"state_point", "state_period", "baseline", "frame"}
        for partition in ("p1", "p2", "p3")
    }


def test_historical_float64_prefix_change_revalidates_certificates_and_results():
    from datetime import UTC, datetime
    from types import SimpleNamespace

    design = SimpleNamespace(spec_hash="d", shape="cross_section")
    graph = dependency_graph(
        design=design,
        sources=("events",),
        definition_hash="d",
        partitions=("p1", "p2"),
    )
    earlier = datetime(2024, 12, 1, tzinfo=UTC)
    plan = plan_refresh(
        namespace="owner",
        dataset="precision",
        definition_hash="d",
        snapshot_id="s2",
        graph=graph,
        changes=ChangeSet(
            (
                Change(
                    "insert",
                    "events",
                    after_time_range=(earlier, datetime(2024, 12, 2, tzinfo=UTC)),
                    revision="r2",
                ),
            ),
            "change_feed",
        ),
    )
    for partition in ("p1", "p2"):
        assert any(
            task.layer == "prefix_certificate" and task.partition == partition
            for task in plan.tasks
        )
        assert any(task.layer == "frame" and task.partition == partition for task in plan.tasks)
