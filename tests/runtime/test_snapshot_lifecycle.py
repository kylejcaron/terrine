import duckdb
import pyarrow as pa
import pytest

from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.state import RuntimeStore


def test_snapshot_references_are_durable_idempotent_and_owner_scoped():
    con = duckdb.connect(":memory:")
    first = RuntimeStore(con, namespace="owner_a")
    second = RuntimeStore(con, namespace="owner_b")
    first.initialize()
    second.initialize()
    first.pin_snapshot("snapshot", kind="saved_plan", reference="plan-1")
    first.pin_snapshot("snapshot", kind="active_run", reference="run-1")
    first.pin_snapshot("snapshot", kind="saved_plan", reference="plan-1")
    assert first.snapshot_references("snapshot") == (
        ("active_run", "run-1"),
        ("saved_plan", "plan-1"),
    )
    assert second.snapshot_references("snapshot") == ()
    first.unpin_snapshot("snapshot", kind="saved_plan", reference="plan-1")
    assert first.snapshot_references("snapshot") == (("active_run", "run-1"),)


def test_snapshot_reference_identity_is_validated():
    store = RuntimeStore(duckdb.connect(":memory:"), namespace="owner")
    store.initialize()
    with pytest.raises(ValueError):
        store.pin_snapshot("", kind="run", reference="run")
    with pytest.raises(ValueError):
        store.pin_snapshot("snapshot", kind="", reference="run")


def test_execution_pins_snapshot_for_active_run_and_published_manifest():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    graph = dependency_graph(sources=("events",), definition_hash="def", partitions=("p1",))
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="def",
        snapshot_id="snapshot",
        graph=graph,
    )
    seen = []

    def publish(plan, outputs, lease):
        assert store.snapshot_references("snapshot") == (("active_run", "run-1"),)
        return "g1"

    result = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="run-1",
        run_task=lambda task, dependencies: pa.table({"task": [task.task_id]}),
        publish=publish,
    )
    seen.append(result.state)
    assert seen == ["succeeded"]
    assert store.snapshot_references("snapshot") == (("published_manifest", "orders:g1"),)
    store.pin_snapshot("snapshot", kind="active_run", reference="run-1")
    execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="run-1",
        run_task=lambda task, dependencies: pa.table({"task": [task.task_id]}),
        publish=publish,
    )
    assert store.snapshot_references("snapshot") == (("published_manifest", "orders:g1"),)
