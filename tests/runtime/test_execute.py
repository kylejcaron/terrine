import duckdb
import pyarrow as pa
import pytest

from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh


def _plan():
    graph = dependency_graph(sources=("events",), definition_hash="def", partitions=("p1",))
    return plan_refresh(
        namespace="owner", dataset="orders", definition_hash="def", snapshot_id="s", graph=graph
    )


def test_duplicate_task_completion_is_idempotent():
    con = duckdb.connect(":memory:")
    calls = []
    publish_calls = []

    def run_task(task, dependencies):
        calls.append(task.task_id)
        return pa.table({"task": [task.task_id]})

    def publish(plan, outputs, lease):
        publish_calls.append(tuple(outputs))
        return "g1"

    first = execute_plan(
        _plan(), con=con, namespace="owner", run_id="run-1", run_task=run_task, publish=publish
    )
    second = execute_plan(
        _plan(), con=con, namespace="owner", run_id="run-1", run_task=run_task, publish=publish
    )
    assert first == second
    assert len(calls) == 4
    assert len(publish_calls) == 1


def test_cancel_before_publish_never_exposes_generation():
    con = duckdb.connect(":memory:")
    published = []
    result = execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="cancelled-run",
        run_task=lambda task, dependencies: pa.table({"id": [1]}),
        publish=lambda plan, outputs, lease: published.append("published"),
        cancelled=lambda: True,
    )
    assert result.state == "cancelled"
    assert published == []


def test_plan_task_bound_is_checked_before_running_work():
    con = duckdb.connect(":memory:")
    calls = []
    with pytest.raises(ValueError):
        execute_plan(
            _plan(),
            con=con,
            namespace="owner",
            run_id="too-many",
            max_tasks=3,
            run_task=lambda task, dependencies: calls.append(task.task_id),
            publish=lambda plan, outputs, lease: None,
        )
    assert calls == []


def test_invalid_namespace_is_rejected_before_sql_execution():
    con = duckdb.connect(":memory:")
    statements = []

    class TrackingConnection:
        def execute(self, query, *args, **kwargs):
            statements.append(query)
            return con.execute(query, *args, **kwargs)

    with pytest.raises(ValueError):
        plan = plan_refresh(
            namespace='owner"; DROP TABLE important; --',
            dataset="orders",
            definition_hash="def",
            snapshot_id="s",
            graph=dependency_graph(sources=("events",), definition_hash="def", partitions=("p1",)),
        )
        execute_plan(
            plan,
            con=TrackingConnection(),
            namespace='owner"; DROP TABLE important; --',
            run_id="invalid-namespace",
            run_task=lambda task, dependencies: pa.table({"id": [1]}),
            publish=lambda plan, outputs, lease: None,
        )
    assert statements == []
