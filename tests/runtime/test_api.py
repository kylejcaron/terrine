import duckdb
import ibis
import pyarrow as pa
import pytest

import terrine.runtime as runtime
from terrine.errors import TerrineError
from terrine.runtime.execute import RunReceipt
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef


class _Executor:
    def __init__(self):
        self.calls = 0

    def run_task(self, task, dependencies):
        self.calls += 1
        return pa.table({"id": [self.calls]})

    def publish(self, plan, outputs, lease, run_id):
        return "g1"


def _plan():
    return plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=dependency_graph(
            sources=("events",), definition_hash="definition", partitions=("p1",)
        ),
    )


def test_runtime_facade_has_explicit_stable_exports():
    assert runtime.__all__ == sorted(runtime.__all__)
    assert {"run_plan", "run_serialized_plan", "find_run_by_request_key"}.issubset(runtime.__all__)


def test_run_plan_requires_executor_for_new_work():
    with pytest.raises(TerrineError) as error:
        runtime.run_plan(_plan(), con=duckdb.connect(":memory:"), namespace="owner")
    assert error.value.code == "terrine.runtime.executor_unavailable"


def test_run_serialized_plan_executes_through_python_facade():
    con = duckdb.connect(":memory:")
    executor = _Executor()

    receipt = runtime.run_serialized_plan(
        _plan().serialize(), con=con, namespace="owner", executor=executor
    )

    assert isinstance(receipt, RunReceipt)
    assert receipt.state == "succeeded"
    assert executor.calls == len(_plan().tasks)


def test_published_generation_uses_receipt_generation_not_current_manifest():
    from terrine.runtime.backends.base import StagedGeneration
    from terrine.runtime.backends.duckdb import DuckDBBackend

    con = duckdb.connect(":memory:")

    class Publisher(_Executor):
        def __init__(self):
            super().__init__()
            self.generation = 0

        def publish(self, plan, outputs, lease, run_id):
            self.generation += 1
            generation = f"g{self.generation}"
            return DuckDBBackend(con, namespace="owner").publish_generation(
                StagedGeneration(
                    plan.dataset,
                    generation,
                    {task_id: output for task_id, output in outputs.items()},
                    run_id=run_id,
                    snapshot_id=plan.snapshot_id,
                ),
                expected_manifest="g1" if generation == "g2" else None,
                lease=lease,
            )

    publisher = Publisher()
    first = runtime.run_plan(_plan(), con=con, namespace="owner", executor=publisher)
    second = runtime.run_plan(_plan(), con=con, namespace="owner", executor=publisher)
    exact = runtime.open_published_generation(first, con=con, namespace="owner")

    assert isinstance(exact, ibis.Table)
    assert exact.execute().to_dict(orient="records") == [{"id": len(_plan().tasks)}]
    assert second.published_generation == "g2"


def test_request_key_replay_is_available_without_executor():
    con = duckdb.connect(":memory:")
    destination = DestinationRef("out.duckdb", "analytics", "orders")
    executor = _Executor()
    first = runtime.run_plan(
        _plan(),
        con=con,
        namespace="owner",
        request_key="response-lost",
        destination=destination,
        executor=executor,
    )

    replay = runtime.run_plan(
        _plan(),
        con=con,
        namespace="owner",
        request_key="response-lost",
        destination=destination,
    )

    assert replay == first
    assert executor.calls == len(_plan().tasks)


def test_public_request_replay_refuses_a_different_plan_without_executor():
    con = duckdb.connect(":memory:")
    destination = DestinationRef("out.duckdb", "analytics", "orders")
    runtime.run_plan(
        _plan(),
        con=con,
        namespace="owner",
        request_key="same-key",
        destination=destination,
        executor=_Executor(),
    )
    changed = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="other-definition",
        snapshot_id="snapshot",
        graph=dependency_graph(
            sources=("events",), definition_hash="other-definition", partitions=("p1",)
        ),
    )

    with pytest.raises(TerrineError) as error:
        runtime.run_plan(
            changed,
            con=con,
            namespace="owner",
            request_key="same-key",
            destination=destination,
        )

    assert error.value.code == "terrine.runtime.request_key_conflict"


def test_empty_refresh_plan_reuses_current_generation_result_selection():
    from terrine.runtime.backends.base import StagedGeneration
    from terrine.runtime.backends.duckdb import DuckDBBackend
    from terrine.runtime.changes import ChangeSet

    con = duckdb.connect(":memory:")
    destination = DestinationRef("out.duckdb", "analytics", "orders")

    class Publisher(_Executor):
        def __init__(self):
            super().__init__()
            self.generation = 0

        def publish(self, plan, outputs, lease, run_id):
            backend = DuckDBBackend(con, namespace="owner")
            if not outputs:
                row = con.execute(
                    'SELECT generation,version FROM "owner".runtime_manifests WHERE dataset=?',
                    [plan.dataset],
                ).fetchone()
                assert row is not None
                generation, version = row
                con.execute(
                    'INSERT INTO "owner".runtime_run_publications VALUES (?, ?, ?, ?)',
                    [run_id, plan.dataset, generation, version],
                )
                return generation
            self.generation += 1
            generation = f"g{self.generation}"
            return backend.publish_generation(
                StagedGeneration(
                    plan.dataset,
                    generation,
                    outputs,
                    run_id=run_id,
                    snapshot_id=plan.snapshot_id,
                ),
                expected_manifest="g1" if generation == "g2" else None,
                lease=lease,
            )

    executor = Publisher()
    first_plan = _plan()
    first = runtime.run_plan(
        first_plan,
        con=con,
        namespace="owner",
        request_key="initial",
        destination=destination,
        executor=executor,
    )
    changed_graph = dependency_graph(
        sources=("events", "exports"), definition_hash="definition", partitions=("p1",)
    )
    changed = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=changed_graph,
    )
    before_changed = executor.calls
    second = runtime.run_plan(
        changed,
        con=con,
        namespace="owner",
        request_key="include-exports",
        destination=destination,
        executor=executor,
    )
    no_op = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=changed_graph,
        changes=ChangeSet((), "revision"),
    )
    receipt = runtime.run_plan(
        no_op,
        con=con,
        namespace="owner",
        request_key="no-op",
        destination=destination,
        executor=executor,
    )
    dependencies = {dependency for task in changed.tasks for dependency in task.dependencies}
    sink_indexes = [
        index + 1 for index, task in enumerate(changed.tasks) if task.task_id not in dependencies
    ]
    expected_second = [{"id": before_changed + index} for index in sink_indexes]

    assert receipt.state == "succeeded"
    assert receipt.published_generation == second.published_generation == "g2"
    assert executor.calls == before_changed + len(changed.tasks)
    assert runtime.open_published_generation(first, con=con, namespace="owner").execute().to_dict(
        orient="records"
    ) == [{"id": len(first_plan.tasks)}]
    assert sorted(
        runtime.open_published_generation(second, con=con, namespace="owner")
        .execute()
        .to_dict(orient="records"),
        key=lambda row: row["id"],
    ) == sorted(expected_second, key=lambda row: row["id"])
    assert sorted(
        runtime.open_published_generation(receipt, con=con, namespace="owner")
        .execute()
        .to_dict(orient="records"),
        key=lambda row: row["id"],
    ) == sorted(expected_second, key=lambda row: row["id"])
