import duckdb
import pyarrow as pa

from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.status import cancel_run


def test_durable_cancellation_before_publication_prevents_generation():
    con = duckdb.connect(":memory:")
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="d",
        snapshot_id="s",
        graph=dependency_graph(sources=("events",), definition_hash="d", partitions=("p",)),
    )
    published = []

    def run_task(task, dependencies):
        cancel_run("cancel-me", con=con, namespace="owner")
        return pa.table({"id": [1]})

    result = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="cancel-me",
        run_task=run_task,
        publish=lambda *args: published.append("published"),
    )
    assert result.state == "cancelled"
    assert published == []


def test_publication_wins_when_it_has_entered_serialized_boundary():
    import threading

    con = duckdb.connect(":memory:")
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="d",
        snapshot_id="s",
        graph=dependency_graph(sources=("events",), definition_hash="d", partitions=("p",)),
    )
    started = threading.Event()
    finish = threading.Event()
    results = []

    def publish(*args):
        started.set()
        assert finish.wait(timeout=5)
        return "g1"

    def run():
        results.append(
            execute_plan(
                plan,
                con=con,
                namespace="owner",
                run_id="publish-wins",
                run_task=lambda task, dependencies: pa.table({"id": [1]}),
                publish=publish,
            )
        )

    executor = threading.Thread(target=run)
    executor.start()
    assert started.wait(timeout=5)
    cancellation = []
    canceller = threading.Thread(
        target=lambda: cancellation.append(cancel_run("publish-wins", con=con, namespace="owner"))
    )
    canceller.start()
    finish.set()
    executor.join(timeout=5)
    canceller.join(timeout=5)
    assert not executor.is_alive() and not canceller.is_alive()
    assert results[0].state == "succeeded"
    assert cancellation[0].disposition == "already_terminal"
