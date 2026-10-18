import os
from pathlib import Path

import duckdb
import pyarrow as pa

from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.state import RuntimeStore
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


def test_cancel_waits_on_publication_boundary(monkeypatch):
    import threading

    import terrine.runtime.status as status_module

    con = duckdb.connect(":memory:")
    plan = plan_refresh(
        namespace="owner",
        dataset="cancel-boundary",
        definition_hash="d",
        snapshot_id="s",
        graph=dependency_graph(sources=("events",), definition_hash="d", partitions=("p",)),
    )
    publishing = threading.Event()
    finish = threading.Event()
    cancel_at_boundary = threading.Event()
    results = []

    def publish(*args):
        publishing.set()
        assert finish.wait(timeout=5)
        return "g1"

    real_lock = status_module.run_publication_lock

    class ObservedLock:
        def __init__(self, lock):
            self.lock = lock

        def __enter__(self):
            if not self.lock.acquire(blocking=False):
                cancel_at_boundary.set()
                self.lock.acquire()
            return self.lock

        def __exit__(self, exc_type, exc_value, traceback):
            self.lock.release()

    def observed_lock(namespace, dataset):
        return ObservedLock(real_lock(namespace, dataset))

    monkeypatch.setattr(status_module, "run_publication_lock", observed_lock)

    def run():
        results.append(
            execute_plan(
                plan=plan,
                con=con,
                namespace="owner",
                run_id="cancel-boundary",
                run_task=lambda task, dependencies: pa.table({"id": [1]}),
                publish=publish,
            )
        )

    executor = threading.Thread(target=run)
    executor.start()
    assert publishing.wait(timeout=5)
    endpoint = ControlEndpoint(Path(f"/tmp/terrine-boundary-{os.getpid()}.sock"))
    server = ControlServer(
        RuntimeStore(con, namespace="owner"), namespace="owner", endpoint=endpoint
    )
    server.start()
    cancellation = []
    canceller = threading.Thread(
        target=lambda: cancellation.append(
            ControlClient(endpoint, namespace="owner").cancel_run("cancel-boundary")
        )
    )
    canceller.start()
    assert cancel_at_boundary.wait(timeout=5)
    finish.set()
    executor.join(timeout=5)
    canceller.join(timeout=5)
    server.close()
    assert not executor.is_alive() and not canceller.is_alive()
    assert results[0].state == "succeeded"
    assert cancellation[0].disposition == "already_terminal"


def test_reexecution_recovers_pending_cancellation():
    con = duckdb.connect(":memory:")
    plan = plan_refresh(
        namespace="owner",
        dataset="recover-cancel",
        definition_hash="d",
        snapshot_id="s",
        graph=dependency_graph(sources=("events",), definition_hash="d", partitions=("p",)),
    )
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, "
        "fingerprint VARCHAR NOT NULL, state VARCHAR NOT NULL, published_generation VARCHAR, "
        "attempt_fence BIGINT, updated_at TIMESTAMP, heartbeat_at TIMESTAMP)"
    )
    con.execute(
        "INSERT INTO owner.runtime_runs VALUES (?, ?, ?, 'cancel_requested', "
        "NULL, NULL, NULL, NULL)",
        ["recover-cancel", plan.dataset, plan.recipe.fingerprint],
    )
    ran = []
    receipt = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="recover-cancel",
        run_task=lambda task, dependencies: ran.append(task),
        publish=lambda *args: "unexpected",
    )
    assert receipt.state == "cancelled"
    assert ran == []
    replay = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="recover-cancel",
        run_task=lambda task, dependencies: ran.append(task),
        publish=lambda *args: "unexpected",
    )
    assert replay.state == "cancelled"
    assert ran == []
