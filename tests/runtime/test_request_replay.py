from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.leases import LeaseStore
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef


def _plan(definition_hash="def"):
    graph = dependency_graph(
        sources=("events",), definition_hash=definition_hash, partitions=("p1",)
    )
    return plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash=definition_hash,
        snapshot_id="snapshot",
        graph=graph,
    )


def test_request_key_replays_same_fingerprint_for_same_destination():
    con = duckdb.connect(":memory:")
    destination = DestinationRef("out.duckdb", "analytics", "orders")
    published = []
    first = execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="run-1",
        request_key="client-request",
        destination=destination,
        run_task=lambda task, dependencies: pa.table({"id": [1]}),
        publish=lambda plan, outputs, lease: published.append("g1") or "g1",
    )
    leases = LeaseStore(con, namespace="owner")
    active_lease = leases.acquire(
        "dataset:orders",
        "other-run",
        ttl=timedelta(minutes=1),
        now=datetime.now(UTC),
    )
    replay = execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="lost-response-retry",
        request_key="client-request",
        destination=destination,
        run_task=lambda task, dependencies: pa.table({"id": [2]}),
        publish=lambda plan, outputs, lease: published.append("unexpected"),
    )
    leases.release(active_lease)

    assert replay == first
    assert published == ["g1"]


def test_request_key_changed_fingerprint_refuses():
    con = duckdb.connect(":memory:")
    destination = DestinationRef("out.duckdb", "analytics", "orders")
    execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="run-1",
        request_key="client-request",
        destination=destination,
        run_task=lambda task, dependencies: pa.table({"id": [1]}),
        publish=lambda plan, outputs, lease: "g1",
    )

    with pytest.raises(ValueError):
        execute_plan(
            _plan("changed"),
            con=con,
            namespace="owner",
            run_id="run-2",
            request_key="client-request",
            destination=destination,
            run_task=lambda task, dependencies: pa.table({"id": [2]}),
            publish=lambda plan, outputs, lease: "g2",
        )


def test_request_key_is_scoped_to_destination():
    con = duckdb.connect(":memory:")
    destinations = (
        DestinationRef("out.duckdb", "analytics", "orders"),
        DestinationRef("out.duckdb", "warehouse", "orders"),
    )
    receipts = []
    for run_id, destination in zip(("run-1", "run-2"), destinations, strict=True):
        receipts.append(
            execute_plan(
                _plan(),
                con=con,
                namespace="owner",
                run_id=run_id,
                request_key="same-key",
                destination=destination,
                run_task=lambda task, dependencies: pa.table({"id": [1]}),
                publish=lambda plan, outputs, lease, generation=run_id: generation,
            )
        )

    assert [receipt.run_id for receipt in receipts] == ["run-1", "run-2"]


def test_unadmitted_request_waiting_on_dataset_lease_is_retryable():
    con = duckdb.connect(":memory:")
    leases = LeaseStore(con, namespace="owner")
    leases.initialize()
    active = leases.acquire(
        "dataset:orders",
        "active-run",
        ttl=timedelta(minutes=1),
        now=datetime.now(UTC),
    )
    destination = DestinationRef("out.duckdb", "analytics", "orders")
    with pytest.raises(ValueError):
        execute_plan(
            _plan(),
            con=con,
            namespace="owner",
            run_id="new-run",
            request_key="waiting-request",
            destination=destination,
            run_task=lambda task, dependencies: pa.table({"id": [1]}),
            publish=lambda plan, outputs, lease: "g1",
        )
    row = con.execute(
        'SELECT state FROM "owner".runtime_runs WHERE run_id=?', ["new-run"]
    ).fetchone()
    assert row is None
    leases.release(active)
    receipt = execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="new-run",
        request_key="waiting-request",
        destination=destination,
        run_task=lambda task, dependencies: pa.table({"id": [1]}),
        publish=lambda plan, outputs, lease: "g1",
    )

    assert receipt.state == "succeeded"


def test_owner_api_recovers_interrupted_matching_key_before_replay():
    from functools import partial

    from terrine.runtime.api import run_plan

    class Executor:
        fail = True

        def run_task(self, task, dependencies):
            if self.fail:
                raise RuntimeError("owner interrupted")
            return pa.table({"id": [task.task_id]})

        def publish(self, plan, outputs, lease, run_id):
            return "g1"

    con = duckdb.connect(":memory:")
    try:
        executor = Executor()
        run = partial(
            run_plan,
            con=con,
            namespace="owner",
            request_key="retry",
            destination=DestinationRef("out.duckdb", "analytics", "orders"),
            executor=executor,
        )
        with pytest.raises(RuntimeError):
            run(_plan())
        original = con.execute("SELECT run_id,attempt_fence FROM owner.runtime_runs").fetchone()
        assert original is not None
        run_id, fence = original
        con.execute("UPDATE owner.runtime_runs SET state='running' WHERE run_id=?", [run_id])
        executor.fail = False
        result = run(_plan())
        assert result.state == "succeeded" and result.run_id == run_id
        renewed = con.execute("SELECT attempt_fence FROM owner.runtime_runs").fetchone()
        assert renewed is not None and renewed[0] > fence
    finally:
        con.close()
