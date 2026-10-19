from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.leases import LeaseStore
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


def test_publication_reference_replacement_requires_current_lease():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    leases = LeaseStore(con, namespace="owner")
    leases.initialize()
    stale = leases.acquire("dataset:orders", "old", ttl=timedelta(minutes=1), now=datetime.now(UTC))
    leases.release(stale)
    with pytest.raises(ValueError, match="lease"):
        store.replace_published_snapshot("orders", "g1", "snapshot", lease=stale)
    assert store.snapshot_references("snapshot") == ()


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
        assert store.snapshot_references("snapshot") == (("active_run", '["run-1",1]'),)
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
    assert store.snapshot_references("snapshot") == (("published_manifest", '["orders","g1"]'),)
    store.pin_snapshot("snapshot", kind="active_run", reference='["run-1",99]')
    execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="run-1",
        run_task=lambda task, dependencies: pa.table({"task": [task.task_id]}),
        publish=publish,
    )
    assert store.snapshot_references("snapshot") == (("published_manifest", '["orders","g1"]'),)


def test_terminal_snapshot_cleanup_retries_transaction_conflict(monkeypatch):
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
    original = RuntimeStore.unpin_snapshot
    attempts = 0

    def conflict_once(self, snapshot_id, *, kind, reference):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise duckdb.TransactionException("concurrent snapshot guard update")
        return original(self, snapshot_id, kind=kind, reference=reference)

    monkeypatch.setattr(RuntimeStore, "unpin_snapshot", conflict_once)
    result = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="run-1",
        run_task=lambda task, dependencies: pa.table({"task": [task.task_id]}),
        publish=lambda plan, outputs, lease: "g1",
    )
    assert result.state == "succeeded"
    assert attempts == 2
    assert store.snapshot_references("snapshot") == (("published_manifest", '["orders","g1"]'),)


def test_gc_reconciles_terminal_run_snapshot_references_after_cleanup_exhaustion(monkeypatch):
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

    def conflict(*args, **kwargs):
        raise duckdb.TransactionException("concurrent snapshot guard update")

    with monkeypatch.context() as scoped:
        scoped.setattr(RuntimeStore, "unpin_snapshot", conflict)
        result = execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id="run-1",
            run_task=lambda task, dependencies: pa.table({"task": [task.task_id]}),
            publish=lambda plan, outputs, lease: "g1",
        )
        assert result.state == "succeeded"
        assert store.snapshot_references("snapshot") == (
            ("active_run", '["run-1",1]'),
            ("published_manifest", '["orders","g1"]'),
        )

    store.release_terminal_run_snapshot_references()
    assert store.snapshot_references("snapshot") == (("published_manifest", '["orders","g1"]'),)


def test_retryable_failed_run_keeps_snapshot_until_successful_retry():
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
    with pytest.raises(RuntimeError, match="task failed"):
        execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id="retry",
            run_task=lambda task, dependencies: (_ for _ in ()).throw(RuntimeError("task failed")),
            publish=lambda plan, outputs, lease: "g1",
        )
    assert store.snapshot_references("snapshot") == (("active_run", '["retry",1]'),)
    execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="retry",
        run_task=lambda task, dependencies: pa.table({"id": [1]}),
        publish=lambda plan, outputs, lease: "g1",
    )
    assert store.snapshot_references("snapshot") == (("published_manifest", '["orders","g1"]'),)


def test_string_publisher_moves_manifest_pin_between_snapshots():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    graph = dependency_graph(sources=("events",), definition_hash="def", partitions=("p1",))
    for index, snapshot in enumerate(("snapshot-1", "snapshot-2"), start=1):
        plan = plan_refresh(
            namespace="owner",
            dataset="orders",
            definition_hash="def",
            snapshot_id=snapshot,
            graph=graph,
        )
        execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id=f"run-{index}",
            run_task=lambda task, dependencies, index=index: pa.table({"id": [index]}),
            publish=lambda plan, outputs, lease, index=index: f"g{index}",
        )
    assert store.snapshot_references("snapshot-1") == ()
    assert store.snapshot_references("snapshot-2") == (("published_manifest", '["orders","g2"]'),)


def test_collected_snapshot_pin_failure_records_run_and_releases_lease():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    store.mark_snapshot_for_gc("snapshot")
    graph = dependency_graph(sources=("events",), definition_hash="def", partitions=("p1",))
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="def",
        snapshot_id="snapshot",
        graph=graph,
    )
    with pytest.raises(ValueError, match="collected"):
        execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id="collected-run",
            run_task=lambda task, dependencies: pa.table({"id": [1]}),
            publish=lambda plan, outputs, lease: "g1",
        )
    run = con.execute(
        'SELECT state FROM "owner".runtime_runs WHERE run_id=?', ["collected-run"]
    ).fetchone()
    assert run is not None and run[0] == "failed"
    assert con.execute('SELECT resource FROM "owner".runtime_leases').fetchall() == []
    from terrine.runtime.events import read_run_events

    lifecycle = read_run_events("collected-run", con=con, namespace="owner").events
    assert [event.payload["state"] for event in lifecycle if event.kind == "lifecycle"] == [
        "queued",
        "running",
        "failed",
    ]


def test_cancel_during_snapshot_pin_failure_does_not_emit_false_failure(monkeypatch):
    from terrine.runtime.events import read_run_events
    from terrine.runtime.status import cancel_run, get_run

    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    store.mark_snapshot_for_gc("snapshot")
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="def",
        snapshot_id="snapshot",
        graph=dependency_graph(sources=("events",), definition_hash="def", partitions=("p1",)),
    )
    original_pin = RuntimeStore.pin_snapshot

    def cancel_then_pin(runtime_store, snapshot_id, *, kind, reference):
        cancel_run("cancel-at-pin", con=con, namespace="owner")
        return original_pin(runtime_store, snapshot_id, kind=kind, reference=reference)

    monkeypatch.setattr(RuntimeStore, "pin_snapshot", cancel_then_pin)
    with pytest.raises(ValueError, match="collected"):
        execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id="cancel-at-pin",
            run_task=lambda task, dependencies: pa.table({"id": [1]}),
            publish=lambda plan, outputs, lease: "g1",
        )

    status = get_run("cancel-at-pin", con=con, namespace="owner")
    events = read_run_events("cancel-at-pin", con=con, namespace="owner").events
    assert status.durable_state == "cancelled"
    assert [event.payload["state"] for event in events if event.kind == "lifecycle"] == [
        "queued",
        "running",
        "cancel_requested",
        "cancelled",
    ]
