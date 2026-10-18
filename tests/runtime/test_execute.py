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


def test_runtime_budget_is_cumulative_across_final_frame_outputs():
    from terrine.data import Budget

    con = duckdb.connect(":memory:")
    graph = dependency_graph(sources=("events",), definition_hash="def", partitions=("p1", "p2"))
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="def",
        snapshot_id="s",
        graph=graph,
        budget=Budget(max_rows=1),
    )

    with pytest.raises(Exception) as error:
        execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id="over-budget",
            run_task=lambda task, dependencies: pa.table({"id": [1]}),
            publish=lambda plan, outputs, lease: "g1",
        )

    assert getattr(error.value, "code", None) == "terrine.budget.rows_exceeded"


def test_expired_attempt_cannot_fail_or_overwrite_successful_retry():
    from datetime import UTC, datetime, timedelta

    con = duckdb.connect(":memory:")
    plan = _plan()
    retried = False

    def run_task(task, dependencies):
        nonlocal retried
        if not retried:
            retried = True
            con.execute(
                'UPDATE "owner".runtime_leases SET expires_at=? WHERE resource=?',
                [(datetime.now(UTC) - timedelta(seconds=1)).isoformat(), "dataset:orders"],
            )
            execute_plan(
                plan,
                con=con,
                namespace="owner",
                run_id="same-run",
                run_task=lambda task, dependencies: pa.table({"id": [2]}),
                publish=lambda plan, outputs, lease: "g2",
                lease_ttl=timedelta(seconds=1),
            )
            return pa.table({"id": [1]})
        return pa.table({"id": [1]})

    with pytest.raises(ValueError, match="lease"):
        execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id="same-run",
            run_task=run_task,
            publish=lambda plan, outputs, lease: "g1",
            lease_ttl=timedelta(seconds=1),
        )

    row = con.execute(
        'SELECT state, published_generation FROM "owner".runtime_runs WHERE run_id=?',
        ["same-run"],
    ).fetchone()
    assert row is not None
    state, generation = row
    assert (state, generation) == ("succeeded", "g2")


def test_existing_runtime_budget_refusal_codes_are_used():
    from terrine.data import Budget
    from terrine.errors import TerrineError

    con = duckdb.connect(":memory:")
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="def",
        snapshot_id="s",
        graph=dependency_graph(sources=("events",), definition_hash="def", partitions=("p1",)),
        budget=Budget(max_rows=0),
    )
    with pytest.raises(TerrineError) as error:
        execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id="coded-budget",
            run_task=lambda task, dependencies: pa.table({"id": [1]}),
            publish=lambda plan, outputs, lease: "g1",
        )
    assert error.value.code == "terrine.budget.rows_exceeded"

    byte_limited_plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="def",
        snapshot_id="s",
        graph=dependency_graph(sources=("events",), definition_hash="def", partitions=("p1",)),
        budget=Budget(max_output_bytes=0),
    )
    with pytest.raises(TerrineError) as byte_error:
        execute_plan(
            byte_limited_plan,
            con=duckdb.connect(":memory:"),
            namespace="owner",
            run_id="coded-byte-budget",
            run_task=lambda task, dependencies: pa.table({"id": [1]}),
            publish=lambda plan, outputs, lease: "g1",
        )
    assert byte_error.value.code == "terrine.budget.output_bytes_exceeded"


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


def test_failed_run_retries_to_a_persisted_success_receipt():
    con = duckdb.connect(":memory:")
    plan = _plan()
    calls = 0
    publish_calls = []

    def run_task(task, dependencies):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("initial failure")
        return pa.table({"id": [1]})

    with pytest.raises(RuntimeError, match="initial failure"):
        execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id="retry-run",
            run_task=run_task,
            publish=lambda plan, outputs, lease: "g1",
        )

    result = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="retry-run",
        run_task=run_task,
        publish=lambda plan, outputs, lease: publish_calls.append("g1") or "g1",
    )
    replay = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="retry-run",
        run_task=run_task,
        publish=lambda plan, outputs, lease: publish_calls.append("unexpected"),
    )
    assert result.state == "succeeded"
    assert replay == result
    assert publish_calls == ["g1"]


def test_lease_heartbeat_keeps_long_task_alive(monkeypatch):
    import threading
    from datetime import timedelta

    from terrine.runtime.leases import LeaseStore

    con = duckdb.connect(":memory:")
    renewed = threading.Event()
    original_renew = LeaseStore.renew

    def renew(store, lease, *, ttl, now):
        result = original_renew(store, lease, ttl=ttl, now=now)
        renewed.set()
        return result

    monkeypatch.setattr(LeaseStore, "renew", renew)

    def run_task(task, dependencies):
        assert renewed.wait(timeout=3)
        return pa.table({"id": [1]})

    result = execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="long-task",
        run_task=run_task,
        publish=lambda plan, outputs, lease: "g1",
        lease_ttl=timedelta(seconds=30),
    )
    assert result.state == "succeeded"


def test_delayed_lease_claim_cannot_overwrite_successful_retry(monkeypatch):
    from datetime import UTC, datetime, timedelta

    from terrine.runtime.leases import LeaseStore

    con = duckdb.connect(":memory:")
    plan = _plan()
    original_acquire = LeaseStore.acquire
    acquisitions = 0

    def delayed_acquire(store, resource, owner, *, ttl, now):
        nonlocal acquisitions
        lease = original_acquire(store, resource, owner, ttl=ttl, now=now)
        acquisitions += 1
        if acquisitions == 1:
            con.execute(
                'UPDATE "owner".runtime_leases SET expires_at=? WHERE resource=?',
                [(datetime.now(UTC) - timedelta(seconds=1)).isoformat(), resource],
            )
            execute_plan(
                plan,
                con=con,
                namespace="owner",
                run_id="delayed-claim",
                run_task=lambda task, dependencies: pa.table({"id": [2]}),
                publish=lambda plan, outputs, lease: "g2",
            )
        return lease

    monkeypatch.setattr(LeaseStore, "acquire", delayed_acquire)

    def unexpected_task(task, dependencies) -> pa.Table:
        raise AssertionError("stale attempt must not execute")

    def unexpected_publish(plan, outputs, lease) -> str:
        raise AssertionError("stale attempt must not publish")

    result = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="delayed-claim",
        run_task=unexpected_task,
        publish=unexpected_publish,
    )
    assert (result.state, result.published_generation) == ("succeeded", "g2")


def test_intermediate_partition_sizes_do_not_consume_final_output_budget():
    from terrine.data import Budget

    graph = dependency_graph(sources=("events",), definition_hash="def", partitions=("p1", "p2"))
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="def",
        snapshot_id="s",
        graph=graph,
        budget=Budget(max_rows=2),
    )
    result = execute_plan(
        plan,
        con=duckdb.connect(":memory:"),
        namespace="owner",
        run_id="intermediate-budget",
        run_task=lambda task, dependencies: pa.table(
            {"id": [1]} if task.layer == "frame" else {"id": list(range(100))}
        ),
        publish=lambda plan, outputs, lease: "g1",
    )
    assert result.state == "succeeded"
