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


def test_execution_persists_lifecycle_events_for_terminal_replay():
    from terrine.runtime.events import read_run_events

    con = duckdb.connect(":memory:")
    execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="evented-run",
        run_task=lambda task, dependencies: pa.table({"task": [task.task_id]}),
        publish=lambda plan, outputs, lease: "g1",
    )

    events = read_run_events("evented-run", con=con, namespace="owner").events
    assert [event.payload["state"] for event in events if event.kind == "lifecycle"] == [
        "queued",
        "running",
        "succeeded",
    ]


def test_prefix_runner_receives_daily_output_for_certificate_plan():
    from types import SimpleNamespace

    con = duckdb.connect(":memory:")
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="def",
        snapshot_id="s",
        graph=dependency_graph(
            sources=("events",),
            design=SimpleNamespace(spec_hash="def", shape=object()),
            partitions=("p1",),
        ),
    )
    observed = {}

    def run_task(task, dependencies):
        if task.layer == "prefix":
            observed.update(dependencies)
        return pa.table({"layer": [task.layer]})

    execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="prefix-dependency",
        run_task=run_task,
        publish=lambda plan, outputs, lease: "g1",
    )
    assert len(observed) == 1
    assert next(iter(observed.values())).to_pylist() == [{"layer": "daily"}]


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


def test_post_commit_publish_error_reconciles_run_and_retains_snapshot():
    from terrine.runtime.backends.base import StagedGeneration
    from terrine.runtime.backends.duckdb import DuckDBBackend
    from terrine.runtime.state import RuntimeStore

    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    backend = DuckDBBackend(con, namespace="owner")

    def publish(plan, outputs, lease):
        backend.fault_injector = lambda boundary: (
            (_ for _ in ()).throw(RuntimeError("lost response"))
            if boundary == "after_commit"
            else None
        )
        return backend.publish_generation(
            StagedGeneration(
                plan.dataset,
                "generation-1",
                {"all": next(iter(outputs.values()))},
                run_id="ambiguous-run",
            ),
            expected_manifest=None,
            lease=lease,
        )

    result = execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="ambiguous-run",
        run_task=lambda task, dependencies: pa.table({"id": [1]}),
        publish=publish,
    )
    assert result.state == "succeeded"
    assert result.published_generation == "generation-1"
    assert store.snapshot_references("s") == (("published_manifest", '["orders","generation-1"]'),)


def test_post_commit_empty_refresh_restores_previous_result_partitions():
    from terrine.runtime.backends.base import StagedGeneration
    from terrine.runtime.backends.duckdb import DuckDBBackend
    from terrine.runtime.changes import ChangeSet

    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    first = execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="prior-run",
        run_task=lambda task, dependencies: pa.table({"id": [7]}),
        publish=lambda plan, outputs, lease: backend.publish_generation(
            StagedGeneration(
                plan.dataset,
                "generation-old",
                {"all": next(iter(outputs.values()))},
                snapshot_id=plan.snapshot_id,
                run_id="prior-run",
            ),
            expected_manifest=None,
            lease=lease,
        ),
    )
    assert first.state == "succeeded"
    con.execute(
        "CREATE TABLE IF NOT EXISTS owner.runtime_run_results "
        "(run_id VARCHAR NOT NULL, partition VARCHAR NOT NULL, "
        "PRIMARY KEY(run_id,partition))"
    )
    con.execute("INSERT INTO owner.runtime_run_results VALUES ('prior-run','all')")
    assert con.execute(
        "SELECT dataset,generation FROM owner.runtime_run_publications WHERE run_id='prior-run'"
    ).fetchone() == ("orders", "generation-old")
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="def",
        snapshot_id="s",
        graph=dependency_graph(sources=("events",), definition_hash="def", partitions=("p1",)),
        changes=ChangeSet((), "revision"),
    )

    def publish_empty(plan, outputs, lease):
        backend.fault_injector = lambda boundary: (
            (_ for _ in ()).throw(RuntimeError("lost response"))
            if boundary == "after_commit"
            else None
        )
        return backend.publish_generation(
            StagedGeneration(
                "orders",
                "generation-empty",
                {"all": pa.table({"id": [7]})},
                snapshot_id="s",
                run_id="empty-run",
            ),
            expected_manifest="generation-old",
            lease=lease,
        )

    result = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="empty-run",
        run_task=lambda task, dependencies: pa.table({"id": []}),
        publish=publish_empty,
    )
    assert result.state == "succeeded"
    assert ("all",) in con.execute(
        "SELECT partition FROM owner.runtime_run_results WHERE run_id='empty-run'"
    ).fetchall()

    same_generation = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="same-generation-run",
        run_task=lambda task, dependencies: pa.table({"id": []}),
        publish=lambda current_plan, outputs, lease: backend.publish_generation(
            StagedGeneration(
                "orders",
                "generation-empty",
                {"all": pa.table({"id": [7]})},
                snapshot_id="s",
                run_id="same-generation-run",
            ),
            expected_manifest="generation-empty",
            lease=lease,
        ),
    )
    assert same_generation.state == "succeeded"
    assert ("all",) in con.execute(
        "SELECT partition FROM owner.runtime_run_results WHERE run_id='same-generation-run'"
    ).fetchall()


def test_expected_base_manifest_mismatch_refuses_before_source_work():
    con = duckdb.connect(":memory:")
    plan = _plan().bind_base_manifest("generation-1")
    con.execute(
        "CREATE SCHEMA owner; CREATE TABLE owner.runtime_manifests "
        "(dataset VARCHAR PRIMARY KEY,generation VARCHAR NOT NULL,version BIGINT NOT NULL)"
    )
    con.execute("INSERT INTO owner.runtime_manifests VALUES ('orders','generation-2',2)")
    work = []
    with pytest.raises(ValueError, match="expected base manifest"):
        execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id="stale-base",
            run_task=lambda task, dependencies: work.append(task.task_id),
            publish=lambda plan, outputs, lease: "g3",
        )
    assert work == []

    assert con.execute(
        "SELECT state FROM owner.runtime_runs WHERE run_id='stale-base'"
    ).fetchone() == ("failed",)


def test_overlapping_saved_base_replay_returns_terminal_receipt(monkeypatch):
    from terrine.runtime.leases import LeaseStore

    con = duckdb.connect(":memory:")
    plan = _plan().bind_base_manifest("generation-1")
    con.execute(
        "CREATE SCHEMA owner; CREATE TABLE owner.runtime_manifests "
        "(dataset VARCHAR PRIMARY KEY,generation VARCHAR NOT NULL,version BIGINT NOT NULL)"
    )
    con.execute("INSERT INTO owner.runtime_manifests VALUES ('orders','generation-1',1)")
    acquire = LeaseStore.acquire

    def finish_before_lease(self, resource, owner, **kwargs):
        con.execute(
            "UPDATE owner.runtime_manifests SET generation='generation-2',version=2 "
            "WHERE dataset='orders'"
        )
        con.execute(
            "UPDATE owner.runtime_runs SET state='succeeded',published_generation='generation-2' "
            "WHERE run_id='overlap'"
        )
        return acquire(self, resource, owner, **kwargs)

    monkeypatch.setattr(LeaseStore, "acquire", finish_before_lease)
    calls = []
    receipt = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="overlap",
        run_task=lambda task, dependencies: (
            calls.append("task"),
            pa.table({"id": [task.task_id]}),
        )[1],
        publish=lambda plan, outputs, lease: (calls.append("publish"), "generation-3")[1],
    )
    assert receipt.state == "succeeded"
    assert receipt.published_generation == "generation-2"
    assert calls == []


def test_retry_reconciles_publication_before_rejecting_its_own_manifest():
    from datetime import UTC, datetime, timedelta

    from terrine.runtime.backends.base import StagedGeneration
    from terrine.runtime.backends.duckdb import DuckDBBackend
    from terrine.runtime.leases import LeaseStore

    con = duckdb.connect(":memory:")
    plan = _plan().bind_base_manifest("generation-1")
    backend = DuckDBBackend(con, namespace="owner")
    backend.initialize()
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR PRIMARY KEY,dataset VARCHAR,"
        "fingerprint VARCHAR,state VARCHAR,published_generation VARCHAR,attempt_fence BIGINT)"
    )
    con.execute(
        "INSERT INTO owner.runtime_manifests "
        "(dataset,generation,version,snapshot_id) VALUES ('orders','generation-1',1,NULL)"
    )
    con.execute("INSERT INTO owner.runtime_publications VALUES ('orders','generation-1',1)")
    con.execute(
        "INSERT INTO owner.runtime_runs VALUES ('ambiguous','orders',?,'running',NULL,1)",
        [plan.recipe.fingerprint],
    )
    leases = LeaseStore(con, namespace="owner")
    lease = leases.acquire(
        "dataset:orders", "ambiguous", ttl=timedelta(seconds=30), now=datetime.now(UTC)
    )
    backend.publish_generation(
        StagedGeneration(
            "orders",
            "generation-2",
            {"all": pa.table({"id": [1]})},
            run_id="ambiguous",
        ),
        expected_manifest="generation-1",
        lease=lease,
    )
    leases.release(lease)
    con.execute(
        "UPDATE owner.runtime_manifests SET generation='generation-3',version=3,"
        "snapshot_id='snapshot-b' WHERE dataset='orders'"
    )
    con.execute("INSERT INTO owner.runtime_publications VALUES ('orders','generation-3',3)")
    con.execute(
        "INSERT INTO owner.runtime_snapshot_manifests VALUES ('orders','generation-3','snapshot-b')"
    )
    con.execute(
        "INSERT INTO owner.runtime_snapshot_references "
        "VALUES ('snapshot-b','published_manifest','[\"orders\",\"generation-3\"]')"
    )

    result = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="ambiguous",
        run_task=lambda task, dependencies: pa.table({"id": [2]}),
        publish=lambda plan, outputs, lease: "wrong-generation",
    )

    assert con.execute(
        "SELECT generation,snapshot_id FROM owner.runtime_snapshot_manifests WHERE dataset='orders'"
    ).fetchone() == ("generation-3", "snapshot-b")
    assert con.execute(
        "SELECT snapshot_id FROM owner.runtime_snapshot_references "
        "WHERE kind='published_manifest' AND reference='[\"orders\",\"generation-3\"]'"
    ).fetchone() == ("snapshot-b",)

    assert result.state == "succeeded"
    assert result.published_generation == "generation-2"
    assert con.execute(
        "SELECT state FROM owner.runtime_runs WHERE run_id='ambiguous'"
    ).fetchone() == ("succeeded",)


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


def test_disjoint_partition_tasks_run_concurrently_with_configured_bound():
    import threading

    con = duckdb.connect(":memory:")
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="def",
        snapshot_id="s",
        graph=dependency_graph(sources=("events",), definition_hash="def", partitions=("p1", "p2")),
    )
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    active = 0
    peak = 0

    def run_task(task, dependencies):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            with con.cursor() as worker_con:
                row = worker_con.execute("SELECT ?", [task.partition]).fetchone()
            assert row is not None
            value = row[0]
            if task.partition in {"p1", "p2"}:
                barrier.wait(timeout=5)
            return pa.table({"task": [value]})
        finally:
            with lock:
                active -= 1

    execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="parallel-partitions",
        run_task=run_task,
        publish=lambda plan, outputs, lease: "g1",
        max_concurrency=2,
    )
    assert peak == 2


def test_successful_partition_output_is_persisted_when_sibling_fails(monkeypatch):
    import threading

    import terrine.runtime.execute as execute_module

    con = duckdb.connect(":memory:")
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="def",
        snapshot_id="s",
        graph=dependency_graph(
            sources=("events",), definition_hash="def", partitions=("p1", "p2", "p3")
        ),
    )
    release_sibling = threading.Event()
    calls = []
    original_wait = execute_module.wait

    def release_after_failure(futures, *, return_when):
        completed, pending = original_wait(futures, return_when=return_when)
        if any(future.exception() is not None for future in completed):
            release_sibling.set()
        return completed, pending

    monkeypatch.setattr(execute_module, "wait", release_after_failure)

    def fail_once(task, dependencies):
        calls.append(task.partition)
        if task.partition == "p1":
            raise RuntimeError("partition failed")
        if task.partition == "p2":
            assert release_sibling.wait(timeout=5)
        return pa.table({"task": [task.task_id]})

    with pytest.raises(RuntimeError, match="partition failed"):
        execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id="sibling-retry",
            run_task=fail_once,
            publish=lambda plan, outputs, lease: "g1",
            max_concurrency=2,
        )
    completed_before_retry = {
        row[0]
        for row in con.execute(
            "SELECT task_id FROM owner.runtime_tasks WHERE run_id=? AND status='complete'",
            ["sibling-retry"],
        ).fetchall()
    }
    assert completed_before_retry
    assert "p3" not in calls
    calls_after_failure = len(calls)
    receipt = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="sibling-retry",
        run_task=lambda task, dependencies: (
            calls.append(task.task_id) or pa.table({"task": [task.task_id]})
        ),
        publish=lambda plan, outputs, lease: "g1",
        max_concurrency=2,
    )
    assert receipt.state == "succeeded"
    assert not completed_before_retry.intersection(calls[calls_after_failure:])


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

    failed_row = con.execute(
        'SELECT attempt_fence FROM "owner".runtime_runs WHERE run_id=?', ["retry-run"]
    ).fetchone()
    assert failed_row is not None
    failed_fence = failed_row[0]
    result = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="retry-run",
        run_task=run_task,
        publish=lambda plan, outputs, lease: publish_calls.append("g1") or "g1",
    )
    succeeded_row = con.execute(
        'SELECT state, attempt_fence FROM "owner".runtime_runs WHERE run_id=?', ["retry-run"]
    ).fetchone()
    assert succeeded_row is not None
    succeeded_state, succeeded_fence = succeeded_row
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
    assert succeeded_state == "succeeded"
    assert succeeded_fence > failed_fence


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
    timestamps = con.execute(
        "SELECT updated_at, heartbeat_at FROM owner.runtime_runs WHERE run_id='long-task'"
    ).fetchone()
    assert timestamps is not None
    updated_at, heartbeat_at = timestamps
    assert updated_at is not None
    assert heartbeat_at is not None
    assert heartbeat_at <= updated_at


def test_cancellation_serializes_with_heartbeat_persistence(monkeypatch):
    import threading
    from datetime import timedelta

    from terrine.runtime.leases import LeaseStore
    from terrine.runtime.status import cancel_run

    con = duckdb.connect(":memory:")
    renewing = threading.Event()
    allow_renew = threading.Event()
    cancel_started = threading.Event()
    cancel_errors = []
    original_renew = LeaseStore.renew

    def renew(store, lease, *, ttl, now):
        renewing.set()
        assert allow_renew.wait(timeout=3)
        return original_renew(store, lease, ttl=ttl, now=now)

    monkeypatch.setattr(LeaseStore, "renew", renew)

    def run_task(task, dependencies):
        assert renewing.wait(timeout=3)

        def cancel():
            cancel_started.set()
            try:
                cancel_run("cancel-heartbeat", con=con.cursor(), namespace="owner")
            except Exception as error:
                cancel_errors.append(error)

        cancellation = threading.Thread(target=cancel)
        cancellation.start()
        assert cancel_started.wait(timeout=3)
        allow_renew.set()
        cancellation.join(timeout=3)
        assert not cancellation.is_alive()
        return pa.table({"id": [1]})

    receipt = execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="cancel-heartbeat",
        run_task=run_task,
        publish=lambda plan, outputs, lease: "g1",
        lease_ttl=timedelta(seconds=30),
    )

    assert cancel_errors == []
    assert receipt.state == "cancelled"


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


def test_stale_claim_during_running_takeover_preserves_newer_run_fence(monkeypatch):
    import threading
    from datetime import UTC, datetime, timedelta

    from terrine.runtime.leases import LeaseStore

    con = duckdb.connect(":memory:")
    plan = _plan()
    old_lease_acquired = threading.Event()
    resume_old_claim = threading.Event()
    new_attempt_running = threading.Event()
    finish_new_attempt = threading.Event()
    original_acquire = LeaseStore.acquire
    acquisition_count = 0
    old_result = []
    old_errors = []
    new_result = []
    new_errors = []

    def delayed_acquire(store, resource, owner, *, ttl, now):
        nonlocal acquisition_count
        lease = original_acquire(store, resource, owner, ttl=ttl, now=now)
        acquisition_count += 1
        if acquisition_count == 1:
            con.execute(
                'UPDATE "owner".runtime_leases SET expires_at=? WHERE resource=?',
                [(datetime.now(UTC) - timedelta(seconds=1)).isoformat(), resource],
            )
            old_lease_acquired.set()
            assert resume_old_claim.wait(timeout=5)
        return lease

    monkeypatch.setattr(LeaseStore, "acquire", delayed_acquire)

    def old_attempt():
        def unexpected_task(task, dependencies) -> pa.Table:
            raise AssertionError("stale claim must not execute")

        try:
            old_result.append(
                execute_plan(
                    plan,
                    con=con,
                    namespace="owner",
                    run_id="running-takeover",
                    run_task=unexpected_task,
                    publish=lambda plan, outputs, lease: "old",
                )
            )
        except Exception as error:
            old_errors.append(error)

    def new_attempt():
        try:
            new_result.append(
                execute_plan(
                    plan,
                    con=con,
                    namespace="owner",
                    run_id="running-takeover",
                    run_task=lambda task, dependencies: (
                        new_attempt_running.set(),
                        finish_new_attempt.wait(timeout=5),
                        pa.table({"id": [2]}),
                    )[-1],
                    publish=lambda plan, outputs, lease: "new",
                )
            )
        except Exception as error:
            new_errors.append(error)

    old_thread = threading.Thread(target=old_attempt)
    old_thread.start()
    assert old_lease_acquired.wait(timeout=5)
    new_thread = threading.Thread(target=new_attempt)
    new_thread.start()
    assert new_attempt_running.wait(timeout=5)
    resume_old_claim.set()
    old_thread.join(timeout=5)
    assert not old_thread.is_alive()
    row = con.execute(
        'SELECT state, attempt_fence FROM "owner".runtime_runs WHERE run_id=?',
        ["running-takeover"],
    ).fetchone()
    assert row is not None
    assert row[0] == "running"
    new_fence = row[1]
    finish_new_attempt.set()
    new_thread.join(timeout=5)
    assert not new_thread.is_alive()
    assert old_errors == []
    assert old_result[0].state == "running"
    assert new_errors == []
    assert new_result[0].published_generation == "new"
    final_row = con.execute(
        'SELECT state, attempt_fence FROM "owner".runtime_runs WHERE run_id=?',
        ["running-takeover"],
    ).fetchone()
    assert final_row is not None
    final_state, final_fence = final_row
    assert (final_state, final_fence) == ("succeeded", new_fence)


def test_stale_task_claim_cannot_replace_takeover_task_fence():
    import threading
    from datetime import UTC, datetime, timedelta

    con = duckdb.connect(":memory:")
    plan = _plan()
    old_claim_at_task_insert = threading.Event()
    resume_old_task_insert = threading.Event()
    new_attempt_running = threading.Event()
    finish_new_attempt = threading.Event()
    old_errors = []
    new_result = []
    new_errors = []
    old_thread_id = []

    class PausingConnection:
        def execute(self, query, *args, **kwargs):
            if threading.get_ident() == old_thread_id[0] and query.startswith(
                'INSERT INTO "owner".runtime_tasks'
            ):
                old_claim_at_task_insert.set()
                assert resume_old_task_insert.wait(timeout=5)
            return con.execute(query, *args, **kwargs)

        def cursor(self):
            return con.cursor()

    connection = PausingConnection()

    def old_attempt():
        old_thread_id.append(threading.get_ident())

        def unexpected_task(task, dependencies) -> pa.Table:
            raise AssertionError("old task claim must be rejected")

        try:
            execute_plan(
                plan,
                con=connection,
                namespace="owner",
                run_id="task-takeover",
                run_task=unexpected_task,
                publish=lambda plan, outputs, lease: "old",
            )
        except Exception as error:
            old_errors.append(error)

    def new_attempt():
        try:
            new_result.append(
                execute_plan(
                    plan,
                    con=connection,
                    namespace="owner",
                    run_id="task-takeover",
                    run_task=lambda task, dependencies: (
                        new_attempt_running.set(),
                        finish_new_attempt.wait(timeout=5),
                        pa.table({"id": [2]}),
                    )[-1],
                    publish=lambda plan, outputs, lease: "new",
                )
            )
        except Exception as error:
            new_errors.append(error)

    old_thread = threading.Thread(target=old_attempt)
    old_thread.start()
    assert old_claim_at_task_insert.wait(timeout=5)
    lease_row = con.execute(
        'SELECT fence, resource FROM "owner".runtime_leases WHERE owner=?',
        ["task-takeover"],
    ).fetchone()
    assert lease_row is not None
    con.execute(
        'UPDATE "owner".runtime_leases SET expires_at=? WHERE resource=?',
        [
            (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
            lease_row[1],
        ],
    )
    new_thread = threading.Thread(target=new_attempt)
    new_thread.start()
    assert new_attempt_running.wait(timeout=5)
    task_row = con.execute(
        'SELECT task_fence FROM "owner".runtime_tasks WHERE run_id=?',
        ["task-takeover"],
    ).fetchone()
    assert task_row is not None
    new_task_fence = task_row[0]
    resume_old_task_insert.set()
    old_thread.join(timeout=5)
    assert not old_thread.is_alive()
    task_row_after_old_resume = con.execute(
        'SELECT task_fence FROM "owner".runtime_tasks WHERE run_id=?',
        ["task-takeover"],
    ).fetchone()
    assert task_row_after_old_resume is not None
    task_fence_after_old_resume = task_row_after_old_resume[0]
    assert task_fence_after_old_resume == new_task_fence
    finish_new_attempt.set()
    new_thread.join(timeout=5)
    assert not new_thread.is_alive()
    assert len(old_errors) == 1
    assert "fenced" in str(old_errors[0])
    assert new_errors == []
    assert new_result[0].published_generation == "new"


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
