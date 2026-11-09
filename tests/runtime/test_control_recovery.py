from __future__ import annotations

import os
import uuid
from pathlib import Path

import duckdb

from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
from terrine.runtime.events import read_run_events
from terrine.runtime.state import RuntimeStore


def test_owner_start_reconciles_expired_runs_and_emits_lifecycle_events():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    schema = store._schema
    con.execute(
        f"INSERT INTO {schema}.runtime_runs (run_id, dataset, fingerprint, state, attempt_fence) "
        "VALUES ('interrupted','orders','fp','running',1)"
    )
    con.execute(
        f"INSERT INTO {schema}.runtime_leases VALUES "
        "('dataset:orders','interrupted',1,TIMESTAMP '2999-01-01')"
    )
    endpoint = ControlEndpoint(Path(f"/tmp/terrine-recovery-{os.getpid()}-{uuid.uuid4().hex}.sock"))
    server = ControlServer(store, namespace="owner", endpoint=endpoint)
    server.start()
    try:
        row = con.execute(
            f"SELECT state FROM {schema}.runtime_runs WHERE run_id='interrupted'"
        ).fetchone()
        assert row is not None and row[0] == "running"
        con.execute(
            f"UPDATE {schema}.runtime_leases SET expires_at=TIMESTAMP '2000-01-01' "
            "WHERE owner='interrupted'"
        )
        status = ControlClient(endpoint, namespace="owner").get_run("interrupted")
        assert status.durable_state == "failed"
        events = read_run_events("interrupted", con=con, namespace="owner").events
        assert [event.payload["state"] for event in events] == ["failed"]
    finally:
        server.close()


def test_queued_run_cancellation_is_terminal_without_worker_acknowledgement():
    from terrine.runtime.status import cancel_run

    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    schema = store._schema
    con.execute(
        f"INSERT INTO {schema}.runtime_runs (run_id, dataset, fingerprint, state) "
        "VALUES ('queued-run','orders','fp','queued')"
    )
    receipt = cancel_run("queued-run", con=con, namespace="owner")

    assert receipt.disposition == "requested"
    assert receipt.status.durable_state == "cancelled"
    assert [
        event.payload["state"]
        for event in read_run_events("queued-run", con=con, namespace="owner").events
    ] == ["cancelled"]


def test_cancel_rechecks_state_when_queued_run_starts_before_cancellation(
    monkeypatch,
):
    import terrine.runtime.status as status_module
    from terrine.runtime.events import event_transaction, record_run_event

    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    schema = store._schema
    con.execute(
        f"INSERT INTO {schema}.runtime_runs (run_id, dataset, fingerprint, state) "
        "VALUES ('racing-run','orders','fp','queued')"
    )
    record_run_event("racing-run", "lifecycle", {"state": "queued"}, con=con, namespace="owner")
    original = status_module.get_run
    first_read = True

    def start_after_queued_read(run_id, *, con, namespace):
        nonlocal first_read
        result = original(run_id, con=con, namespace=namespace)
        if first_read:
            first_read = False
            with event_transaction(con, namespace=namespace):
                con.execute(
                    f"UPDATE {schema}.runtime_runs SET state='running' WHERE run_id=?",
                    [run_id],
                )
                record_run_event(
                    run_id,
                    "lifecycle",
                    {"state": "running"},
                    con=con,
                    namespace=namespace,
                    _in_transaction=True,
                )
        return result

    monkeypatch.setattr(status_module, "get_run", start_after_queued_read)
    receipt = status_module.cancel_run("racing-run", con=con, namespace="owner")

    assert receipt.status.durable_state == "cancel_requested"


def test_restart_restores_no_op_selection_before_recovered_success():
    from datetime import UTC, datetime, timedelta

    import pyarrow as pa

    from terrine.compile.provenance import EXECUTION_REVISION
    from terrine.runtime.backends.duckdb import DuckDBBackend, StagedGeneration
    from terrine.runtime.changes import ChangeSet
    from terrine.runtime.graph import dependency_graph
    from terrine.runtime.leases import LeaseStore
    from terrine.runtime.planner import plan_refresh
    from terrine.runtime.saved_plan import (
        DestinationRef,
        ModelReference,
        SavedPlan,
        _bind_saved_plan,
    )
    from terrine.spec import Design, FixedDate

    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    schema = store._schema
    leases = LeaseStore(con, namespace="owner")
    backend = DuckDBBackend(con, namespace="owner")
    prior = leases.acquire(
        "dataset:orders", "prior", ttl=timedelta(minutes=1), now=datetime.now(UTC)
    )
    backend.publish_generation(
        StagedGeneration(
            "orders",
            "g2",
            {"frame-new": pa.table({"id": [1]})},
            run_id="prior",
            execution_revision=EXECUTION_REVISION,
        ),
        expected_manifest=None,
        lease=prior,
    )
    leases.release(prior)
    design = Design("user", FixedDate("2025-01-02"))
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash=design.spec_hash,
        snapshot_id="snapshot",
        graph=dependency_graph(sources=("events",)),
        changes=ChangeSet((), "operator"),
        reuse_base_generation="g2",
        reuse_base_revision=backend.generation_execution_revision("orders", "g2"),
    )
    saved = SavedPlan.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference("model", "catalog", "model", "units", "readiness", "events"),
        destination=DestinationRef("output.duckdb", "owner", "orders"),
        namespace="owner",
        expected_base_manifest="g2",
    )
    bound = _bind_saved_plan(saved, namespace="owner")
    con.execute(
        f"INSERT INTO {schema}.runtime_dataset_requests VALUES (?,?)", ["request", saved.to_json()]
    )
    noop = leases.acquire("dataset:orders", "noop", ttl=timedelta(minutes=1), now=datetime.now(UTC))
    con.execute(
        f"INSERT INTO {schema}.runtime_runs "
        "(run_id,dataset,fingerprint,state,published_generation,attempt_fence) VALUES "
        "('prior','orders',?,'succeeded','g2',NULL),('noop','orders',?,'running',NULL,?)",
        [bound.definition.fingerprint, bound.definition.fingerprint, noop.fence],
    )
    backend.reuse_generation(
        "orders",
        expected_manifest="g2",
        lease=noop,
        run_id="noop",
        execution_revision=bound.definition.execution_revision,
    )
    leases.release(noop)
    con.execute(f"INSERT INTO {schema}.runtime_run_results VALUES ('prior','frame-new')")
    server = ControlServer(
        store,
        namespace="owner",
        endpoint=ControlEndpoint(Path(f"/tmp/terrine-recovery-copy-{uuid.uuid4().hex}.sock")),
    )
    server.start()
    try:
        status = ControlClient(server.endpoint, namespace="owner").get_run("noop")
        assert status.durable_state == "succeeded"
        assert status.pinned_result_ref == "g2"
        state = con.execute(
            f"SELECT state FROM {schema}.runtime_runs WHERE run_id='noop'"
        ).fetchone()
        selection = con.execute(
            f"SELECT partition FROM {schema}.runtime_run_results WHERE run_id='noop'"
        ).fetchall()
        assert state == ("succeeded",)
        assert selection == [("frame-new",)]
    finally:
        server.close()
