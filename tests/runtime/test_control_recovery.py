from __future__ import annotations

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
        f"CREATE TABLE {schema}.runtime_runs "
        "(run_id VARCHAR PRIMARY KEY,dataset VARCHAR,fingerprint VARCHAR,state VARCHAR,"
        "published_generation VARCHAR,attempt_fence BIGINT)"
    )
    con.execute(
        f"INSERT INTO {schema}.runtime_runs VALUES ('interrupted','orders','fp','running',NULL,1)"
    )
    con.execute(
        f"CREATE TABLE {schema}.runtime_leases "
        "(resource VARCHAR PRIMARY KEY,owner VARCHAR,fence BIGINT,expires_at TIMESTAMP)"
    )
    con.execute(
        f"INSERT INTO {schema}.runtime_leases VALUES "
        "('dataset:orders','interrupted',1,TIMESTAMP '2999-01-01')"
    )
    con.execute(
        f"CREATE TABLE {schema}.runtime_run_publications "
        "(run_id VARCHAR PRIMARY KEY,dataset VARCHAR,generation VARCHAR,version BIGINT)"
    )
    server = ControlServer(
        store, namespace="owner", endpoint=ControlEndpoint(Path("/tmp/terrine-recovery-test.sock"))
    )
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
        status = ControlClient(
            ControlEndpoint(Path("/tmp/terrine-recovery-test.sock")), namespace="owner"
        ).get_run("interrupted")
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
        f"CREATE TABLE {schema}.runtime_runs "
        "(run_id VARCHAR PRIMARY KEY,dataset VARCHAR,fingerprint VARCHAR,state VARCHAR,"
        "published_generation VARCHAR,attempt_fence BIGINT,updated_at TIMESTAMP,"
        "heartbeat_at TIMESTAMP)"
    )
    con.execute(
        f"INSERT INTO {schema}.runtime_runs VALUES "
        "('queued-run','orders','fp','queued',NULL,NULL,NULL,NULL)"
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
        f"CREATE TABLE {schema}.runtime_runs "
        "(run_id VARCHAR PRIMARY KEY,dataset VARCHAR,fingerprint VARCHAR,state VARCHAR,"
        "published_generation VARCHAR,attempt_fence BIGINT,updated_at TIMESTAMP,"
        "heartbeat_at TIMESTAMP)"
    )
    con.execute(
        f"INSERT INTO {schema}.runtime_runs VALUES "
        "('racing-run','orders','fp','queued',NULL,NULL,NULL,NULL)"
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
