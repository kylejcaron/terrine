import os
from pathlib import Path

import duckdb
import pytest

from terrine.errors import TerrineError
from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
from terrine.runtime.events import record_run_event
from terrine.runtime.state import RuntimeStore


def test_control_endpoint_lifetime_and_client_uses_owner_connection(tmp_path):
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, "
        "fingerprint VARCHAR NOT NULL, state VARCHAR NOT NULL, published_generation VARCHAR, "
        "attempt_fence BIGINT)"
    )
    con.execute("INSERT INTO owner.runtime_runs VALUES ('r1','d','f','running',NULL,1)")
    endpoint = ControlEndpoint(Path(f"/tmp/terrine-control-{os.getpid()}.sock"))
    server = ControlServer(store, namespace="owner", endpoint=endpoint)
    server.start()
    try:
        status = ControlClient(endpoint, namespace="owner").get_run("r1")
        assert status.run_id == "r1"
        assert endpoint.path.exists()
    finally:
        server.close()
    assert not endpoint.path.exists()


def test_control_server_refuses_non_socket_endpoint_without_deleting_it(tmp_path):
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    target = tmp_path / "runtime.db"
    target.write_bytes(b"keep")
    server = ControlServer(store, namespace="owner", endpoint=ControlEndpoint(target))

    try:
        server.start()
    except FileExistsError:
        pass
    else:
        raise AssertionError("non-socket control endpoint was accepted")
    assert target.read_bytes() == b"keep"


def test_control_preserves_run_refusal_code():
    con = duckdb.connect(":memory:")
    endpoint = ControlEndpoint(Path(f"/tmp/terrine-code-{os.getpid()}.sock"))
    server = ControlServer(
        RuntimeStore(con, namespace="owner"), namespace="owner", endpoint=endpoint
    )
    server.start()
    try:
        with pytest.raises(TerrineError) as error:
            ControlClient(endpoint, namespace="owner").get_run("missing")
    finally:
        server.close()
    assert error.value.code == "terrine.runtime.run_unavailable"


def test_control_returns_full_bounded_event_page():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    from datetime import UTC, datetime

    started = datetime(2026, 10, 18, tzinfo=UTC)
    record_run_event(
        "r1", "phase", {"phase": "0000-" + "x" * 100}, con=con, namespace="owner", at=started
    )
    con.execute(
        "INSERT INTO owner.runtime_run_events "
        "SELECT 'r1', i+1, TIMESTAMP '2026-10-18' + i * INTERVAL '1 second', "
        "'phase', json_object('phase', lpad(i::VARCHAR, 4, '0') || '-' || repeat('x', 100)) "
        "FROM range(1, 1000) AS rows(i)"
    )
    endpoint = ControlEndpoint(Path(f"/tmp/terrine-events-{os.getpid()}.sock"))
    server = ControlServer(store, namespace="owner", endpoint=endpoint)
    server.start()
    try:
        page = ControlClient(endpoint, namespace="owner").read_run_events("r1", limit=1000)
    finally:
        server.close()

    assert len(page.events) == 1000


def test_control_client_returns_typed_run_status_and_event_page(tmp_path):
    from datetime import UTC

    from terrine.runtime.events import EventPage, RunEvent
    from terrine.runtime.status import RunStatus

    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, "
        "fingerprint VARCHAR NOT NULL, state VARCHAR NOT NULL, published_generation VARCHAR, "
        "attempt_fence BIGINT)"
    )
    con.execute("INSERT INTO owner.runtime_runs VALUES ('r1','d','f','running',NULL,1)")
    record_run_event("r1", "lifecycle", {"state": "running"}, con=con, namespace="owner")
    endpoint = ControlEndpoint(Path(f"/tmp/terrine-typed-{os.getpid()}.sock"))
    server = ControlServer(store, namespace="owner", endpoint=endpoint)
    server.start()
    try:
        client = ControlClient(endpoint, namespace="owner")
        status = client.get_run("r1")
        page = client.read_run_events("r1")
    finally:
        server.close()

    assert isinstance(status, RunStatus)
    assert status.durable_state == "running"
    assert isinstance(page, EventPage)
    assert isinstance(page.events[0], RunEvent)
    assert page.events[0].at.tzinfo == UTC
