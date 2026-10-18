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
        assert status["run_id"] == "r1"
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
    for sequence in range(1000):
        record_run_event(
            "r1", "phase", {"phase": f"{sequence:04d}-" + "x" * 100}, con=con, namespace="owner"
        )
    endpoint = ControlEndpoint(Path(f"/tmp/terrine-events-{os.getpid()}.sock"))
    server = ControlServer(store, namespace="owner", endpoint=endpoint)
    server.start()
    try:
        page = ControlClient(endpoint, namespace="owner").read_run_events("r1", limit=1000)
    finally:
        server.close()
    assert len(page["events"]) == 1000
