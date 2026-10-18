import os
from pathlib import Path

import duckdb

from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
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
