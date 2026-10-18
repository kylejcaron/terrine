from __future__ import annotations

import os
import subprocess
import sys

import duckdb

from terrine.runtime.control import ControlEndpoint, ControlServer
from terrine.runtime.state import RuntimeStore


def test_separate_process_status_and_cancel_use_owner_connection(tmp_path):
    database = tmp_path / "runtime-state.duckdb"
    con = duckdb.connect(str(database))
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, "
        "fingerprint VARCHAR NOT NULL, state VARCHAR NOT NULL, published_generation VARCHAR, "
        "attempt_fence BIGINT)"
    )
    con.execute("INSERT INTO owner.runtime_runs VALUES ('r1','d','f','running',NULL,1)")
    con.execute("ALTER TABLE owner.runtime_runs ADD COLUMN request_key VARCHAR")
    con.execute("ALTER TABLE owner.runtime_runs ADD COLUMN destination_json VARCHAR")
    con.execute(
        "UPDATE owner.runtime_runs SET request_key=?, destination_json=?",
        ["req", '{"database":"out.duckdb","dataset":"d","schema":"analytics"}'],
    )
    endpoint = ControlEndpoint(tmp_path / f"terrine-control-{os.getpid()}.sock")
    server = ControlServer(store, namespace="owner", endpoint=endpoint)
    server.start()
    try:
        script = (
            "from terrine.runtime.control import ControlClient, ControlEndpoint; "
            f"client=ControlClient(ControlEndpoint({str(endpoint.path)!r}), namespace='owner'); "
            "assert client.get_run('r1').run_id=='r1'; "
            "assert client.cancel_run('r1').disposition=='requested'; "
            "from terrine.runtime.recipe import DestinationRef; "
            "assert client.find_run_by_request_key('req', destination="
            "DestinationRef('out.duckdb','analytics','d')).run_id=='r1'"
        )
        subprocess.run([sys.executable, "-c", script], check=True, timeout=20)
        assert con.execute("SELECT state FROM owner.runtime_runs WHERE run_id='r1'").fetchone() == (
            "cancel_requested",
        )
    finally:
        server.close()
        con.close()
