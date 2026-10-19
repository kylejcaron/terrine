from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb

from terrine.runtime.control import ControlEndpoint, ControlServer
from terrine.runtime.state import RuntimeStore
from tests._timing import HANG_GUARD_SECONDS


def test_separate_process_status_and_cancel_use_owner_connection(tmp_path):
    database = tmp_path / "runtime-state.duckdb"
    con = duckdb.connect(str(database))
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    from terrine.runtime.leases import LeaseStore

    LeaseStore(con, namespace="owner").initialize()
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, "
        "fingerprint VARCHAR NOT NULL, state VARCHAR NOT NULL, published_generation VARCHAR, "
        "attempt_fence BIGINT)"
    )
    # Model an active owner run so startup recovery does not mark it interrupted.
    from terrine.runtime.leases import LeaseStore

    LeaseStore(con, namespace="owner").initialize()
    con.execute("INSERT INTO owner.runtime_runs VALUES ('r1','d','f','running',NULL,1)")
    con.execute(
        "INSERT INTO owner.runtime_leases VALUES "
        "('dataset:d','r1',1,current_timestamp + INTERVAL '1 day')"
    )
    con.execute("ALTER TABLE owner.runtime_runs ADD COLUMN request_key VARCHAR")
    con.execute("ALTER TABLE owner.runtime_runs ADD COLUMN destination_json VARCHAR")
    con.execute(
        "UPDATE owner.runtime_runs SET request_key=?, destination_json=?",
        ["req", '{"database":"out.duckdb","dataset":"d","schema":"analytics"}'],
    )
    endpoint = ControlEndpoint(Path(f"/tmp/terrine-control-{os.getpid()}.sock"))
    server = ControlServer(store, namespace="owner", endpoint=endpoint)
    server.start()
    try:
        script = (
            "from terrine.runtime.control import ControlClient, ControlEndpoint; "
            f"client=ControlClient(ControlEndpoint({str(endpoint.path)!r}), namespace='owner'); "
            "assert client.get_run('r1').run_id=='r1'; "
            "assert client.cancel_run('r1').disposition=='requested'; "
            "from terrine.runtime.recipe import DestinationRef; "
            "receipt=client.find_run_by_request_key('req', destination="
            "DestinationRef('out.duckdb','analytics','d')); "
            "assert receipt is not None and receipt.run_id=='r1', repr(receipt)"
        )
        subprocess.run([sys.executable, "-c", script], check=True, timeout=HANG_GUARD_SECONDS)
        assert con.execute("SELECT state FROM owner.runtime_runs WHERE run_id='r1'").fetchone() == (
            "cancel_requested",
        )
    finally:
        server.close()
        con.close()
