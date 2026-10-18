from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pyarrow as pa

from terrine.runtime.control import ControlEndpoint, ControlServer
from terrine.runtime.events import record_run_event
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.state import RuntimeStore


class _Executor:
    def run_task(self, task, dependencies):
        return pa.table({"id": [task.task_id]})

    def publish(self, plan, outputs, lease, run_id):
        return f"g-{run_id}"


def test_cli_status_events_and_cancel_use_owner_ipc():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR PRIMARY KEY,dataset VARCHAR,"
        "fingerprint VARCHAR,state VARCHAR,published_generation VARCHAR,attempt_fence BIGINT)"
    )
    con.execute("INSERT INTO owner.runtime_runs VALUES ('r1','orders','fp','running',NULL,1)")
    record_run_event("r1", "lifecycle", {"state": "running"}, con=con, namespace="owner")
    endpoint = ControlEndpoint(Path(f"/tmp/tcli-{os.getpid()}.sock"))
    server = ControlServer(store, namespace="owner", endpoint=endpoint, executor=_Executor())
    server.start()
    env = {
        **os.environ,
        "TERRINE_RUNTIME_SOCKET": str(endpoint.path),
        "TERRINE_RUNTIME_NAMESPACE": "owner",
    }
    script = str(Path(sys.executable).with_name("terrine"))
    try:
        plan = plan_refresh(
            namespace="owner",
            dataset="orders",
            definition_hash="definition",
            snapshot_id="snapshot",
            graph=dependency_graph(
                sources=("orders",), definition_hash="definition", partitions=("all",)
            ),
        )
        plan_file = Path(f"/tmp/tcli-{os.getpid()}.json")
        plan_file.write_bytes(plan.serialize())
        planned = subprocess.run(
            [script, "plan", str(plan_file)],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        assert planned.stdout.strip() == plan.serialize().decode()
        command = [
            script,
            "run",
            str(plan_file),
            "--request-key",
            "shared",
            "--destination-database",
            "out.duckdb",
            "--destination-schema",
            "analytics",
            "--destination-dataset",
            "orders",
        ]
        first_run = subprocess.run(command, check=True, capture_output=True, text=True, env=env)
        replay = subprocess.run(command, check=True, capture_output=True, text=True, env=env)
        assert json.loads(first_run.stdout)["run_id"] == json.loads(replay.stdout)["run_id"]
        different_destination = command.copy()
        database_index = different_destination.index("--destination-database") + 1
        different_destination[database_index] = "other.duckdb"
        isolated = subprocess.run(
            different_destination, check=True, capture_output=True, text=True, env=env
        )
        assert json.loads(isolated.stdout)["run_id"] != json.loads(first_run.stdout)["run_id"]
        plan_file.unlink()
        status = subprocess.run(
            [script, "status", "r1"],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        assert json.loads(status.stdout)["durable_state"] == "running"
        events = subprocess.run(
            [script, "events", "r1"],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        event_page = json.loads(events.stdout)
        assert [item["payload"]["state"] for item in event_page["events"]] == ["running"]
        cancelled = subprocess.run(
            [script, "cancel", "r1"],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        assert json.loads(cancelled.stdout)["disposition"] == "requested"
        assert con.execute("SELECT state FROM owner.runtime_runs WHERE run_id='r1'").fetchone() == (
            "cancel_requested",
        )
    finally:
        server.close()
