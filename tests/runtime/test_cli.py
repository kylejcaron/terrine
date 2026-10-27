from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import duckdb
import pyarrow as pa

from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
from terrine.runtime.events import record_run_event
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.saved_plan import DestinationRef
from terrine.runtime.state import RuntimeStore


def _run_cli(command, *, check, capture_output, text, env):
    from terrine.runtime.cli import main

    stdout, stderr = StringIO(), StringIO()
    with patch.dict(os.environ, env), redirect_stdout(stdout), redirect_stderr(stderr):
        returncode = main(command[1:])
    result = subprocess.CompletedProcess(command, returncode, stdout.getvalue(), stderr.getvalue())
    if check:
        result.check_returncode()
    return result


class _Executor:
    def run_task(self, task, dependencies):
        return pa.table({"id": [task.task_id]})

    def publish(self, plan, outputs, lease, run_id):
        return f"g-{run_id}"


def test_cli_plan_refuses_unknown_scan_under_strict_budget(tmp_path):
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    endpoint = ControlEndpoint(Path(f"/tmp/tcli-strict-{os.getpid()}.sock"))
    server = ControlServer(store, namespace="owner", endpoint=endpoint)
    server.start()
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=dependency_graph(
            sources=("orders",), definition_hash="definition", partitions=("all",)
        ),
    )
    payload = json.loads(plan.serialize())
    payload["budget"] = {
        "max_output_bytes": None,
        "max_scan_bytes": 1,
        "max_rows": None,
        "strict_scan": True,
    }
    fingerprint_payload = {
        key: payload[key]
        for key in (
            "schema_version",
            "namespace",
            "dataset",
            "definition_hash",
            "snapshot_id",
            "tasks",
            "change_evidence",
            "budget",
            "expected_base_manifest",
            "expected_base_manifest_bound",
        )
    }
    payload["fingerprint"] = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    plan_file = tmp_path / "strict-plan.json"
    plan_file.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    env = {
        **os.environ,
        "TERRINE_RUNTIME_SOCKET": str(endpoint.path),
        "TERRINE_RUNTIME_NAMESPACE": "owner",
    }
    script = str(Path(sys.executable).with_name("terrine"))
    try:
        result = _run_cli(
            [script, "plan", str(plan_file)],
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 2
        assert json.loads(result.stderr)["code"] == "terrine.budget.scan_unknown"
        assert result.stdout == ""
    finally:
        server.close()


def test_request_key_lookup_on_fresh_owner_returns_none():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    endpoint = ControlEndpoint(Path(f"/tmp/tcli-fresh-{os.getpid()}.sock"))
    server = ControlServer(store, namespace="owner", endpoint=endpoint)
    server.start()
    try:
        client = ControlClient(endpoint, namespace="owner")
        destination = DestinationRef("out.duckdb", "analytics", "orders")
        assert client.find_run_by_request_key("missing", destination=destination) is None
    finally:
        server.close()


def test_cli_status_events_and_cancel_use_owner_ipc():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR PRIMARY KEY,dataset VARCHAR,"
        "fingerprint VARCHAR,state VARCHAR,published_generation VARCHAR,attempt_fence BIGINT)"
    )
    con.execute("INSERT INTO owner.runtime_runs VALUES ('r1','orders','fp','queued',NULL,1)")
    record_run_event("r1", "lifecycle", {"state": "queued"}, con=con, namespace="owner")
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
        planned = _run_cli(
            [script, "plan", str(plan_file)],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        assert planned.stdout.strip() == plan.serialize().decode()
        large_plan = plan_refresh(
            namespace="owner",
            dataset="orders",
            definition_hash="definition",
            snapshot_id="snapshot-large",
            graph=dependency_graph(
                sources=("orders",),
                definition_hash="definition",
                partitions=tuple(f"partition-{index:03}" for index in range(100)),
            ),
        )
        assert len(large_plan.serialize()) > 65536
        plan_file.write_bytes(large_plan.serialize())
        large_planned = _run_cli(
            [script, "plan", str(plan_file)],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        assert large_planned.stdout.strip() == large_plan.serialize().decode()
        plan_file.write_bytes(plan.serialize())
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
        first_run = _run_cli(command, check=True, capture_output=True, text=True, env=env)
        replay = _run_cli(command, check=True, capture_output=True, text=True, env=env)
        assert json.loads(first_run.stdout)["run_id"] == json.loads(replay.stdout)["run_id"]
        lookup = _run_cli(
            [
                script,
                "run",
                "--lookup-request-key",
                "shared",
                "--destination-database",
                "out.duckdb",
                "--destination-schema",
                "analytics",
                "--destination-dataset",
                "orders",
            ],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        assert json.loads(lookup.stdout)["run_id"] == json.loads(first_run.stdout)["run_id"]
        different_destination = command.copy()
        database_index = different_destination.index("--destination-database") + 1
        different_destination[database_index] = "other.duckdb"
        isolated = _run_cli(
            different_destination, check=True, capture_output=True, text=True, env=env
        )
        assert json.loads(isolated.stdout)["run_id"] != json.loads(first_run.stdout)["run_id"]
        plan_file.unlink()
        status = _run_cli(
            [script, "status", "r1"],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        assert json.loads(status.stdout)["durable_state"] == "queued"
        events = _run_cli(
            [script, "events", "r1"],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        event_page = json.loads(events.stdout)
        assert [item["payload"]["state"] for item in event_page["events"]] == ["queued"]
        cancelled = _run_cli(
            [script, "cancel", "r1"],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        assert json.loads(cancelled.stdout)["disposition"] == "requested"
        assert con.execute("SELECT state FROM owner.runtime_runs WHERE run_id='r1'").fetchone() == (
            "cancelled",
        )
    finally:
        server.close()
