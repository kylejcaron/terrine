from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa

from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef
from terrine.runtime.state import RuntimeStore
from tests._timing import HANG_GUARD_SECONDS


def test_response_loss_then_fresh_process_lookup_returns_same_run(tmp_path):
    database = tmp_path / "runtime.duckdb"
    first = r"""
import os
import duckdb
import pyarrow as pa
from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef
con=duckdb.connect(os.environ['TERRINE_DB'])
plan=plan_refresh(
    namespace='owner',
    dataset='orders',
    definition_hash='def',
    snapshot_id='s',
    graph=dependency_graph(
        sources=('events',), definition_hash='def', partitions=('p1',)
    ),
)
execute_plan(
    plan,
    con=con,
    namespace='owner',
    run_id='lost-response-run',
    request_key='lost-key',
    destination=DestinationRef('out.duckdb','analytics','orders'),
    run_task=lambda task,deps: pa.table({'id':[1]}),
    publish=lambda plan,outputs,lease:'g1',
)
os._exit(73)
"""
    first_result = subprocess.run(
        [sys.executable, "-c", first],
        env={**os.environ, "TERRINE_DB": str(database)},
        check=False,
        timeout=HANG_GUARD_SECONDS,
    )
    assert first_result.returncode == 73
    second = r"""
import json, os, duckdb
from terrine.runtime.api import find_run_by_request_key
from terrine.runtime.recipe import DestinationRef
con=duckdb.connect(os.environ['TERRINE_DB'])
receipt=find_run_by_request_key(
    'lost-key',
    destination=DestinationRef('out.duckdb','analytics','orders'),
    con=con,
    namespace='owner',
)
print(json.dumps(None if receipt is None else {
    'run_id':receipt.run_id,
    'state':receipt.state,
    'generation':receipt.published_generation,
}))
"""
    result = subprocess.run(
        [sys.executable, "-c", second],
        env={**os.environ, "TERRINE_DB": str(database)},
        check=True,
        capture_output=True,
        text=True,
        timeout=HANG_GUARD_SECONDS,
    )

    assert json.loads(result.stdout) == {
        "run_id": "lost-response-run",
        "state": "succeeded",
        "generation": "g1",
    }


def test_lost_response_lookup_routes_through_owner_holding_duckdb(tmp_path):
    con = duckdb.connect(str(tmp_path / "runtime.duckdb"))
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    endpoint = ControlEndpoint(Path(f"/tmp/terrine-lost-{os.getpid()}.sock"))

    class Executor:
        def run_task(self, task, dependencies):
            return pa.table({"id": [1]})

        def publish(self, plan, outputs, lease, run_id):
            return "generation-1"

    server = ControlServer(store, namespace="owner", endpoint=endpoint, executor=Executor())
    server.start()
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=dependency_graph(
            sources=("events",), definition_hash="definition", partitions=("p1",)
        ),
    )
    destination = DestinationRef("out.duckdb", "analytics", "orders")
    try:
        request = {
            "namespace": "owner",
            "operation": "run",
            "serialized": plan.serialize().decode(),
            "request_key": "lost-key",
            "destination": {
                "database": destination.database,
                "schema": destination.schema,
                "dataset": destination.dataset,
            },
        }
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(endpoint.path))
            client.sendall(json.dumps(request).encode() + b"\n")
            # Drop the response channel immediately after submission.
        client = ControlClient(endpoint, namespace="owner")
        deadline = time.monotonic() + 20
        receipt = None
        while receipt is None or receipt.state != "succeeded":
            receipt = client.find_run_by_request_key("lost-key", destination=destination)
            if receipt is not None and receipt.state == "succeeded":
                break
            if time.monotonic() >= deadline:
                raise AssertionError("owner did not durably complete the lost-response request")
            time.sleep(0.01)
        assert receipt is not None
        assert receipt.state == "succeeded"
        assert receipt.published_generation == "generation-1"
        assert con.execute("SELECT state FROM owner.runtime_runs").fetchone() == ("succeeded",)
    finally:
        server.close()
        con.close()
