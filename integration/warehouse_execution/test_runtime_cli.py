import json
import os
import subprocess
import sys
from pathlib import Path
from threading import Event

import duckdb
import pyarrow as pa
import pytest

from terrine.runtime.control import ControlEndpoint, ControlServer
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.state import RuntimeStore
from tests._timing import HANG_GUARD_SECONDS


def test_cli_run_waits_for_long_owner_execution_receipt():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    started = Event()
    release = Event()

    class SlowExecutor:
        def run_task(self, task, dependencies):
            started.set()
            release.wait(timeout=HANG_GUARD_SECONDS)
            return pa.table({"id": [task.task_id]})

        def publish(self, plan, outputs, lease, run_id):
            return f"g-{run_id}"

    endpoint = ControlEndpoint(Path(f"/tmp/tclilong-{os.getpid()}.sock"))
    server = ControlServer(store, namespace="owner", endpoint=endpoint, executor=SlowExecutor())
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
    plan_file = Path(f"/tmp/tclilong-{os.getpid()}.json")
    plan_file.write_bytes(plan.serialize())
    env = {
        **os.environ,
        "TERRINE_RUNTIME_SOCKET": str(endpoint.path),
        "TERRINE_RUNTIME_NAMESPACE": "owner",
    }
    process = subprocess.Popen(
        [str(Path(sys.executable).with_name("terrine")), "run", str(plan_file)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        assert started.wait(timeout=HANG_GUARD_SECONDS)
        with pytest.raises(subprocess.TimeoutExpired):
            process.communicate(timeout=2.2)
    finally:
        release.set()
    stdout, _stderr = process.communicate(timeout=HANG_GUARD_SECONDS)
    plan_file.unlink()
    server.close()
    assert process.returncode == 0
    assert json.loads(stdout)["state"] == "succeeded"
