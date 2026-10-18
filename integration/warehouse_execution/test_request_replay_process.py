from __future__ import annotations

import json
import os
import subprocess
import sys


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
plan=plan_refresh(namespace='owner', dataset='orders', definition_hash='def', snapshot_id='s', graph=dependency_graph(sources=('events',), definition_hash='def', partitions=('p1',)))
execute_plan(plan, con=con, namespace='owner', run_id='lost-response-run', request_key='lost-key', destination=DestinationRef('out.duckdb','analytics','orders'), run_task=lambda task,deps: pa.table({'id':[1]}), publish=lambda plan,outputs,lease:'g1')
os._exit(73)
"""
    first_result = subprocess.run(
        [sys.executable, "-c", first],
        env={**os.environ, "TERRINE_DB": str(database)},
        check=False,
        timeout=20,
    )
    assert first_result.returncode == 73
    second = r"""
import json, os, duckdb
from terrine.runtime.api import find_run_by_request_key
from terrine.runtime.recipe import DestinationRef
con=duckdb.connect(os.environ['TERRINE_DB'])
receipt=find_run_by_request_key('lost-key', destination=DestinationRef('out.duckdb','analytics','orders'), con=con, namespace='owner')
print(json.dumps(None if receipt is None else {'run_id':receipt.run_id,'state':receipt.state,'generation':receipt.published_generation}))
"""
    result = subprocess.run(
        [sys.executable, "-c", second],
        env={**os.environ, "TERRINE_DB": str(database)},
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert json.loads(result.stdout) == {
        "run_id": "lost-response-run",
        "state": "succeeded",
        "generation": "g1",
    }
