from __future__ import annotations

import json
import os
import subprocess
import sys

import ibis
import pytest

from terrine.data import Budget, Coverage
from terrine.model import Model
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg


def _catalog() -> Catalog:
    return Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["value"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {"value": {"source": "events", "column": "value"}},
            "properties": {},
            "dimensions": {},
            "measures": {"total": {"fact": "value", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "entity": "user", "measure": "total"}},
        }
    )


def _model(con, catalog: Catalog) -> Model:
    return Model.from_catalog(
        catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )


@pytest.mark.parametrize("interrupted", [False, True])
def test_saved_plan_materializes_retained_model_and_design_in_fresh_process(tmp_path, interrupted):
    import pyarrow as pa

    from terrine.model import UnitSource
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.runtime.state import RuntimeStore

    state_path = tmp_path / "owner.duckdb"
    snapshot_root = tmp_path / "snapshots"
    con = ibis.duckdb.connect(str(state_path))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01 09:00:00', 1, 3)")
    con.raw_sql("CREATE TABLE users(id INTEGER)")
    con.raw_sql("INSERT INTO users VALUES (1)")
    catalog = _catalog()
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
        units={"user": UnitSource("users", "id")},
    )
    design = Design(
        "user", FixedDate("2025-01-02"), features={"total": agg("total", window=("-1d", "0s"))}
    )
    expected = design.build(model).materialize(Budget())
    expected_path = tmp_path / "expected.arrow"
    with pa.OSFile(str(expected_path), "wb") as sink:
        with pa.ipc.new_file(sink, expected.to_arrow().schema) as writer:
            writer.write_table(expected.to_arrow())
    owner = RuntimeOwner(state_path=state_path, snapshot_root=snapshot_root)
    snapshot = SnapshotAuthority(owner).capture(
        model, design=design, con=con, namespace="runtime", run_id="planned", budget=Budget()
    )
    plan = plan_refresh(
        namespace="runtime",
        dataset="study",
        definition_hash=design.spec_hash,
        snapshot_id=snapshot.snapshot_id,
        graph=dependency_graph(model, design, sources=("events",), partitions=("all",)),
    )
    recipe = SavedPlanRecipe.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference.from_model(model, owner_key="workspace/model"),
        destination=DestinationRef(str(state_path), "analytics", "study"),
        namespace="runtime",
        expected_base_manifest=None,
    )
    store = RuntimeStore(con.con, namespace="runtime")
    store.initialize()
    recipe.pin_snapshot(store)
    recipe_path = tmp_path / "plan.json"
    catalog_path = tmp_path / "catalog.json"
    recipe_path.write_text(recipe.to_json(), encoding="utf-8")
    catalog_path.write_text(
        json.dumps(catalog.model_dump(mode="json"), sort_keys=True), encoding="utf-8"
    )
    con.raw_sql("UPDATE events SET value=99")
    con.disconnect()
    script = r"""
import json, os, sys, threading
import ibis
import pyarrow as pa
from terrine.data import Coverage, MaterializedFrame
from terrine.model import Model, UnitSource
from terrine.runtime import run_serialized_plan, open_published_frame
from terrine.runtime.api import DesignExecutor
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.recipe import SavedPlanRecipe
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.semantic.models import Catalog

state_path, snapshot_root, recipe_path, catalog_path, expected_path, interrupted = sys.argv[1:]
connection = ibis.duckdb.connect(state_path)
class OwnerResolver:
    def resolve(self, reference):
        assert reference.owner_key == 'workspace/model'
        catalog = Catalog.model_validate(json.loads(open(catalog_path).read()))
        return Model.from_catalog(catalog, con=connection,
            readiness={'events': Coverage('2025-01-01', '2025-02-01')},
            units={'user': UnitSource('users', 'id')})
serialized = open(recipe_path).read()
if interrupted == 'True':
    original_task = DesignExecutor.run_snapshot_task
    completed = []
    def interrupted_task(self, context, task, dependencies):
        if completed:
            while connection.con.execute(
                "SELECT heartbeat_at FROM runtime.runtime_runs"
            ).fetchone()[0] is None:
                threading.Event().wait(0.01)
            os._exit(73)
        result = original_task(self, context, task, dependencies)
        completed.append(task.task_id)
        return result
    DesignExecutor.run_snapshot_task = interrupted_task
recipe = SavedPlanRecipe.from_json(serialized)
owner = RuntimeOwner(state_path=state_path, snapshot_root=snapshot_root)
receipt = run_serialized_plan(serialized, con=connection.con, namespace='runtime',
    request_key='fresh-process', resolver=OwnerResolver(),
    snapshot_authority=SnapshotAuthority(owner))
assert receipt.state == 'succeeded'
published = open_published_frame(receipt, con=connection.con, namespace='runtime')
expected = MaterializedFrame.from_arrow(pa.ipc.open_file(expected_path).read_all())
assert published.table.to_pylist() == expected.table.to_pylist()
assert published.columns == expected.columns
assert published.table.schema.remove_metadata() == expected.table.schema.remove_metadata()
assert published.table.to_pylist()[0]['total'] == 3
assert published.provenance.execution_snapshot.snapshot_id == (
    json.loads(recipe.plan_json)['snapshot_id'])
assert run_serialized_plan(serialized, con=connection.con, namespace='runtime',
    request_key='fresh-process') == receipt
assert connection.table('events').execute()['value'].tolist() == [99]
connection.disconnect()
"""
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(state_path),
            str(snapshot_root),
            str(recipe_path),
            str(catalog_path),
            str(expected_path),
            str(interrupted),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
        env=os.environ.copy(),
    )
    assert process.returncode == (73 if interrupted else 0), process.stderr
    if interrupted:
        from pathlib import Path
        from uuid import uuid4

        from terrine.runtime import get_run, open_published_frame
        from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer

        reopened = ibis.duckdb.connect(str(state_path))
        try:
            run_id = reopened.con.execute("SELECT run_id FROM runtime.runtime_runs").fetchone()[0]
            disconnected = get_run(run_id, con=reopened.con, namespace="runtime")
            assert disconnected.durable_state == "running"
            assert disconnected.heartbeat_at is not None
            assert disconnected.phase == "executing"
            assert disconnected.progress["completed_tasks"] == 1
            retained_task = reopened.con.execute(
                "SELECT task_id,output_blob,task_fence FROM runtime.runtime_tasks "
                "WHERE status='complete'"
            ).fetchone()
            reopened.con.execute(
                "UPDATE runtime.runtime_leases SET expires_at=TIMESTAMP '2000-01-01'"
            )
            stale = get_run(run_id, con=reopened.con, namespace="runtime")
            assert stale.durable_state == "running"
            assert stale.freshness == "stale"
            assert stale.heartbeat_at == disconnected.heartbeat_at

            class Resolver:
                def resolve(self, reference):
                    return Model.from_catalog(
                        catalog,
                        con=reopened,
                        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
                        units={"user": UnitSource("users", "id")},
                    )

            endpoint = ControlEndpoint(Path(f"/tmp/terrine-resume-{uuid4().hex}.sock"))
            server = ControlServer(
                RuntimeStore(reopened.con, namespace="runtime"),
                namespace="runtime",
                endpoint=endpoint,
                resolver=Resolver(),
                snapshot_authority=SnapshotAuthority(owner),
            )
            server.start()
            try:
                client = ControlClient(endpoint, namespace="runtime")
                recovered = client.get_run(run_id)
                assert recovered.durable_state == "failed"
                assert recovered.error_code == "terrine.runtime.owner_interrupted"
                resumed = client.run(recipe.to_json(), request_key="fresh-process")
                assert resumed.run_id == run_id
                assert resumed.state == "succeeded"
                frame = open_published_frame(resumed, con=reopened.con, namespace="runtime")
                assert frame.table.to_pylist() == expected.table.to_pylist()
                assert frame.columns == expected.columns
                assert (
                    reopened.con.execute(
                        "SELECT task_id,output_blob,task_fence FROM runtime.runtime_tasks "
                        "WHERE task_id=?",
                        [retained_task[0]],
                    ).fetchone()
                    == retained_task
                )
                resumed_events = client.read_run_events(
                    run_id, after=disconnected.latest_event_cursor
                )
                assert [event.payload["state"] for event in resumed_events.events] == [
                    "failed",
                    "running",
                    "succeeded",
                ]
                assert client.get_run(run_id).progress["completed_tasks"] == len(plan.tasks)
            finally:
                server.close()
        finally:
            reopened.disconnect()


def test_reopenable_snapshot_resumes_in_fresh_process_after_source_mutation(tmp_path):
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.snapshot import SnapshotAuthority

    state_path = tmp_path / "snapshot-owner.duckdb"
    snapshot_root = tmp_path / "snapshots"
    con = ibis.duckdb.connect(str(state_path))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
    catalog = _catalog()
    model = _model(con, catalog)
    owner = RuntimeOwner(state_path=state_path, snapshot_root=snapshot_root)
    design = Design(
        "user", FixedDate("2025-01-15"), features={"total": agg("total", window=("-1d", "0s"))}
    )
    receipt = SnapshotAuthority(owner).capture(
        model,
        design=design,
        con=con,
        namespace="runtime",
        run_id="snapshot-run",
        budget=Budget(),
    )
    con.raw_sql("UPDATE events SET value=99")
    con.disconnect()

    script = r"""
import json, sys
import ibis
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.snapshot import ReopenableSnapshot, SnapshotAuthority
from terrine.semantic.models import Catalog

state_path, snapshot_root, receipt_path = sys.argv[1:]
owner = RuntimeOwner(state_path=state_path, snapshot_root=snapshot_root)
receipt = ReopenableSnapshot.from_json(open(receipt_path).read())
catalog = Catalog.model_validate(json.loads(receipt.catalog_json))
connection = ibis.duckdb.connect(state_path)
execution = SnapshotAuthority(owner).reopen(receipt, con=connection, namespace='runtime')
relation = execution.resolve(catalog.sources['events'].relation)
assert execution.execute(relation).to_pylist()[0]['value'] == 3
execution.close()
connection.disconnect()
"""
    subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(state_path),
            str(snapshot_root),
            receipt.receipt_path,
        ],
        check=True,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
