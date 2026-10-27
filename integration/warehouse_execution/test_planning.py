import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import duckdb
import ibis

from terrine.data import UnitSource
from terrine.model import Model
from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
from terrine.runtime.graph import dependency_graph
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import plan_refresh
from terrine.runtime.planning import OwnerPlanner, PlanInvocation, PlanRequest
from terrine.runtime.saved_plan import DestinationRef, ModelReference
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design
from tests.runtime.test_planning import _request


def test_plan_request_uses_owner_ipc_from_process_without_database_access(tmp_path):
    request = _request()
    database = tmp_path / "runtime.duckdb"
    con = duckdb.connect(str(database))
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    plan = plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash=request.design.spec_hash,
        snapshot_id="snapshot-1",
        graph=dependency_graph(sources=("events",), definition_hash=request.design.spec_hash),
    )
    expected = PlanInvocation(
        request.invocation_id,
        plan.serialize().decode(),
        "request-key",
        request.destination,
        plan.snapshot_id,
        None,
    )

    class Planner:
        def plan(self, actual, *, con, namespace):
            assert actual == request
            assert namespace == "owner"
            return expected

    endpoint = ControlEndpoint(Path(f"/tmp/terrine-plan-{os.getpid()}-{uuid.uuid4().hex[:8]}.sock"))
    server = ControlServer(store, namespace="owner", endpoint=endpoint, plan_planner=Planner())
    server.start()
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(request.to_dict()))
    try:
        script = (
            "import json,sys; "
            "from pathlib import Path; "
            "from terrine.runtime.control import ControlClient,ControlEndpoint; "
            "from terrine.runtime.planning import PlanRequest; "
            "payload=json.loads(Path(sys.argv[2]).read_text()); "
            "result=ControlClient(ControlEndpoint(Path(sys.argv[1])),namespace='owner').plan("
            "PlanRequest.from_dict(payload)); "
            "print(json.dumps(result.to_dict(),sort_keys=True))"
        )
        completed = subprocess.run(
            [sys.executable, "-c", script, str(endpoint.path), str(request_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        invocation = PlanInvocation.from_dict(json.loads(completed.stdout))
        repeated = ControlClient(endpoint, namespace="owner").plan(request)
    finally:
        server.close()
        con.close()

    assert invocation == expected
    assert repeated == expected


def test_roster_only_owner_plan_runs_without_fact_source_work(tmp_path):
    from terrine.runtime.api import open_published_frame, run_serialized_plan
    from tests.runtime.test_saved_plan import _model

    database = tmp_path / "roster.duckdb"
    connection = ibis.duckdb.connect(database=str(database))
    connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    connection.raw_sql("CREATE TABLE users (id INTEGER)")
    connection.raw_sql("INSERT INTO users VALUES (7)")
    model = Model.from_catalog(
        _model(connection).catalog,
        con=connection,
        units={"user": UnitSource("users", "id")},
        readiness={},
    )
    request = PlanRequest(
        "roster-only",
        ModelReference.from_model(model, owner_key="owner-model"),
        Design("user", FixedDate("2025-01-15")),
        DestinationRef(str(database), "published", "study"),
    )

    class Resolver:
        def resolve(self, reference):
            return model

    authority = SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots"))
    planner = OwnerPlanner(resolver=Resolver(), snapshot_authority=authority)
    try:
        invocation = planner.plan(request, con=connection.con, namespace="owner")
        connection.raw_sql("DROP TABLE events")
        connection.raw_sql("UPDATE users SET id=99")
        receipt = run_serialized_plan(
            invocation.saved_plan_json,
            con=connection.con,
            namespace="owner",
            request_key=invocation.request_key,
            resolver=Resolver(),
            snapshot_authority=authority,
        )
        published = open_published_frame(receipt, con=connection.con, namespace="owner")
        assert receipt.state == "succeeded"
        assert published.table["unit"].to_pylist() == [7]
        assert published.provenance.eligibility_summary.retained == 1
    finally:
        connection.disconnect()
