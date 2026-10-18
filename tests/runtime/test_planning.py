import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import cast

import duckdb
import ibis
import pytest

from terrine.data import Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
from terrine.runtime.graph import dependency_graph
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import plan_refresh
from terrine.runtime.planning import OwnerPlanner, PlanInvocation, PlanRequest
from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.spec import agg
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design


def _request():
    return PlanRequest(
        "airflow-run-1",
        ModelReference("owner", "catalog", "model", "units", "readiness", "events"),
        Design("user", FixedDate("2025-01-15")),
        DestinationRef("warehouse.duckdb", "published", "study"),
    )


def test_plan_request_round_trips_as_versioned_owner_payload():
    request = _request()

    restored = PlanRequest.from_dict(request.to_dict())

    assert restored == request
    assert restored.schema_version == 1


def test_plan_request_refuses_unknown_wire_version():
    data = _request().to_dict()
    data["schema_version"] = 99

    with pytest.raises(TerrineError) as error:
        PlanRequest.from_dict(data)

    assert error.value.code == "terrine.runtime.plan_request_invalid"


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


def test_owner_planner_persists_same_snapshot_and_manifest_by_invocation(tmp_path):
    from tests.runtime.test_recipe import _model

    database = tmp_path / "catalog.duckdb"
    ibis_connection = ibis.duckdb.connect(database=str(database))
    ibis_connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")

    ibis_connection.raw_sql("CREATE TABLE users (id INTEGER)")
    base_model = _model(ibis_connection)
    model = Model.from_catalog(
        base_model.catalog,
        con=ibis_connection,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
    )
    raw_connection = ibis_connection.con
    store = RuntimeStore(raw_connection, namespace="runtime")
    store.initialize()
    request = PlanRequest(
        "materialize-1",
        ModelReference.from_model(model, owner_key="owner-model"),
        Design(
            "user",
            FixedDate("2025-01-15"),
            features={"total": agg("total", window=("-1d", "0s"))},
            outcomes={},
        ),
        DestinationRef(str(database), "published", "study"),
    )

    class Resolver:
        def resolve(self, reference):
            return model

    planner = OwnerPlanner(
        resolver=Resolver(),
        snapshot_authority=SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots")),
    )
    try:
        first = planner.plan(request, con=raw_connection, namespace="runtime")
        second = planner.plan(request, con=raw_connection, namespace="runtime")
        with pytest.raises(TerrineError) as conflict:
            planner.plan(
                PlanRequest(
                    request.invocation_id,
                    request.model,
                    request.design,
                    request.destination,
                    requested_scope="different-scope",
                ),
                con=raw_connection,
                namespace="runtime",
            )
        raw_connection.execute("INSERT INTO events VALUES ('2025-01-10', 1, 1)")
        fresh = planner.plan(
            PlanRequest(
                "materialize-2",
                request.model,
                request.design,
                request.destination,
            ),
            con=raw_connection,
            namespace="runtime",
        )
        assert fresh.snapshot_id != first.snapshot_id
        assert fresh.request_key != first.request_key
        persisted = raw_connection.execute(
            "SELECT invocation_json FROM runtime.runtime_plan_invocations WHERE invocation_id=?",
            [request.invocation_id],
        ).fetchone()
        saved_invocation = PlanInvocation.from_dict(json.loads(persisted[0]))
        assert saved_invocation.recipe_json
        recipe = SavedPlanRecipe.from_json(saved_invocation.recipe_json)
        assert recipe.model == request.model
        assert recipe.design_json
        assert recipe.expected_base_manifest == first.expected_base_manifest
    finally:
        ibis_connection.disconnect()

    assert second == first
    assert second.snapshot_id == first.snapshot_id
    assert second.expected_base_manifest is None
    assert conflict.value.code == "terrine.runtime.plan_invocation_conflict"


def test_owner_plan_rolls_back_snapshot_pin_when_invocation_persistence_fails(
    tmp_path, monkeypatch
):
    from tests.runtime.test_recipe import _model

    database = tmp_path / "catalog-atomic.duckdb"
    connection = ibis.duckdb.connect(database=str(database))
    connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    connection.raw_sql("CREATE TABLE users (id INTEGER)")
    base_model = _model(connection)
    model = Model.from_catalog(
        base_model.catalog,
        con=connection,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
    )
    request = PlanRequest(
        "materialize-atomic",
        ModelReference.from_model(model, owner_key="owner-model"),
        Design(
            "user",
            FixedDate("2025-01-15"),
            features={"total": agg("total", window=("-1d", "0s"))},
            outcomes={},
        ),
        DestinationRef(str(database), "published", "study"),
    )

    class Resolver:
        def resolve(self, reference):
            return model

    planner = OwnerPlanner(
        resolver=Resolver(),
        snapshot_authority=SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots")),
    )
    original_pin = SavedPlanRecipe.pin_snapshot

    def fail_after_pin(recipe, store, *, reference=None, _in_transaction=False):
        original_pin(
            recipe,
            store,
            reference=reference,
            _in_transaction=_in_transaction,
        )
        raise RuntimeError("invocation persistence interrupted")

    monkeypatch.setattr(SavedPlanRecipe, "pin_snapshot", fail_after_pin)
    try:
        with pytest.raises(RuntimeError, match="invocation persistence interrupted"):
            planner.plan(request, con=connection.con, namespace="runtime")
        references = connection.con.execute(
            "SELECT snapshot_id FROM runtime.runtime_snapshot_references "
            "WHERE kind='saved_plan' AND reference=?",
            [request.invocation_id],
        ).fetchall()
        assert references == []
        invocations = connection.con.execute(
            "SELECT invocation_id FROM runtime.runtime_plan_invocations"
        ).fetchall()
        assert invocations == []
    finally:
        connection.disconnect()


def test_owner_plan_refuses_unsupported_runtime_shape_before_resolving_sources():
    from terrine.errors import TerrineError
    from terrine.spec.sequence import EventSequence
    from terrine.windows import Window

    request = PlanRequest(
        "unsupported-shape",
        _request().model,
        Design(
            "user",
            FixedDate("2025-01-15"),
            shape=EventSequence("purchase", Window("0s", "1d"), max_events=3),
        ),
        _request().destination,
    )
    con = duckdb.connect(":memory:")

    class Resolver:
        resolved = False

        def resolve(self, reference):
            self.resolved = True
            raise AssertionError("unsupported shapes must be refused before source resolution")

    class SnapshotSpy:
        captured = False

        def capture(self, *args, **kwargs):
            self.captured = True
            raise AssertionError("unsupported shapes must be refused before snapshot capture")

    snapshot_authority = SnapshotSpy()
    resolver = Resolver()
    planner = OwnerPlanner(
        resolver=resolver,
        snapshot_authority=cast(SnapshotAuthority, snapshot_authority),
    )
    try:
        with pytest.raises(TerrineError) as error:
            planner.plan(request, con=con, namespace="owner")
    finally:
        con.close()

    assert error.value.code == "terrine.compile.runtime_shape_unsupported"
    assert not resolver.resolved
    assert not snapshot_authority.captured


def test_roster_only_dependency_graph_contains_no_fact_source_work():
    from types import SimpleNamespace

    request = _request()
    graph = dependency_graph(
        SimpleNamespace(units={"user": object()}),
        request.design,
        sources=(),
        definition_hash=request.design.spec_hash,
    )

    assert [node.layer for node in graph.nodes] == ["roster", "anchor", "frame"]
    assert [node.source for node in graph.nodes] == ["user", "user", "user"]
    assert [(source.layer, target.layer) for source, target in graph.edges] == [
        ("roster", "anchor"),
        ("anchor", "frame"),
    ]
