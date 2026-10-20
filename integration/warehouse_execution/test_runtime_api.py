import copy
import json
import pickle
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

import ibis
import pyarrow as pa
import pytest

import terrine.runtime as runtime
from terrine.data import Budget, Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.execution.backend import backend_provider
from terrine.model import Model
from terrine.runtime.graph import dependency_graph
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import plan_refresh
from terrine.runtime.planning import OwnerPlanner, PlanInvocation, PlanRequest
from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg
from tests.runtime.test_snapshot import _catalog, _design


def test_saved_recipe_execution_reopens_retained_snapshot_for_each_task(tmp_path):
    from pathlib import Path

    from terrine.data import Budget, Coverage
    from terrine.model import Model, UnitSource
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.recipe import ModelReference, SavedPlanRecipe
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.runtime.state import RuntimeStore
    from terrine.semantic.models import Catalog
    from terrine.spec import Design, FixedDate, agg

    path = tmp_path / "owner.duckdb"
    con = ibis.duckdb.connect(str(path))
    con.raw_sql("CREATE TABLE events(ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
    con.raw_sql("CREATE TABLE unused_events(ts TIMESTAMP, other_id INTEGER)")
    con.raw_sql("CREATE TABLE users(id INTEGER)")
    con.raw_sql("INSERT INTO users VALUES (1)")
    con.raw_sql("CREATE TABLE other_users(id INTEGER)")
    con.raw_sql("INSERT INTO other_users VALUES (999)")
    catalog = Catalog.model_validate(
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
                },
                "unused": {
                    "relation": {"kind": "table", "name": "unused_events"},
                    "timestamp": "ts",
                    "entities": {"other": "other_id"},
                    "facts": [],
                    "properties": [],
                    "dimensions": [],
                },
            },
            "facts": {"value": {"source": "events", "column": "value"}},
            "properties": {},
            "dimensions": {},
            "measures": {"total": {"fact": "value", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "entity": "user", "measure": "total"}},
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        units={
            "user": UnitSource("users", "id"),
            "other": UnitSource("other_users", "id"),
        },
        readiness={
            "events": Coverage("2025-01-01", "2025-02-01"),
            "unused": Coverage("2025-01-01", "2025-02-01"),
        },
    )
    design = Design(
        "user",
        FixedDate("2025-01-02T00:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    ordinary = design.build(model).materialize()
    owner = RuntimeOwner(state_path=path, snapshot_root=tmp_path / "snapshots")
    authority = SnapshotAuthority(owner)
    store = RuntimeStore(con.con, namespace="runtime")
    store.initialize()
    snapshot = authority.capture(
        model, design=design, con=con, namespace="runtime", run_id="saved", budget=Budget()
    )
    graph = dependency_graph(
        model,
        design,
        sources=("events",),
        definition_hash=design.spec_hash,
        partitions=("all",),
    )
    plan = plan_refresh(
        namespace="runtime",
        dataset="orders",
        definition_hash=design.spec_hash,
        snapshot_id=snapshot.snapshot_id,
        graph=graph,
    )
    recipe = SavedPlanRecipe.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference.from_model(model, owner_key="model"),
        destination=DestinationRef("output.duckdb", "analytics", "orders"),
        namespace="runtime",
        expected_base_manifest=None,
    )
    recipe.pin_snapshot(store)
    with pytest.raises(TerrineError) as destination_error:
        runtime.run_serialized_plan(
            recipe.to_json(),
            con=con.con,
            namespace="runtime",
            destination=DestinationRef("other.duckdb", "analytics", "orders"),
            request_key="wrong-destination",
        )
    assert destination_error.value.code == "terrine.runtime.plan_recipe_mismatch"

    class Resolver:
        def resolve(self, reference):
            return model

    class SnapshotExecutor:
        def __init__(self):
            self.observed = []
            self.fail_first = True

        def run_snapshot_task(self, context, task, dependencies):
            if self.fail_first:
                self.fail_first = False
                raise RuntimeError("retry saved recipe")
            relation = context.resolve(catalog.sources["events"].relation)
            self.observed.append(context.execute(relation).to_pylist()[0]["value"])
            if len(self.observed) == 1:
                con.con.execute("UPDATE events SET value=99")
            return pa.table({"id": [task.task_id]})

        def publish(self, plan, outputs, lease, run_id):
            return "generation-1"

    executor = SnapshotExecutor()
    with pytest.raises(RuntimeError, match="retry saved recipe"):
        runtime.run_serialized_plan(
            recipe.to_json(),
            con=con.con,
            namespace="runtime",
            request_key="saved-run",
            executor=cast(Any, executor),
            resolver=Resolver(),
            snapshot_authority=authority,
        )
    failed = runtime.find_run_by_request_key(
        "saved-run",
        destination=DestinationRef("output.duckdb", "analytics", "orders"),
        con=con.con,
        namespace="runtime",
    )
    assert failed is not None
    failed_status = runtime.get_run(failed.run_id, con=con.con, namespace="runtime")
    assert failed_status.phase == "failed"
    assert failed_status.error_code == "terrine.runtime.execution_failed"
    assert failed_status.progress == {
        "completed_tasks": 0,
        "running_tasks": 0,
        "total_tasks": len(plan.tasks),
    }
    conflicting_plan = plan
    conflicting_recipe = SavedPlanRecipe.create(
        plan_json=conflicting_plan.serialize(),
        design=design,
        model=ModelReference.from_model(model, owner_key="model"),
        destination=DestinationRef("output.duckdb", "analytics", "orders"),
        namespace="runtime",
        expected_base_manifest="different-base",
    )
    with pytest.raises(TerrineError) as error:
        runtime.run_serialized_plan(
            conflicting_recipe.to_json(),
            con=con.con,
            namespace="runtime",
            request_key="saved-run",
        )
    assert error.value.code == "terrine.runtime.request_key_conflict"

    result = runtime.run_serialized_plan(
        recipe.to_json(),
        con=con.con,
        namespace="runtime",
        request_key="saved-run",
        executor=cast(Any, executor),
        resolver=Resolver(),
        snapshot_authority=authority,
    )
    replay = runtime.run_serialized_plan(
        recipe.to_json(),
        con=con.con,
        namespace="runtime",
        request_key="saved-run",
    )
    assert replay == result
    assert result.state == "succeeded"
    assert executor.observed == [3] * len(plan.tasks)

    from dataclasses import replace

    for binding in ModelReference.__dataclass_fields__:
        changed_reference = replace(recipe.model, **{binding: f"changed-{binding}"})
        changed_recipe = SavedPlanRecipe.create(
            plan_json=recipe.plan_json,
            design=design,
            model=changed_reference,
            destination=recipe.destination,
            namespace="runtime",
            expected_base_manifest=None,
        )
        with pytest.raises(TerrineError) as changed_error:
            runtime.run_serialized_plan(
                changed_recipe.to_json(), con=con.con, namespace="runtime", request_key="saved-run"
            )
        assert changed_error.value.code == "terrine.runtime.request_key_conflict"

    built_in = runtime.run_serialized_plan(
        recipe.to_json(),
        con=con.con,
        namespace="runtime",
        request_key="built-in-scoped-model",
        resolver=Resolver(),
        snapshot_authority=authority,
    )
    published = runtime.open_published_frame(built_in, con=con.con, namespace="runtime")
    assert built_in.state == "succeeded"
    assert published.table.num_rows == 1
    assert published.table["total"].to_pylist() == [3.0]
    status = runtime.get_run(built_in.run_id, con=con.con, namespace="runtime")
    assert status.phase == "succeeded"
    assert status.error_code is None
    assert status.progress == {
        "completed_tasks": len(plan.tasks),
        "running_tasks": 0,
        "total_tasks": len(plan.tasks),
    }
    assert status.cost["capture"] == {
        "snapshot_id": snapshot.snapshot_id,
        "artifact_bytes": Path(snapshot.artifact_path).stat().st_size,
        "root_count": 2,
        "scan_bytes": None,
    }
    assert status.cost["status"] == "observed"
    assert status.cost["scope"] == "completed_task_artifacts"
    assert status.cost["attempts"] == len(plan.tasks)
    assert status.cost["output_rows"] == published.table.num_rows
    assert status.cost["output_bytes"] == published.table.nbytes
    assert status.cost["intermediate_output_bytes"] > 0
    assert status.cost["scan_bytes"] is None
    assert status.readiness == {
        "events": {
            "start": "2025-01-01T00:00:00+00:00",
            "end_exclusive": "2025-02-02T00:00:00+00:00",
        }
    }

    from sqlglot import exp, parse_one

    class RuntimeQuerySpy:
        def __init__(self):
            self.queries = []

        def execute(self, sql, *args, **kwargs):
            self.queries.append(sql)
            for table in parse_one(sql, read="duckdb").find_all(exp.Table):
                assert table.db == "information_schema" or (
                    table.db == "runtime" and (not table.name or table.name.startswith("runtime_"))
                )
            return con.con.execute(sql, *args, **kwargs)

    query_spy = RuntimeQuerySpy()
    assert runtime.get_run(built_in.run_id, con=query_spy, namespace="runtime") == status
    assert runtime.read_run_events(built_in.run_id, con=query_spy, namespace="runtime").events
    assert query_spy.queries

    from dataclasses import fields

    assert {
        field.name: getattr(published.provenance, field.name)
        for field in fields(published.provenance)
        if field.name not in {"source_generations", "execution_snapshot"}
    } == {
        field.name: getattr(ordinary.provenance, field.name)
        for field in fields(ordinary.provenance)
        if field.name not in {"source_generations", "execution_snapshot"}
    }


@pytest.mark.parametrize("expression", ["none", "metric", "relative_metric"])
def test_rosterless_event_snapshot_executes_all_required_sources(tmp_path, expression):
    from terrine.compile.dispatch import compile_design
    from terrine.spec import EventAnchor, event_time
    from terrine.spec.validate import validate_design

    state_path = tmp_path / "owner.duckdb"
    owner = RuntimeOwner(state_path=state_path, snapshot_root=tmp_path / "snapshots")
    con = ibis.duckdb.connect(str(state_path))
    definition = _catalog().model_dump(mode="python")
    instants = {
        "events": "2025-01-03 12:00:00",
        "anchors": "2025-01-05",
        "selectors": "2025-01-03",
        "predecessors": "2025-01-02 12:00:00",
    }
    try:
        for source, instant in instants.items():
            con.raw_sql(f"CREATE TABLE {source} (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
            con.raw_sql(f"INSERT INTO {source} VALUES ('{instant}', 1, 7)")
            if source != "events":
                definition["sources"][source] = {
                    "relation": {"kind": "table", "name": source},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": [source],
                    "properties": [],
                    "dimensions": [],
                }
                definition["facts"][source] = {"source": source, "column": "value"}
        model = Model.from_catalog(
            Catalog.model_validate(definition),
            con=con,
            readiness={name: Coverage("2025-01-01", "2025-02-01") for name in instants},
        )
        features = {}
        required = {"anchors"}
        if expression != "none":
            relative_to = None
            window = ("-4d", "0d")
            required.add("events")
            if expression == "relative_metric":
                relative_to = event_time(
                    "selectors",
                    window=("0d", "1d"),
                    relative_to=event_time("predecessors", window=("-3d", "-2d")),
                )
                window = ("0d", "1d")
                required.update(("selectors", "predecessors"))
            features["total"] = agg("total", window=window, relative_to=relative_to)
        design = Design("user", EventAnchor("anchors"), features=features)
        authority = SnapshotAuthority(owner)
        receipt = authority.capture(
            model, design=design, con=con, namespace="runtime", run_id="events", budget=Budget()
        )
        assert {entry["relation_key"] for entry in receipt.roots} == required
        for source in instants:
            con.raw_sql(f"DELETE FROM {source}")
        with authority.reopen(receipt, con=con, namespace="runtime") as execution:
            execution_model = model._for_execution(
                execution,
                source_names=set(validate_design(design, model).required_sources),
                unit_name=design.unit,
            )
            compiled = compile_design(design, execution_model)
            compiled.validate(execute=execution.execute)
            rows = execution.execute(compiled.table).to_pylist()
        assert len(rows) == 1
        assert rows[0]["unit"] == 1
        assert rows[0]["t0"] == datetime(2025, 1, 5, tzinfo=UTC)
        if features:
            assert rows[0]["total"] == 7
    finally:
        con.disconnect()


def test_owner_planner_persists_same_snapshot_and_manifest_by_invocation(tmp_path, monkeypatch):
    import terrine.runtime.planning as planning

    captured = False
    original_validate = planning.validate_compiled_output
    original_capture = SnapshotAuthority.capture

    def capture(*args, **kwargs):
        nonlocal captured
        snapshot = original_capture(*args, **kwargs)
        captured = True
        return snapshot

    def validate_on_snapshot(compiled, execute):
        assert captured
        assert execute.__self__ is not model.con
        return original_validate(compiled, execute)

    monkeypatch.setattr(SnapshotAuthority, "capture", capture)
    monkeypatch.setattr(planning, "validate_compiled_output", validate_on_snapshot)
    from tests.runtime.test_recipe import _model

    database = tmp_path / "catalog.duckdb"
    ibis_connection = ibis.duckdb.connect(database=str(database))
    ibis_connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    ibis_connection.raw_sql("CREATE TABLE users (id INTEGER)")

    ibis_connection.raw_sql(
        "CREATE TABLE unused_events AS SELECT ts, user_id, value FROM events WHERE false"
    )
    from terrine.semantic.models import TableRef

    base_model = _model(ibis_connection)
    unused_source = base_model.catalog.sources["events"].model_copy(
        update={
            "relation": TableRef(kind="table", name="unused_events"),
            "facts": (),
            "properties": (),
            "dimensions": (),
        }
    )
    catalog = base_model.catalog.model_copy(
        update={"sources": {**base_model.catalog.sources, "unused": unused_source}}
    )
    model = Model.from_catalog(
        catalog,
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
        raw_connection.execute(
            "CREATE TABLE runtime.runtime_manifests "
            "(dataset VARCHAR PRIMARY KEY, generation VARCHAR NOT NULL, version BIGINT, "
            "snapshot_id VARCHAR)"
        )
        raw_connection.execute(
            "INSERT INTO runtime.runtime_manifests VALUES ('study', 'generation-current', 1, NULL)"
        )
        raw_connection.execute(
            "DELETE FROM runtime.runtime_snapshot_manifests WHERE dataset='study'"
        )

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
        roster_only = planner.plan(
            PlanRequest(
                "materialize-roster-only",
                request.model,
                Design("user", FixedDate("2025-01-15"), features={}, outcomes={}),
                request.destination,
            ),
            con=raw_connection,
            namespace="RUNTIME",
        )
        assert roster_only.expected_base_manifest == "generation-current"
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
    assert fresh.expected_base_manifest == "generation-current"
    assert conflict.value.code == "terrine.runtime.plan_invocation_conflict"


def test_unknown_snapshot_refuses_only_retained_execution_before_source_work(tmp_path, monkeypatch):
    path = tmp_path / "owner.duckdb"
    con = ibis.duckdb.connect(str(path))
    try:
        con.con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
        con.con.execute("CREATE TABLE users AS SELECT 1 AS user_id")
        model = Model.from_catalog(
            _catalog(),
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-02-01")},
            units={"user": UnitSource("users", "user_id")},
        )
        provider = backend_provider(con)
        monkeypatch.setattr(
            provider, "capabilities", replace(provider.capabilities, retained_snapshot=None)
        )
        frame = _design().build(model)
        assert frame.materialize().table["total"].to_pylist() == [3]
        con.con.execute("DROP TABLE events")
        owner = RuntimeOwner(state_path=path, snapshot_root=tmp_path / "snapshots")
        with pytest.raises(TerrineError) as error:
            SnapshotAuthority(owner).capture(
                model, design=_design(), con=con, namespace="runtime", run_id="run", budget=Budget()
            )
        assert error.value.code == "terrine.backend.capability_unavailable"
        assert error.value.context["operation"] == "retained_snapshot"
        assert error.value.context["status"] == "unverified"
        assert not owner.snapshot_root.exists()
        for restored in (copy.deepcopy(error.value), pickle.loads(pickle.dumps(error.value))):
            assert restored.context == error.value.context
            with pytest.raises(TypeError):
                restored.context["status"] = "verified"  # ty: ignore[invalid-assignment]
    finally:
        con.disconnect()
