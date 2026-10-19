from typing import Any, cast

import duckdb
import ibis
import pyarrow as pa
import pytest

import terrine.runtime as runtime
from terrine.errors import TerrineError
from terrine.runtime.execute import RunReceipt
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef


class _Executor:
    def __init__(self):
        self.calls = 0

    def run_task(self, task, dependencies):
        self.calls += 1
        return pa.table({"id": [self.calls]})

    def publish(self, plan, outputs, lease, run_id):
        return "g1"


def _plan():
    return plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=dependency_graph(
            sources=("events",), definition_hash="definition", partitions=("p1",)
        ),
    )


def test_runtime_facade_has_explicit_stable_exports():
    assert runtime.__all__ == sorted(runtime.__all__)
    assert {"run_plan", "run_serialized_plan", "find_run_by_request_key"}.issubset(runtime.__all__)


def test_run_plan_requires_executor_for_new_work():
    with pytest.raises(TerrineError) as error:
        runtime.run_plan(_plan(), con=duckdb.connect(":memory:"), namespace="owner")
    assert error.value.code == "terrine.runtime.executor_unavailable"


def test_run_serialized_plan_executes_through_python_facade():
    con = duckdb.connect(":memory:")
    executor = _Executor()

    receipt = runtime.run_serialized_plan(
        _plan().serialize(), con=con, namespace="owner", executor=executor
    )

    assert isinstance(receipt, RunReceipt)
    assert receipt.state == "succeeded"
    assert executor.calls == len(_plan().tasks)


def test_saved_recipe_execution_reopens_retained_snapshot_for_each_task(tmp_path):
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
    assert (
        runtime.find_run_by_request_key(
            "saved-run",
            destination=DestinationRef("output.duckdb", "analytics", "orders"),
            con=con.con,
            namespace="runtime",
        )
        is not None
    )
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


def test_custom_saved_recipe_executor_is_not_limited_by_builtin_shape_preflight(tmp_path):
    from terrine.data import Budget, Coverage
    from terrine.model import Model
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
                }
            },
            "facts": {"value": {"source": "events", "column": "value"}},
            "properties": {},
            "dimensions": {},
            "measures": {"total": {"fact": "value", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "entity": "user", "measure": "total"}},
        }
    )
    model = Model.from_catalog(
        catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )
    design = Design(
        "user",
        FixedDate("2025-01-02T00:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
        shape="unsupported-test-shape",
    )
    owner = RuntimeOwner(state_path=path, snapshot_root=tmp_path / "snapshots")
    authority = SnapshotAuthority(owner)
    store = RuntimeStore(con.con, namespace="runtime")
    store.initialize()
    snapshot = authority.capture(
        model, design=design, con=con, namespace="runtime", run_id="custom", budget=Budget()
    )
    plan = plan_refresh(
        namespace="runtime",
        dataset="orders",
        definition_hash=design.spec_hash,
        snapshot_id=snapshot.snapshot_id,
        graph=dependency_graph(
            model, design, sources=("events",), definition_hash=design.spec_hash
        ),
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

    class Resolver:
        def resolve(self, reference):
            return model

    class CustomExecutor:
        def run_snapshot_task(self, context, task, dependencies):
            return pa.table({"id": [task.task_id]})

        def publish(self, plan, outputs, lease, run_id):
            return "custom-generation"

    result = runtime.run_serialized_plan(
        recipe.to_json(),
        con=con.con,
        namespace="runtime",
        executor=CustomExecutor(),
        resolver=Resolver(),
        snapshot_authority=authority,
    )
    assert result.state == "succeeded"


def test_published_generation_uses_receipt_generation_not_current_manifest():
    from terrine.runtime.backends.base import StagedGeneration
    from terrine.runtime.backends.duckdb import DuckDBBackend

    con = duckdb.connect(":memory:")

    class Publisher(_Executor):
        def __init__(self):
            super().__init__()
            self.generation = 0

        def publish(self, plan, outputs, lease, run_id):
            self.generation += 1
            generation = f"g{self.generation}"
            return DuckDBBackend(con, namespace="owner").publish_generation(
                StagedGeneration(
                    plan.dataset,
                    generation,
                    {task_id: output for task_id, output in outputs.items()},
                    run_id=run_id,
                    snapshot_id=plan.snapshot_id,
                ),
                expected_manifest="g1" if generation == "g2" else None,
                lease=lease,
            )

    publisher = Publisher()
    first = runtime.run_plan(_plan(), con=con, namespace="owner", executor=publisher)
    second = runtime.run_plan(_plan(), con=con, namespace="owner", executor=publisher)
    exact = runtime.open_published_generation(first, con=con, namespace="owner")

    assert isinstance(exact, ibis.Table)
    assert exact.execute().to_dict(orient="records") == [{"id": len(_plan().tasks)}]
    assert second.published_generation == "g2"


def test_published_frame_reader_roundtrips_exact_generation_metadata():
    from datetime import UTC, datetime

    from terrine.data import ColumnMeta, MaterializedFrame, Roles
    from terrine.runtime.backends.base import StagedGeneration
    from terrine.runtime.backends.duckdb import DuckDBBackend
    from tests.test_metadata import _provenance

    con = duckdb.connect(":memory:")
    clean = MaterializedFrame(
        pa.table(
            {
                "unit": ["u"],
                "anchor": ["a"],
                "t0": pa.array(
                    [datetime(2025, 1, 1, tzinfo=UTC)], type=pa.timestamp("us", tz="UTC")
                ),
                "revenue": [3],
                "revenue_complete": [True],
                "revenue_reason": [None],
            }
        ),
        Roles("unit", "anchor", "t0", None, (), ("revenue",)),
        {"revenue": ColumnMeta(None, "outcome", None, None, "revenue_complete", "revenue_reason")},
        _provenance(),
    )

    class Publisher(_Executor):
        def run_task(self, task, dependencies):
            return clean.to_arrow()

        def publish(self, plan, outputs, lease, run_id):
            return DuckDBBackend(con, namespace="owner").publish_generation(
                StagedGeneration(
                    plan.dataset,
                    "g1",
                    outputs,
                    run_id=run_id,
                    snapshot_id=plan.snapshot_id,
                ),
                expected_manifest=None,
                lease=lease,
            )

    receipt = runtime.run_plan(_plan(), con=con, namespace="owner", executor=Publisher())
    restored = runtime.open_published_frame(receipt, con=con, namespace="owner")

    assert restored.roles == clean.roles
    assert restored.columns == clean.columns
    assert restored.provenance == clean.provenance
    assert restored.table.column("revenue").to_pylist() == clean.table.column("revenue").to_pylist()


def test_request_key_replay_is_available_without_executor():
    con = duckdb.connect(":memory:")
    destination = DestinationRef("out.duckdb", "analytics", "orders")
    executor = _Executor()
    first = runtime.run_plan(
        _plan(),
        con=con,
        namespace="owner",
        request_key="response-lost",
        destination=destination,
        executor=executor,
    )

    replay = runtime.run_plan(
        _plan(),
        con=con,
        namespace="owner",
        request_key="response-lost",
        destination=destination,
    )

    assert replay == first
    assert executor.calls == len(_plan().tasks)


def test_public_request_replay_refuses_a_different_plan_without_executor():
    con = duckdb.connect(":memory:")
    destination = DestinationRef("out.duckdb", "analytics", "orders")
    runtime.run_plan(
        _plan(),
        con=con,
        namespace="owner",
        request_key="same-key",
        destination=destination,
        executor=_Executor(),
    )
    changed = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="other-definition",
        snapshot_id="snapshot",
        graph=dependency_graph(
            sources=("events",), definition_hash="other-definition", partitions=("p1",)
        ),
    )

    with pytest.raises(TerrineError) as error:
        runtime.run_plan(
            changed,
            con=con,
            namespace="owner",
            request_key="same-key",
            destination=destination,
        )

    assert error.value.code == "terrine.runtime.request_key_conflict"


def test_empty_refresh_plan_reuses_current_generation_result_selection():
    from terrine.runtime.backends.base import StagedGeneration
    from terrine.runtime.backends.duckdb import DuckDBBackend
    from terrine.runtime.changes import ChangeSet

    con = duckdb.connect(":memory:")
    destination = DestinationRef("out.duckdb", "analytics", "orders")

    class Publisher(_Executor):
        def __init__(self):
            super().__init__()
            self.generation = 0

        def publish(self, plan, outputs, lease, run_id):
            backend = DuckDBBackend(con, namespace="owner")
            if not outputs:
                row = con.execute(
                    'SELECT generation,version FROM "owner".runtime_manifests WHERE dataset=?',
                    [plan.dataset],
                ).fetchone()
                assert row is not None
                generation, version = row
                con.execute(
                    'INSERT INTO "owner".runtime_run_publications VALUES (?, ?, ?, ?)',
                    [run_id, plan.dataset, generation, version],
                )
                return generation
            self.generation += 1
            generation = f"g{self.generation}"
            return backend.publish_generation(
                StagedGeneration(
                    plan.dataset,
                    generation,
                    outputs,
                    run_id=run_id,
                    snapshot_id=plan.snapshot_id,
                ),
                expected_manifest="g1" if generation == "g2" else None,
                lease=lease,
            )

    executor = Publisher()
    first_plan = _plan()
    first = runtime.run_plan(
        first_plan,
        con=con,
        namespace="owner",
        request_key="initial",
        destination=destination,
        executor=executor,
    )
    changed_graph = dependency_graph(
        sources=("events", "exports"), definition_hash="definition", partitions=("p1",)
    )
    changed = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=changed_graph,
    )
    before_changed = executor.calls
    second = runtime.run_plan(
        changed,
        con=con,
        namespace="owner",
        request_key="include-exports",
        destination=destination,
        executor=executor,
    )
    no_op = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=changed_graph,
        changes=ChangeSet((), "revision"),
    )
    receipt = runtime.run_plan(
        no_op,
        con=con,
        namespace="owner",
        request_key="no-op",
        destination=destination,
        executor=executor,
    )
    dependencies = {dependency for task in changed.tasks for dependency in task.dependencies}
    sink_indexes = [
        index + 1 for index, task in enumerate(changed.tasks) if task.task_id not in dependencies
    ]
    expected_second = [{"id": before_changed + index} for index in sink_indexes]

    assert receipt.state == "succeeded"
    assert receipt.published_generation == second.published_generation == "g2"
    assert executor.calls == before_changed + len(changed.tasks)
    assert runtime.open_published_generation(first, con=con, namespace="owner").execute().to_dict(
        orient="records"
    ) == [{"id": len(first_plan.tasks)}]
    assert sorted(
        runtime.open_published_generation(second, con=con, namespace="owner")
        .execute()
        .to_dict(orient="records"),
        key=lambda row: row["id"],
    ) == sorted(expected_second, key=lambda row: row["id"])
    assert sorted(
        runtime.open_published_generation(receipt, con=con, namespace="owner")
        .execute()
        .to_dict(orient="records"),
        key=lambda row: row["id"],
    ) == sorted(expected_second, key=lambda row: row["id"])


def test_empty_plan_with_string_publisher_remains_successful():
    from terrine.runtime.changes import ChangeSet

    con = duckdb.connect(":memory:")
    no_op = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=dependency_graph(
            sources=("events",), definition_hash="definition", partitions=("p1",)
        ),
        changes=ChangeSet((), "revision"),
    )

    class NoOpExecutor:
        def run_task(self, task, dependencies):
            raise AssertionError("empty plan must not execute tasks")

        def publish(self, plan, outputs, lease, run_id):
            assert not outputs
            return "g0"

    receipt = runtime.run_plan(no_op, con=con, namespace="owner", executor=NoOpExecutor())

    assert receipt.state == "succeeded"
    assert receipt.published_generation == "g0"


def test_design_executor_refuses_multiple_output_partitions():
    from terrine.runtime.api import _preflight_design_partitions

    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=dependency_graph(
            sources=("events",), definition_hash="definition", partitions=("p1", "p2")
        ),
    )

    with pytest.raises(TerrineError) as error:
        _preflight_design_partitions(plan)

    assert error.value.code == "terrine.runtime.design_partition_unsupported"


def test_custom_saved_recipe_executor_keeps_state_panel_budget_refusal():
    from types import SimpleNamespace

    from terrine.runtime.recipe import ModelReference, SavedPlanRecipe
    from terrine.spec import Design, Schedule
    from terrine.spec.state_panel import StateObservations, StatePanel
    from tests.runtime.test_recipe import _model

    con = ibis.duckdb.connect()
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    model = _model(con)
    design = Design(
        "user",
        Schedule("1d", "2025-01-02", "2025-01-04"),
        shape=StatePanel(StateObservations("status", ("new", "old")), max_periods=3),
    )
    plan = plan_refresh(
        namespace="runtime",
        dataset="study",
        definition_hash=design.spec_hash,
        snapshot_id="snapshot",
        graph=dependency_graph(
            model, design, sources=("events",), definition_hash=design.spec_hash
        ),
    )
    recipe = SavedPlanRecipe.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference.from_model(model, owner_key="model"),
        destination=DestinationRef("output.duckdb", "analytics", "study"),
        namespace="runtime",
        expected_base_manifest=None,
    )

    class Resolver:
        def resolve(self, reference):
            return model

    class SnapshotAuthority:
        def find_retained(self, snapshot_id, *, namespace):
            return SimpleNamespace(model_hash=model.model_hash)

    class CustomExecutor:
        def run_snapshot_task(self, context, task, dependencies):
            raise AssertionError("budget refusal must precede custom execution")

    with pytest.raises(TerrineError) as error:
        runtime.run_serialized_plan(
            recipe.to_json(),
            con=con.con,
            namespace="runtime",
            executor=CustomExecutor(),
            resolver=Resolver(),
            snapshot_authority=cast(Any, SnapshotAuthority()),
        )

    assert error.value.code == "terrine.state_panel.budget_required"
    con.disconnect()
