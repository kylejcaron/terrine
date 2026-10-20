from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import replace
from decimal import Decimal

import ibis
import pytest

from terrine.compile.anchors import compile_anchors
from terrine.compile.columns import compile_columns
from terrine.compile.layers import plan_layers
from terrine.compile.materialize import compose_materialized_frame
from terrine.data import Budget, Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg
from tests._timing import HANG_GUARD_SECONDS
from tests.compile.test_columns import column_model as column_model
from tests.compile.test_persisted_layers import _persisted_prefixes


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
        timeout=HANG_GUARD_SECONDS,
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


def test_persisted_daily_prefix_anchor_reuse_matches_ordinary_frame_compiler(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-03T00:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    ordinary = compile_columns(design, column_model)
    persisted = _persisted_prefixes(design, column_model)
    anchor_key = next(
        key for key in plan_layers(design, column_model).keys if key.layer == "anchor"
    )
    anchor_table = compile_anchors(design, column_model).table
    anchor_arrow = column_model.con.to_pyarrow(anchor_table)
    persisted_anchor = ibis.memtable(anchor_arrow)
    persisted[replace(anchor_key, snapshot_id="same-snapshot")] = persisted_anchor
    reused = compose_materialized_frame(design, column_model, persisted=persisted)
    assert reused._anchors.table is persisted_anchor
    ordinary.validate(execute=column_model.con.to_pyarrow)
    reused.validate(execute=column_model.con.to_pyarrow)
    assert column_model.con.to_pyarrow(ordinary.audit_table.order_by("unit")).equals(
        column_model.con.to_pyarrow(reused.audit_table.order_by("unit"))
    )
    assert ordinary.columns == reused.columns


def test_runtime_and_frame_share_unknown_eligibility_refusal(column_model):
    from terrine.runtime.materialize import compose_runtime_frame
    from terrine.spec import all_of, asof, eq, gt

    design = Design(
        "user",
        FixedDate("2025-01-03T00:00:00+00:00"),
        eligibility={
            "eligible": all_of(
                eq(asof("tier", before="0s"), "not-present"),
                gt(asof("tier", before="-2d"), "a"),
            )
        },
    )
    frame = design.build(column_model)
    refusals = []
    for operation in (
        lambda: frame.materialize(),
        lambda: compose_runtime_frame(design, column_model, persisted={}),
    ):
        with pytest.raises(TerrineError) as error:
            operation()
        refusals.append((error.value.code, error.value.context))
    assert refusals[0] == refusals[1]
    assert refusals[0][0] == "terrine.study.eligibility_unknown"


@pytest.mark.parametrize("shape", ["sequential", "event_panel"])
def test_persisted_shape_matrix_preserves_panel_rows_and_metadata(column_model, shape):
    from datetime import UTC, datetime

    from terrine.compile.dispatch import compile_design
    from terrine.data import Coverage
    from terrine.spec import EventAnchor, EventPanel, Initiation, Schedule, Sequential

    column_model = Model.from_catalog(
        column_model.catalog,
        con=column_model.con,
        units=column_model.units,
        readiness={"events": Coverage("2025-01-01", "2025-01-05")},
    )
    if shape == "sequential":
        design = Sequential(
            Design(
                "user",
                Schedule("12h", "2025-01-03", "2025-01-05"),
                features={"total": agg("total", window=("-1d", "0s"))},
            ),
            treatment=Initiation("events", "amount", datetime(2025, 1, 1, tzinfo=UTC)),
        )
    else:
        design = EventPanel(
            Design(
                "user",
                EventAnchor("amount", "first"),
                features={"total": agg("total", window=("-1h", "0s"))},
            ),
            periods=(-1, 0, 1),
            period_seconds=3600,
        )
    ordinary = compile_design(design, column_model)
    persisted = _persisted_prefixes(design, column_model)
    anchor_key = next(
        key for key in plan_layers(design, column_model).keys if key.layer == "anchor"
    )
    parent = ibis.memtable(column_model.con.to_pyarrow(compile_anchors(design, column_model).table))
    persisted[replace(anchor_key, snapshot_id="same-snapshot")] = parent
    reused = compose_materialized_frame(design, column_model, persisted=persisted)
    ordinary.validate(execute=column_model.con.to_pyarrow)
    reused.validate(execute=column_model.con.to_pyarrow)
    assert column_model.con.to_pyarrow(ordinary.audit_table.order_by("unit", "t0")).equals(
        column_model.con.to_pyarrow(reused.audit_table.order_by("unit", "t0"))
    )
    assert reused.roles == ordinary.roles
    assert reused.columns == ordinary.columns
    assert reused.expression_timings == ordinary.expression_timings
    assert reused.sequence_meta == ordinary.sequence_meta
    assert reused.range_plans == ordinary.range_plans


@pytest.mark.parametrize("state_kind", ["binned", "observed"])
@pytest.mark.parametrize(
    ("every", "totals", "fresh_totals"),
    [
        ("1d", [5, 10], [105, 210]),
        ("12h", [5, 8, 10, 7], [105, 208, 210, 107]),
    ],
)
def test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors(
    column_model, state_kind, every, totals, fresh_totals
):
    from terrine.compile.dispatch import compile_design
    from terrine.compile.layers import build_layer
    from terrine.data import Coverage
    from terrine.spec import BinnedMetricState, Schedule, StateObservations, StatePanel

    con = column_model.con
    con.raw_sql(
        "INSERT INTO events VALUES "
        "('a', '2025-01-01 12:00:00+00', 5, 'old'), "
        "('a', '2025-01-03 12:00:00+00', 6, 'new')"
    )
    definition = column_model.catalog.model_dump(mode="python")
    readiness = {"events": Coverage("2025-01-01", "2025-01-05")}
    state: BinnedMetricState | StateObservations = BinnedMetricState(
        "total", cutpoints=(20,), levels=("low", "high")
    )
    if state_kind == "observed":
        con.raw_sql(
            "CREATE TABLE statuses AS SELECT user_id, ts::TIMESTAMPTZ AS ts, state "
            "FROM (VALUES "
            "('a', '2025-01-02 00:00:00+00', 'active'), "
            "('a', '2025-01-03 00:00:00+00', 'active'), "
            "('a', '2025-01-04 00:00:00+00', 'closed')) AS points(user_id, ts, state)"
        )
        definition["sources"]["statuses"] = {
            "relation": {"kind": "table", "name": "statuses"},
            "timestamp": "ts",
            "entities": {"user": "user_id"},
            "facts": [],
            "properties": ["state"],
            "dimensions": [],
        }
        definition["properties"]["state"] = {
            "source": "statuses",
            "column": "state",
            "dtype": "string",
            "as_of": "event_time",
        }
        readiness["statuses"] = Coverage("2025-01-01", "2025-01-05")
        state = StateObservations("state", ("active", "closed"), absorbing=("closed",))
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        units=column_model.units,
        readiness=readiness,
    )
    design = Design(
        "user",
        Schedule(every, "2025-01-02", "2025-01-04"),
        features={"total": agg("total", window=("-1d", "0s"))},
        shape=StatePanel(
            state,
            max_periods=len(totals),
            baseline_features={"baseline_total": agg("total", window=("-1d", "0s"))},
        ),
    )
    ordinary = compile_design(design, model)
    expected = con.to_pyarrow(ordinary.table.filter(ordinary.table.unit == "a").order_by("t0"))
    assert expected["total"].to_pylist() == [Decimal(value) for value in totals]
    assert expected["baseline_total"].to_pylist() == [Decimal(5)] * len(totals)
    persisted = {
        replace(key, snapshot_id="frozen"): con.create_table(
            f"frozen_state_layer_{index}", obj=build_layer(design, model, key)
        )
        for index, key in enumerate(plan_layers(design, model).keys)
    }
    anchor_key = next(key for key in persisted if key.layer == "anchor")
    anchors = persisted[anchor_key]
    persisted[anchor_key] = anchors.filter(anchors.unit == "a")
    con.raw_sql("UPDATE events SET amount=amount+100")

    cached = compose_materialized_frame(design, model, persisted=persisted)
    actual = con.to_pyarrow(cached.table.order_by("t0"))
    assert actual.equals(expected)
    assert {key.resolution for key in persisted if key.layer == "prefix"} == (
        {"day"} if every == "1d" else {"day", "event"}
    )
    assert cached._anchors.table is persisted[anchor_key]
    assert cached.roles == ordinary.roles
    assert cached.columns == ordinary.columns
    assert cached.expression_timings == ordinary.expression_timings
    fresh = compile_design(design, model)
    assert con.to_pyarrow(fresh.table.filter(fresh.table.unit == "a").order_by("t0"))[
        "total"
    ].to_pylist() == [Decimal(value) for value in fresh_totals]
