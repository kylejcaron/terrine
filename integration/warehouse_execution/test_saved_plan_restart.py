from __future__ import annotations

import json
import os
import subprocess
import sys

import ibis

import pyarrow as pa

from terrine.semantic.models import Catalog
from terrine.data import Budget, Coverage
from terrine.model import Model
from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
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


def test_saved_plan_resolves_in_fresh_process_from_owner_config(tmp_path):
    state_path = tmp_path / "owner.duckdb"
    con = ibis.duckdb.connect(str(state_path))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    catalog = _catalog()
    model = _model(con, catalog)
    design = Design("user", FixedDate("2025-01-15"))
    plan = plan_refresh(
        namespace="runtime",
        dataset="study",
        definition_hash=design.spec_hash,
        snapshot_id="snapshot-1",
        graph=dependency_graph(
            design=design, sources=("events",), definition_hash=design.spec_hash
        ),
    )
    recipe = SavedPlanRecipe.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference.from_model(model, owner_key="workspace/model"),
        destination=DestinationRef(str(state_path), "analytics", "study"),
        namespace="runtime",
        expected_base_manifest=None,
    )

    recipe_path = tmp_path / "plan.json"
    catalog_path = tmp_path / "catalog.json"
    recipe_path.write_text(recipe.to_json(), encoding="utf-8")
    catalog_path.write_text(
        json.dumps(catalog.model_dump(mode="json"), sort_keys=True), encoding="utf-8"
    )
    con.disconnect()
    script = r"""
import json, sys
import ibis
import pyarrow as pa
from terrine.data import Coverage
from terrine.model import Model
from terrine.runtime.execute import execute_plan
from terrine.runtime.recipe import SavedPlanRecipe, resolve_plan
from terrine.semantic.models import Catalog

state_path, recipe_path, catalog_path = sys.argv[1:]
class OwnerResolver:
    def resolve(self, reference):
        assert reference.owner_key == 'workspace/model'
        self.connection = ibis.duckdb.connect(state_path)
        catalog = Catalog.model_validate(json.loads(open(catalog_path).read()))
        return Model.from_catalog(catalog, con=self.connection,
            readiness={'events': Coverage('2025-01-01', '2025-02-01')})
recipe = SavedPlanRecipe.from_json(open(recipe_path).read())
resolver = OwnerResolver()
plan, model, design, destination = resolve_plan(
    recipe, resolver=resolver, namespace='runtime')
assert model.model_hash == recipe.model.model_hash
assert design.spec_hash == recipe.design_hash
assert destination.database == state_path
receipt = execute_plan(plan, con=resolver.connection.con, namespace='runtime',
    run_id='fresh-process', run_task=lambda task, dependencies:
        pa.table({'layer': [task.layer]}),
    publish=lambda plan, outputs, lease: 'generation-1')
assert receipt.state == 'succeeded'
resolver.connection.disconnect()
"""
    subprocess.run(
        [sys.executable, "-c", script, str(state_path), str(recipe_path), str(catalog_path)],
        check=True,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )


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
