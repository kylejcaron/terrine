from __future__ import annotations

import ibis
import pyarrow as pa

from terrine.data import Budget, Coverage
from terrine.model import Model
from terrine.runtime import open_published_generation, run_serialized_plan
from terrine.runtime.backends.base import StagedGeneration
from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.graph import dependency_graph
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
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


def test_partition_and_retry_reopen_same_retained_input_authority(tmp_path):
    path = tmp_path / "owner.duckdb"
    con = ibis.duckdb.connect(str(path))
    con.raw_sql("CREATE TABLE events(ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
    catalog = _catalog()
    model = Model.from_catalog(
        catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )
    design = Design(
        "user",
        FixedDate("2025-01-02T00:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    owner = RuntimeOwner(state_path=path, snapshot_root=tmp_path / "snapshots")
    authority = SnapshotAuthority(owner)
    store = RuntimeStore(con.con, namespace="runtime")
    store.initialize()
    retained = authority.capture(
        model, design=design, con=con, namespace="runtime", run_id="initial", budget=Budget()
    )
    plan = plan_refresh(
        namespace="runtime",
        dataset="orders",
        definition_hash=design.spec_hash,
        snapshot_id=retained.snapshot_id,
        graph=dependency_graph(model, design, sources=("events",), partitions=("p1", "p2")),
    )
    destination = DestinationRef("output.duckdb", "analytics", "orders")
    recipe = SavedPlanRecipe.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference.from_model(model, owner_key="model"),
        destination=destination,
        namespace="runtime",
        expected_base_manifest=None,
    )
    recipe.pin_snapshot(store)

    class Resolver:
        def resolve(self, reference):
            assert reference == recipe.model
            return model

    class SnapshotExecutor:
        def __init__(self):
            self.seen = []
            self.attempted = set()
            self.backend = DuckDBBackend(con.con, namespace="runtime")

        def run_snapshot_task(self, context, task, dependencies):
            relation = context.resolve(catalog.sources["events"].relation)
            value = context.execute(relation).to_pylist()[0]["value"]
            self.seen.append((task.partition, value))
            if task.partition == "p1" and task.partition not in self.attempted:
                self.attempted.add(task.partition)
                con.raw_sql("UPDATE events SET value=99")
                raise RuntimeError("retry partition")
            if task.partition == "p1":
                con.raw_sql("UPDATE events SET value=101")
            return pa.table({"partition": [task.partition], "value": [value]})

        def publish(self, resolved_plan, outputs, lease, run_id):
            generation = "g1"
            return self.backend.publish_generation(
                StagedGeneration(
                    resolved_plan.dataset,
                    generation,
                    outputs,
                    run_id=run_id,
                    snapshot_id=resolved_plan.snapshot_id,
                ),
                expected_manifest=None,
                lease=lease,
            )

    executor = SnapshotExecutor()
    try:
        import pytest

        with pytest.raises(RuntimeError, match="retry partition"):
            run_serialized_plan(
                recipe.to_json(),
                con=con.con,
                namespace="runtime",
                request_key="partition-run",
                executor=executor,
                resolver=Resolver(),
                snapshot_authority=authority,
            )
        receipt = run_serialized_plan(
            recipe.to_json(),
            con=con.con,
            namespace="runtime",
            request_key="partition-run",
            executor=executor,
            resolver=Resolver(),
            snapshot_authority=authority,
        )
        assert receipt.state == "succeeded"
        assert executor.seen
        assert {value for _, value in executor.seen} == {3}
        published = open_published_generation(receipt, con=con.con, namespace="runtime")
        result = published.execute().to_dict("records")
        assert sorted(result, key=lambda row: row["partition"]) == [
            {"partition": "p1", "value": 3},
            {"partition": "p2", "value": 3},
        ]
        assert con.raw_sql("SELECT value FROM events").fetchone() == (101,)
    finally:
        con.disconnect()
