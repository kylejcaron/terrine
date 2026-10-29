from __future__ import annotations

from pathlib import Path

import ibis
import pyarrow as pa
import pytest

from terrine.data import Budget, Coverage
from terrine.errors import TerrineError
from terrine.model import Model, UnitSource
from terrine.runtime import open_published_generation, run_serialized_plan
from terrine.runtime.backends.base import StagedGeneration
from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.graph import dependency_graph
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import plan_refresh
from terrine.runtime.saved_plan import DestinationRef, ModelReference, SavedPlan
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.semantic.models import Catalog, TableRef
from terrine.spec import Design, FixedDate, agg, asof


def _catalog() -> Catalog:
    return Catalog.model_validate(
        {
            "schema_version": 2,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["value"],
                    "properties": ["tier"],
                    "dimensions": ["history"],
                }
            },
            "events": {"value": {"source": "events", "column": "value"}},
            "properties": {
                "tier": {
                    "source": "events",
                    "column": "tier",
                    "dtype": "string",
                    "as_of": "event_time",
                    "dimension": "history",
                }
            },
            "dimensions": {
                "history": {
                    "relation": {"kind": "table", "name": "user_tier"},
                    "unit": "user",
                    "key": "user_id",
                    "properties": ["tier"],
                    "validity": {"valid_from": "starts", "valid_to": "ends"},
                }
            },
            "measures": {"total": {"event": "value", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "unit": "user", "measure": "total"}},
        }
    )


@pytest.mark.parametrize("corrupt_after_partial_output", [False, True])
def test_partition_and_retry_reopen_same_retained_input_authority(
    tmp_path, corrupt_after_partial_output
):
    path = tmp_path / "owner.duckdb"
    con = ibis.duckdb.connect(str(path))
    con.raw_sql("CREATE TABLE events(ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
    con.raw_sql("CREATE TABLE users(user_id INTEGER, exited TIMESTAMP)")
    con.raw_sql("INSERT INTO users VALUES (1, NULL)")
    con.raw_sql(
        "CREATE TABLE user_tier(user_id INTEGER, tier VARCHAR, starts TIMESTAMP, ends TIMESTAMP)"
    )
    con.raw_sql("INSERT INTO user_tier VALUES (1, 'gold', '2024-01-01', '2026-01-01')")
    catalog = _catalog()
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
        units={"user": UnitSource("users", "user_id", exited_at="exited")},
    )
    design = Design(
        "user",
        FixedDate("2025-01-02T00:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s")), "tier": asof("tier", before="0s")},
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
    saved_plan = SavedPlan.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference.from_model(model, owner_key="model"),
        destination=destination,
        namespace="runtime",
        expected_base_manifest=None,
    )
    saved_plan.pin_snapshot(store)

    class Resolver:
        def resolve(self, reference):
            assert reference == saved_plan.model
            return model

    failure = RuntimeError("retry partition")

    class SnapshotExecutor:
        def __init__(self):
            self.seen = []
            self.attempted = set()
            self.backend = DuckDBBackend(con.con, namespace="runtime")

        def run_snapshot_task(self, context, task, dependencies):
            relation = context.resolve(catalog.sources["events"].relation)
            value = context.execute(relation).to_pylist()[0]["value"]
            tier = context.execute(
                context.resolve(catalog.dimensions["history"].relation)
            ).to_pylist()[0]["tier"]
            users = context.execute(
                context.resolve(TableRef(kind="table", name="users"))
            ).to_pylist()
            active = users[0]["exited"] is None
            self.seen.append((task.partition, value, tier, active))
            if task.partition == "p1" and task.partition not in self.attempted:
                self.attempted.add(task.partition)
                con.raw_sql("UPDATE events SET value=99")
                raise failure
            if task.partition == "p1":
                con.raw_sql("UPDATE events SET value=101")
                con.raw_sql("UPDATE user_tier SET tier='silver'")
                con.raw_sql("UPDATE users SET exited='2025-01-02'")
                if corrupt_after_partial_output:
                    with Path(retained.artifact_path).open("ab") as stream:
                        stream.write(b"changed")
            return pa.table(
                {
                    "partition": [task.partition],
                    "value": [value],
                    "tier": [tier],
                    "active": [active],
                }
            )

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
        with pytest.raises(RuntimeError) as error:
            run_serialized_plan(
                saved_plan.to_json(),
                con=con.con,
                namespace="runtime",
                request_key="partition-run",
                executor=executor,
                resolver=Resolver(),
                snapshot_authority=authority,
            )
        assert error.value is failure
        if corrupt_after_partial_output:
            with pytest.raises(TerrineError) as error:
                run_serialized_plan(
                    saved_plan.to_json(),
                    con=con.con,
                    namespace="runtime",
                    request_key="partition-run",
                    executor=executor,
                    resolver=Resolver(),
                    snapshot_authority=authority,
                )
            assert error.value.code == "terrine.runtime.snapshot_invalid"
            assert (
                con.con.execute(
                    "SELECT count(*) FROM runtime.runtime_tasks WHERE status='complete'"
                ).fetchone()[0]
                > 0
            )
            assert executor.backend.current_generation(destination.dataset) is None
            assert con.con.execute(
                "SELECT state,published_generation FROM runtime.runtime_runs"
            ).fetchone() == ("failed", None)
            return
        receipt = run_serialized_plan(
            saved_plan.to_json(),
            con=con.con,
            namespace="runtime",
            request_key="partition-run",
            executor=executor,
            resolver=Resolver(),
            snapshot_authority=authority,
        )
        assert receipt.state == "succeeded"
        assert executor.seen
        assert {(value, tier, active) for _, value, tier, active in executor.seen} == {
            (3, "gold", True)
        }
        published = open_published_generation(receipt, con=con.con, namespace="runtime")
        result = published.execute().to_dict("records")
        assert sorted(result, key=lambda row: row["partition"]) == [
            {"partition": "p1", "value": 3, "tier": "gold", "active": True},
            {"partition": "p2", "value": 3, "tier": "gold", "active": True},
        ]
        assert con.raw_sql("SELECT value FROM events").fetchone() == (101,)
        assert con.raw_sql("SELECT tier FROM user_tier").fetchone() == ("silver",)
        assert con.raw_sql("SELECT exited IS NOT NULL FROM users").fetchone() == (True,)
    finally:
        con.disconnect()
