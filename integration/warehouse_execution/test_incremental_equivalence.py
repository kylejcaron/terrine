from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


def _catalog():
    from terrine.semantic.models import Catalog

    return Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "occurred_at",
                    "entities": {"user": "user_id"},
                    "facts": ["amount"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {"amount": {"source": "events", "column": "amount"}},
            "properties": {},
            "dimensions": {},
            "measures": {"total": {"fact": "amount", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "entity": "user", "measure": "total"}},
        }
    )


class _Resolver:
    def __init__(self, model):
        self.model = model

    def resolve(self, reference):
        return self.model


def test_runtime_generation_matches_clean_rebuild_after_declared_correction():
    script = Path(__file__).parents[2] / "examples" / "materialization_runtime.py"
    result = subprocess.run(
        [sys.executable, str(script)],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    first, corrected = (json.loads(line) for line in result.stdout.splitlines())
    first["rows"].sort(key=lambda row: row["user_id"])
    corrected["rows"].sort(key=lambda row: row["user_id"])
    assert first["generation"]
    assert corrected["generation"] != first["generation"]
    assert first["rows"] == [{"user_id": 1, "total": 3}, {"user_id": 2, "total": 4}]
    assert corrected["rows"] == [{"user_id": 1, "total": 8}, {"user_id": 2, "total": 4}]


@pytest.mark.parametrize(
    ("scenario", "initial_total", "corrected_total"),
    [
        ("insert", 3, 8),
        ("update", 3, 7),
        ("delete", 8, 3),
        ("late_arrival", 3, 3),
        ("readiness_advance", 3, 3),
    ],
)
def test_saved_runtime_correction_matches_clean_rebuild_of_new_generation(
    tmp_path, scenario, initial_total, corrected_total
):
    import ibis

    from terrine.compile.columns import compile_columns
    from terrine.data import Budget, Coverage, UnitSource
    from terrine.model import Model
    from terrine.runtime import open_published_frame, run_serialized_plan
    from terrine.runtime.changes import Change, ChangeSet
    from terrine.runtime.graph import dependency_graph
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planner import plan_refresh
    from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.runtime.state import RuntimeStore
    from terrine.spec import Design, FixedDate, agg

    state_path = tmp_path / "runtime.duckdb"
    con = ibis.duckdb.connect(str(state_path))
    try:
        con.raw_sql("CREATE TABLE events(occurred_at TIMESTAMP, user_id BIGINT, amount BIGINT)")
        con.raw_sql("INSERT INTO events VALUES ('2025-01-01 09:00:00', 1, 3)")
        if scenario == "delete":
            con.raw_sql("INSERT INTO events VALUES ('2025-01-01 11:00:00', 1, 5)")
        con.raw_sql("CREATE TABLE users(user_id BIGINT PRIMARY KEY)")
        con.raw_sql("INSERT INTO users VALUES (1)")
        catalog = _catalog()
        units = {"user": UnitSource(table="users", key="user_id")}
        readiness_end = "2025-01-03" if scenario == "readiness_advance" else "2025-02-01"
        model = Model.from_catalog(
            catalog,
            con=con,
            readiness={"events": Coverage("2024-12-30", readiness_end)},
            units=units,
        )
        design = Design(
            "user",
            FixedDate("2025-01-02T00:00:00+00:00"),
            features={"total": agg("total", window=("-1d", "0s"))},
        )
        owner = RuntimeOwner(state_path=state_path, snapshot_root=tmp_path / "snapshots")
        authority = SnapshotAuthority(owner)
        namespace = "analytics_owner"
        store = RuntimeStore(con.con, namespace=namespace)
        store.initialize()
        destination = DestinationRef(str(state_path), "published", "orders")

        def run_generation(*, name, model, changes=None, expected_manifest=None):
            snapshot = authority.capture(
                model,
                design=design,
                con=con,
                namespace=namespace,
                run_id=name,
                budget=Budget(),
            )
            graph = dependency_graph(model, design, sources=("events",), partitions=("all",))
            plan = plan_refresh(
                namespace=namespace,
                dataset=destination.dataset,
                definition_hash=design.spec_hash,
                snapshot_id=snapshot.snapshot_id,
                graph=graph,
                changes=changes,
            )
            recipe = SavedPlanRecipe.create(
                plan_json=plan.serialize(),
                design=design,
                model=ModelReference.from_model(model, owner_key="events-model"),
                destination=destination,
                namespace=namespace,
                expected_base_manifest=expected_manifest,
            )
            recipe.pin_snapshot(store)
            receipt = run_serialized_plan(
                recipe.to_json(),
                con=con.con,
                namespace=namespace,
                request_key=f"runtime:{scenario}:{name}",
                resolver=_Resolver(model),
                snapshot_authority=authority,
            )
            published = open_published_frame(receipt, con=con.con, namespace=namespace)
            rows = sorted(
                [
                    {"user_id": row["unit"], "total": row["total"]}
                    for row in published.table.to_pylist()
                ],
                key=lambda row: row["user_id"],
            )
            clean = compile_columns(design, model)
            clean.validate(execute=model.con.to_pyarrow)
            clean_rows = sorted(
                [
                    {"user_id": row["unit"], "total": row["total"]}
                    for row in model.con.to_pyarrow(clean.table).to_pylist()
                ],
                key=lambda row: row["user_id"],
            )
            assert rows == clean_rows
            assert published.columns == clean.columns
            return receipt, rows

        initial_receipt, initial_rows = run_generation(name="initial", model=model)
        assert initial_rows == [{"user_id": 1, "total": initial_total}]

        if scenario == "insert":
            con.raw_sql("INSERT INTO events VALUES ('2025-01-01 11:00:00', 1, 5)")
            change = Change("insert", "events", revision="insert-1")
        elif scenario == "update":
            con.raw_sql("UPDATE events SET amount=7 WHERE user_id=1")
            change = Change("update", "events", revision="update-1")
        elif scenario == "delete":
            con.raw_sql("DELETE FROM events WHERE amount=5")
            change = Change("delete", "events", revision="delete-1")
        elif scenario == "late_arrival":
            con.raw_sql("INSERT INTO events VALUES ('2024-12-31 11:00:00', 1, 9)")
            change = Change("insert", "events", revision="late-arrival-1")
        else:
            readiness_end = "2025-01-04"
            model = Model.from_catalog(
                catalog,
                con=con,
                readiness={"events": Coverage("2024-12-30", readiness_end)},
                units=units,
            )
            change = Change("readiness", "events", revision="coverage-advance-1")

        corrected_receipt, corrected_rows = run_generation(
            name="correction",
            model=model,
            changes=ChangeSet((change,), "change_feed"),
            expected_manifest=initial_receipt.published_generation,
        )
        assert corrected_receipt.published_generation != initial_receipt.published_generation
        assert corrected_rows == [{"user_id": 1, "total": corrected_total}]
    finally:
        con.disconnect()
