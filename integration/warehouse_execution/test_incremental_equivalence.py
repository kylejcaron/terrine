from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


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
    assert first == {
        "generation": "generation-1",
        "rows": [{"user_id": 1, "total": 3}, {"user_id": 2, "total": 4}],
    }
    assert corrected == {
        "generation": "generation-2",
        "rows": [{"user_id": 1, "total": 8}, {"user_id": 2, "total": 4}],
    }


@pytest.mark.parametrize(
    ("scenario", "expected_total"),
    [
        ("insert", 8),
        ("update", 7),
        ("delete", 3),
        ("late_arrival", 3),
        ("readiness_advance", 3),

    ],
)
def test_saved_runtime_plan_matches_clean_rebuild_for_event_corrections(
    tmp_path, scenario, expected_total
):
    import ibis

    from examples.materialization_runtime import (
        DesignExecutor,
        ModelResolver,
        _catalog,
        _clean_rebuild_rows,
    )
    from terrine.data import Budget, Coverage, UnitSource
    from terrine.model import Model
    from terrine.runtime import open_published_generation, run_serialized_plan
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
        con.raw_sql("CREATE TABLE users(user_id BIGINT PRIMARY KEY)")
        con.raw_sql("INSERT INTO users VALUES (1)")
        catalog = _catalog()
        units = {"user": UnitSource(table="users", key="user_id")}
        coverage = (
            Coverage("2025-01-01", "2025-01-02")
            if scenario == "readiness_advance"
            else Coverage("2024-12-30", "2025-02-01")
        )
        model = Model.from_catalog(catalog, con=con, readiness={"events": coverage}, units=units)
        design = Design(
            "user", FixedDate("2025-01-02T00:00:00+00:00"),
            features={"total": agg("total", window=("-1d", "0s"))},
        )
        if scenario == "insert":
            con.raw_sql("INSERT INTO events VALUES ('2025-01-01 11:00:00', 1, 5)")
        elif scenario == "update":
            con.raw_sql("UPDATE events SET amount=7 WHERE user_id=1")
        elif scenario == "delete":
            con.raw_sql(
                "INSERT INTO events VALUES ('2025-01-01 11:00:00', 1, 5); "
                "DELETE FROM events WHERE amount=5"
            )
        elif scenario == "late_arrival":
            con.raw_sql("INSERT INTO events VALUES ('2024-12-31 11:00:00', 1, 9)")
        elif scenario == "readiness_advance":
            model = Model.from_catalog(
                catalog,
                con=con,
                readiness={"events": Coverage("2025-01-01", "2025-01-03")},
                units=units,
            )

        owner = RuntimeOwner(state_path=state_path, snapshot_root=tmp_path / "snapshots")
        authority = SnapshotAuthority(owner)
        namespace = "analytics_owner"
        store = RuntimeStore(con.con, namespace=namespace)
        store.initialize()
        snapshot = authority.capture(
            model, design=design, con=con, namespace=namespace,
            run_id=f"runtime-{scenario}", budget=Budget(),
        )
        destination = DestinationRef(str(state_path), "published", "orders")
        graph = dependency_graph(
            model, design, sources=("events",), partitions=("all",)
        )
        plan = plan_refresh(
            namespace=namespace,
            dataset=destination.dataset,
            definition_hash=design.spec_hash,
            snapshot_id=snapshot.snapshot_id,
            graph=graph,
            changes=ChangeSet(
                (
                    Change(
                        "readiness"
                        if scenario == "readiness_advance"
                        else "insert"
                        if scenario == "late_arrival"
                        else scenario,
                        "events",
                        revision=scenario,
                    ),
                ),
                "change_feed",
            ),
        )
        assert any(task.layer == "frame" for task in plan.tasks)
        recipe = SavedPlanRecipe.create(
            plan_json=plan.serialize(),
            design=design,
            model=ModelReference.from_model(model, owner_key="events-model"),
            destination=destination,
            namespace=namespace,
            expected_base_manifest=None,
        )
        recipe.pin_snapshot(store)
        executor = DesignExecutor(con.con, catalog, design, namespace=namespace)
        receipt = run_serialized_plan(
            recipe.to_json(),
            con=con.con,
            namespace=namespace,
            request_key=f"runtime:{scenario}",
            executor=executor,
            resolver=ModelResolver(model),
            snapshot_authority=authority,
        )
        published = open_published_generation(receipt, con=con.con, namespace=namespace)
        rows = sorted(published.execute().to_dict("records"), key=lambda row: row["user_id"])
        clean_rows = _clean_rebuild_rows(model, design)
        assert rows == clean_rows
        expected_row = {"user_id": 1, "total": expected_total}
        assert rows == [expected_row]
    finally:
        con.disconnect()
