from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

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
                    "properties": ["tier"],
                    "dimensions": ["history"],
                },
                "statuses": {
                    "relation": {"kind": "table", "name": "statuses"},
                    "timestamp": "occurred_at",
                    "entities": {"user": "user_id"},
                    "facts": ["score"],
                    "properties": ["state"],
                    "dimensions": [],
                },
            },
            "facts": {
                "amount": {"source": "events", "column": "amount"},
                "score": {"source": "statuses", "column": "score"},
            },
            "properties": {
                "tier": {
                    "source": "events",
                    "column": "tier",
                    "dtype": "string",
                    "as_of": "event_time",
                    "dimension": "history",
                },
                "state": {
                    "source": "statuses",
                    "column": "state",
                    "dtype": "string",
                    "as_of": "event_time",
                },
            },
            "dimensions": {
                "history": {
                    "relation": {"kind": "table", "name": "user_tier"},
                    "source_entity": "user",
                    "entity_column": "user_id",
                    "properties": ["tier"],
                    "validity": {"valid_from": "starts", "valid_to": "ends"},
                }
            },
            "measures": {"total": {"fact": "amount", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "entity": "user", "measure": "total"}},
        }
    )


class _Resolver:
    def __init__(self, model):
        self.model = model

    def resolve(self, reference):
        return self.model


@pytest.mark.parametrize(
    ("scenario", "initial_totals", "corrected_totals"),
    [
        ("insert", [3], [8]),
        ("update", [3], [7]),
        ("delete", [8], [3]),
        ("late_arrival", [3], [3]),
        ("readiness_advance", [None], [3]),
        ("no_op", [3], [3]),
        ("roster_exit_and_reentry", [3], []),
        ("strict_asof_dimension", [3], [3]),
        ("nested_selector", [3], [3]),
        ("state_panel", [3, 0], [7, 0]),
        ("each_anchor_removal", [0, 3], [0]),
        ("schema_change", [3], [3]),
        ("state_panel_baseline", [3, 0], [3, 0]),
        ("state_panel_terminal", [3], [3, 0]),
    ],
)
def test_saved_runtime_correction_matches_clean_rebuild_of_new_generation(
    tmp_path, scenario, initial_totals, corrected_totals
):
    import ibis

    from terrine.data import Budget, Coverage, UnitSource
    from terrine.model import Model
    from terrine.runtime import (
        open_published_frame,
        open_published_generation,
        run_serialized_plan,
    )
    from terrine.runtime.changes import Change, ChangeSet
    from terrine.runtime.graph import dependency_graph
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planner import plan_refresh
    from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.runtime.state import RuntimeStore
    from terrine.spec import (
        BinnedMetricState,
        Design,
        EventAnchor,
        FixedDate,
        Schedule,
        StateObservations,
        StatePanel,
        agg,
        asof,
        event_time,
    )

    state_path = tmp_path / "runtime.duckdb"
    con = ibis.duckdb.connect(str(state_path))
    try:
        con.raw_sql("CREATE TABLE events(occurred_at TIMESTAMP, user_id BIGINT, amount BIGINT)")
        if scenario == "schema_change":
            con.raw_sql("ALTER TABLE events ALTER COLUMN amount TYPE INTEGER")
        con.raw_sql("INSERT INTO events VALUES ('2025-01-01 09:00:00', 1, 3)")
        if scenario == "delete":
            con.raw_sql("INSERT INTO events VALUES ('2025-01-01 11:00:00', 1, 5)")
        con.raw_sql("CREATE TABLE users(user_id BIGINT PRIMARY KEY, exited TIMESTAMP)")
        con.raw_sql("INSERT INTO users VALUES (1, NULL)")
        con.raw_sql(
            "CREATE TABLE user_tier(user_id BIGINT, tier VARCHAR, starts TIMESTAMP, ends TIMESTAMP)"
        )
        con.raw_sql(
            "INSERT INTO user_tier VALUES "
            "(1, 'gold', '2024-01-01', '2025-01-02'), "
            "(1, 'silver', '2025-01-02', '2026-01-01')"
        )
        con.raw_sql(
            "CREATE TABLE statuses("
            "occurred_at TIMESTAMP, user_id BIGINT, score BIGINT, state VARCHAR)"
        )
        if scenario == "strict_asof_dimension":
            con.raw_sql("INSERT INTO events VALUES ('2025-01-02 00:00:00', 1, 101)")
        elif scenario == "nested_selector":
            con.raw_sql("INSERT INTO events VALUES ('2024-12-31 11:00:00', 1, 9)")
            con.raw_sql("INSERT INTO statuses VALUES ('2024-12-31 10:30:00', 1, 1, NULL)")
        elif scenario == "each_anchor_removal":
            con.raw_sql("ALTER TABLE events ADD COLUMN event_id VARCHAR")
            con.raw_sql("UPDATE events SET event_id='first'")
            con.raw_sql("INSERT INTO events VALUES ('2025-01-01 12:00:00', 1, 5, 'second')")
        elif scenario == "state_panel_terminal":
            con.raw_sql(
                "INSERT INTO statuses VALUES "
                "('2025-01-02', 1, 0, 'active'), ('2025-01-03', 1, 0, 'closed')"
            )
        catalog = _catalog()
        units = {"user": UnitSource(table="users", key="user_id", exited_at="exited")}
        readiness_end = "2024-12-31" if scenario == "readiness_advance" else "2025-02-01"
        readiness = {
            "events": Coverage("2024-12-30", readiness_end),
            "statuses": Coverage("2024-12-30", "2025-02-01"),
        }
        model = Model.from_catalog(
            catalog,
            con=con,
            readiness=readiness,
            units=units,
            event_ids={"events": "event_id"} if scenario == "each_anchor_removal" else None,
        )
        features = {"total": agg("total", window=("-1d", "0s"))}
        if scenario == "late_arrival":
            features["history"] = agg("total", window=("-3d", "0s"))
        if scenario == "strict_asof_dimension":
            features["tier"] = asof("tier", before="0s")
        elif scenario == "nested_selector":
            origin = event_time("amount", window=("-2d", "-1d"))
            features["nested_time"] = event_time("score", window=("-2h", "0s"), relative_to=origin)
        if scenario.startswith("state_panel"):
            design = Design(
                "user",
                Schedule("1d", "2025-01-02", "2025-01-04"),
                features=features,
                shape=StatePanel(
                    (
                        StateObservations("state", ("active", "closed"), absorbing=("closed",))
                        if scenario == "state_panel_terminal"
                        else BinnedMetricState("total", cutpoints=(5,), levels=("low", "high"))
                    ),
                    max_periods=2,
                    baseline_features=(
                        {"baseline_tier": asof("tier", before="0s")}
                        if scenario == "state_panel_baseline"
                        else None
                    ),
                ),
            )
        else:
            design = Design(
                "user",
                EventAnchor("amount", "each")
                if scenario == "each_anchor_removal"
                else FixedDate("2025-01-02T00:00:00+00:00"),
                features=features,
            )
        owner = RuntimeOwner(state_path=state_path, snapshot_root=tmp_path / "snapshots")
        authority = SnapshotAuthority(owner)
        namespace = "analytics_owner"
        store = RuntimeStore(con.con, namespace=namespace)
        store.initialize()
        destination = DestinationRef(str(state_path), "published", "orders")
        budget = Budget(max_rows=10, max_output_bytes=100_000)
        frames = {}
        snapshots = {}
        prefixes = {}

        def ordered_rows(frame):
            return sorted(frame.table.to_pylist(), key=lambda row: (row["unit"], row["t0"]))

        def run_generation(*, name, model, changes=None, expected_manifest=None):
            snapshot = authority.capture(
                model,
                design=design,
                con=con,
                namespace=namespace,
                run_id=name,
                budget=budget,
            )
            snapshots[name] = snapshot
            graph = dependency_graph(model, design, sources=("events",), partitions=("all",))
            plan = plan_refresh(
                namespace=namespace,
                dataset=destination.dataset,
                definition_hash=design.spec_hash,
                snapshot_id=snapshot.snapshot_id,
                graph=graph,
                changes=changes,
                budget=budget,
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
            frames[name] = published
            clean = design.build(model).materialize(budget)
            rows = ordered_rows(published)
            assert rows == ordered_rows(clean)
            assert published.columns == clean.columns
            assert published.table.schema.remove_metadata() == clean.table.schema.remove_metadata()
            bound_snapshot = snapshot if plan.tasks else snapshots["initial"]
            expected_generations = {
                key: hashlib.sha256(f"{bound_snapshot.artifact_hash}:{key}".encode()).hexdigest()
                for kind, names in (
                    ("source", model.catalog.sources),
                    ("dimension", model.catalog.dimensions),
                    ("roster", (design.unit,)),
                )
                for name in names
                for key in (json.dumps([kind, name], separators=(",", ":")),)
            }
            assert published.provenance.source_generations == expected_generations
            assert (
                published.provenance.execution_snapshot.source_generations == expected_generations
            )
            assert published.provenance.execution_snapshot.snapshot_id == bound_snapshot.snapshot_id
            assert (
                published.provenance.execution_snapshot.input_binding_id
                == bound_snapshot.snapshot_id
            )
            assert published.provenance.execution_snapshot.replay_equivalence == "verified"
            prefixes[name] = tuple(
                hashlib.sha256(
                    con.con.execute(
                        f"SELECT output_blob FROM {store._schema}.runtime_tasks "
                        "WHERE run_id=? AND task_id=? AND status='complete'",
                        [receipt.run_id, task.task_id],
                    ).fetchone()[0]
                ).hexdigest()
                for task in plan.tasks
                if task.layer == "prefix"
            )
            task_count = con.con.execute(
                f"SELECT count(*) FROM {store._schema}.runtime_tasks WHERE run_id=?",
                [receipt.run_id],
            ).fetchone()[0]
            replay = run_serialized_plan(
                recipe.to_json(),
                con=con.con,
                namespace=namespace,
                request_key=f"runtime:{scenario}:{name}",
            )
            assert replay == receipt
            assert open_published_frame(replay, con=con.con, namespace=namespace).table.equals(
                published.table
            )
            assert (
                con.con.execute(
                    f"SELECT count(*) FROM {store._schema}.runtime_tasks WHERE run_id=?",
                    [receipt.run_id],
                ).fetchone()[0]
                == task_count
            )
            assert type(published) is type(clean)
            assert published.columns["total"].numeric_policy == "exact"
            if scenario.startswith("state_panel"):
                lazy = open_published_generation(receipt, con=con.con, namespace=namespace)
                ends = (
                    lazy.order_by("period_index")
                    .select(end=lazy.t0 + lazy.gap)
                    .execute()["end"]
                    .tolist()
                )
                assert ends == [row["to_time"] for row in rows]
            return receipt, rows

        initial_receipt, initial_rows = run_generation(name="initial", model=model)
        assert [row["total"] for row in initial_rows] == initial_totals
        if scenario == "readiness_advance":
            total = frames["initial"].columns["total"]
            assert initial_rows[0][total.complete_column] is False
            assert initial_rows[0][total.reason_column] == "incomplete_coverage"
        if scenario == "strict_asof_dimension":
            assert initial_rows[0]["tier"] == "gold"
        elif scenario == "nested_selector":
            assert initial_rows[0]["nested_time"] == datetime(2024, 12, 31, 10, 30, tzinfo=UTC)
        elif scenario == "state_panel":
            assert [(row["from_state"], row["to_state"]) for row in initial_rows] == [
                ("low", "low"),
                ("low", "low"),
            ]
        elif scenario == "each_anchor_removal":
            assert [row["t0"] for row in initial_rows] == [
                datetime(2025, 1, 1, 9, tzinfo=UTC),
                datetime(2025, 1, 1, 12, tzinfo=UTC),
            ]
            assert len({row["anchor_id"] for row in initial_rows}) == 2
        elif scenario == "state_panel_baseline":
            assert [row["baseline_tier"] for row in initial_rows] == ["gold", "gold"]
        elif scenario == "state_panel_terminal":
            assert [(row["terminal"], row["to_state"]) for row in initial_rows] == [
                (True, "closed")
            ]
        elif scenario == "schema_change":
            assert str(con.table("events").schema()["amount"]) == "int32"
        elif scenario == "late_arrival":
            assert [row["history"] for row in initial_rows] == [3]

        if scenario == "insert":
            con.raw_sql("INSERT INTO events VALUES ('2025-01-01 11:00:00', 1, 5)")
            change = Change("insert", "events", revision="insert-1")
        elif scenario in ("update", "state_panel"):
            con.raw_sql("UPDATE events SET amount=7 WHERE user_id=1")
            change = Change("update", "events", revision="update-1")
        elif scenario == "delete":
            con.raw_sql("DELETE FROM events WHERE amount=5")
            change = Change("delete", "events", revision="delete-1")
        elif scenario == "late_arrival":
            con.raw_sql("INSERT INTO events VALUES ('2024-12-31 11:00:00', 1, 9)")
            change = Change("insert", "events", revision="late-arrival-1")
        elif scenario == "roster_exit_and_reentry":
            con.raw_sql("UPDATE users SET exited='2025-01-02'")
            change = Change("roster", "users", revision="exit-1")
        elif scenario in ("strict_asof_dimension", "state_panel_baseline"):
            con.raw_sql("UPDATE user_tier SET tier='bronze' WHERE tier='gold'")
            change = Change("update", "history", revision="tier-correction-1")
        elif scenario == "nested_selector":
            con.raw_sql("UPDATE statuses SET occurred_at='2024-12-31 09:30:00'")
            change = Change(
                "update",
                "statuses",
                before_time_range=(
                    datetime(2024, 12, 31, 10, 30, tzinfo=UTC),
                    datetime(2024, 12, 31, 10, 31, tzinfo=UTC),
                ),
                after_time_range=(
                    datetime(2024, 12, 31, 9, 30, tzinfo=UTC),
                    datetime(2024, 12, 31, 9, 31, tzinfo=UTC),
                ),
            )
        elif scenario == "each_anchor_removal":
            con.raw_sql("DELETE FROM events WHERE event_id='first'")
            change = Change("delete", "events", revision="remove-first-anchor")
        elif scenario == "schema_change":
            con.raw_sql("ALTER TABLE events ALTER COLUMN amount TYPE BIGINT")
            model = Model.from_catalog(catalog, con=con, readiness=readiness, units=units)
            change = Change("definition", "events", revision="amount-type-widening")
        elif scenario == "state_panel_terminal":
            con.raw_sql("UPDATE statuses SET state='active' WHERE state='closed'")
            con.raw_sql("INSERT INTO statuses VALUES ('2025-01-04', 1, 0, 'closed')")
            change = Change("update", "statuses", revision="terminal-boundary-correction")
        elif scenario == "no_op":
            change = None
        else:
            readiness["events"] = Coverage("2024-12-30", "2025-01-04")
            model = Model.from_catalog(catalog, con=con, readiness=readiness, units=units)
            change = Change("readiness", "events", revision="coverage-advance-1")

        corrected_receipt, corrected_rows = run_generation(
            name="correction",
            model=model,
            changes=ChangeSet((change,), "change_feed") if change else ChangeSet((), "revision"),
            expected_manifest=initial_receipt.published_generation,
        )
        if scenario == "no_op":
            assert corrected_receipt.published_generation == initial_receipt.published_generation
            assert frames["correction"].provenance == frames["initial"].provenance
        else:
            assert corrected_receipt.published_generation != initial_receipt.published_generation
        assert [row["total"] for row in corrected_rows] == corrected_totals
        if scenario == "readiness_advance":
            total = frames["correction"].columns["total"]
            assert corrected_rows[0][total.complete_column] is True
            assert corrected_rows[0][total.reason_column] is None
        if scenario == "strict_asof_dimension":
            assert corrected_rows[0]["tier"] == "bronze"
        elif scenario == "nested_selector":
            assert corrected_rows[0]["nested_time"] == datetime(2024, 12, 31, 9, 30, tzinfo=UTC)
        elif scenario == "state_panel":
            assert [(row["from_state"], row["to_state"]) for row in corrected_rows] == [
                ("high", "low"),
                ("low", "low"),
            ]
        elif scenario == "each_anchor_removal":
            assert corrected_rows[0]["anchor_id"] == initial_rows[1]["anchor_id"]
            assert initial_rows[0]["anchor_id"] not in {row["anchor_id"] for row in corrected_rows}
        elif scenario == "state_panel_baseline":
            assert [row["baseline_tier"] for row in corrected_rows] == ["bronze", "bronze"]
        elif scenario == "state_panel_terminal":
            assert [(row["terminal"], row["to_state"]) for row in corrected_rows] == [
                (False, "active"),
                (True, "closed"),
            ]
            assert corrected_rows[-1]["to_time"] == datetime(2025, 1, 4, tzinfo=UTC)
        elif scenario == "schema_change":
            assert str(con.table("events").schema()["amount"]) == "int64"
        elif scenario == "late_arrival":
            assert [row["history"] for row in corrected_rows] == [12]
            assert len(prefixes["initial"]) == len(prefixes["correction"]) == 1
            assert prefixes["initial"] != prefixes["correction"]
        elif scenario == "roster_exit_and_reentry":
            con.raw_sql("UPDATE users SET exited=NULL")
            reentry_receipt, reentry_rows = run_generation(
                name="reentry",
                model=model,
                changes=ChangeSet(
                    (Change("roster", "users", revision="reentry-1"),), "change_feed"
                ),
                expected_manifest=corrected_receipt.published_generation,
            )
            assert reentry_receipt.published_generation != corrected_receipt.published_generation
            assert reentry_rows == initial_rows
            exited = open_published_frame(corrected_receipt, con=con.con, namespace=namespace)
            assert exited.table.to_pylist() == []
        original = open_published_frame(initial_receipt, con=con.con, namespace=namespace)
        assert ordered_rows(original) == initial_rows
    finally:
        con.disconnect()
