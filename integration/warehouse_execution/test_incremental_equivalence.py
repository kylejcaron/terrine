from __future__ import annotations

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
                    "properties": [],
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
                }
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
        ("readiness_advance", [3], [3]),
        ("roster_exit_and_reentry", [3], []),
        ("strict_asof_dimension", [3], [3]),
        ("nested_selector", [3], [3]),
        ("state_panel", [3, 0], [7, 0]),
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
        FixedDate,
        Schedule,
        StatePanel,
        agg,
        asof,
        event_time,
    )

    state_path = tmp_path / "runtime.duckdb"
    con = ibis.duckdb.connect(str(state_path))
    try:
        con.raw_sql("CREATE TABLE events(occurred_at TIMESTAMP, user_id BIGINT, amount BIGINT)")
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
        con.raw_sql("CREATE TABLE statuses(occurred_at TIMESTAMP, user_id BIGINT, score BIGINT)")
        if scenario == "strict_asof_dimension":
            con.raw_sql("INSERT INTO events VALUES ('2025-01-02 00:00:00', 1, 101)")
        elif scenario == "nested_selector":
            con.raw_sql("INSERT INTO events VALUES ('2024-12-31 11:00:00', 1, 9)")
            con.raw_sql("INSERT INTO statuses VALUES ('2024-12-31 10:30:00', 1, 1)")
        catalog = _catalog()
        units = {"user": UnitSource(table="users", key="user_id", exited_at="exited")}
        readiness_end = "2025-01-03" if scenario == "readiness_advance" else "2025-02-01"
        readiness = {
            "events": Coverage("2024-12-30", readiness_end),
            "statuses": Coverage("2024-12-30", "2025-02-01"),
        }
        model = Model.from_catalog(catalog, con=con, readiness=readiness, units=units)
        features = {"total": agg("total", window=("-1d", "0s"))}
        if scenario == "strict_asof_dimension":
            features["tier"] = asof("tier", before="0s")
        elif scenario == "nested_selector":
            origin = event_time("amount", window=("-2d", "-1d"))
            features["nested_time"] = event_time("score", window=("-2h", "0s"), relative_to=origin)
        if scenario == "state_panel":
            design = Design(
                "user",
                Schedule("1d", "2025-01-02", "2025-01-04"),
                features=features,
                shape=StatePanel(
                    BinnedMetricState("total", cutpoints=(5,), levels=("low", "high")),
                    max_periods=2,
                ),
            )
        else:
            design = Design("user", FixedDate("2025-01-02T00:00:00+00:00"), features=features)
        owner = RuntimeOwner(state_path=state_path, snapshot_root=tmp_path / "snapshots")
        authority = SnapshotAuthority(owner)
        namespace = "analytics_owner"
        store = RuntimeStore(con.con, namespace=namespace)
        store.initialize()
        destination = DestinationRef(str(state_path), "published", "orders")
        budget = Budget(max_rows=10, max_output_bytes=100_000)

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
            clean = design.build(model).materialize(budget)
            rows = ordered_rows(published)
            assert rows == ordered_rows(clean)
            assert published.columns == clean.columns
            assert type(published) is type(clean)
            assert published.columns["total"].numeric_policy == "exact"
            if scenario == "state_panel":
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
        if scenario == "strict_asof_dimension":
            assert initial_rows[0]["tier"] == "gold"
        elif scenario == "nested_selector":
            assert initial_rows[0]["nested_time"] == datetime(2024, 12, 31, 10, 30, tzinfo=UTC)
        elif scenario == "state_panel":
            assert [(row["from_state"], row["to_state"]) for row in initial_rows] == [
                ("low", "low"),
                ("low", "low"),
            ]

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
        elif scenario == "strict_asof_dimension":
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
        else:
            readiness["events"] = Coverage("2024-12-30", "2025-01-04")
            model = Model.from_catalog(catalog, con=con, readiness=readiness, units=units)
            change = Change("readiness", "events", revision="coverage-advance-1")

        corrected_receipt, corrected_rows = run_generation(
            name="correction",
            model=model,
            changes=ChangeSet((change,), "change_feed"),
            expected_manifest=initial_receipt.published_generation,
        )
        assert corrected_receipt.published_generation != initial_receipt.published_generation
        assert [row["total"] for row in corrected_rows] == corrected_totals
        if scenario == "strict_asof_dimension":
            assert corrected_rows[0]["tier"] == "bronze"
        elif scenario == "nested_selector":
            assert corrected_rows[0]["nested_time"] == datetime(2024, 12, 31, 9, 30, tzinfo=UTC)
        elif scenario == "state_panel":
            assert [(row["from_state"], row["to_state"]) for row in corrected_rows] == [
                ("high", "low"),
                ("low", "low"),
            ]
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
