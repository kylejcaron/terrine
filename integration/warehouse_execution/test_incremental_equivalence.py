# ruff: noqa: F811
from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from terrine.compile.columns import compile_columns
from terrine.compile.layers import measure_identity, plan_layers
from terrine.compile.materialize import compose_materialized_frame
from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import Catalog
from terrine.spec import Design, EventAnchor, FixedDate, agg, asof, event_time, gt
from terrine.temporal.daily import _sparse_group, resolve_measure
from terrine.temporal.prefix import sparse_prefix
from tests.compile.test_columns import column_model  # noqa: F401


def _catalog():
    from terrine.semantic.models import Catalog

    return Catalog.model_validate(
        {
            "schema_version": 2,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "occurred_at",
                    "units": {"user": "user_id"},
                    "events": ["amount"],
                    "properties": ["tier"],
                    "dimensions": ["history"],
                },
                "statuses": {
                    "relation": {"kind": "table", "name": "statuses"},
                    "time": "occurred_at",
                    "units": {"user": "user_id"},
                    "events": ["score"],
                    "properties": ["state"],
                    "dimensions": [],
                },
            },
            "events": {
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
                    "unit": "user",
                    "key": "user_id",
                    "properties": ["tier"],
                    "validity": {"valid_from": "starts", "valid_to": "ends"},
                }
            },
            "measures": {"total": {"event": "amount", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "unit": "user", "measure": "total"}},
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
    from terrine.runtime.saved_plan import DestinationRef, ModelReference, SavedPlan
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
                Schedule("1d", "2025-01-02", "2025-01-03"),
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
            saved_plan = SavedPlan.create(
                plan_json=plan.serialize(),
                design=design,
                model=ModelReference.from_model(model, owner_key="events-model"),
                destination=destination,
                namespace=namespace,
                expected_base_manifest=expected_manifest,
            )
            saved_plan.pin_snapshot(store)
            receipt = run_serialized_plan(
                saved_plan.to_json(),
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
                saved_plan.to_json(),
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


def _same_snapshot_prefixes(design, model):
    measures = {
        measure_identity(measure): measure
        for metric in model.metrics.values()
        for measure in metric.measures
    }
    persisted = {}
    for key in plan_layers(design, model).keys:
        if key.layer != "prefix":
            continue
        source = model.sources[key.source]
        events = normalize_events(
            model._relation(source.relation), source, source.units[design.unit]
        )
        if key.measure is None:
            raise AssertionError("planned temporal layer has no measure identity")
        measure = resolve_measure(events, measures[key.measure], source=source)
        grouped = _sparse_group(
            events, measure, {"_timestamp": events._timestamp}, "_unit", source=source
        )
        persisted[replace(key, snapshot_id="pinned-snapshot")] = sparse_prefix(
            grouped, measure, resolution="event"
        )
    return persisted


def _float_model(column_model):
    con = column_model.con
    con.raw_sql("ALTER TABLE events ALTER amount TYPE DOUBLE")
    return Model.from_catalog(
        Catalog.model_validate(column_model.catalog.model_dump(mode="python")),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
    )


def test_same_snapshot_incremental_compilation_matches_clean_values_and_eligibility(column_model):
    model = _float_model(column_model)
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
        eligibility={"positive": gt(agg("total", window=("-1d", "0s")), 0.0)},
    )
    clean = compile_columns(design, model)
    incremental = compose_materialized_frame(
        design, model, persisted=_same_snapshot_prefixes(design, model)
    )
    clean.validate(execute=model.con.to_pyarrow)
    incremental.validate(execute=model.con.to_pyarrow)
    assert model.con.to_pyarrow(clean.audit_table.order_by("unit")).equals(
        model.con.to_pyarrow(incremental.audit_table.order_by("unit"))
    )
    assert model.con.to_pyarrow(clean.table.order_by("unit")).equals(
        model.con.to_pyarrow(incremental.table.order_by("unit"))
    )
    assert clean.columns == incremental.columns


def test_historical_1e16_insert_has_identical_exact_total(column_model):
    model = _float_model(column_model)
    model = Model.from_catalog(
        model.catalog,
        con=model.con,
        readiness={"events": Coverage("2024-12-31", "2025-01-02")},
        units=model.units,
    )
    model.con.raw_sql(
        "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-01 12:00:00+00', 1e16, 'old')"
    )
    model.con.raw_sql(
        "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-02 11:59:57+00', 1.0, 'old')"
    )
    model.con.raw_sql(
        "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-02 11:59:58+00', -1e16, 'old')"
    )
    model.con.raw_sql(
        "UPDATE events SET amount=1.0 WHERE user_id='a' AND ts = TIMESTAMPTZ "
        "'2025-01-02 11:59:59+00'"
    )
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-2d", "0s"))},
    )
    clean = compile_columns(design, model)
    incremental = compose_materialized_frame(
        design, model, persisted=_same_snapshot_prefixes(design, model)
    )
    audits = []
    for compiled in (clean, incremental):
        compiled.validate(execute=model.con.to_pyarrow)
        audits.append(model.con.to_pyarrow(compiled.audit_table.order_by("unit")))
    assert audits[0].equals(audits[1])
    assert clean.columns == incremental.columns
    assert [row["total"] for row in audits[0].to_pylist()] == [2.0, None]


@pytest.mark.parametrize(
    "scenario",
    (
        "insert",
        "update",
        "delete",
        "late_arrival",
        "dimension_correction",
        "roster_exit",
        "readiness_advance",
        "readiness_retraction",
        "definition_change",
    ),
)
def test_same_snapshot_incremental_matches_clean_after_correction(column_model, scenario):
    model = _float_model(column_model)
    con = model.con
    model = Model.from_catalog(
        model.catalog,
        con=con,
        readiness={"events": Coverage("2024-12-31", "2025-01-03")},
        units=model.units,
    )
    if scenario == "insert":
        con.raw_sql(
            "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-02 10:00:00+00', 11.0, 'new')"
        )
    elif scenario == "update":
        con.raw_sql(
            "UPDATE events SET amount=13.0 WHERE user_id='a' AND ts=TIMESTAMPTZ "
            "'2025-01-02 11:59:59+00'"
        )
    elif scenario == "delete":
        con.raw_sql(
            "DELETE FROM events WHERE user_id='a' AND ts=TIMESTAMPTZ '2025-01-02 11:59:59+00'"
        )
    elif scenario == "late_arrival":
        con.raw_sql(
            "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-01 12:00:00+00', 17.0, 'late')"
        )
    elif scenario == "dimension_correction":
        con.raw_sql(
            "CREATE TABLE tier_history AS SELECT 'a' AS id, 'gold' AS tier, "
            "TIMESTAMPTZ '2024-12-01' AS starts, TIMESTAMPTZ '2025-02-01' AS ends"
        )
        definition = model.catalog.model_dump(mode="python")
        definition["sources"]["events"]["dimensions"] = ("history",)
        definition["properties"]["tier"]["dimension"] = "history"
        definition["dimensions"]["history"] = {
            "relation": {"kind": "table", "name": "tier_history"},
            "unit": "user",
            "key": "id",
            "properties": ("tier",),
            "validity": {"valid_from": "starts", "valid_to": "ends"},
        }
        model = Model.from_catalog(
            Catalog.model_validate(definition),
            con=con,
            readiness=model.readiness,
            units=model.units,
        )
        con.raw_sql("UPDATE tier_history SET tier='silver' WHERE id='a'")
    elif scenario == "roster_exit":
        con.raw_sql("ALTER TABLE users ADD COLUMN exited TIMESTAMPTZ")
        con.raw_sql("UPDATE users SET exited=TIMESTAMPTZ '2025-01-02 12:00:00+00' WHERE id='a'")
        model = Model.from_catalog(
            model.catalog,
            con=con,
            readiness=model.readiness,
            units={"user": UnitSource(table="users", key="id", exited_at="exited")},
        )
    elif scenario == "readiness_advance":
        model = Model.from_catalog(
            model.catalog,
            con=con,
            readiness={"events": Coverage("2024-12-31", "2025-01-03")},
            units=model.units,
        )
    elif scenario == "readiness_retraction":
        model = Model.from_catalog(
            model.catalog,
            con=con,
            readiness={"events": Coverage("2024-12-31", "2025-01-01")},
            units=model.units,
        )

    features = {"total": agg("total", window=("-2d", "0s")), "tier": asof("tier", before="0s")}
    outcomes = {"after": agg("total", window=("0s", "1d"))}
    if scenario == "definition_change":
        features["recent"] = agg("total", window=("-1d", "0s"))
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features=features,
        outcomes=outcomes,
        eligibility={"positive": gt(agg("total", window=("-2d", "0s")), 0.0)},
    )
    clean = compile_columns(design, model)
    incremental = compose_materialized_frame(
        design, model, persisted=_same_snapshot_prefixes(design, model)
    )
    clean.validate(execute=model.con.to_pyarrow)
    incremental.validate(execute=model.con.to_pyarrow)
    assert model.con.to_pyarrow(clean.audit_table.order_by("unit")).equals(
        model.con.to_pyarrow(incremental.audit_table.order_by("unit"))
    )
    assert model.con.to_pyarrow(clean.table.order_by("unit")).equals(
        model.con.to_pyarrow(incremental.table.order_by("unit"))
    )
    assert clean.columns == incremental.columns
    audit_rows = model.con.to_pyarrow(clean.audit_table).to_pylist()
    audit_by_unit = {row["unit"]: row for row in audit_rows}
    if scenario == "readiness_retraction":
        assert audit_by_unit["a"]["total"] is None
    elif scenario != "roster_exit":
        expected_totals = {
            "insert": 14.0,
            "update": 13.0,
            "delete": 0.0,
            "late_arrival": 20.0,
            "dimension_correction": 3.0,
            "readiness_advance": 3.0,
            "definition_change": 3.0,
        }
        assert audit_by_unit["a"]["total"] == expected_totals[scenario]
    if scenario == "dimension_correction":
        assert audit_by_unit["a"]["tier"] == "silver"
    if scenario == "definition_change":
        assert audit_by_unit["a"]["recent"] == 3.0
    if scenario == "roster_exit":
        assert "a" not in model.con.to_pyarrow(incremental.audit_table)["unit"].to_pylist()


@pytest.mark.parametrize("changed_source", ("numerator", "denominator"))
def test_ratio_sources_change_independently_match_clean_compilation(column_model, changed_source):
    con = column_model.con
    con.raw_sql("""
        CREATE TABLE n AS SELECT * FROM (VALUES
            ('a', TIMESTAMPTZ '2025-01-02 09:00:00+00', 2.0),
            ('a', TIMESTAMPTZ '2025-01-02 10:00:00+00', 3.0)
        ) AS rows(user_id, ts, amount)
    """)
    con.raw_sql("""
        CREATE TABLE d AS SELECT * FROM (VALUES
            ('a', TIMESTAMPTZ '2025-01-02 09:00:00+00'),
            ('a', TIMESTAMPTZ '2025-01-02 10:00:00+00')
        ) AS rows(customer_id, ts)
    """)
    catalog = Catalog.model_validate(
        {
            "schema_version": 2,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "numerator": {
                    "relation": {"kind": "table", "name": "n"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["click"],
                    "properties": [],
                    "dimensions": [],
                },
                "denominator": {
                    "relation": {"kind": "table", "name": "d"},
                    "time": "ts",
                    "units": {"user": "customer_id"},
                    "events": ["visit"],
                    "properties": [],
                    "dimensions": [],
                },
            },
            "events": {
                "click": {"source": "numerator", "column": "amount"},
                "visit": {"source": "denominator", "column": None},
            },
            "properties": {},
            "dimensions": {},
            "measures": {
                "clicks": {"event": "click", "aggregation": "sum"},
                "visits": {"event": "visit", "aggregation": "count"},
            },
            "metrics": {
                "rate": {
                    "kind": "ratio",
                    "unit": "user",
                    "numerator": "clicks",
                    "denominator": "visits",
                }
            },
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={
            "numerator": Coverage("2025-01-01", "2025-01-02"),
            "denominator": Coverage("2025-01-01", "2025-01-02"),
        },
        units=column_model.units,
    )
    if changed_source == "numerator":
        con.raw_sql("UPDATE n SET amount=7.0 WHERE user_id='a' AND amount=3.0")
    else:
        con.raw_sql("INSERT INTO d VALUES ('a', TIMESTAMPTZ '2025-01-02 11:00:00+00')")

    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"rate": agg("rate", window=("-1d", "0s"))},
    )
    clean = compile_columns(design, model)
    incremental = compose_materialized_frame(
        design, model, persisted=_same_snapshot_prefixes(design, model)
    )
    clean.validate(execute=con.to_pyarrow)
    incremental.validate(execute=con.to_pyarrow)
    assert con.to_pyarrow(clean.audit_table.order_by("unit")).equals(
        con.to_pyarrow(incremental.audit_table.order_by("unit"))
    )
    assert con.to_pyarrow(clean.table.order_by("unit")).equals(
        con.to_pyarrow(incremental.table.order_by("unit"))
    )
    assert clean.columns == incremental.columns
    expected_rate = 4.5 if changed_source == "numerator" else 5 / 3
    rates = {row["unit"]: row["rate"] for row in con.to_pyarrow(clean.table).to_pylist()}
    assert rates["a"] == pytest.approx(expected_rate)


@pytest.mark.parametrize(("policy", "correction"), [("first", "insert"), ("nth", "delete")])
def test_event_anchor_identity_corrections_match_clean_compilation(
    column_model, policy, correction
):
    model = _float_model(column_model)
    con = model.con
    con.raw_sql("ALTER TABLE events ADD COLUMN event_id VARCHAR")
    con.raw_sql(
        "UPDATE events SET event_id=CASE WHEN ts=TIMESTAMPTZ '2025-01-02 11:59:59+00' "
        "THEN 'event-1' ELSE 'event-2' END"
    )
    model = Model.from_catalog(
        model.catalog,
        con=con,
        readiness=model.readiness,
        units=model.units,
        event_ids={"events": "event_id"},
    )
    if policy == "nth":
        con.raw_sql(
            "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-02 11:00:00+00', 2.0, "
            "'third', 'event-3')"
        )
    anchor = (
        EventAnchor("amount", policy, n=2) if policy == "nth" else EventAnchor("amount", policy)
    )
    if correction == "insert":
        con.raw_sql(
            "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-02 10:00:00+00', 5.0, "
            "'earlier', 'event-0')"
        )
    else:
        con.raw_sql("DELETE FROM events WHERE event_id='event-1'")
    design = Design("user", anchor, features={"total": agg("total", window=("-1d", "0s"))})
    clean = compile_columns(design, model)
    incremental = compose_materialized_frame(
        design, model, persisted=_same_snapshot_prefixes(design, model)
    )
    clean.validate(execute=model.con.to_pyarrow)
    incremental.validate(execute=model.con.to_pyarrow)
    assert model.con.to_pyarrow(clean.audit_table.order_by("unit")).equals(
        model.con.to_pyarrow(incremental.audit_table.order_by("unit"))
    )
    assert model.con.to_pyarrow(clean.table.order_by("unit")).equals(
        model.con.to_pyarrow(incremental.table.order_by("unit"))
    )
    anchor_rows = [
        row for row in model.con.to_pyarrow(clean.audit_table).to_pylist() if row["unit"] == "a"
    ]
    assert len(anchor_rows) == 1
    expected_t0 = (
        datetime(2025, 1, 2, 10, tzinfo=UTC)
        if policy == "first"
        else datetime(2025, 1, 2, 12, tzinfo=UTC)
    )
    assert anchor_rows[0]["t0"] == expected_t0


def test_nested_selector_correction_outside_t0_window_matches_clean_frame(column_model):
    from terrine.runtime.changes import Change, ChangeSet
    from terrine.runtime.graph import dependency_graph
    from terrine.runtime.planner import plan_refresh

    con = column_model.con
    con.raw_sql("INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-01 11:00:00+00', 1, 'old')")
    con.raw_sql(
        "CREATE TABLE statuses (user_id VARCHAR, ts TIMESTAMPTZ, score BIGINT); "
        "INSERT INTO statuses VALUES ('a', TIMESTAMPTZ '2025-01-01 10:30:00+00', 1)"
    )
    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["statuses"] = {
        "relation": {"kind": "table", "name": "statuses"},
        "time": "ts",
        "units": {"user": "user_id"},
        "events": ["score"],
        "properties": [],
        "dimensions": [],
    }
    definition["events"]["score"] = {"source": "statuses", "column": "score"}
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness={
            "events": Coverage("2024-12-31", "2025-01-03"),
            "statuses": Coverage("2024-12-31", "2025-01-03"),
        },
        units=column_model.units,
    )
    origin = event_time("amount", window=("-2d", "-1d"))
    nested = event_time("score", window=("-2h", "0s"), relative_to=origin)
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"nested_time": nested},
        outcomes={"exact_total": agg("total", window=("0s", "1d"))},
    )

    con.raw_sql("UPDATE statuses SET ts=TIMESTAMPTZ '2025-01-01 09:30:00+00' WHERE score=1")
    changed_at = datetime(2025, 1, 1, 9, 30, tzinfo=UTC)
    plan = plan_refresh(
        namespace="owner",
        dataset="nested",
        definition_hash=design.spec_hash,
        snapshot_id="same-snapshot",
        graph=dependency_graph(model, design, partitions=("all",)),
        changes=ChangeSet(
            (
                Change(
                    "update",
                    "statuses",
                    before_time_range=(changed_at, changed_at.replace(hour=11)),
                    after_time_range=(changed_at, changed_at.replace(hour=10)),
                ),
            ),
            "change_feed",
        ),
    )
    assert any(task.layer == "frame" for task in plan.tasks)
    clean = compile_columns(design, model)
    incremental = compose_materialized_frame(
        design, model, persisted=_same_snapshot_prefixes(design, model)
    )
    clean.validate(execute=con.to_pyarrow)
    incremental.validate(execute=con.to_pyarrow)
    clean_rows = con.to_pyarrow(clean.table.order_by("unit")).to_pylist()
    incremental_rows = con.to_pyarrow(incremental.table.order_by("unit")).to_pylist()
    assert clean_rows == incremental_rows
    row = next(row for row in clean_rows if row["unit"] == "a")
    assert (row["nested_time"], row["exact_total"]) == (
        datetime(2025, 1, 1, 9, 30, tzinfo=UTC),
        7,
    )
