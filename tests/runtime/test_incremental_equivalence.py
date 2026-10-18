# ruff: noqa: F811
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from terrine.compile.columns import compile_columns
from terrine.compile.layers import measure_identity, plan_layers
from terrine.compile.materialize import compose_materialized_frame
from terrine.data import Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import Catalog
from terrine.spec import Design, EventAnchor, FixedDate, agg, asof, event_time, gt
from terrine.temporal.daily import _sparse_group, resolve_measure
from terrine.temporal.prefix import sparse_prefix
from tests.compile.test_columns import column_model  # noqa: F401,F811


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
            model._relation(source.relation), source, source.entities[design.unit]
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


def test_historical_1e16_insert_has_identical_float64_refusal(column_model):
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
    refusals = []
    for compiled in (clean, incremental):
        with pytest.raises(TerrineError) as error:
            compiled.validate(execute=model.con.to_pyarrow)
            model.con.to_pyarrow(compiled.audit_table)
        refusals.append((error.value.code, error.value.context))
    assert refusals[0] == refusals[1]
    assert refusals[0][0] == "terrine.temporal.float64_precision"


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
            "source_entity": "user",
            "entity_column": "id",
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
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "numerator": {
                    "relation": {"kind": "table", "name": "n"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["click"],
                    "properties": [],
                    "dimensions": [],
                },
                "denominator": {
                    "relation": {"kind": "table", "name": "d"},
                    "timestamp": "ts",
                    "entities": {"user": "customer_id"},
                    "facts": ["visit"],
                    "properties": [],
                    "dimensions": [],
                },
            },
            "facts": {
                "click": {"source": "numerator", "column": "amount"},
                "visit": {"source": "denominator", "column": None},
            },
            "properties": {},
            "dimensions": {},
            "measures": {
                "clicks": {"fact": "click", "aggregation": "sum"},
                "visits": {"fact": "visit", "aggregation": "count"},
            },
            "metrics": {
                "rate": {
                    "kind": "ratio",
                    "entity": "user",
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
        "timestamp": "ts",
        "entities": {"user": "user_id"},
        "facts": ["score"],
        "properties": [],
        "dimensions": [],
    }
    definition["facts"]["score"] = {"source": "statuses", "column": "score"}
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
