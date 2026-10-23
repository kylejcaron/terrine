from datetime import UTC, datetime
from decimal import Decimal

import ibis
import pytest

from terrine.data import Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import (
    Design,
    FixedDate,
    agg,
    all_of,
    asof,
    eq,
    event_time,
    gt,
    is_null,
)


@pytest.fixture
def column_model():
    con = ibis.duckdb.connect()
    con.raw_sql("""
        CREATE TABLE events AS SELECT user_id, ts::TIMESTAMPTZ AS ts,
            amount::BIGINT AS amount, tier::VARCHAR AS tier
        FROM (VALUES
            ('a', '2025-01-02 11:59:59+00', 3, 'old'),
            ('a', '2025-01-02 12:00:00+00', 7, 'new'),
            ('b', '2025-01-02 11:59:59+00', NULL, NULL)
        ) AS data(user_id, ts, amount, tier)
    """)
    con.raw_sql("CREATE TABLE users AS SELECT * FROM (VALUES ('a'), ('b')) AS u(id)")
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["amount"],
                    "properties": ["tier"],
                    "dimensions": [],
                }
            },
            "facts": {"amount": {"source": "events", "column": "amount"}},
            "properties": {
                "tier": {
                    "source": "events",
                    "column": "tier",
                    "dtype": "string",
                    "as_of": "event_time",
                }
            },
            "dimensions": {},
            "measures": {"total": {"fact": "amount", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "entity": "user", "measure": "total"}},
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-02")},
    )
    try:
        yield model
    finally:
        con.disconnect()


def _compile(model, **kwargs):
    from terrine.compile.columns import compile_columns

    return compile_columns(
        Design("user", FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)), **kwargs), model
    )


def test_false_conjunct_excludes_unknown_without_hiding_candidate_audit(column_model):
    compiled = _compile(
        column_model,
        eligibility={
            "eligible": all_of(
                eq(asof("tier", before="0s"), "not-present"), gt(asof("tier", before="-2d"), "a")
            )
        },
    )
    rows = column_model.con.to_pyarrow(compiled.audit_table.order_by("unit")).to_pylist()
    assert [r["__terrine_eligible"] for r in rows] == [False, None]
    meta = compiled.columns["eligible"]
    assert rows[0][meta.complete_column] is True
    assert rows[0][meta.reason_column] is None
    assert rows[1][meta.reason_column] == "all_values_null"
    assert column_model.con.to_pyarrow(compiled.table).num_rows == 0
    cutoff = compiled.eligibility_cutoffs["eligible"]
    assert (cutoff.latest_seconds, cutoff.strict, cutoff.knowledge) == (
        0,
        True,
        "retrospective_effective_time",
    )


@pytest.mark.parametrize("bound", ["source", "unit"])
@pytest.mark.parametrize("value", ["old", None])
def test_carried_inline_state_before_observation_start_is_unknown(column_model, bound, value):
    con = column_model.con
    con.raw_sql("DELETE FROM events")
    con.raw_sql(
        "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2024-12-31', 1, "
        + ("NULL" if value is None else "'old'")
        + ")"
    )
    if bound == "unit":
        con.raw_sql("ALTER TABLE users ADD COLUMN observed_from TIMESTAMPTZ")
        con.raw_sql("UPDATE users SET observed_from = TIMESTAMPTZ '2025-01-01'")
        column_model = Model.from_catalog(
            column_model.catalog,
            con=con,
            readiness={"events": Coverage("2024-12-01", "2025-01-02")},
            units={"user": UnitSource("users", "id", observed_from="observed_from")},
        )
    compiled = _compile(
        column_model,
        features={"state": asof("tier", before="0s")},
        eligibility={"is_missing": is_null(asof("tier", before="0s"))},
    )
    row = con.to_pyarrow(compiled.audit_table.filter(compiled.audit_table.unit == "a")).to_pylist()[
        0
    ]
    meta = compiled.columns["state"]
    assert row["state"] is None
    assert row[meta.complete_column] is False
    assert row[meta.reason_column] == (
        "missing_history" if bound == "source" else "incomplete_observation"
    )
    assert row["is_missing"] is None


@pytest.mark.parametrize("names", [("same", "SAME"), ("é", "e\u0301")])
def test_output_namespace_collisions_refuse_before_compilation(column_model, names):
    with pytest.raises(TerrineError) as error:
        _compile(
            column_model,
            features={names[0]: asof("tier", before=0)},
            eligibility={names[1]: is_null(asof("tier", before=0))},
        )
    assert error.value.code == "terrine.spec.output_name_conflict"


@pytest.mark.parametrize("name", ["UNIT", "treatment", "__Terrine_private"])
def test_reserved_output_names_refuse(column_model, name):
    with pytest.raises(TerrineError) as error:
        _compile(column_model, features={name: asof("tier", before=0)})
    assert error.value.code == "terrine.spec.reserved_output_name"


def test_reobserved_null_is_known_and_duplicate_inline_observations_dedupe(column_model):
    column_model.con.raw_sql("INSERT INTO events SELECT * FROM events WHERE user_id = 'b'")
    column_model.con.raw_sql("""
        INSERT INTO events VALUES ('b', TIMESTAMPTZ '2024-12-31', 1, 'stale')
    """)
    compiled = _compile(column_model, eligibility={"known_null": is_null(asof("tier", before=0))})
    compiled.validate(execute=column_model.con.to_pyarrow)
    rows = column_model.con.to_pyarrow(compiled.audit_table.order_by("unit")).to_pylist()
    assert [r["known_null"] for r in rows] == [False, True]
    assert column_model.con.to_pyarrow(compiled.table).num_rows == 1


def test_conflicting_inline_history_is_refused_even_for_false_eligibility(column_model):
    column_model.con.raw_sql("""
        INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-02 11:59:59+00', 3, 'conflict')
    """)
    compiled = _compile(
        column_model, eligibility={"eligible": eq(asof("tier", before=0), "absent")}
    )
    with pytest.raises(TerrineError) as error:
        compiled.validate(execute=column_model.con.to_pyarrow)
    assert error.value.code == "terrine.temporal.duplicate_history"


def test_exact_attribute_predicates_do_not_narrow_large_integer_or_decimal(column_model):
    con = column_model.con
    con.raw_sql("ALTER TABLE events ADD COLUMN large BIGINT")
    con.raw_sql("ALTER TABLE events ADD COLUMN money DECIMAL(20, 2)")
    con.raw_sql("UPDATE events SET large = 9007199254740993, money = 9007199254740993.01")
    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["properties"] += ("large", "money")
    definition["properties"].update(
        {
            "large": {"source": "events", "column": "large", "dtype": "int", "as_of": "event_time"},
            "money": {
                "source": "events",
                "column": "money",
                "dtype": "float",
                "as_of": "event_time",
            },
        }
    )
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    compiled = _compile(
        model,
        eligibility={
            "integer": gt(asof("large", before=0), 9007199254740992),
            "decimal": eq(asof("money", before=0), "9007199254740993.01"),
        },
    )
    rows = con.to_pyarrow(compiled.audit_table).to_pylist()
    assert [(r["integer"], r["decimal"]) for r in rows] == [(True, True), (True, True)]


@pytest.mark.parametrize("authority", ["range", "changes"])
def test_explicit_validity_can_cover_gap_but_derived_change_interval_cannot(
    column_model, authority
):
    con = column_model.con
    con.raw_sql("""
        CREATE TABLE tier_history AS SELECT 'a' AS id, 'gold' AS tier,
            TIMESTAMPTZ '2024-12-01' AS starts, TIMESTAMPTZ '2025-02-01' AS ends
    """)
    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["dimensions"] = ("history",)
    definition["properties"]["tier"]["dimension"] = "history"
    definition["dimensions"]["history"] = {
        "relation": {"kind": "table", "name": "tier_history"},
        "source_entity": "user",
        "entity_column": "id",
        "properties": ("tier",),
        "validity": {"valid_from": "starts", "valid_to": "ends"}
        if authority == "range"
        else {"changed_at": "starts"},
    }
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    compiled = _compile(model, features={"state": asof("tier", before=0)})
    compiled.validate(execute=con.to_pyarrow)
    row = con.to_pyarrow(compiled.table.filter(compiled.table.unit == "a")).to_pylist()[0]
    meta = compiled.columns["state"]
    assert row["state"] == ("gold" if authority == "range" else None)
    assert row[meta.complete_column] is (authority == "range")
    assert row[meta.reason_column] == (None if authority == "range" else "missing_history")


def test_static_inline_conflicts_refuse_instead_of_choosing_one_value(column_model):
    definition = column_model.catalog.model_dump(mode="python")
    definition["properties"]["tier"]["as_of"] = "static"
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=column_model.con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    compiled = _compile(model, features={"state": asof("tier", before=0)})
    with pytest.raises(TerrineError) as error:
        compiled.validate(execute=column_model.con.to_pyarrow)
    assert error.value.code == "terrine.temporal.static_key_multiplicity"


def test_event_time_selector_and_relative_metric_execute(column_model):
    column_model.con.raw_sql("""
        INSERT INTO events VALUES
            ('a', TIMESTAMPTZ '2025-01-02 12:00:01+00', NULL, NULL)
    """)
    selector = event_time("amount", window=("0s", "1s"))
    ordinary = agg("total", window=("1s", "2s"))
    relative = agg("total", window=("1s", "2s"), relative_to=selector)
    compiled = _compile(
        column_model,
        outcomes={
            "selected_at": selector,
            "ordinary_amount": ordinary,
            "selected_amount": relative,
        },
    )
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["selected_at"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[0]["ordinary_amount"] is None
    assert rows[0]["selected_amount"] is None
    amount_meta = compiled.columns["ordinary_amount"]
    relative_meta = compiled.columns["selected_amount"]
    ordinary_state = (
        rows[0]["ordinary_amount"],
        rows[0][amount_meta.complete_column],
        rows[0][amount_meta.reason_column],
    )
    relative_state = (
        rows[0]["selected_amount"],
        rows[0][relative_meta.complete_column],
        rows[0][relative_meta.reason_column],
    )
    assert ordinary_state == relative_state
    assert ordinary_state == (None, True, "all_values_null")
    timing = compiled.expression_timings["selected_amount"]
    assert timing.kind == "event_time"
    assert timing.selector_ref is not None
    assert timing.relative_offsets == (1, 2)
    assert timing.support_seconds == (1, 3)
    assert timing.selector_support_seconds[timing.selector_ref] == (0, 1)
    assert timing.point_time_column in rows[0]
    assert timing.start_bound_column in rows[0]
    assert timing.end_bound_column in rows[0]
    assert rows[0][timing.point_time_column] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[0][timing.start_bound_column] == datetime(2025, 1, 2, 12, 0, 1, tzinfo=UTC)
    assert rows[0][timing.end_bound_column] == datetime(2025, 1, 2, 12, 0, 2, tzinfo=UTC)
    assert compiled.feature_cutoffs == {}


def test_relative_feature_cutoff_uses_absolute_support_and_selector_refs(column_model):
    first = event_time("amount", window=("-2d", "-1d"))
    second = event_time("amount", window=("0s", "1h"), relative_to=first)
    compiled = _compile(
        column_model,
        features={"relative_metric": agg("total", window=("0s", "1h"), relative_to=second)},
    )
    timing = compiled.expression_timings["relative_metric"]
    cutoff = compiled.feature_cutoffs["relative_metric"]
    assert timing.support_seconds == (-172800, -79200)
    assert len(timing.selector_dependencies) == 2
    assert len(timing.selector_support_seconds) == 2
    assert timing.selector_ref == repr(second)
    assert cutoff.latest_seconds == -79200
    row = column_model.con.to_pyarrow(compiled.table).to_pylist()[0]
    assert timing.point_time_column in row
    assert timing.start_bound_column in row
    assert timing.end_bound_column in row


def test_reused_event_time_chain_allocates_unique_support_columns(column_model):
    first = event_time("amount", window=("0s", "1s"))
    second = event_time("amount", window=("0s", "1s"), relative_to=first)
    compiled = _compile(
        column_model,
        outcomes={
            "first_time": first,
            "second_time": second,
            "relative_amount": agg("total", window=("0s", "1s"), relative_to=second),
        },
    )
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["first_time"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[0]["second_time"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[0]["relative_amount"] == Decimal(7)


def test_relative_attribute_asof_uses_selected_event_time(column_model):
    column_model.con.raw_sql("""
        INSERT INTO events VALUES
            ('b', TIMESTAMPTZ '2025-01-02 12:00:00+00', 1, NULL)
    """)
    selector = event_time("amount", window=("0s", "1s"))
    compiled = _compile(
        column_model,
        outcomes={
            "tier_at_anchor": asof("tier", before="0s"),
            "tier_before_selected": asof("tier", before="0s", relative_to=selector),
        },
    )
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["tier_before_selected"] == "old"
    ordinary_meta = compiled.columns["tier_at_anchor"]
    relative_meta = compiled.columns["tier_before_selected"]
    ordinary_state = (
        rows[1]["tier_at_anchor"],
        rows[1][ordinary_meta.complete_column],
        rows[1][ordinary_meta.reason_column],
    )
    relative_state = (
        rows[1]["tier_before_selected"],
        rows[1][relative_meta.complete_column],
        rows[1][relative_meta.reason_column],
    )
    assert ordinary_state == relative_state
    assert ordinary_state == (None, True, "all_values_null")
    timing = compiled.expression_timings["tier_before_selected"]
    assert timing.kind == "event_time"
    assert timing.relative_offsets == (0, 0)
    assert timing.support_seconds == (0, 1)


def test_event_time_selector_requires_source_readiness_for_its_full_search_window(column_model):
    selector = event_time("amount", window=("2d", "3d"))
    compiled = _compile(column_model, outcomes={"selected_at": selector})
    compiled.validate(execute=column_model.con.to_pyarrow)
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    meta = compiled.columns["selected_at"]
    assert meta.complete_column is not None
    assert meta.reason_column is not None
    assert all(row[meta.complete_column] is False for row in rows)
    assert all(row[meta.reason_column] == "incomplete_coverage" for row in rows)


def test_event_time_selector_resolves_dimension_backed_label_property(column_model):
    con = column_model.con
    con.raw_sql("""
        INSERT INTO events VALUES
            ('b', TIMESTAMPTZ '2025-01-02 12:00:01+00', 9, NULL)
    """)
    con.raw_sql("""
        CREATE TABLE tier_history AS SELECT 'a' AS id, 'gold' AS tier,
            TIMESTAMPTZ '2024-12-01' AS starts, TIMESTAMPTZ '2025-02-01' AS ends
    """)
    definition = column_model.catalog.model_dump(mode="python")
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
        readiness=column_model.readiness,
        units=column_model.units,
    )
    selector = event_time("amount", window=("0s", "2s"), label_property="tier", equals="gold")
    nth_two = event_time(
        "amount",
        window=("0s", "2s"),
        label_property="tier",
        equals="gold",
        occurrence="nth",
        n=2,
    )
    compiled = _compile(model, outcomes={"selected_at": selector, "nth_two": nth_two})
    compiled.validate(execute=con.to_pyarrow)
    rows = con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["selected_at"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[1]["selected_at"] is None
    label_meta = compiled.columns["selected_at"]
    assert label_meta.complete_column is not None and label_meta.reason_column is not None
    assert rows[1][label_meta.complete_column] is False
    assert rows[1][label_meta.reason_column] == "incomplete_label_history"
    nth_meta = compiled.columns["nth_two"]
    assert rows[1]["nth_two"] is None
    assert nth_meta.complete_column is not None and nth_meta.reason_column is not None
    assert rows[1][nth_meta.complete_column] is True
    assert rows[1][nth_meta.reason_column] is None


def test_event_time_selector_metadata_distinguishes_known_absence_and_missing_origin(
    column_model,
):
    known_absent = event_time("amount", window=("-2s", "-1s"))
    selected = event_time("amount", window=("0s", "1s"))
    compiled = _compile(
        column_model,
        eligibility={"known_absent": is_null(known_absent)},
        outcomes={
            "selected_at": selected,
            "selected_amount": agg("total", window=("0s", "1s"), relative_to=selected),
        },
    )
    compiled.validate(execute=column_model.con.to_pyarrow)
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert [row["known_absent"] for row in rows] == [True, True]
    selector_meta = compiled.columns["selected_at"]
    assert selector_meta.complete_column is not None
    assert selector_meta.reason_column is not None
    assert rows[1]["selected_at"] is None
    assert rows[1][selector_meta.complete_column] is True
    amount_meta = compiled.columns["selected_amount"]
    assert amount_meta.complete_column is not None
    assert amount_meta.reason_column is not None
    assert rows[1]["selected_amount"] is None
    assert rows[1][amount_meta.complete_column] is False
    assert rows[1][amount_meta.reason_column] == "missing_selector_origin"


def test_event_time_selector_refuses_ambiguous_timestamp_ties(column_model):
    column_model.con.raw_sql("""
        INSERT INTO events VALUES
            ('a', TIMESTAMPTZ '2025-01-02 12:00:00+00', 8, 'duplicate')
    """)
    compiled = _compile(
        column_model,
        outcomes={"selected_at": event_time("amount", window=("0s", "1s"))},
    )
    with pytest.raises(TerrineError) as error:
        compiled.validate(execute=column_model.con.to_pyarrow)
    assert error.value.code == "terrine.compile.anchor_integrity"
    assert error.value.context["check_key"] == "tied_event_timestamps"


def test_event_time_selector_respects_unit_observation_interval(column_model):
    con = column_model.con
    con.create_table(
        "observed_users",
        ibis.memtable(
            {
                "id": ["a", "b"],
                "observed_from": [
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, tzinfo=UTC),
                ],
                "observed_until": [
                    datetime(2025, 1, 3, tzinfo=UTC),
                    datetime(2025, 1, 2, 11, 59, 59, tzinfo=UTC),
                ],
            }
        ),
    )
    model = Model.from_catalog(
        column_model.catalog,
        con=con,
        units={
            "user": UnitSource(
                "observed_users",
                "id",
                observed_from="observed_from",
                observed_until="observed_until",
            )
        },
        readiness=column_model.readiness,
    )
    compiled = _compile(model, outcomes={"selected_at": event_time("amount", window=("0s", "1s"))})
    compiled.validate(execute=con.to_pyarrow)
    rows = con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    meta = compiled.columns["selected_at"]
    assert meta.complete_column is not None and meta.reason_column is not None
    assert rows[0][meta.complete_column] is True
    assert rows[1][meta.complete_column] is False
    assert rows[1][meta.reason_column] == "incomplete_observation"


def test_event_time_selector_normalizes_naive_source_timestamps_to_utc(column_model):
    con = column_model.con
    con.raw_sql("""
        CREATE TABLE naive_events AS
        SELECT user_id, TIMESTAMP '2025-01-02 12:00:00' AS ts, amount, tier
        FROM events
        WHERE user_id = 'a' AND ts = TIMESTAMPTZ '2025-01-02 12:00:00+00'
    """)
    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["relation"] = {"kind": "table", "name": "naive_events"}
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    compiled = _compile(model, outcomes={"selected_at": event_time("amount", window=("0s", "1s"))})
    compiled.validate(execute=con.to_pyarrow)
    row = con.to_pyarrow(compiled.table).to_pylist()[0]
    assert row["selected_at"] == datetime(2025, 1, 2, 12, tzinfo=UTC)


def test_event_time_selector_treats_value_less_fact_as_row_occurrence(column_model):
    con = column_model.con
    con.raw_sql("""
        CREATE TABLE row_events AS
        SELECT user_id, ts, amount, tier,
            row_number() OVER (PARTITION BY user_id ORDER BY ts) AS event_id
        FROM events
    """)
    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["relation"] = {"kind": "table", "name": "row_events"}
    definition["sources"]["events"]["facts"] = ("amount", "presence")
    definition["facts"]["presence"] = {"source": "events", "column": None}
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
        event_ids={"events": "event_id"},
    )
    compiled = _compile(
        model, outcomes={"selected_at": event_time("presence", window=("0s", "1s"))}
    )
    compiled.validate(execute=con.to_pyarrow)
    rows = con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["selected_at"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[1]["selected_at"] is None
