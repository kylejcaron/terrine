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
    Treatment,
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


@pytest.mark.slow
def test_named_windows_asof_treatment_and_independent_completeness(column_model):
    compiled = _compile(
        column_model,
        features={
            'sum "before"': agg("total", window=("-1s", "0s")),
            "tier": asof("tier", before="0s"),
            "x); DROP TABLE events; --": asof("tier", before="0s"),
        },
        outcomes={
            "after": agg("total", window=("0s", "1s")),
            "unavailable": agg("total", window=("0s", "2d")),
        },
        treatment=Treatment("tier", known_at_t0=True),
    )
    compiled.validate(execute=column_model.con.to_pyarrow)
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert [(r['sum "before"'], r["after"], r["tier"], r["treatment"]) for r in rows] == [
        (Decimal(3), Decimal(7), "old", "new"),
        (None, Decimal(0), None, None),
    ]
    assert compiled.roles.features == ('sum "before"', "tier", "x); DROP TABLE events; --")
    assert [r["x); DROP TABLE events; --"] for r in rows] == ["old", None]
    assert compiled.roles.outcomes == ("after", "unavailable")
    assert compiled.roles.treatment == "treatment"
    assert compiled.columns["after"].window.end == 1
    assert compiled.expression_timings["after"].kind == "fixed_t0"
    assert compiled.expression_timings["after"].support_seconds == (0, 1)
    assert compiled.feature_cutoffs["tier"].strict is True
    assert compiled.feature_cutoffs["tier"].knowledge == "retrospective_effective_time"
    null_meta = compiled.columns['sum "before"']
    assert rows[1][null_meta.complete_column] is True
    assert rows[1][null_meta.reason_column] == "all_values_null"
    missing_meta = compiled.columns["unavailable"]
    assert all(r[missing_meta.reason_column] == "incomplete_coverage" for r in rows)


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


@pytest.mark.slow
def test_checked_predicate_interval_is_unknown_but_empty_zero_is_singleton(column_model):
    con = column_model.con
    con.raw_sql("ALTER TABLE events ALTER amount TYPE DOUBLE")
    con.raw_sql("UPDATE events SET amount = 0.1 WHERE user_id = 'a'")
    model = Model.from_catalog(
        column_model.catalog, con=con, readiness=column_model.readiness, units=column_model.units
    )
    total = agg("total", window=("-1s", "0s"))
    compiled = _compile(
        model,
        eligibility={
            "near": eq(total, 0.1),
            "above": gt(total, 0.0),
            "below": gt(total, 1.0),
            "overlap": gt(total, 0.1),
            "outside": eq(total, 1.0),
            "zero": eq(agg("total", window=("-3s", "-2s")), 0.0),
        },
    )
    compiled.validate(execute=con.to_pyarrow)
    rows = con.to_pyarrow(compiled.audit_table.order_by("unit")).to_pylist()
    assert [r["zero"] for r in rows] == [True, True]
    assert rows[0]["near"] is None
    assert rows[0][compiled.columns["near"].reason_column] == "uncertain_comparison"
    assert (rows[0]["above"], rows[0]["below"], rows[0]["overlap"], rows[0]["outside"]) == (
        True,
        False,
        None,
        False,
    )
    assert rows[0][compiled.columns["overlap"].reason_column] == "uncertain_comparison"


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


@pytest.mark.slow
def test_required_predicate_endpoint_overflow_refuses_even_with_false_conjunct(column_model):
    con = column_model.con
    con.raw_sql("ALTER TABLE events ALTER amount TYPE DOUBLE")
    con.raw_sql("UPDATE events SET amount = 0.0 WHERE user_id = 'a'")
    con.raw_sql("""
        UPDATE events SET amount = 1.7976931348623157e308
        WHERE user_id = 'a' AND ts < TIMESTAMPTZ '2025-01-02 12:00:00+00'
    """)
    model = Model.from_catalog(
        column_model.catalog, con=con, readiness=column_model.readiness, units=column_model.units
    )
    total = agg("total", window=("-1s", "0s"))
    admitted = _compile(model, features={"huge": total})
    admitted.validate(execute=con.to_pyarrow)
    rows = con.to_pyarrow(admitted.table.order_by("unit")).to_pylist()
    assert rows[0]["huge"] == float.fromhex("0x1.fffffffffffffp+1023")
    compiled = _compile(
        model,
        eligibility={
            "eligible": all_of(
                eq(asof("tier", before=0), "absent"),
                gt(total, 0.0),
            )
        },
    )
    with pytest.raises(TerrineError) as error:
        compiled.validate(execute=con.to_pyarrow)
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failure"] == "bound_overflow"


def test_event_time_selector_and_relative_metric_execute(column_model):
    selector = event_time("amount", window=("0s", "1s"))
    compiled = _compile(
        column_model,
        outcomes={
            "selected_at": selector,
            "selected_amount": agg("total", window=("0s", "1s"), relative_to=selector),
        },
    )
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["selected_at"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[0]["selected_amount"] == Decimal(7)
    timing = compiled.expression_timings["selected_amount"]
    assert timing.kind == "event_time"
    assert timing.selector_ref is not None
    assert timing.relative_offsets == (0, 1)
    assert timing.support_seconds == (0, 2)
    assert timing.selector_support_seconds[timing.selector_ref] == (0, 1)


def test_relative_attribute_asof_uses_selected_event_time(column_model):
    selector = event_time("amount", window=("0s", "1s"))
    compiled = _compile(
        column_model,
        outcomes={"tier_before_selected": asof("tier", before="0s", relative_to=selector)},
    )
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["tier_before_selected"] == "old"
    timing = compiled.expression_timings["tier_before_selected"]
    assert timing.kind == "event_time"
    assert timing.relative_offsets == (0, 0)
    assert timing.support_seconds == (0, 1)


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
