from decimal import Decimal

import pytest

from terrine.data import Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Treatment, agg, all_of, asof, eq, event_time, gt
from tests.compile.test_columns import _compile
from tests.compile.test_columns import column_model as column_model  # noqa: F401


@pytest.fixture
def column_model_fixture(column_model):
    yield column_model


def test_named_windows_asof_treatment_and_independent_completeness(column_model_fixture):
    compiled = _compile(
        column_model_fixture,
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
    compiled.validate(execute=column_model_fixture.con.to_pyarrow)
    rows = column_model_fixture.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
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


def test_float_predicates_compare_the_rounded_value_and_empty_zero_is_singleton(
    column_model_fixture,
):
    con = column_model_fixture.con
    con.raw_sql("ALTER TABLE events ALTER amount TYPE DOUBLE")
    con.raw_sql("UPDATE events SET amount = 0.1 WHERE user_id = 'a'")
    model = Model.from_catalog(
        column_model_fixture.catalog,
        con=con,
        readiness=column_model_fixture.readiness,
        units=column_model_fixture.units,
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
    assert (
        rows[0]["near"],
        rows[0]["above"],
        rows[0]["below"],
        rows[0]["overlap"],
        rows[0]["outside"],
    ) == (True, True, False, False, False)
    assert all(
        rows[0][compiled.columns[name].reason_column] is None
        for name in ("near", "above", "below", "overlap", "outside")
    )


def test_required_predicate_window_overflow_refuses_even_with_false_conjunct(
    column_model_fixture,
):
    con = column_model_fixture.con
    con.raw_sql("ALTER TABLE events ALTER amount TYPE DOUBLE")
    con.raw_sql("UPDATE events SET amount = 0.0 WHERE user_id = 'a'")
    con.raw_sql("""
        UPDATE events SET amount = 1.7976931348623157e308
        WHERE user_id = 'a' AND ts < TIMESTAMPTZ '2025-01-02 12:00:00+00'
    """)
    model = Model.from_catalog(
        column_model_fixture.catalog,
        con=con,
        readiness=column_model_fixture.readiness,
        units=column_model_fixture.units,
    )
    total = agg("total", window=("-1s", "0s"))
    admitted = _compile(model, features={"huge": total})
    admitted.validate(execute=con.to_pyarrow)
    rows = con.to_pyarrow(admitted.table.order_by("unit")).to_pylist()
    assert rows[0]["huge"] == float.fromhex("0x1.fffffffffffffp+1023")
    con.raw_sql("""
        INSERT INTO events VALUES
            ('a', TIMESTAMPTZ '2025-01-02 11:59:59.5+00', 1.7976931348623157e308, 'old')
    """)
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


    con = column_model.con
    con.raw_sql("""
        DELETE FROM events WHERE user_id = 'a'
    """)
    con.raw_sql("""
        INSERT INTO events VALUES
            ('a', TIMESTAMPTZ '2024-12-28 12:00:00+00', 1, 'old'),
            ('a', TIMESTAMPTZ '2025-01-01 12:00:00+00', 2, 'new')
    """)
    model = Model.from_catalog(
        column_model.catalog,
        con=con,
        readiness={"events": Coverage("2024-12-20", "2025-01-03")},
        units=column_model.units,
    )
    selector = event_time("amount", window=("-5d", "0s"), occurrence="last")
    compiled = _compile(
        model,
        features={
            "last_order_at": selector,
            "amount_before_last_order": agg(
                "total", window=("-5d", "0s"), relative_to=selector
            ),
        },
    )
    rows = con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["last_order_at"].isoformat() == "2025-01-01T12:00:00+00:00"
    assert rows[0]["amount_before_last_order"] == Decimal(1)
