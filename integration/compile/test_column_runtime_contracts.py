from decimal import Decimal

import pytest

from terrine.errors import TerrineError
from terrine.model import Model
from terrine.spec import Treatment, agg, all_of, asof, eq, gt
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


def test_checked_predicate_interval_is_unknown_but_empty_zero_is_singleton(column_model_fixture):
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
    assert rows[0]["near"] is None
    assert rows[0][compiled.columns["near"].reason_column] == "uncertain_comparison"
    assert (rows[0]["above"], rows[0]["below"], rows[0]["overlap"], rows[0]["outside"]) == (
        True,
        False,
        None,
        False,
    )
    assert rows[0][compiled.columns["overlap"].reason_column] == "uncertain_comparison"


def test_required_predicate_endpoint_overflow_refuses_even_with_false_conjunct(
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
