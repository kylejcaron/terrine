from dataclasses import replace
from decimal import Decimal

import ibis
import pytest

from terrine import UnitSource, preview
from terrine.compile.shapes.ltv import compile_ltv
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.spec import CohortSpec, Design, EventAnchor, agg
from terrine.spec.trajectory import Trajectory
from tests.compile.test_ltv import _collect, _trajectory_model


def test_ltv_period_and_cumulative_values_match_hand_sum():
    con, model = _trajectory_model()
    try:
        design = Design(
            "user",
            EventAnchor("purchase", occurrence="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=3, cumulative=True),
            ),
        )
        table = _collect(con, compile_ltv(design, model))
        rows = sorted(table.to_pylist(), key=lambda row: (row["unit"], row["age"]))
        u1 = [row for row in rows if row["unit"] == "u1"]
        assert [(row["age"], row["period_value"], row["cumulative_value"]) for row in u1] == [
            (0, 10, 10),
            (1, 5, 15),
            (2, 0, 15),
        ]
        u2 = [row for row in rows if row["unit"] == "u2"]
        assert all(row["period_value"] == 0 and row["cumulative_value"] == 0 for row in u2)
    finally:
        con.disconnect()


def test_ltv_declared_metric_width_is_independent_of_period_lowering():
    con, model = _trajectory_model(unit="u1")
    try:
        catalog = model.catalog.model_dump(mode="python")
        catalog["measures"]["net_revenue_sum"]["window_days"] = 2
        declared = Model.from_catalog(
            catalog, con=con, units={"user": UnitSource("users", "id")}, readiness=model.readiness
        )
        valid = Design(
            "user",
            EventAnchor("purchase"),
            shape=CohortSpec("purchase", Trajectory("net_revenue", "2d", periods=3)),
        )
        wide = replace(valid, features={"pre": agg("decimal_revenue", window=("-1d", "0s"))})
        for design in (valid, wide):
            incompatible = replace(
                design, shape=CohortSpec("purchase", Trajectory("net_revenue", "7d", periods=3))
            )
            with pytest.raises(TerrineError) as rejected:
                incompatible.validate(declared)
            assert rejected.value.code == "terrine.design.metric_window_conflict"
            actual = design.build(declared).materialize().to_pyarrow()
            by_age = dict(
                zip(actual["age"].to_pylist(), actual["period_value"].to_pylist(), strict=True)
            )
            assert by_age == {0: 0, 1: 10, 2: 0}
    finally:
        con.disconnect()


def test_compact_ltv_preview_reports_the_fact_read():
    con, model = _trajectory_model()
    try:
        design = Design(
            "user",
            EventAnchor("purchase"),
            shape=CohortSpec("purchase", Trajectory("net_revenue", "1d", periods=3)),
        )
        reads = preview(design, model).source_reads
        assert any(read.source == "events" for read in reads)
        assert all(read.physical_key is None for read in reads)
    finally:
        con.disconnect()


def test_compact_ltv_has_the_same_first_exact_float_hazard_as_wide_ltv():
    con, model = _trajectory_model(unit="u1")
    try:
        con.raw_sql("""
            UPDATE events SET float_amount = 'NaN'::DOUBLE
                WHERE ts = TIMESTAMPTZ '2025-01-03 00:00:00+00';
            INSERT INTO events (user_id, ts, kind, amount, float_amount)
                VALUES ('u1', TIMESTAMPTZ '2025-01-11 00:00:00+00', 'revenue', 0, 1e100)
        """)
        compact = Design(
            "user",
            EventAnchor("purchase"),
            shape=CohortSpec("purchase", Trajectory("float_revenue", "1d", periods=11)),
        )
        wide = replace(compact, features={"pre": agg("net_revenue", window=("-1d", "0s"))})
        for design in (wide, compact):
            with pytest.raises(TerrineError) as refused:
                design.build(model).materialize()
            assert refused.value.code == "terrine.temporal.float64_precision"
            assert refused.value.context["reason"] == "out_of_range"
    finally:
        con.disconnect()


def test_ltv_sql_budget_does_not_grow_with_ages():
    con, model = _trajectory_model()
    try:
        for periods in (10, 100):
            design = Design(
                "user",
                EventAnchor("purchase"),
                shape=CohortSpec(
                    "purchase", Trajectory("net_revenue", "1d", periods, cumulative=True)
                ),
            )
            assert len(ibis.to_sql(design.build(model).expr)) <= 30_000
    finally:
        con.disconnect()


def test_float_feature_metadata_survives_exact_trajectory_compilation():
    con, model = _trajectory_model()
    try:
        design = Design(
            "user",
            EventAnchor("purchase", occurrence="first"),
            features={"float_feature": agg("float_revenue", window=("-7d", "0s"))},
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=1, cumulative=False),
            ),
        )
        frame = compile_ltv(design, model)
        table = _collect(con, frame)
        feature = frame.columns["float_feature"]
        assert feature.numeric_policy is None
        for column in (
            feature.observed_from_column,
            feature.observed_until_column,
            feature.complete_column,
            feature.reason_column,
        ):
            assert column is not None and column in table.column_names
        assert frame.columns["period_value"].numeric_policy in (None, "exact")
    finally:
        con.disconnect()


@pytest.mark.parametrize(
    "value",
    [
        "60000000000000000000000000000000000000",
        "-60000000000000000000000000000000000000",
    ],
)
def test_scale_zero_decimal_cumulative_overflow_refuses(value):
    con, model = _trajectory_model(decimal_type="DECIMAL(38, 0)")
    try:
        con.raw_sql(
            f"UPDATE events SET decimal_amount = {value} WHERE user_id = 'u1' AND kind = 'revenue'"
        )
        frame = compile_ltv(
            Design(
                "user",
                EventAnchor("purchase", occurrence="first"),
                shape=CohortSpec(
                    origin="purchase",
                    trajectory=Trajectory("decimal_revenue", "7d", periods=2, cumulative=True),
                ),
            ),
            model,
        )
        with pytest.raises(TerrineError) as error:
            _collect(con, frame)
        assert error.value.code == "terrine.temporal.exact_overflow"
        assert error.value.context["output"] == "cumulative_value"
    finally:
        con.disconnect()


def test_missing_decimal_period_preserves_cumulative_reason_when_known_subtotal_overflows():
    con, model = _trajectory_model(decimal_type="DECIMAL(38, 0)")
    try:
        con.raw_sql(
            "UPDATE events SET decimal_amount = NULL "
            "WHERE user_id = 'u1' AND kind = 'revenue' "
            "AND ts = TIMESTAMPTZ '2025-01-03 00:00:00+00'"
        )
        con.raw_sql(
            "UPDATE events SET decimal_amount = NULL WHERE user_id = 'u1' AND kind = 'purchase'"
        )
        con.raw_sql(
            "UPDATE events SET decimal_amount = 60000000000000000000000000000000000000 "
            "WHERE user_id = 'u1' AND kind = 'revenue' "
            "AND ts = TIMESTAMPTZ '2025-01-10 00:00:00+00'"
        )
        con.raw_sql("""
            INSERT INTO events (user_id, ts, kind, amount, decimal_amount, float_amount)
            VALUES (
                'u1', TIMESTAMPTZ '2025-01-17 00:00:00+00', 'revenue', 0,
                60000000000000000000000000000000000000, 0
            )
        """)
        frame = compile_ltv(
            Design(
                "user",
                EventAnchor("purchase", occurrence="first"),
                shape=CohortSpec(
                    origin="purchase",
                    trajectory=Trajectory("decimal_revenue", "7d", periods=3, cumulative=True),
                ),
            ),
            model,
        )
        rows = sorted(
            (row for row in _collect(con, frame).to_pylist() if row["unit"] == "u1"),
            key=lambda row: row["age"],
        )
        assert [row["period_value"] for row in rows] == [
            None,
            Decimal("60000000000000000000000000000000000000"),
            Decimal("60000000000000000000000000000000000000"),
        ]
        assert all(row["cumulative_value"] is None for row in rows)
        assert all(not row["cumulative_value__complete"] for row in rows)
        assert [row["cumulative_value__reason"] for row in rows] == [
            "all_values_null",
            "all_values_null",
            "all_values_null",
        ]
    finally:
        con.disconnect()
