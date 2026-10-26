from decimal import Decimal

import pytest

from terrine.compile.shapes.ltv import compile_ltv
from terrine.errors import TerrineError
from terrine.spec import CohortSpec, Design, EventAnchor, agg
from terrine.spec.trajectory import Trajectory
from tests.compile.test_ltv import _collect, _trajectory_model


def test_ltv_period_and_cumulative_values_match_hand_sum():
    con, model = _trajectory_model()
    try:
        design = Design(
            "user",
            EventAnchor("purchase", which="first"),
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


def test_float_feature_metadata_survives_exact_trajectory_compilation():
    con, model = _trajectory_model()
    try:
        design = Design(
            "user",
            EventAnchor("purchase", which="first"),
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
                EventAnchor("purchase", which="first"),
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
                EventAnchor("purchase", which="first"),
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
