from __future__ import annotations

import ibis
import yaml

from terrine import recipes
from terrine.model import Model
from terrine.spec import (
    CohortSpec,
    Design,
    EventAnchor,
    FixedDate,
    Treatment,
    agg,
    asof,
    event_time,
)
from terrine.spec.trajectory import Trajectory
from tests.semantic.test_from_tables import WINDOW, create_tables, rich_model


def _rebound(model: Model) -> Model:
    return Model.from_catalog(
        yaml.safe_load(model.to_catalog()),
        con=model.con,
        units=model.units,
        readiness=model.readiness,
        event_ids=model.event_ids,
    )


def _table(model, design):
    frame = design.build(model)
    return model.con.to_pyarrow(frame.expr.order_by("unit"))


def test_to_catalog_round_trip_builds_equal_dataset():
    con = ibis.duckdb.connect()
    try:
        create_tables(con)
        model = rich_model(con)
        rebound = _rebound(model)
        design = Design(
            "customer",
            FixedDate("2025-02-01"),
            features={
                "refunds": agg("refunds", window=WINDOW),
                "large_paid": agg("large_paid", window=WINDOW),
                "refund_rate": agg("refund_rate", window=WINDOW),
                "first_refund": event_time(
                    "orders", window=WINDOW, label_property="status", equals="refunded"
                ),
                "plan_at_t0": asof("plan", before="0s"),
                "plan_change_at_t0": asof("plan_change", before="0s"),
            },
            treatment=Treatment("plan_change"),
        )
        table = _table(model, design)
        assert table.equals(_table(rebound, design))
        rows = {row["unit"]: row for row in table.to_pylist()}
        assert (rows["alice"]["refunds"], rows["bob"]["refunds"]) == (1, 1)
        assert (rows["alice"]["large_paid"], rows["bob"]["large_paid"]) == (1, 1)
        assert rows["alice"]["refund_rate"] == rows["bob"]["refund_rate"] == 1 / 3
        assert (rows["alice"]["plan_at_t0"], rows["alice"]["plan_change_at_t0"]) == (
            "basic",
            "basic",
        )
        assert rows["alice"]["treatment"] == "pro"
        assert rows["bob"]["plan_at_t0__reason"] == "missing_history"
        assert rows["bob"]["plan_at_t0__complete"] is False

        early = Design(
            "customer",
            FixedDate("2023-12-15"),
            features={
                "plan_at_t0": asof("plan", before="0s"),
                "plan_change_at_t0": asof("plan_change", before="0s"),
            },
        )
        early_table = _table(model, early)
        assert early_table.equals(_table(rebound, early))
        assert set(early_table.column("plan_at_t0__reason").to_pylist()) == {"incomplete_coverage"}
        assert set(early_table.column("plan_change_at_t0__reason").to_pylist()) == {
            "incomplete_coverage"
        }
    finally:
        con.disconnect()


def test_from_tables_ltv_cumulative_age_zero_includes_origin_event():
    con = ibis.duckdb.connect()
    try:
        create_tables(con)
        model = rich_model(con)
        low_level = Design(
            "customer",
            EventAnchor("orders", which="first"),
            shape=CohortSpec("orders", Trajectory("spend", "7d", periods=2, cumulative=True)),
        )
        design = recipes.ltv(
            "customer", start_event="orders", metric="spend", period="7d", periods=2
        )
        assert design == low_level
        assert design.spec_hash == low_level.spec_hash
        rows = model.con.to_pyarrow(design.build(model).expr.order_by("unit", "age")).to_pylist()
        rebound = _rebound(model)
        assert (
            rows
            == model.con.to_pyarrow(
                low_level.build(rebound).expr.order_by("unit", "age")
            ).to_pylist()
        )
        by_unit_age = {(row["unit"], row["age"]): row["cumulative_value"] for row in rows}
        # alice: Jan 5 (30) origin; Jan 10 (60) in age 0; Jan 15 (15) in age 1.
        # bob: Jan 6 (80) origin; Jan 12 (45) in age 0; Jan 20 (10) beyond age 1.
        assert by_unit_age == {
            ("alice", 0): 90.0,
            ("alice", 1): 105.0,
            ("bob", 0): 125.0,
            ("bob", 1): 125.0,
        }
    finally:
        con.disconnect()
