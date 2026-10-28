from __future__ import annotations

import ibis

from terrine import recipes
from terrine.data import Coverage, Events, UnitSource
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import CohortSpec, Design, EventAnchor, FixedDate, agg
from terrine.spec.trajectory import Trajectory


def _fixture():
    con = ibis.duckdb.connect()
    con.raw_sql("""
        CREATE TABLE users AS SELECT * FROM (VALUES ('u1'), ('u2')) AS u(id)
    """)
    con.raw_sql("""
        CREATE TABLE orders AS SELECT * FROM (VALUES
            ('u1', TIMESTAMPTZ '2025-01-02 00:00:00+00', 7::BIGINT),
            ('u1', TIMESTAMPTZ '2025-01-04 00:00:00+00', 13::BIGINT)
        ) AS o(user_id, ts, amount)
    """)
    return con


def _native_model(con):
    catalog = Catalog.model_validate({
        "schema_version": 2,
        "dialect": "duckdb",
        "day_boundary": "UTC",
        "sources": {
            "orders": {
                "relation": {"kind": "table", "name": "orders"},
                "time": "ts",
                "units": {"user": "user_id"},
                "events": ["orders", "orders_amount"],
                "properties": [],
                "dimensions": [],
            }
        },
        "events": {
            "orders": {"source": "orders", "column": None},
            "orders_amount": {"source": "orders", "column": "amount"},
        },
        "properties": {},
        "dimensions": {},
        "measures": {
            "orders_count": {"event": "orders", "aggregation": "count"},
            "orders_amount_sum": {"event": "orders_amount", "aggregation": "sum"},
            "orders_amount_mean": {"event": "orders_amount", "aggregation": "avg_event"},
        },
        "metrics": {
            name: {"kind": "mean", "unit": "user", "measure": name}
            for name in ("orders_count", "orders_amount_sum", "orders_amount_mean")
        },
    })
    return Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "id")},
        readiness={"orders": Coverage("2025-01-01", "2025-02-28")},
    )


def _collect(model, design):
    frame = design.build(model)
    sort_columns = ("unit", "age") if "age" in frame.expr.columns else ("unit",)
    return frame, model.con.to_pyarrow(frame.expr.order_by(*sort_columns)).to_pylist()


def test_from_tables_aggregates_match_native_catalog_and_preserve_availability():
    con = _fixture()
    try:
        generated = Model.from_tables(
            con,
            units={"user": UnitSource("users", "id")},
            events={
                "orders": Events(
                    "orders",
                    "ts",
                    {"user": "user_id"},
                    ("amount",),
                    ("2025-01-01", "2025-02-28"),
                )
            },
        )
        native = _native_model(con)
        assert "orders" in generated.catalog.sources
        assert "orders" in generated.catalog.events
        assert generated.catalog.events["orders"].source == "orders"
        design = Design(
            "user",
            FixedDate("2025-01-03"),
            outcomes={
                "orders_count": agg("orders_count", window=("0d", "2d")),
                "orders_amount_sum": agg("orders_amount_sum", window=("0d", "2d")),
                "orders_amount_mean": agg("orders_amount_mean", window=("0d", "2d")),
            },
        )
        generated_frame, generated_rows = _collect(generated, design)
        _, native_rows = _collect(native, design)
        assert generated_rows == native_rows
        by_unit = {row["unit"]: row for row in generated_rows}
        assert by_unit["u1"]["orders_count"] == 1
        assert by_unit["u1"]["orders_amount_sum"] == 13
        assert by_unit["u1"]["orders_amount_mean"] == 13
        assert by_unit["u2"]["orders_count"] == 0
        assert by_unit["u2"]["orders_amount_sum"] == 0
        assert by_unit["u2"]["orders_amount_mean"] is None
        mean_reason = generated_frame.columns["orders_amount_mean"].reason_column
        assert by_unit["u2"][mean_reason] == "no_observations"

        incomplete = Design(
            "user",
            FixedDate("2025-02-28"),
            outcomes={
                name: agg(name, window=("0d", "2d"))
                for name in ("orders_count", "orders_amount_sum", "orders_amount_mean")
            },
        )
        incomplete_frame, incomplete_rows_raw = _collect(generated, incomplete)
        _, native_incomplete_rows = _collect(native, incomplete)
        assert incomplete_rows_raw == native_incomplete_rows
        incomplete_rows = {row["unit"]: row for row in incomplete_rows_raw}
        for name in ("orders_count", "orders_amount_sum", "orders_amount_mean"):
            assert incomplete_rows["u1"][name] is None
            assert incomplete_rows["u2"][name] is None
            reason_column = incomplete_frame.columns[name].reason_column
            assert incomplete_rows["u1"][reason_column] == "incomplete_coverage"
            assert incomplete_rows["u2"][reason_column] == "incomplete_coverage"
    finally:
        con.disconnect()


def test_from_tables_ltv_cumulative_age_zero_includes_origin_event():
    con = _fixture()
    try:
        generated = Model.from_tables(
            con,
            units={"user": UnitSource("users", "id")},
            events={
                "orders": Events(
                    "orders",
                    "ts",
                    {"user": "user_id"},
                    ("amount",),
                    ("2025-01-01", "2025-02-28"),
                )
            },
        )
        native = _native_model(con)
        low_level = Design(
            "user",
            EventAnchor("orders", which="first"),
            shape=CohortSpec(
                "orders",
                Trajectory("orders_amount_sum", "30d", periods=2, cumulative=True),
            ),
        )
        design = recipes.ltv(
            "user",
            start_event="orders",
            metric="orders_amount_sum",
            period="30d",
            periods=2,
        )
        assert design == low_level
        assert design.spec_hash == low_level.spec_hash
        generated_frame, generated_rows = _collect(generated, design)
        _, native_rows = _collect(native, low_level)
        assert generated_rows == native_rows
        u1 = [row for row in generated_rows if row["unit"] == "u1"]
        age_zero = next(row for row in u1 if row["age"] == 0)
        assert age_zero["cumulative_value"] == 20
    finally:
        con.disconnect()
