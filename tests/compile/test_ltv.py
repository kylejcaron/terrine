from __future__ import annotations

import ibis

from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import CohortSpec, Design, EventAnchor
from terrine.spec.trajectory import Trajectory


def _trajectory_model():
    con = ibis.duckdb.connect()
    con.raw_sql("""
        CREATE TABLE events AS SELECT * FROM (VALUES
            ('u1', TIMESTAMPTZ '2025-01-01 00:00:00+00', 'purchase', 0::BIGINT),
            ('u1', TIMESTAMPTZ '2025-01-03 00:00:00+00', 'revenue', 10::BIGINT),
            ('u1', TIMESTAMPTZ '2025-01-10 00:00:00+00', 'revenue', 5::BIGINT),
            ('u2', TIMESTAMPTZ '2025-01-01 00:00:00+00', 'purchase', 0::BIGINT),
            ('u3', TIMESTAMPTZ '2025-01-15 00:00:00+00', 'purchase', 0::BIGINT)
        ) AS e(user_id, ts, kind, amount)
    """)
    con.raw_sql("CREATE TABLE users AS SELECT * FROM (VALUES ('u1'), ('u2'), ('u3')) AS u(id)")
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
                    "facts": ["purchase", "revenue"],
                    "properties": ["kind"],
                    "dimensions": [],
                }
            },
            "facts": {
                "purchase": {"source": "events", "column": None},
                "revenue": {"source": "events", "column": "amount"},
            },
            "properties": {
                "kind": {
                    "source": "events",
                    "column": "kind",
                    "dtype": "string",
                    "as_of": "event_time",
                }
            },
            "dimensions": {},
            "measures": {"net_revenue_sum": {"fact": "revenue", "aggregation": "sum"}},
            "metrics": {
                "net_revenue": {"kind": "mean", "entity": "user", "measure": "net_revenue_sum"}
            },
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )
    return con, model


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
        table = design.build(model).to_pyarrow()
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


def test_cumulative_partitions_repeated_anchors_and_propagates_missing_period():
    con, model = _trajectory_model()
    try:
        con.raw_sql(
            "INSERT INTO events VALUES "
            "('u2', TIMESTAMPTZ '2025-01-10 00:00:00+00', 'revenue', NULL)"
        )
        design = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=3, cumulative=True),
            ),
        )
        from terrine.compile.shapes.ltv import compile_ltv

        compiled = compile_ltv(design, model)
        repeated_anchor = compiled.table.mutate(anchor_id=ibis.literal("repeated"))
        rows = con.to_pyarrow(compiled.table.union(repeated_anchor, distinct=False)).to_pylist()
        repeated_u1 = [
            row for row in rows if row["unit"] == "u1" and row["anchor_id"] == "repeated"
        ]
        assert sorted(row["cumulative_value"] for row in repeated_u1) == [10, 15, 15]
        u2 = sorted(
            (row for row in rows if row["unit"] == "u2" and row["anchor_id"] != "repeated"),
            key=lambda row: row["age"],
        )
        assert u2[1]["period_value"] is None
        assert u2[1]["cumulative_value"] is None
        assert u2[2]["period_value"] == 0
        assert u2[2]["cumulative_value"] is None
        assert u2[2]["cumulative_value_reason"] == u2[1]["period_value_reason"]
    finally:
        con.disconnect()
