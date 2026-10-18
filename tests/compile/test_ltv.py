from __future__ import annotations

import ibis
import pytest

from terrine.data import Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import CohortSpec, Design, EventAnchor, agg
from terrine.spec.trajectory import Trajectory


def _trajectory_model(*, decimal_type="DECIMAL(18, 2)"):
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
    con.raw_sql(f"ALTER TABLE events ADD COLUMN decimal_amount {decimal_type}")
    con.raw_sql("UPDATE events SET decimal_amount = amount::DECIMAL(18, 2) / 100")
    con.raw_sql("ALTER TABLE events ADD COLUMN float_amount DOUBLE")
    con.raw_sql("CREATE TABLE users AS SELECT * FROM (VALUES ('u1'), ('u2'), ('u3')) AS u(id)")
    con.raw_sql("UPDATE events SET float_amount = amount::DOUBLE / 100")
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
                    "facts": ["purchase", "revenue", "decimal_revenue", "float_revenue"],
                    "properties": ["kind"],
                    "dimensions": [],
                }
            },
            "facts": {
                "purchase": {"source": "events", "column": None},
                "revenue": {"source": "events", "column": "amount"},
                "decimal_revenue": {"source": "events", "column": "decimal_amount"},
                "float_revenue": {"source": "events", "column": "float_amount"},
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
            "measures": {
                "net_revenue_sum": {"fact": "revenue", "aggregation": "sum"},
                "decimal_revenue_sum": {"fact": "decimal_revenue", "aggregation": "sum"},
                "float_revenue_sum": {"fact": "float_revenue", "aggregation": "sum"},
            },
            "metrics": {
                "net_revenue": {"kind": "mean", "entity": "user", "measure": "net_revenue_sum"},
                "decimal_revenue": {
                    "kind": "mean",
                    "entity": "user",
                    "measure": "decimal_revenue_sum",
                },
                "float_revenue": {
                    "kind": "mean",
                    "entity": "user",
                    "measure": "float_revenue_sum",
                },
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
            "INSERT INTO events (user_id, ts, kind, amount) VALUES "
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


def test_float_feature_certificates_survive_exact_trajectory_compilation():
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
        frame = design.build(model)
        table = frame.to_pyarrow()
        feature = frame.columns["float_feature"]
        assert feature.numeric_policy == "checked_float64_v1"
        for column in (
            feature.observed_from_column,
            feature.observed_until_column,
            feature.complete_column,
            feature.reason_column,
            feature.absolute_error_column,
        ):
            assert column is not None and column in table.column_names
        assert frame.columns["period_value"].numeric_policy in (None, "exact")
    finally:
        con.disconnect()


def test_cutoff_nulls_period_error_certificate_with_its_value():
    from datetime import UTC, datetime

    con, model = _trajectory_model()
    try:
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("float_revenue", "7d", periods=3, cumulative=False),
            ),
            administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
        ).build(model)
        table = frame.to_pyarrow().to_pylist()
        young = next(row for row in table if row["unit"] == "u3" and row["age"] == 1)
        error_column = frame.columns["period_value"].absolute_error_column
        assert error_column is not None
        assert young["period_value"] is None
        assert young[error_column] is None
    finally:
        con.disconnect()


def test_cumulative_float_results_enforce_final_tolerance():
    con, model = _trajectory_model()
    try:
        con.raw_sql(
            "UPDATE events SET float_amount = 1e12 "
            "WHERE user_id = 'u1' AND kind = 'revenue' AND ts = "
            "TIMESTAMPTZ '2025-01-03 00:00:00+00'"
        )
        con.raw_sql(
            "UPDATE events SET float_amount = -1e12 "
            "WHERE user_id = 'u1' AND kind = 'revenue' AND ts = "
            "TIMESTAMPTZ '2025-01-10 00:00:00+00'"
        )
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("float_revenue", "7d", periods=3, cumulative=True),
            ),
        ).build(model)
        with pytest.raises(TerrineError) as error:
            frame.materialize()
        assert error.value.code == "terrine.temporal.float64_precision"
    finally:
        con.disconnect()


def test_trajectory_generated_names_are_reserved_from_features():
    con, model = _trajectory_model()
    try:
        design = Design(
            "user",
            EventAnchor("purchase", which="first"),
            features={"period_value": agg("net_revenue", window=("-7d", "0s"))},
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=1, cumulative=False),
            ),
        )
        with pytest.raises(TerrineError) as error:
            design.build(model)
        assert error.value.code == "terrine.spec.reserved_output_name"
    finally:
        con.disconnect()


def test_scale_zero_decimal_cumulative_retains_wide_values():
    con, model = _trajectory_model(decimal_type="DECIMAL(38, 0)")
    try:
        con.raw_sql(
            "UPDATE events SET decimal_amount = 10000000000000000000000000000000000000 "
            "WHERE user_id = 'u1' AND kind = 'revenue'"
        )
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("decimal_revenue", "7d", periods=1, cumulative=True),
            ),
        ).build(model)
        rows = frame.to_pyarrow().to_pylist()
        row = next(row for row in rows if row["unit"] == "u1")
        assert row["cumulative_value"] == 10**37
    finally:
        con.disconnect()
