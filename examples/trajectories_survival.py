"""Build an LTV trajectory and same-relation cohort summary on local DuckDB."""

from __future__ import annotations

from datetime import UTC, datetime

import ibis

from terrine.data import Budget, Coverage, UnitSource
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import CohortSpec, Design, EventAnchor
from terrine.spec.trajectory import CohortGrouping, Trajectory


def main() -> None:
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
    design = Design(
        "user",
        EventAnchor("purchase", which="first"),
        shape=CohortSpec(
            origin="purchase",
            trajectory=Trajectory("net_revenue", "7d", periods=3, cumulative=True),
        ),
        administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
    )
    frame = design.build(model)
    cohorts = frame.cohorts(
        grouping=CohortGrouping(
            bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"
        ),
        value_column="cumulative_value",
    )
    try:
        for row in cohorts.to_pyarrow(
            budget=Budget(max_rows=100, max_output_bytes=1_000_000)
        ).to_pylist():
            print(row)
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
