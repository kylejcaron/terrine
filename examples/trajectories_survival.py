"""Local DuckDB walkthrough for cohort datasets and portable Arrow export."""

from datetime import UTC, datetime

import ibis

from terrine import Budget, Coverage, UnitSource, Model
from terrine.export import to_surrogate_arrow
from terrine.spec import (
    CohortGrouping,
    CohortSpec,
    Design,
    EventAnchor,
    LifeTable,
    TimeToEvent,
    Trajectory,
)


def _model():
    con = ibis.duckdb.connect()
    con.raw_sql("""
        CREATE TABLE events AS SELECT * FROM (VALUES
            ('u1', TIMESTAMPTZ '2025-01-01 00:00:00+00', 'purchase', 0::BIGINT),
            ('u1', TIMESTAMPTZ '2025-01-03 00:00:00+00', 'revenue', 10::BIGINT),
            ('u1', TIMESTAMPTZ '2025-01-10 00:00:00+00', 'revenue', 5::BIGINT),
            ('u1', TIMESTAMPTZ '2025-01-11 00:00:00+00', 'churn', 0::BIGINT),
            ('u2', TIMESTAMPTZ '2025-01-01 00:00:00+00', 'purchase', 0::BIGINT),
            ('u3', TIMESTAMPTZ '2025-01-15 00:00:00+00', 'purchase', 0::BIGINT),
            ('u3', TIMESTAMPTZ '2025-01-18 00:00:00+00', 'churn', 0::BIGINT)
        ) AS e(user_id, ts, kind, amount)
    """)
    con.raw_sql("CREATE TABLE churn_events AS SELECT user_id, ts FROM events WHERE kind = 'churn'")
    con.raw_sql("CREATE TABLE users AS SELECT * FROM (VALUES ('u1'), ('u2'), ('u3')) AS u(id)")
    catalog = {
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
            },
            "churn_events": {
                "relation": {"kind": "table", "name": "churn_events"},
                "timestamp": "ts",
                "entities": {"user": "user_id"},
                "facts": ["churn"],
                "properties": [],
                "dimensions": [],
            },
        },
        "facts": {
            "purchase": {"source": "events", "column": None},
            "revenue": {"source": "events", "column": "amount"},
            "churn": {"source": "churn_events", "column": None},
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
            "net_revenue": {
                "kind": "mean",
                "entity": "user",
                "measure": "net_revenue_sum",
            }
        },
    }
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "id")},
        readiness={
            "events": Coverage("2025-01-01", "2025-01-31"),
            "churn_events": Coverage("2025-01-01", "2025-01-31"),
        },
    )
    return con, model


def main() -> None:
    con, model = _model()
    try:
        design = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                "purchase", Trajectory("net_revenue", "7d", periods=3, cumulative=True)
            ),
            administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
        )
        cohort = design.build(model).cohorts(
            grouping=CohortGrouping("7d", datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"),
            value_column="cumulative_value",
        )
        budget = Budget(max_rows=100, max_output_bytes=1_000_000)
        cohort_arrow = cohort.to_pyarrow(budget=budget)
        print("Cohort rows:", cohort_arrow.to_pylist())
        readiness = model.readiness
        later_design = Design(
            "user",
            design.anchor,
            shape=design.shape,
            administrative_cutoff=datetime(2025, 1, 25, tzinfo=UTC),
        )
        later_rows = later_design.build(model).to_pyarrow(budget=budget).to_pylist()
        print(
            "Later cutoff completeness (readiness unchanged):",
            readiness == model.readiness,
            sum(not row["cumulative_value__complete"] for row in later_rows),
        )

        life = (
            Design(
                "user",
                EventAnchor("purchase", which="first"),
                shape=LifeTable(TimeToEvent("churn"), "7d"),
                administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
            )
            .build(model)
            .materialize(budget=budget)
        )
        print("Risk-set table rows:", life.table.to_pylist())
        surrogate = to_surrogate_arrow(life, features=("time_bin",), outcomes=("at_risk", "events"))
        restored = type(surrogate).from_arrow(surrogate.to_arrow())
        print("Surrogate metadata:", dict(restored.metadata))
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
