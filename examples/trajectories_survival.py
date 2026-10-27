"""Local DuckDB walkthrough for cohort datasets and portable Arrow export."""

from datetime import UTC, datetime, timedelta

import ibis

from terrine import Budget, Coverage, Model, UnitSource, asof, recipes
from terrine.export import to_surrogate_arrow
from terrine.spec import CohortGrouping


def _model():
    con = ibis.duckdb.connect()
    con.raw_sql("""
        CREATE TABLE events AS SELECT * FROM (VALUES
            ('u1', TIMESTAMPTZ '2025-01-01 00:00:00+00', 'purchase', 7::BIGINT),
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
    con.raw_sql("""
        CREATE TABLE profiles AS SELECT * FROM (VALUES
            ('u1', TIMESTAMPTZ '2024-12-30 00:00:00+00', 'north'),
            ('u2', TIMESTAMPTZ '2024-12-30 00:00:00+00', 'south'),
            ('u3', TIMESTAMPTZ '2024-12-30 00:00:00+00', 'north')
        ) AS p(user_id, ts, region)
    """)
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
            "profiles": {
                "relation": {"kind": "table", "name": "profiles"},
                "timestamp": "ts",
                "entities": {"user": "user_id"},
                "facts": [],
                "properties": ["region"],
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
            },
            "region": {
                "source": "profiles",
                "column": "region",
                "dtype": "string",
                "as_of": "event_time",
            },
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
            "profiles": Coverage("2024-12-30", "2025-01-31"),
        },
    )
    return con, model


def main() -> None:
    con, model = _model()
    try:
        design = recipes.ltv(
            "user", start_event="purchase", metric="net_revenue", period="7d", periods=3,
            administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
        )
        budget = Budget(max_rows=100, max_output_bytes=1_000_000)
        ltv_rows = design.build(model).materialize(budget).table.to_pylist()
        u1 = {row["age"]: row for row in ltv_rows if row["unit"] == "u1"}
        assert u1[0]["cumulative_value"] == 17  # Origin amount 7 plus later amount 10.
        assert u1[1]["cumulative_value"] == 22
        immature = next(row for row in ltv_rows if row["unit"] == "u3" and row["age"] == 1)
        assert immature["cumulative_value"] is None
        assert immature["cumulative_value__reason"] == "administrative_cutoff"
        print("LTV origin-inclusive values:", [u1[age]["cumulative_value"] for age in (0, 1)])
        cohort = design.build(model).cohorts(
            grouping=CohortGrouping("7d", datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"),
            value_column="cumulative_value",
        )
        cohort_arrow = cohort.to_pyarrow(budget=budget)
        print("Cohort rows:", cohort_arrow.to_pylist())
        readiness = model.readiness
        later_design = recipes.ltv(
            "user", start_event="purchase", metric="net_revenue", period="7d", periods=3,
            administrative_cutoff=datetime(2025, 1, 29, tzinfo=UTC),
        )
        later_rows = later_design.build(model).to_pyarrow(budget=budget).to_pylist()
        assert readiness == model.readiness
        matured = next(row for row in later_rows if row["unit"] == "u3" and row["age"] == 1)
        assert matured["cumulative_value"] == 0
        assert matured["cumulative_value__complete"]
        print(
            "Later cutoff completeness (readiness unchanged):",
            readiness == model.readiness,
            sum(not row["cumulative_value__complete"] for row in later_rows),
        )

        survival = recipes.survival(
            "user", start_event="purchase", end_event="churn",
            administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
        ).build(model).materialize(budget)
        records = {row["unit"]: row for row in survival.table.to_pylist()}
        assert records["u1"]["event"] is True
        assert records["u1"]["duration"] == timedelta(days=10)
        assert records["u2"]["event"] is False
        assert records["u2"]["duration"] == timedelta(days=21)
        print("Survival durations:", {
            unit: row["duration"] for unit, row in records.items()
        })

        life = recipes.life_table(
            "user", start_event="purchase", end_event="churn", bin="7d",
            features={"region": asof("region", before="-1d")},
            group_by=("region",),
            administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
        ).build(model).materialize(budget)
        assert set(life.table["region"].to_pylist()) == {"north", "south"}
        assert life.roles.features == ("region", "time_bin")
        assert life.roles.outcomes == (
            "at_risk", "events", "censored", "delayed_entries", "event_rate", "censor_rate",
        )
        assert life.roles.treatment is None
        print("Risk-set table rows:", life.table.to_pylist())
        surrogate = to_surrogate_arrow(life, features=("time_bin",), outcomes=("at_risk", "events"))
        restored = type(surrogate).from_arrow(surrogate.to_arrow())
        print("Surrogate metadata:", dict(restored.metadata))
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
