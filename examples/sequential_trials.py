"""Build schedule and event-time panel frames over a tiny in-memory DuckDB catalog."""

from datetime import UTC, date, datetime, timedelta

import ibis
import pyarrow as pa

from terrine import Budget, Coverage, Model, UnitSource, recipes
from terrine.data import MaterializedFrame
from terrine.spec import (
    Design,
    EventAnchor,
    EventPanel,
    Initiation,
    Schedule,
    Sequential,
    from_json,
    to_json,
)


def main() -> None:
    con = ibis.duckdb.connect()
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["a", "a", "b", "b"],
                "event_time": [
                    datetime(2025, 1, 2, tzinfo=UTC),
                    datetime(2025, 1, 5, tzinfo=UTC),
                    datetime(2025, 1, 3, tzinfo=UTC),
                    datetime(2025, 1, 6, tzinfo=UTC),
                ],
                "event_id": ["a1", "a2", "b1", "b2"],
            }
        ),
    )
    con.create_table("users", pa.table({"user_id": ["a", "b", "never_started"]}))
    con.create_table(
        "assignments",
        pa.table({"user_id": ["a"], "ts": [datetime(2025, 1, 1, tzinfo=UTC)]}),
    )
    catalog = {
            "schema_version": 2,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "event_time",
                    "units": {"user": "user_id"},
                    "events": ["purchase"],
                    "properties": [],
                    "dimensions": [],
                },
                "assignments": {
                    "relation": {"kind": "table", "name": "assignments"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["started"],
                    "properties": [],
                    "dimensions": [],
                },
            },
            "events": {
                "purchase": {"source": "events", "column": None},
                "started": {"source": "assignments", "column": None},
            },
            "properties": {},
            "dimensions": {},
            "measures": {},
            "metrics": {},
        }
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={
            "events": Coverage("2025-01-01", "2025-02-01"),
            "assignments": Coverage("2025-01-01", "2025-02-01"),
        },
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    try:
        treatment = Initiation(
            "assignments", "started", history_start=datetime(2025, 1, 1, tzinfo=UTC)
        )
        sequential = recipes.trial_panel(
            "user", every="14d", start="2025-01-01", end="2025-02-01",
            treatment=treatment,
        )
        events = recipes.event_panel(
            "user", event="purchase", periods=(-1, 0, 1), period="1d",
        )
        assert sequential == Sequential(
            Design("user", Schedule("14d", "2025-01-01", "2025-02-01")),
            treatment=treatment,
        )
        assert events == EventPanel(
            Design("user", EventAnchor("purchase")),
            periods=(-1, 0, 1), period_seconds=86_400,
        )
        assert from_json(to_json(sequential)) == sequential

        budget = Budget(max_rows=20, max_output_bytes=100_000)
        for design in (sequential, events):
            frame = design.build(model).materialize(budget)
            restored = MaterializedFrame.from_arrow(frame.to_arrow())
            assert restored.table.equals(frame.table)
            rows = restored.table.to_pylist()
            if design == sequential:
                trial_dates = sorted({row["t0"].date() for row in rows})
                assert trial_dates == [date(2025, 1, 1), date(2025, 1, 15), date(2025, 1, 29)]
                assert restored.roles.treatment == "treatment"
                assert any(row["treatment"] for row in rows)
                assert any(not row["treatment"] for row in rows)
                print("Scheduled trial dates:", trial_dates)
            else:
                assert {row["event_period"] for row in rows} == {-1, 0, 1}
                origins = {"a": datetime(2025, 1, 2, tzinfo=UTC),
                           "b": datetime(2025, 1, 3, tzinfo=UTC)}
                assert all(
                    row["t0"] == origins[row["unit"]] + timedelta(days=row["event_period"])
                    for row in rows
                )
            print(design.shape, restored.table.num_rows, restored.provenance.shape)
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
