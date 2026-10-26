"""Build schedule and event-time panel frames over a tiny in-memory DuckDB catalog."""

from datetime import UTC, datetime

import ibis
import pyarrow as pa

from terrine import Budget, Coverage, Model, UnitSource
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
    con.create_table("users", pa.table({"user_id": ["a", "b"]}))
    catalog = {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "event_time",
                    "entities": {"user": "user_id"},
                    "facts": ["purchase"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {"purchase": {"source": "events", "column": None}},
            "properties": {},
            "dimensions": {},
            "measures": {},
            "metrics": {},
        }
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    try:
        treatment = Initiation("events", "purchase", history_start=datetime(2025, 1, 1, tzinfo=UTC))
        schedule_base = Design("user", Schedule("1d", "2025-01-03", "2025-01-04"))
        sequential = Sequential(schedule_base, treatment=treatment)
        event_panel = EventPanel(
            Design("user", EventAnchor("purchase", "first")),
            periods=(-1, 0, 1),
            period_seconds=86_400,
            reference_period=-1,
        )
        assert (
            Sequential(
                Design("user", Schedule("1d", "2025-01-03", "2025-01-04")),
                treatment=treatment,
            )
            == sequential
        )
        assert from_json(to_json(sequential)) == sequential

        budget = Budget(max_rows=20, max_output_bytes=100_000)
        for design in (sequential, event_panel):
            frame = design.build(model).materialize(budget)
            restored = MaterializedFrame.from_arrow(frame.to_arrow())
            assert restored.table.equals(frame.table)
            print(design.shape, restored.table.num_rows, restored.provenance.shape)
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
