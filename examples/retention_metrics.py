"""Build exact named retention outcomes from independent occurrence identities."""

from datetime import UTC, datetime

import ibis
import pyarrow as pa

from terrine import Coverage, Design, FixedDate, Model, UnitSource, retention


def main() -> None:
    con = ibis.duckdb.connect()
    try:
        con.create_table(
            "events",
            pa.table(
                {
                    "user_id": ["u-1", "u-1", "u-1"],
                    "occurrence_id": ["purchase-1", "purchase-1", "purchase-2"],
                    "name": ["purchase"] * 3,
                    "occurred_at": [datetime(2025, 3, day, tzinfo=UTC) for day in (1, 2, 3)],
                }
            ),
        )
        con.create_table(
            "users",
            pa.table(
                {
                    "id": ["u-1", "u-2", "u-3"],
                    "observed_until": [datetime(2025, 3, day, tzinfo=UTC) for day in (4, 2, 4)],
                }
            ),
        )
        catalog = {
                "schema_version": 2,
                "day_boundary": "UTC",
                "dialect": "duckdb",
                "sources": {
                    "events": {
                        "relation": {"kind": "table", "name": "events"},
                        "time": "occurred_at",
                        "units": {"user": "user_id"},
                        "events": ["purchase_occurrence"],
                        "properties": ["event_name"],
                        "dimensions": [],
                    }
                },
                "events": {"purchase_occurrence": {"source": "events", "column": "occurrence_id"}},
                "properties": {
                    "event_name": {
                        "source": "events",
                        "column": "name",
                        "dtype": "string",
                        "as_of": "event_time",
                    }
                },
                "dimensions": {},
                "measures": {},
                "metrics": {},
            }
        model = Model.from_catalog(
            catalog,
            con=con,
            units={"user": UnitSource("users", "id", observed_until="observed_until")},
            readiness={"events": Coverage("2025-03-01", "2025-03-03")},
        )
        design = Design(
            "user",
            FixedDate("2025-03-01"),
            outcomes={
                "retained_two_days": retention(
                    "purchase_occurrence",
                    label_property="event_name",
                    target_event="purchase",
                    thresholds=(1, 2),
                    horizon="2d",
                ),
                "retained_open": retention(
                    "purchase_occurrence",
                    label_property="event_name",
                    target_event="purchase",
                    thresholds=(1, 2),
                    horizon="open_ended",
                ),
            },
        )
        result = design.build(model).materialize()
        rows = result.to_arrow().sort_by("unit").to_pylist()
        assert [(row["retained_two_days"], row["retained_open"]) for row in rows] == [
            (1, 2),
            (None, None),
            (0, 0),
        ]
        print([{name: row[name] for name in ("unit", *design.outcomes)} for row in rows])
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
