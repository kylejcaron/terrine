"""Build bounded event-position and Arrow-array datasets against local DuckDB."""

from datetime import UTC, datetime

import ibis
import pyarrow as pa

import terrine as tr
from terrine import recipes
from terrine.spec import Design, EventAnchor, EventSequence, FunnelSpec, SessionSpec
from terrine.windows import Window


def main() -> None:
    con = ibis.duckdb.connect()
    try:
        con.create_table("users", pa.table({"user_id": ["u1", "u2", "u3"]}))
        con.create_table(
            "limited_users",
            pa.table(
                {
                    "user_id": ["u1", "u2", "u3"],
                    "observed_until": pa.array(
                        [
                            datetime(2025, 3, 2, tzinfo=UTC),
                            datetime(2025, 3, 1, 12, 30, tzinfo=UTC),
                            datetime(2025, 3, 1, 12, tzinfo=UTC),
                        ],
                        type=pa.timestamp("us", tz="UTC"),
                    ),
                }
            ),
        )
        con.create_table(
            "events",
            pa.table(
                {
                    "user_id": ["u1", "u1", "u1", "u1", "u2", "u2", "u3"],
                    "ts": pa.array(
                        [
                            datetime(2025, 3, 1, 12, tzinfo=UTC),
                            datetime(2025, 3, 1, 12, tzinfo=UTC),
                            datetime(2025, 3, 1, 12, tzinfo=UTC),
                            datetime(2025, 3, 1, 13, tzinfo=UTC),
                            datetime(2025, 3, 1, 12, tzinfo=UTC),
                            datetime(2025, 3, 1, 12, tzinfo=UTC),
                            datetime(2025, 3, 1, 9, tzinfo=UTC),
                        ],
                        type=pa.timestamp("us", tz="UTC"),
                    ),
                    "event_id": [
                        "signup-u1",
                        "z-view",
                        "a-view",
                        "purchase",
                        "signup-u2",
                        "null-label",
                        "signup-u3",
                    ],
                    "activity": [None, "event", "event", "event", None, "event", None],
                    "signup": ["start", None, None, None, "start", None, "start"],
                    "kind": [None, "view", "view", "purchase", None, None, None],
                }
            ),
        )
        catalog = {
                "schema_version": 1,
                "day_boundary": "UTC",
                "dialect": "duckdb",
                "sources": {
                    "events": {
                        "relation": {"kind": "table", "name": "events"},
                        "timestamp": "ts",
                        "entities": {"user": "user_id"},
                        "facts": ["activity", "signup"],
                        "properties": ["kind"],
                        "dimensions": [],
                    }
                },
                "facts": {
                    "activity": {"source": "events", "column": "activity"},
                    "signup": {"source": "events", "column": "signup"},
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
                "measures": {},
                "metrics": {},
            }
        model = tr.Model.from_catalog(
            catalog,
            con=con,
            units={"user": tr.UnitSource("users", "user_id")},
            event_ids={"events": "event_id"},
            readiness={"events": tr.Coverage("2025-03-01", "2025-03-02")},
        )
        budget = tr.Budget(max_rows=100, max_output_bytes=1_000_000)
        sequence = recipes.event_sequence(
            "user", event="activity", horizon="2h", max_events=4, session_gap="30m",
        ).build(model).materialize(budget)
        recipe_rows = sequence.table.to_pylist()
        headers = {row["unit"]: row for row in recipe_rows if not row["is_event"]}
        assert {unit: row["event_count"] for unit, row in headers.items()} == {
            "u1": 3, "u2": 1, "u3": 0,
        }
        assert headers["u3"]["event_count__complete"]
        u1_events = sorted(
            (row for row in recipe_rows if row["is_event"] and row["unit"] == "u1"),
            key=lambda row: (row["event_time"], row["event_id"]),
        )
        assert [row["event_id"] for row in u1_events] == ["a-view", "z-view", "purchase"]
        assert [row["session_index"] for row in u1_events] == [0, 0, 1]
        print("Event-sequence recipe counts:", {
            unit: row["event_count"] for unit, row in headers.items()
        })

        # Use the low-level shape for different anchor/event facts, properties,
        # funnels, or an array representation.
        design = Design(
            "user",
            EventAnchor("signup", "first"),
            shape=EventSequence(
                "activity",
                Window("0s", "2h"),
                max_events=4,
                properties=("kind",),
                sessions=SessionSpec("1h"),
                funnel=FunnelSpec("kind", ("view", "purchase"), within="2h"),
            ),
        )
        positions = design.build(model).materialize(budget)
        children = [row for row in positions.table.to_pylist() if row["is_event"]]
        assert [row["event_id"] for row in children if row["unit"] == "u1"] == [
            "a-view",
            "z-view",
            "purchase",
        ]
        empty_header = next(
            row
            for row in positions.table.to_pylist()
            if row["unit"] == "u3" and not row["is_event"]
        )
        assert empty_header["event_count"] == 0 and empty_header["event_count__complete"]
        null_child = next(row for row in children if row["unit"] == "u2")
        assert null_child["kind"] is None and null_child["kind__reason"] == "known_null"
        reopened = type(positions).from_arrow(positions.to_arrow())
        assert reopened.provenance.sequence_meta == positions.provenance.sequence_meta

        limited_model = tr.Model.from_catalog(
            catalog,
            con=con,
            units={
                "user": tr.UnitSource("limited_users", "user_id", observed_until="observed_until")
            },
            event_ids={"events": "event_id"},
            readiness={"events": tr.Coverage("2025-03-01", "2025-03-02")},
        )
        limited = design.build(limited_model).materialize(budget)
        limited_header = next(
            row for row in limited.table.to_pylist() if row["unit"] == "u2" and not row["is_event"]
        )
        assert (
            limited_header["event_count"] is None
            and not limited_header["event_count__complete"]
            and limited_header["event_count__reason"] == "unit_observation"
        )

        arrays_design = Design(
            "user",
            EventAnchor("signup", "first"),
            shape=EventSequence(
                "activity", Window("0s", "2h"), max_events=4, representation="arrays"
            ),
        )
        arrays = arrays_design.build(model).materialize(budget)
        assert arrays.table.num_rows == 3
        print(positions.table.to_pylist())
        print(arrays.table.to_pylist())
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
