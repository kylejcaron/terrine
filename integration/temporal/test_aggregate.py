from datetime import UTC, date, datetime

import ibis
import pyarrow as pa

import terrine as tr
from terrine.data import Events, UnitSource
from terrine.model import Model
from terrine.spec import Design, FixedDate, agg


def test_count_distinct_strings_matches_hand_query():
    con = ibis.duckdb.connect()
    con.create_table("users", pa.table({"user_id": ["alice", "bob", "carol"]}))
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["alice", "alice", "alice"],
                "occurred_at": pa.array(
                    [
                        datetime(2025, 1, 2, 10, tzinfo=UTC),
                        datetime(2025, 1, 2, 11, tzinfo=UTC),
                        datetime(2025, 1, 2, 12, tzinfo=UTC),
                    ],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "sku": ["a", "b", "a"],
                "day": pa.array(
                    [date(2025, 1, 2), date(2025, 1, 2), date(2025, 1, 2)],
                    type=pa.date32(),
                ),
                "active": [True, False, True],
            }
        ),
    )
    model = Model.from_tables(
        con,
        units={"user": UnitSource("users", "user_id")},
        events={
            "events": Events(
                "events",
                time="occurred_at",
                key="user_id",
                complete=("2025-01-01", "2025-01-02"),
                metrics={
                    "skus": tr.count_distinct("sku"),
                    "days": tr.count_distinct("day"),
                    "active_flags": tr.count_distinct("active"),
                },
            )
        },
    )
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 3, tzinfo=UTC)),
        features={
            name: agg(name, window=("-1d", "0d"))
            for name in ("skus", "days", "active_flags")
        },
    )
    actual = design.build(model).materialize().to_pyarrow().sort_by("unit").to_pylist()
    expected = con.raw_sql(
        "SELECT count(DISTINCT sku), count(DISTINCT day), count(DISTINCT active) "
        "FROM events WHERE occurred_at >= TIMESTAMPTZ '2025-01-02 00:00:00+00' "
        "AND occurred_at < TIMESTAMPTZ '2025-01-03 00:00:00+00'"
    ).fetchone()
    assert [
        (row["unit"], row["skus"], row["days"], row["active_flags"]) for row in actual
    ] == [
        ("alice", 2, 1, 2),
        ("bob", 0, 0, 0),
        ("carol", 0, 0, 0),
    ]
    assert expected == (2, 1, 2)
    con.disconnect()
