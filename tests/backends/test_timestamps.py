from datetime import UTC, datetime

import ibis

from terrine.backends.timestamps import timestamp_parts, utc_instant, utc_wall_clock


def _new_york_session():
    con = ibis.duckdb.connect()
    con.raw_sql("SET TimeZone = 'America/New_York'")
    con.raw_sql(
        "CREATE TABLE t AS SELECT TIMESTAMPTZ '2025-03-09 06:30:00+00' AS aware, "
        "TIMESTAMP '2025-03-09 06:30:00' AS naive"
    )
    return con, con.table("t")


def test_utc_conversions_ignore_the_session_time_zone():
    con, t = _new_york_session()
    row = con.to_pyarrow(
        t.select(wall=utc_wall_clock(t.aware), instant=utc_instant(t.naive))
    ).to_pylist()[0]
    assert row["wall"] == datetime(2025, 3, 9, 6, 30)
    assert row["instant"] == datetime(2025, 3, 9, 6, 30, tzinfo=UTC)


def test_aware_timestamp_parts_ignore_the_session_time_zone():
    con, t = _new_york_session()
    rank, seconds, nanos = timestamp_parts(t.aware)
    row = con.to_pyarrow(t.select(rank=rank, seconds=seconds, nanos=nanos)).to_pylist()[0]
    assert row == {"rank": 0, "seconds": 1_741_501_800, "nanos": 0}
