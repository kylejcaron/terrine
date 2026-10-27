import ibis
import pytest

from terrine.backends.timestamps import (
    lossless_scale,
    timestamp_parts,
    timestamp_text,
    utc_wall_clock,
)


def _new_york_session():
    con = ibis.duckdb.connect()
    con.raw_sql("SET TimeZone = 'America/New_York'")
    con.raw_sql(
        "CREATE TABLE t AS SELECT TIMESTAMPTZ '2025-03-09 06:30:00+00' AS aware, "
        "TIMESTAMP '2025-03-09 06:30:00' AS naive"
    )
    return con, con.table("t")


def test_aware_timestamp_parts_ignore_the_session_time_zone():
    con, t = _new_york_session()
    rank, seconds, nanos = timestamp_parts(t.aware)
    row = con.to_pyarrow(t.select(rank=rank, seconds=seconds, nanos=nanos)).to_pylist()[0]
    assert row == {"rank": 0, "seconds": 1_741_501_800, "nanos": 0}


def test_timestamp_text_keeps_microsecond_distinct_instants_apart():
    con = ibis.duckdb.connect()
    con.raw_sql(
        "CREATE TABLE t AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2025-01-01 12:00:00.000001', TIMESTAMPTZ '2025-01-01 12:00:00.000001+00'), "
        "(TIMESTAMP '2025-01-01 12:00:00.000002', TIMESTAMPTZ '2025-01-01 12:00:00.000002+00')"
        ") AS t(naive, aware)"
    )
    t = con.table("t")
    rows = con.to_pyarrow(
        t.select(naive=timestamp_text(t.naive), aware=timestamp_text(utc_wall_clock(t.aware)))
    ).to_pylist()
    assert len({row["naive"] for row in rows}) == 2
    assert [row["naive"] for row in rows] == [row["aware"] for row in rows]


@pytest.mark.parametrize(
    ("scale", "expected"),
    [(None, None), (0, 6), (3, 6), (6, 6), (7, 9), (9, 9)],
)
def test_lossless_scale_promotes_to_the_nearest_supported_precision(scale, expected):
    assert lossless_scale(scale) == expected
