from datetime import UTC, datetime, timedelta
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import pyarrow as pa
import pytest

from terrine.backends.dialect import current_dialect, dialect_scope
from terrine.backends.timestamps import (
    compare_timestamps,
    lookup_keys,
    lossless_scale,
    timestamp_parts,
    timestamp_text,
    utc_instant,
    utc_wall_clock,
)
from terrine.errors import TerrineError


@pytest.fixture
def connection():
    con = ibis.duckdb.connect()
    try:
        yield con
    finally:
        con.disconnect()


def test_explicit_timestamp_backend_overrides_literal_default():
    dtype = dt.Timestamp(scale=9)
    value = cast(ir.TimestampValue, ibis.literal("2025-01-01 00:00:00", type=dtype))
    with dialect_scope("snowflake"), pytest.raises(TerrineError) as caught:
        timestamp_parts(value, backend="postgres")
    assert caught.value.code == "terrine.temporal.timestamp_order_unsupported"
    assert dict(caught.value.context) == {"dtype": str(dtype)}


def test_explicit_timestamp_backend_preserves_nested_parts_and_scope(connection):
    baseline = current_dialect().name
    value = cast(ir.TimestampValue, ibis.literal("infinity").cast(dt.Timestamp(scale=6)))
    with dialect_scope("snowflake"):
        rank, seconds, nanos = timestamp_parts(value, backend="duckdb")
        assert current_dialect().name == "snowflake"
        query = (
            ibis.struct({"rank": rank, "seconds": seconds, "nanos": nanos}).name("parts").as_table()
        )
        actual = connection.to_pyarrow(query)["parts"][0].as_py()
        assert (actual["rank"], actual["seconds"], actual["nanos"]) == (1, 0, 0)
        assert current_dialect().name == "snowflake"
    assert current_dialect().name == baseline


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


def test_portable_policy_conversions_and_parts_ignore_the_session_time_zone():
    """Typed epoch deltas rebuild instants and wall clocks across a DST boundary."""
    con, t = _new_york_session()
    con.raw_sql(
        "INSERT INTO t VALUES (TIMESTAMPTZ '2025-07-01 12:00:00.123456+00', "
        "TIMESTAMP '2025-07-01 12:00:00.123456'), "
        "(TIMESTAMPTZ '1969-12-31 23:59:59.999999+00', TIMESTAMP '1969-12-31 23:59:59.999999')"
    )
    with dialect_scope("generic"):
        rank, seconds, nanos = timestamp_parts(t.aware)
        rows = con.to_pyarrow(
            t.select(
                wall=utc_wall_clock(t.aware),
                instant=utc_instant(t.naive),
                text=timestamp_text(t.aware),
                rank=rank,
                seconds=seconds,
                nanos=nanos,
            ).order_by("seconds")
        ).to_pylist()
    assert [(row["rank"], row["seconds"], row["nanos"]) for row in rows] == [
        (0, 0, -1000),
        (0, 1_741_501_800, 0),
        (0, 1_751_371_200, 123_456_000),
    ]
    for row in rows:
        assert row["instant"].replace(tzinfo=None) == row["wall"]
        assert row["text"] == str(row["wall"])


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


def test_duckdb_lookup_keys_are_native_for_microsecond_pairs_and_structs_for_nanoseconds(
    connection,
):
    t = ibis.table({"aware": "timestamp('UTC')", "naive": "timestamp", "ns": "timestamp(9)"})
    bound = connection.sql(
        "SELECT TIMESTAMPTZ '2025-01-01 12:00:00+00' AS aware, "
        "TIMESTAMP '2025-01-01 12:00:00' AS naive"
    )
    with dialect_scope("duckdb"):
        point, state = lookup_keys(t.aware, t.aware)
        assert point.equals(t.aware) and state.equals(t.aware)
        point, state = lookup_keys(t.aware, t.naive)
        assert state.type() == dt.Timestamp(timezone="UTC") and point.equals(t.aware)
        point, state = lookup_keys(t.naive, t.aware)
        assert point.type() == dt.Timestamp(timezone="UTC") and state.equals(t.aware)
        point, state = lookup_keys(bound.aware, bound.naive)
        assert point.type() == state.type() == dt.Timestamp(timezone="UTC", scale=6)
        point, state = lookup_keys(t.aware, t.ns)
        assert point.type().is_struct() and state.type().is_struct()
        point, state = lookup_keys(t.ns, t.ns)
        assert point.type().is_struct() and state.type().is_struct()
        with pytest.raises(TerrineError) as caught:
            lookup_keys(t.aware, t.aware.cast(dt.Timestamp(scale=3)))
        assert caught.value.code == "terrine.temporal.timestamp_order_unsupported"
    with dialect_scope("postgres"):
        point, state = lookup_keys(t.aware, t.naive)
        assert point.type() == state.type() == dt.Timestamp(scale=6)


def test_duckdb_microsecond_comparisons_order_infinities_nulls_and_naive_utc_clocks():
    """Aware against naive instants under a non-UTC session, against their rank order."""
    con, _ = _new_york_session()
    texts = [
        "-infinity",
        "1969-12-31 23:59:59.999999",
        "1970-01-01 00:00:00",
        "2025-03-09 06:30:00",
        "2025-03-09 06:30:00.000001",
        "infinity",
        None,
    ]
    values = ", ".join(
        f"({index}, {'NULL' if text is None else repr(text)})" for index, text in enumerate(texts)
    )
    ops = ("lt", "le", "gt", "ge", "eq")
    try:
        con.raw_sql(
            "CREATE TABLE v AS SELECT i, timezone('UTC', CAST(text AS TIMESTAMP)) AS aware, "
            f"CAST(text AS TIMESTAMP) AS naive FROM (VALUES {values}) AS v(i, text)"
        )
        left, right = con.table("v").view(), con.table("v").view()
        pairs = left.cross_join(right, lname="left_{name}", rname="right_{name}")
        rows = con.to_pyarrow(
            pairs.select(
                "left_i",
                "right_i",
                **{op: compare_timestamps(pairs.left_aware, pairs.right_naive, op) for op in ops},
            )
        ).to_pylist()
    finally:
        con.disconnect()
    missing = len(texts) - 1
    assert len(rows) == len(texts) ** 2
    for row in rows:
        a, b = row["left_i"], row["right_i"]
        expected = {"lt": a < b, "le": a <= b, "gt": a > b, "ge": a >= b, "eq": a == b}
        if missing in (a, b):
            expected = dict.fromkeys(ops)
        assert {op: row[op] for op in ops} == expected, (a, b)


def test_duckdb_mixed_utc_comparisons_preserve_finite_timestamp_endpoints():
    """Naive UTC clocks keep their full domain when compared to aware instants."""
    anchor = datetime(2025, 1, 2, tzinfo=UTC)
    anchor_us = (anchor - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)
    coordinates = [
        -9_223_372_022_400_000_000,
        -1,
        0,
        1,
        anchor_us - 1,
        anchor_us,
        anchor_us + 1,
        2**63 - 2,
        None,
    ]
    con, _ = _new_york_session()
    operations = ("lt", "le", "gt", "ge", "eq")
    try:
        table = con.create_table(
            "finite_endpoints",
            pa.table(
                {
                    "i": list(range(len(coordinates))),
                    "at": pa.array(coordinates, pa.int64()).cast(pa.timestamp("us")),
                }
            ),
        )
        point = cast(ir.TimestampValue, ibis.literal(anchor))
        rows = con.to_pyarrow(
            table.select(
                "i",
                **{op: compare_timestamps(table.at, point, op) for op in operations},
                **{f"reverse_{op}": compare_timestamps(point, table.at, op) for op in operations},
            )
        ).to_pylist()
    finally:
        con.disconnect()
    assert len(rows) == len(coordinates)
    for row in rows:
        coordinate = coordinates[row["i"]]
        for prefix, left, right in (
            ("", coordinate, anchor_us),
            ("reverse_", anchor_us, coordinate),
        ):
            expected = (
                dict.fromkeys(operations)
                if left is None or right is None
                else {
                    "lt": left < right,
                    "le": left <= right,
                    "gt": left > right,
                    "ge": left >= right,
                    "eq": left == right,
                }
            )
            assert {op: row[prefix + op] for op in operations} == expected


@pytest.mark.parametrize(
    ("scale", "expected"),
    [(None, None), (0, 6), (3, 6), (6, 6), (7, 9), (9, 9)],
)
def test_lossless_scale_promotes_to_the_nearest_supported_precision(scale, expected):
    assert lossless_scale(scale) == expected
