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


def test_duckdb_microsecond_comparisons_order_shifted_infinities_and_naive_utc_clocks():
    """Mixed clocks and virtual endpoints follow independent integer/rank order."""
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
    offsets = (0, -1, 1, -(2**63), 2**63 - 1)
    epoch = datetime(1970, 1, 1)
    coordinates = [
        None
        if text is None
        else (-1, 0)
        if text == "-infinity"
        else (1, 0)
        if text == "infinity"
        else (0, (datetime.fromisoformat(text) - epoch) // timedelta(microseconds=1) * 1000)
        for text in texts
    ]
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
                **{
                    f"{op}_{index}": compare_timestamps(
                        pairs.left_aware, pairs.right_naive, op, right_offset_seconds=offset
                    )
                    for index, offset in enumerate(offsets)
                    for op in ops
                },
            )
        ).to_pylist()
    finally:
        con.disconnect()
    assert len(rows) == len(texts) ** 2
    for row in rows:
        left, right = coordinates[row["left_i"]], coordinates[row["right_i"]]
        for index, offset in enumerate(offsets):
            if left is None or right is None:
                expected = dict.fromkeys(ops)
            else:
                rank, nanos = right
                shifted = (rank, nanos + offset * 10**9 if rank == 0 else nanos)
                expected = {
                    "lt": left < shifted,
                    "le": left <= shifted,
                    "gt": left > shifted,
                    "ge": left >= shifted,
                    "eq": left == shifted,
                }
            assert {op: row[f"{op}_{index}"] for op in ops} == expected, (left, right, offset)


def test_duckdb_mixed_utc_comparisons_preserve_finite_timestamp_endpoints():
    """Naive UTC clocks keep their full domain when compared to aware instants."""
    anchor = datetime(2025, 1, 2, tzinfo=UTC)
    anchor_us = (anchor - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)
    minimum, maximum = -9_223_372_022_400_000_000, 2**63 - 2
    coordinates = [
        minimum,
        -1,
        0,
        1,
        anchor_us - 1,
        anchor_us,
        anchor_us + 1,
        maximum,
        None,
    ]
    pairs: list[tuple[int | None, int | None]] = [(value, anchor_us) for value in coordinates]
    pairs.extend(
        (edge + left, edge + right)
        for edge, step in ((minimum, 1), (maximum, -1))
        for left, right in ((0, 0), (0, step), (step, step))
    )
    pairs.append((0, None))

    @ibis.udf.scalar.builtin(
        name="make_timestamptz", signature=((dt.int64,), dt.Timestamp(timezone="UTC", scale=6))
    )
    def aware_from_microseconds(value):
        """Construct an aware instant directly from native microsecond ticks."""

    @ibis.udf.scalar.builtin(
        name="epoch_us", signature=((dt.Timestamp(timezone="UTC", scale=6),), dt.int64)
    )
    def epoch_microseconds(value):
        """Read native ticks without formatting an out-of-Python-range datetime."""

    con, _ = _new_york_session()
    operations = ("lt", "le", "gt", "ge", "eq")
    try:
        table = con.create_table(
            "finite_endpoints",
            pa.table(
                {
                    "i": list(range(len(pairs))),
                    "at": pa.array([left for left, _ in pairs], pa.int64()).cast(
                        pa.timestamp("us")
                    ),
                    "aware_us": pa.array([right for _, right in pairs], pa.int64()),
                }
            ),
        )
        point = cast(ir.TimestampValue, aware_from_microseconds(table.aware_us))
        rows = con.to_pyarrow(
            table.select(
                "i",
                aware_ticks=epoch_microseconds(point),
                **{op: compare_timestamps(table.at, point, op) for op in operations},
                **{f"reverse_{op}": compare_timestamps(point, table.at, op) for op in operations},
            )
        ).to_pylist()
    finally:
        con.disconnect()
    assert len(rows) == len(pairs)
    for row in rows:
        coordinate, point_us = pairs[row["i"]]
        assert row["aware_ticks"] == point_us
        for prefix, left, right in (
            ("", coordinate, point_us),
            ("reverse_", point_us, coordinate),
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
            assert {op: row[prefix + op] for op in operations} == expected, (prefix, left, right)


def test_duckdb_native_elapsed_shifts_preserve_dst_boundaries_and_declines(connection):
    """Elapsed seconds stay exact; uncertain types and virtual endpoints decline."""
    connection.raw_sql("SET TimeZone = 'America/New_York'")
    lower = datetime(1, 1, 1, tzinfo=UTC)
    upper = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    spring = datetime(2025, 3, 9, 6, 30, tzinfo=UTC)
    span = (upper - lower) // timedelta(seconds=1)
    cases = [
        (lower, span, True),
        (upper, -span, True),
        (spring, 86400, True),
        (spring + timedelta(days=1), -86400, True),
        (lower, -1, False),
        (upper, 1, False),
        (spring, -(2**63), False),
        (spring, 2**63 - 1, False),
    ]
    probes = []
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    expected = []
    operations = ("lt", "le", "gt", "ge", "eq")
    with dialect_scope("duckdb"):
        dialect = current_dialect()
        for index, (origin, offset, admitted) in enumerate(cases):
            # The selected source instant is independent of the declared shift.
            event = (
                lower + timedelta(microseconds=999999)
                if index == 1
                else upper.replace(microsecond=0)
                if index == 0
                else spring + timedelta(days=1)
            )
            left = cast(ir.TimestampValue, ibis.literal(event))
            right = cast(ir.TimestampValue, ibis.literal(origin))
            keys = dialect.native_shifted_comparison_keys(
                left, right, offset_seconds=offset, right_bounds=(origin, origin)
            )
            assert (keys is not None) == admitted
            probes.append(
                ibis.literal(index)
                .name("i")
                .as_table()
                .select(
                    "i",
                    **{
                        op: getattr(keys[0], f"__{op}__")(keys[1])
                        if keys is not None
                        else compare_timestamps(left, right, op, right_offset_seconds=offset)
                        for op in operations
                    },
                )
            )
            a = (event - epoch) // timedelta(microseconds=1)
            b = (origin - epoch) // timedelta(microseconds=1) + offset * 10**6
            expected.append(dict(lt=a < b, le=a <= b, gt=a > b, ge=a >= b, eq=a == b))
        source = ibis.table(
            {
                "utc": dt.Timestamp(timezone="UTC", scale=6),
                "naive": dt.Timestamp(scale=6),
                "ns": dt.Timestamp(scale=9),
                "local": dt.Timestamp(timezone="America/New_York", scale=6),
                "ms": dt.Timestamp(timezone="UTC", scale=3),
            }
        )
        for field in ("naive", "ns", "local", "ms"):
            assert (
                dialect.native_shifted_comparison_keys(
                    source[field], source.utc, offset_seconds=1, right_bounds=(spring, spring)
                )
                is None
            )
            assert (
                dialect.native_shifted_comparison_keys(
                    source.utc, source[field], offset_seconds=1, right_bounds=(spring, spring)
                )
                is None
            )
        with dialect_scope("generic"):
            assert (
                current_dialect().native_shifted_comparison_keys(
                    source.utc, source.utc, offset_seconds=1, right_bounds=(spring, spring)
                )
                is None
            )
    rows = connection.to_pyarrow(ibis.union(*probes)).to_pylist()
    assert len(rows) == len(cases)
    for row in rows:
        assert {op: row[op] for op in operations} == expected[row["i"]]


@pytest.mark.parametrize(
    ("scale", "expected"),
    [(None, None), (0, 6), (3, 6), (6, 6), (7, 9), (9, 9)],
)
def test_lossless_scale_promotes_to_the_nearest_supported_precision(scale, expected):
    assert lossless_scale(scale) == expected
