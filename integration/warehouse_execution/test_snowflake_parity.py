"""Snowflake window metrics, study shapes, timestamps and floats against DuckDB.

Data, catalogs and designs are shared with the PostgreSQL probes; only loading
differs, so the backends are checked on identical inputs. Tables are created
with Snowflake's default upper-case names and quoted lower-case columns, as the
shared catalogs spell them.
"""

from __future__ import annotations

import itertools
import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

import ibis
import pytest

import terrine as tr
from integration.warehouse_execution import test_postgres_shapes as shapes
from integration.warehouse_execution import test_postgres_windows as windows
from integration.warehouse_execution.snowflake_fixtures import (
    snowflake_scratch as snowflake_scratch,  # noqa: F401
)
from terrine.errors import TerrineError

pytestmark = pytest.mark.snowflake

_STAMP = "YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM"


def _quote(value) -> str:
    return "NULL" if value is None else repr(value)


def _load_windows(scratch, amount_type: str = "NUMBER(12, 2)") -> None:
    scratch.sql('CREATE TABLE users("user_id" VARCHAR)')
    scratch.sql(
        'CREATE TABLE events("user_id" VARCHAR, "occurred_at" TIMESTAMP_TZ(9), '
        f'"qty" NUMBER(38, 0), "amount" {amount_type}, "weight" FLOAT, "light" FLOAT)'
    )
    scratch.sql("INSERT INTO users VALUES " + ", ".join(f"('{user}')" for user in windows._USERS))
    scratch.sql(
        "INSERT INTO events VALUES "
        + ", ".join(
            f"('{user}', '{at} +0000', {qty}, {amount}, {weight}, NULL)"
            for user, at, qty, amount, weight in windows._EVENTS
        )
    )


def _load_shapes(scratch) -> None:
    scratch.sql('CREATE TABLE users("user_id" VARCHAR)')
    scratch.sql(
        'CREATE TABLE events("user_id" VARCHAR, "occurred_at" TIMESTAMP_TZ(9), '
        '"amount" NUMBER(10, 2), "tier" VARCHAR, "event_id" VARCHAR)'
    )
    scratch.sql("INSERT INTO users VALUES " + ", ".join(f"('{user}')" for user in shapes._USERS))
    scratch.sql(
        "INSERT INTO events VALUES "
        + ", ".join(
            f"('{user}', '{at} +0000', {amount}, {_quote(tier)}, "
            f"'00000000-0000-0000-0000-{index:012d}')"
            for index, (user, at, amount, tier) in enumerate(shapes._EVENTS)
        )
    )
    scratch.sql(
        'CREATE TABLE states("user_id" VARCHAR, "observed_at" TIMESTAMP_TZ(9), "state" VARCHAR)'
    )
    scratch.sql(
        "INSERT INTO states VALUES "
        + ", ".join(f"('{user}', '{at} +0000', '{state}')" for user, at, state in shapes._STATES)
    )


def _duckdb_windows():
    con = ibis.duckdb.connect()
    windows._load(con, timestamp_type="TIMESTAMPTZ", amount_type="DECIMAL(12, 2)")
    return con


def _duckdb_shapes():
    con = ibis.duckdb.connect()
    shapes._load(con, timestamp_type="TIMESTAMPTZ", amount_type="DECIMAL(10, 2)")
    return con


def test_window_metrics_match_duckdb(snowflake_scratch, tmp_path):  # noqa: F811
    _load_windows(snowflake_scratch)
    design = windows._design(
        outcomes={
            "orders_after": tr.agg("orders", window=("0s", "1d")),
            "spend_after": tr.agg("spend", window=("0s", "1d")),
        },
        orders_before=tr.agg("orders", window=("-1d", "0s")),
        quantity_before=tr.agg("quantity", window=("-1d", "0s")),
        heavy_orders_week=tr.agg("heavy_orders", window=("-7d", "0s")),
        spend_three_days=tr.agg("spend", window=("-3d", "0s")),
        average_week=tr.agg("average_spend", window=("-7d", "0s")),
        per_order_before=tr.agg("spend_per_order", window=("-3d", "0s")),
    )
    expected = windows._rows(windows._model(_duckdb_windows(), tmp_path, "duckdb"), design)
    actual = windows._rows(windows._model(snowflake_scratch.con, tmp_path, "snowflake"), design)
    windows._assert_close(actual, expected)
    by_unit = {row["unit"]: row for row in actual}
    assert by_unit["a"]["quantity_before"] == 9_000_000_000_001
    assert by_unit["a"]["spend_three_days"] == Decimal("7.15")


def test_exact_overflow_is_a_coded_refusal_and_the_boundary_succeeds(snowflake_scratch, tmp_path):  # noqa: F811
    scratch = snowflake_scratch
    _load_windows(scratch, amount_type="NUMBER(38, 0)")
    model = windows._model(scratch.con, tmp_path, "snowflake")
    design = windows._design(spend=tr.agg("spend", window=("-30d", "0s")))
    big = 10**38 - 1
    scratch.sql(
        f"INSERT INTO events VALUES ('d', '2025-01-01 00:00:00 +0000', 0, {big - 1}, 1.0, NULL), "
        "('d', '2025-01-01 01:00:00 +0000', 0, 1, 1.0, NULL)"
    )
    assert {row["unit"]: row["spend"] for row in windows._rows(model, design)}["d"] == Decimal(big)
    scratch.sql("INSERT INTO events VALUES ('d', '2025-01-01 02:00:00 +0000', 0, 1, 1.0, NULL)")
    with pytest.raises(TerrineError) as error:
        windows._rows(model, design)
    assert error.value.code == "terrine.temporal.exact_overflow"


_SIZE_CATALOG = """schema_version: 1
day_boundary: UTC
dialect: snowflake
sources:
  orders:
    relation: {kind: table, name: events}
    timestamp: occurred_at
    entities: {user: user_id}
    facts: [order]
    properties: [size]
    dimensions: []
facts:
  order: {source: orders, column: null}
properties:
  size: {source: orders, column: qty, dtype: int, as_of: event_time}
dimensions: {}
measures:
  above_1e19:
    fact: order
    aggregation: count
    filters: [{property: size, op: gt, values: [10000000000000000000]}]
  from_1e19:
    fact: order
    aggregation: count
    filters: [{property: size, op: gte, values: [10000000000000000000]}]
  above_1e20:
    fact: order
    aggregation: count
    filters: [{property: size, op: gt, values: [100000000000000000000]}]
  below_1e40:
    fact: order
    aggregation: count
    filters: [{property: size, op: lt, values: ["1e40"]}]
metrics:
  above_1e19: {kind: mean, entity: user, measure: above_1e19}
  from_1e19: {kind: mean, entity: user, measure: from_1e19}
  above_1e20: {kind: mean, entity: user, measure: above_1e20}
  below_1e40: {kind: mean, entity: user, measure: below_1e40}
"""


def test_integer_filters_compare_number_columns_beyond_int64(snowflake_scratch, tmp_path):  # noqa: F811
    # NUMBER(38, 0) resolves as decimal(38, 0): thresholds compare against its
    # 38-digit range, and one beyond it settles without a literal.
    scratch = snowflake_scratch
    scratch.sql('CREATE TABLE users("user_id" VARCHAR)')
    scratch.sql(
        'CREATE TABLE events("user_id" VARCHAR, "occurred_at" TIMESTAMP_TZ(9), "qty" NUMBER(38, 0))'
    )
    scratch.sql("INSERT INTO users VALUES ('d')")
    scratch.sql(
        "INSERT INTO events VALUES "
        + ", ".join(
            f"('d', '2025-01-01 {hour:02d}:00:00 +0000', {qty})"
            for hour, qty in enumerate((1, 10**19, 12345678901234567890123))
        )
    )
    path = tmp_path / "size.yaml"
    path.write_text(_SIZE_CATALOG, encoding="utf-8")
    model = tr.Model.from_catalog(
        path,
        con=scratch.con,
        units={"user": tr.UnitSource("users", "user_id")},
        readiness={"orders": tr.Coverage("2024-12-01", "2025-02-01")},
    )
    names = ("above_1e19", "from_1e19", "above_1e20", "below_1e40")
    design = windows._design(**{name: tr.agg(name, window=("-1d", "0s")) for name in names})
    (row,) = windows._rows(model, design)
    assert [row[name] for name in names] == [1, 2, 1, 3]


def test_wide_integer_keys_and_properties_keep_their_digits(snowflake_scratch, tmp_path):  # noqa: F811
    # A NUMBER(38, 0) unit key or integer property beyond int64 reaches the
    # output with its digits instead of failing the Arrow export's cast.
    scratch = snowflake_scratch
    scratch.sql('CREATE TABLE users("user_id" NUMBER(38, 0))')
    scratch.sql(
        'CREATE TABLE events("user_id" NUMBER(38, 0), "occurred_at" TIMESTAMP_TZ(9), '
        '"qty" NUMBER(38, 0))'
    )
    wide = 10**19
    scratch.sql(f"INSERT INTO users VALUES ({wide}), (7)")
    scratch.sql(
        "INSERT INTO events VALUES "
        f"({wide}, '2025-01-01 00:00:00 +0000', {wide}), (7, '2025-01-01 00:00:00 +0000', 7)"
    )
    path = tmp_path / "size.yaml"
    path.write_text(_SIZE_CATALOG, encoding="utf-8")
    model = tr.Model.from_catalog(
        path,
        con=scratch.con,
        units={"user": tr.UnitSource("users", "user_id")},
        readiness={"orders": tr.Coverage("2024-12-01", "2025-02-01")},
    )
    design = windows._design(
        size_now=tr.asof("size", before="0d"),
        from_1e19=tr.agg("from_1e19", window=("-1d", "0s")),
    )
    rows = {row["unit"]: row for row in windows._rows(model, design)}
    assert set(rows) == {wide, 7}
    assert rows[wide]["size_now"] == wide
    assert rows[7]["size_now"] == 7
    assert [rows[unit]["from_1e19"] for unit in (wide, 7)] == [1, 0]


@pytest.mark.parametrize("name", sorted(shapes._DESIGNS))
def test_design_matches_duckdb(snowflake_scratch, tmp_path, name):  # noqa: F811
    _load_shapes(snowflake_scratch)
    design = shapes._DESIGNS[name]
    expected = shapes._rows(shapes._model(_duckdb_shapes(), tmp_path, "duckdb"), design)
    actual = shapes._rows(shapes._model(snowflake_scratch.con, tmp_path, "snowflake"), design)
    assert expected, "the DuckDB reference produced no rows"
    assert actual == expected


def test_event_ids_break_first_event_ties(snowflake_scratch, tmp_path):  # noqa: F811
    _load_shapes(snowflake_scratch)
    event_ids = {"orders": "event_id"}
    design = shapes._DESIGNS["time_to_first_order"]
    expected = shapes._rows(shapes._model(_duckdb_shapes(), tmp_path, "duckdb", event_ids), design)
    actual = shapes._rows(
        shapes._model(snowflake_scratch.con, tmp_path, "snowflake", event_ids), design
    )
    assert actual == expected


def test_cohort_summaries_match_duckdb(snowflake_scratch, tmp_path):  # noqa: F811
    _load_shapes(snowflake_scratch)
    design = tr.Design(
        "user",
        tr.EventAnchor("order", which="first"),
        shape=tr.CohortSpec("order", tr.Trajectory("spend", "1d", periods=2, cumulative=True)),
        administrative_cutoff=datetime(2025, 1, 6, tzinfo=UTC),
    )

    def summary(model, statistic):
        grouping = tr.CohortGrouping("1d", datetime(2025, 1, 1, tzinfo=UTC), statistic=statistic)
        cohorts = design.build(model).cohorts(grouping=grouping, value_column="cumulative_value")
        rows = cohorts.to_pyarrow(budget=shapes._BUDGET).to_pylist()
        return sorted(rows, key=lambda row: tuple(str(value) for value in row.values()))

    duckdb = _duckdb_shapes()
    for statistic in ("total", "mean"):
        expected = summary(shapes._model(duckdb, tmp_path, "duckdb"), statistic)
        actual = summary(shapes._model(snowflake_scratch.con, tmp_path, "snowflake"), statistic)
        assert expected, "the DuckDB reference produced no rows"
        windows._assert_close(actual, expected)


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# Nanosecond instants over Snowflake's whole range, including both sides of the
# epoch and both Arrow-unrepresentable extremes.
_INSTANTS = {
    "earliest": "0001-01-01 00:00:00",
    "before_epoch_ns": "1969-12-31 23:59:59.999999999",
    "before_epoch": "1969-12-31 23:59:59.999999",
    "epoch": "1970-01-01 00:00:00",
    "after_epoch_ns": "1970-01-01 00:00:00.000000001",
    "ordinary": "2025-06-01 12:34:56.123456789",
    "next_second": "2025-06-01 12:34:57.123456789",
    "latest": "9999-12-31 23:59:59.999999999",
    "missing": None,
}


def _reference(text: str | None) -> tuple[int, int, int] | None:
    """``(rank, seconds, nanos)`` truncated toward zero, in exact integers."""
    if text is None:
        return None
    clock, _, fraction = text.partition(".")
    whole = datetime.fromisoformat(clock + "+00:00")
    nanoseconds = (whole - _EPOCH) // timedelta(microseconds=1) * 1000
    nanoseconds += int(fraction.ljust(9, "0")) if fraction else 0
    seconds = abs(nanoseconds) // 10**9 * (1 if nanoseconds >= 0 else -1)
    return (0, seconds, nanoseconds - seconds * 10**9)


def test_timestamp_parts_and_comparisons_match_the_reference(snowflake_scratch):  # noqa: F811
    from integration.warehouse_execution import test_postgres_timestamps as reference
    from terrine.backends.dialect import dialect_scope
    from terrine.backends.timestamps import compare_timestamps, timestamp_parts

    scratch = snowflake_scratch
    scratch.sql('CREATE TABLE instants("name" VARCHAR, "instant" TIMESTAMP_TZ(9))')
    for name, text in _INSTANTS.items():
        value = "NULL" if text is None else f"TO_TIMESTAMP_TZ('{text} +0000', '{_STAMP}')"
        scratch.sql(f"INSERT INTO instants SELECT '{name}', {value}")
    table = scratch.con.table("INSTANTS", database=(scratch.database, scratch.schema))
    with dialect_scope("snowflake"):
        rank, seconds, nanos = timestamp_parts(table["instant"])
        parts = {
            row["name"]: None
            if row["rank"] is None
            else (row["rank"], row["seconds"], row["nanos"])
            for row in table.select("name", rank=rank, seconds=seconds, nanos=nanos)
            .to_pyarrow()
            .to_pylist()
        }
        assert parts == {name: _reference(text) for name, text in _INSTANTS.items()}

        left = table.select(left_name=table["name"], left_at=table["instant"])
        right = table.select(right_name=table["name"], right_at=table["instant"])
        pairs = left.cross_join(right)
        ops: tuple[Literal["lt", "le", "gt", "ge", "eq"], ...] = ("lt", "le", "gt", "ge", "eq")
        cases = list(itertools.product(ops, (0, 1, -86_400)))
        result = pairs.select(
            "left_name",
            "right_name",
            **{
                f"{op}_{index}": compare_timestamps(
                    pairs.left_at, pairs.right_at, op, right_offset_seconds=offset
                )
                for index, (op, offset) in enumerate(cases)
            },
        ).to_pyarrow()
    for row in result.to_pylist():
        for index, (op, offset) in enumerate(cases):
            expected = reference._compare(
                _reference(_INSTANTS[row["left_name"]]),
                _reference(_INSTANTS[row["right_name"]]),
                op,
                offset,
            )
            assert row[f"{op}_{index}"] == expected, (
                op,
                offset,
                row["left_name"],
                row["right_name"],
            )


def test_float_windows_match_duckdb_within_tolerance(snowflake_scratch, tmp_path):  # noqa: F811
    _load_windows(snowflake_scratch)
    # History holds 1e15: a binary64 running sum would lose the window's small
    # values, while exact state cancels the history and rounds the decimal sum once.
    rows = (
        ("2024-12-20 00:00:00", "1e15"),
        ("2024-12-31 12:00:00", "0.1"),
        ("2025-01-01 06:00:00", "0.2"),
        ("2025-01-01 12:00:00", "0.30000000000000004"),
    )
    snowflake_scratch.sql(
        "INSERT INTO events VALUES "
        + ", ".join(f"('d', '{at} +0000', 1, 10, {weight}, NULL)" for at, weight in rows)
    )
    duckdb = _duckdb_windows()
    duckdb.raw_sql(
        "INSERT INTO events VALUES "
        + ", ".join(f"('d', '{at}+00', 1, 10, {weight}, NULL)" for at, weight in rows)
    )
    design = windows._design(
        outcomes={"weight_after": tr.agg("weight", window=("0s", "1d"))},
        weight_before=tr.agg("weight", window=("-1d", "0s")),
        weight_week=tr.agg("weight", window=("-7d", "0s")),
        spend_per_weight=tr.agg("spend_per_weight", window=("-7d", "0s")),
    )
    expected = windows._rows(windows._model(duckdb, tmp_path, "duckdb"), design)
    actual = windows._rows(windows._model(snowflake_scratch.con, tmp_path, "snowflake"), design)
    windows._assert_close(actual, expected)
    by_unit = {row["unit"]: row for row in actual}
    reference = {row["unit"]: row for row in expected}
    # Snowflake rounds each float to 18 significant digits on entry (its
    # shortest text is not available row-wise), so the exact window sums can
    # differ from DuckDB's by under an ulp before the single rounding; here
    # 0.100000000000000006 + 0.200000000000000011 + 0.300000000000000044 and
    # DuckDB's 0.60000000000000004 round to the same double.
    for name in ("weight_before", "weight_week", "spend_per_weight"):
        got, want = by_unit["d"][name], reference["d"][name]
        assert abs(got - want) <= math.ulp(want), (name, got, want)
    assert by_unit["d"]["weight_week"] == reference["d"]["weight_week"] == 0.6000000000000001


@pytest.mark.parametrize(
    ("rows", "metric", "failure"),
    [
        # Non-finite history before a non-empty window refuses, as on DuckDB.
        (
            (("2024-12-20 00:00:00", "'NaN'::FLOAT"), ("2025-01-01 12:00:00", "1.0")),
            "weight",
            "nonfinite_input",
        ),
        # A coefficient beyond NUMBER(38, 0)'s digits.
        ((("2025-01-01 12:00:00", "1e39"),), "weight", "bound_overflow"),
        # A dropped denominator must not read as a zero denominator.
        ((("2025-01-01 12:00:00", "1e39"),), "spend_per_weight", "bound_overflow"),
        # A quotient beyond binary64's range (10 / 1e-320): TRY_TO_DOUBLE returns
        # NULL for it, which must not pass as a missing value.
        ((("2025-01-01 12:00:00", "1e-320"),), "spend_per_weight", "bound_overflow"),
        # The unit's scale is the most fractional digits any value's text needs:
        # 1e-20's 18-digit text 9.99999999999999945e-21 sets 38, so 1e20 needs 59.
        (
            (("2024-12-31 12:00:00", "'1e-20'::FLOAT"), ("2025-01-01 12:00:00", "'1e20'::FLOAT")),
            "weight",
            "bound_overflow",
        ),
    ],
)
def test_float_window_failures_are_coded_refusals(
    snowflake_scratch, tmp_path, rows, metric, failure
):  # noqa: F811
    _load_windows(snowflake_scratch)
    snowflake_scratch.sql(
        "INSERT INTO events VALUES "
        + ", ".join(f"('d', '{at} +0000', 1, 10, {weight}, NULL)" for at, weight in rows)
    )
    design = windows._design(value=tr.agg(metric, window=("-3d", "0s")))
    with pytest.raises(TerrineError) as error:
        windows._rows(windows._model(snowflake_scratch.con, tmp_path, "snowflake"), design)
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failure"] == failure


def test_float_sum_beyond_38_digits_is_an_exact_overflow(snowflake_scratch, tmp_path):  # noqa: F811
    _load_windows(snowflake_scratch)
    # Each coefficient fits; their running sum (1.8e38) leaves NUMBER(38, 0)'s
    # 128-bit accumulator, where Snowflake raises (sums between 1e38 and about
    # 1.7e38 are held exactly without an error).
    snowflake_scratch.sql(
        "INSERT INTO events VALUES "
        "('d', '2025-01-01 11:00:00 +0000', 1, 10, 9e37, NULL), "
        "('d', '2025-01-01 12:00:00 +0000', 1, 10, 9e37, NULL)"
    )
    design = windows._design(value=tr.agg("weight", window=("-3d", "0s")))
    with pytest.raises(TerrineError) as error:
        windows._rows(windows._model(snowflake_scratch.con, tmp_path, "snowflake"), design)
    assert error.value.code == "terrine.temporal.exact_overflow"


def test_boundary_and_tied_first_events_match_duckdb(snowflake_scratch, tmp_path):  # noqa: F811
    """An order exactly at the anchor instant is the first one (the as-of join is
    inclusive there), and two orders at one instant are ordered by event id."""
    _load_shapes(snowflake_scratch)
    duckdb = _duckdb_shapes()
    rows = [
        ("d", "2025-01-01 12:00:00", "2.00", "silver", "00000000-0000-0000-0000-000000000101"),
        ("d", "2025-01-01 12:00:00", "1.00", "gold", "00000000-0000-0000-0000-000000000100"),
        ("c", "2025-01-01 12:00:00", "3.00", "gold", "00000000-0000-0000-0000-000000000102"),
        # Exactly at the as-of instants of as_of_interleaved_units (t0 and t0 - 1d).
        ("d", "2025-01-02 03:00:00", "4.00", "bronze", "00000000-0000-0000-0000-000000000103"),
        ("d", "2025-01-01 03:00:00", "5.00", "silver", "00000000-0000-0000-0000-000000000104"),
    ]
    snowflake_scratch.sql(
        "INSERT INTO events VALUES "
        + ", ".join(
            f"('{u}', '{at} +0000', {amount}, '{tier}', '{eid}')"
            for u, at, amount, tier, eid in rows
        )
    )
    duckdb.raw_sql(
        "INSERT INTO events VALUES "
        + ", ".join(
            f"('{u}', '{at}+00', {amount}, '{tier}', '{eid}')" for u, at, amount, tier, eid in rows
        )
    )
    event_ids = {"orders": "event_id"}

    def compare(name: str) -> None:
        design = shapes._DESIGNS[name]
        expected = shapes._rows(shapes._model(duckdb, tmp_path, "duckdb", event_ids), design)
        actual = shapes._rows(
            shapes._model(snowflake_scratch.con, tmp_path, "snowflake", event_ids), design
        )
        assert actual == expected, name

    compare("time_to_first_order")
    compare("time_to_order_with_entry_and_exit")
    # Tied rows would give the tier property two values at one instant.
    tied = "00000000-0000-0000-0000-000000000101"
    snowflake_scratch.sql(f"""DELETE FROM events WHERE "event_id" = '{tied}'""")
    duckdb.raw_sql(f"DELETE FROM events WHERE event_id = '{tied}'")
    compare("as_of_interleaved_units")


def test_microsecond_apart_events_anchor_separately(snowflake_scratch, tmp_path):  # noqa: F811
    """Event-anchor identities render the instant at full precision: the
    session's default timestamp text keeps three fractional digits, which would
    merge these two orders into one ambiguous anchor."""
    _load_shapes(snowflake_scratch)
    duckdb = _duckdb_shapes()
    rows = [
        ("d", "2025-01-01 12:00:00.000001", "1.00", "gold", "00000000-0000-0000-0000-000000000201"),
        ("d", "2025-01-01 12:00:00.000002", "2.00", "gold", "00000000-0000-0000-0000-000000000202"),
    ]
    snowflake_scratch.sql(
        "INSERT INTO events VALUES "
        + ", ".join(
            f"('{u}', '{at} +0000', {amount}, '{tier}', '{eid}')"
            for u, at, amount, tier, eid in rows
        )
    )
    duckdb.raw_sql(
        "INSERT INTO events VALUES "
        + ", ".join(
            f"('{u}', '{at}+00', {amount}, '{tier}', '{eid}')" for u, at, amount, tier, eid in rows
        )
    )
    design = tr.Design(
        "user",
        tr.EventAnchor("order", which="each"),
        outcomes={"spend_next_day": tr.agg("spend", window=("0s", "1d"))},
    )
    expected = shapes._rows(shapes._model(duckdb, tmp_path, "duckdb"), design)
    actual = shapes._rows(shapes._model(snowflake_scratch.con, tmp_path, "snowflake"), design)
    assert actual == expected
    assert sum(row["unit"] == "d" for row in actual) == 2


def _load_millisecond_shapes(scratch, duckdb) -> None:
    """The shapes data at millisecond precision, in TIMESTAMP_NTZ(3) and
    DuckDB's TIMESTAMP_MS, with instants exact at that scale on both."""
    events = [
        ("a", "2025-01-01 08:00:00.001", "5.00", "bronze"),
        ("b", "2025-01-01 09:00:00.500", "1.00", "silver"),
        ("a", "2025-01-02 00:00:00.999", "2.50", "gold"),
        ("b", "2025-01-02 06:00:00.000", "4.00", None),
        ("a", "2025-01-03 12:00:00.250", "1.25", "gold"),
        ("c", "2025-01-03 23:59:59.999", "9.99", "bronze"),
        ("b", "2025-01-04 00:00:00.001", "3.00", "gold"),
    ]
    states = [
        ("a", "2025-01-02 00:00:00.001", "active"),
        ("a", "2025-01-04 00:00:00.000", "closed"),
    ]
    scratch.sql('CREATE TABLE users("user_id" VARCHAR)')
    scratch.sql(
        'CREATE TABLE events("user_id" VARCHAR, "occurred_at" TIMESTAMP_NTZ(3), '
        '"amount" NUMBER(10, 2), "tier" VARCHAR, "event_id" VARCHAR)'
    )
    scratch.sql(
        'CREATE TABLE states("user_id" VARCHAR, "observed_at" TIMESTAMP_NTZ(3), "state" VARCHAR)'
    )
    duckdb.raw_sql("CREATE TABLE users(user_id VARCHAR)")
    duckdb.raw_sql(
        "CREATE TABLE events(user_id VARCHAR, occurred_at TIMESTAMP_MS, amount DECIMAL(10, 2), "
        "tier VARCHAR, event_id VARCHAR)"
    )
    duckdb.raw_sql("CREATE TABLE states(user_id VARCHAR, observed_at TIMESTAMP_MS, state VARCHAR)")
    for run in (scratch.sql, duckdb.raw_sql):
        run("INSERT INTO users VALUES " + ", ".join(f"('{user}')" for user in shapes._USERS))
        run(
            "INSERT INTO events VALUES "
            + ", ".join(
                f"('{user}', '{at}', {amount}, {_quote(tier)}, "
                f"'00000000-0000-0000-0000-{index:012d}')"
                for index, (user, at, amount, tier) in enumerate(events)
            )
        )
        run(
            "INSERT INTO states VALUES "
            + ", ".join(f"('{user}', '{at}', '{state}')" for user, at, state in states)
        )


def test_millisecond_sources_match_duckdb(snowflake_scratch, tmp_path):  # noqa: F811
    """A TIMESTAMP_NTZ(3) event source promotes to the microsecond axis without
    loss, on Snowflake as on DuckDB's TIMESTAMP_MS."""
    duckdb = ibis.duckdb.connect()
    _load_millisecond_shapes(snowflake_scratch, duckdb)
    design = shapes._DESIGNS["time_to_first_order"]
    expected = shapes._rows(shapes._model(duckdb, tmp_path, "duckdb"), design)
    actual = shapes._rows(shapes._model(snowflake_scratch.con, tmp_path, "snowflake"), design)
    assert actual == expected
    by_unit = {row["unit"]: row for row in actual}
    # Units with an order before t0 are not at risk; c's first order keeps its
    # millisecond and d is censored at the cutoff.
    assert set(by_unit) == {"c", "d"}
    assert by_unit["c"]["event_time"] == datetime(2025, 1, 3, 23, 59, 59, 999_000, tzinfo=UTC)


def test_repeated_subnormal_window_stays_within_the_absolute_bound(snowflake_scratch, tmp_path):  # noqa: F811
    """DuckDB sums the shortest text 5e-324 of each value; Snowflake sums its
    18-digit text 4.94065645841246544e-324, so with n subnormal inputs the two
    results may differ by subnormal ulps:
    |s_sf - s_duckdb| <= 2**-51 * sum|x| + (n + 1) * 2**-1074."""
    _load_windows(snowflake_scratch)
    duckdb = _duckdb_windows()
    n = 200
    rows = [(f"2025-01-01 {i // 60:02d}:{i % 60:02d}:00", "5e-324") for i in range(n)]
    snowflake_scratch.sql(
        "INSERT INTO events VALUES "
        + ", ".join(f"('d', '{at} +0000', 1, 10, '{weight}'::FLOAT, NULL)" for at, weight in rows)
    )
    duckdb.raw_sql(
        "INSERT INTO events VALUES "
        + ", ".join(f"('d', '{at}+00', 1, 10, {weight}, NULL)" for at, weight in rows)
    )
    design = windows._design(weight=tr.agg("weight", window=("-1d", "0s")))
    want = {
        row["unit"]: row["weight"]
        for row in windows._rows(windows._model(duckdb, tmp_path, "duckdb"), design)
    }["d"]
    got = {
        row["unit"]: row["weight"]
        for row in windows._rows(
            windows._model(snowflake_scratch.con, tmp_path, "snowflake"), design
        )
    }["d"]
    assert got > 0 and want > 0
    assert abs(got - want) <= 2**-51 * (n * 5e-324) + (n + 1) * 2**-1074, (got, want)
