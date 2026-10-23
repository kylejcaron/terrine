"""PostgreSQL window metrics against the DuckDB reference backend."""

from __future__ import annotations

import math
from decimal import Decimal

import ibis
import pytest

import terrine as tr
from integration.warehouse_execution.postgres_fixtures import connect_postgres
from integration.warehouse_execution.postgres_fixtures import (
    postgres_scratch as postgres_scratch,  # noqa: F401
)
from terrine.errors import TerrineError

pytestmark = pytest.mark.postgres

_CATALOG = """schema_version: 1
day_boundary: UTC
dialect: {dialect}
sources:
  orders:
    relation: {{kind: table, name: events}}
    timestamp: occurred_at
    entities: {{user: user_id}}
    facts: [order, quantity, amount, weight, light]
    properties: [heaviness]
    dimensions: []
facts:
  order: {{source: orders, column: null}}
  quantity: {{source: orders, column: qty}}
  amount: {{source: orders, column: amount}}
  weight: {{source: orders, column: weight}}
  light: {{source: orders, column: light}}
properties:
  heaviness: {{source: orders, column: weight, dtype: float, as_of: event_time}}
dimensions: {{}}
measures:
  order_count: {{fact: order, aggregation: count}}
  heavy_order_count:
    fact: order
    aggregation: count
    filters: [{{property: heaviness, op: gte, values: [1.0]}}]
  quantity_total: {{fact: quantity, aggregation: sum}}
  amount_total: {{fact: amount, aggregation: sum}}
  amount_average: {{fact: amount, aggregation: avg_event}}
  weight_total: {{fact: weight, aggregation: sum}}
  light_total: {{fact: light, aggregation: sum}}
metrics:
  orders: {{kind: mean, entity: user, measure: order_count}}
  heavy_orders: {{kind: mean, entity: user, measure: heavy_order_count}}
  quantity: {{kind: mean, entity: user, measure: quantity_total}}
  spend: {{kind: mean, entity: user, measure: amount_total}}
  average_spend: {{kind: mean, entity: user, measure: amount_average}}
  weight: {{kind: mean, entity: user, measure: weight_total}}
  light: {{kind: mean, entity: user, measure: light_total}}
  spend_per_order: {{kind: ratio, entity: user, numerator: amount_total, denominator: order_count}}
  spend_per_weight:
    {{kind: ratio, entity: user, numerator: amount_total, denominator: weight_total}}
"""

# Boundary rows: exactly t0 - 1d (inside [-1d, 0)), exactly t0 (outcome, not
# feature) and exactly t0 + 1d (outside [0, 1d)); negative and large values.
_EVENTS = [
    ("a", "2025-01-01 00:00:00", 1, "10.25", 0.5),
    ("a", "2025-01-01 23:59:59.999999", 9_000_000_000_000, "-3.10", 0.25),
    ("a", "2025-01-02 00:00:00", 2, "4.00", 1.0),
    ("a", "2025-01-02 23:59:59.999999", 3, "0.01", 1.0),
    ("a", "2025-01-03 00:00:00", 4, "100.00", 1.0),
    ("b", "2024-12-31 23:59:59.999999", 5, "7.77", 1.0),
    ("b", "2025-01-02 12:00:00", -6, "-0.50", 1.0),
    ("c", "2024-12-30 00:00:00", 7, "1.11", 1.0),
]
_USERS = ["a", "b", "c", "d"]


def _load(con, *, timestamp_type: str, amount_type: str) -> None:
    con.raw_sql("CREATE TABLE users(user_id VARCHAR)")
    con.raw_sql(
        f"CREATE TABLE events(user_id VARCHAR, occurred_at {timestamp_type}, "
        f"qty BIGINT, amount {amount_type}, weight DOUBLE PRECISION, light REAL)"
    )
    con.raw_sql("INSERT INTO users VALUES " + ", ".join(f"('{user}')" for user in _USERS))
    # REAL values such as 0.1 widen to binary64 (0.10000000149011612) on both backends.
    con.raw_sql(
        "INSERT INTO events VALUES "
        + ", ".join(
            f"('{user}', '{at}+00', {qty}, {amount}, {weight}, {weight / 10})"
            for user, at, qty, amount, weight in _EVENTS
        )
    )


def _model(con, tmp_path, dialect: str):
    path = tmp_path / f"{dialect}.yaml"
    path.write_text(_CATALOG.format(dialect=dialect), encoding="utf-8")
    return tr.Model.from_catalog(
        path,
        con=con,
        units={"user": tr.UnitSource("users", "user_id")},
        readiness={"orders": tr.Coverage("2024-12-01", "2025-02-01")},
    )


def _design(outcomes=None, **features) -> tr.Design:
    return tr.Design("user", tr.FixedDate("2025-01-02"), features=features, outcomes=outcomes or {})


_DESIGN = _design(
    outcomes={
        "orders_after": tr.agg("orders", window=("0s", "1d")),
        "spend_after": tr.agg("spend", window=("0s", "1d")),
    },
    orders_before=tr.agg("orders", window=("-1d", "0s")),
    quantity_before=tr.agg("quantity", window=("-1d", "0s")),
    quantity_week=tr.agg("quantity", window=("-7d", "0s")),
    # An exact count filtered on a floating property.
    heavy_orders_week=tr.agg("heavy_orders", window=("-7d", "0s")),
    spend_before=tr.agg("spend", window=("-1d", "0s")),
    spend_three_days=tr.agg("spend", window=("-3d", "0s")),
)
_BUDGET = tr.Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False)


def _rows(model, design=_DESIGN):
    table = design.build(model).materialize(_BUDGET).table
    kept = [name for name in table.column_names if name not in {"t0", "anchor_id"}]
    rows = table.select(kept).to_pylist()
    return sorted(rows, key=lambda row: row["unit"])


def test_counts_and_sums_match_duckdb(postgres_scratch, tmp_path):  # noqa: F811
    duckdb = ibis.duckdb.connect()
    _load(duckdb, timestamp_type="TIMESTAMPTZ", amount_type="DECIMAL(12, 2)")
    _load(postgres_scratch.con, timestamp_type="timestamptz", amount_type="numeric(12, 2)")
    expected = _rows(_model(duckdb, tmp_path, "duckdb"))
    actual = _rows(_model(postgres_scratch.con, tmp_path, "postgres"))
    assert actual == expected
    by_unit = {row["unit"]: row for row in actual}
    assert by_unit["a"]["orders_before"] == 2
    assert by_unit["a"]["quantity_before"] == 9_000_000_000_001
    assert by_unit["a"]["spend_before"] == Decimal("7.15")
    assert by_unit["d"]["spend_before"] == Decimal("0.00")
    assert [by_unit[unit]["heavy_orders_week"] for unit in "abcd"] == [0, 1, 1, 0]


def test_exact_overflow_is_a_coded_refusal_and_the_boundary_succeeds(postgres_scratch, tmp_path):  # noqa: F811
    con = postgres_scratch.con
    _load(con, timestamp_type="timestamptz", amount_type="numeric(38, 0)")
    model = _model(con, tmp_path, "postgres")
    design = _design(spend=tr.agg("spend", window=("-30d", "0s")))
    big = 10**38 - 1
    con.raw_sql(
        f"INSERT INTO events VALUES ('d', '2025-01-01 00:00:00+00', 0, {big - 1}, 1.0), "
        "('d', '2025-01-01 01:00:00+00', 0, 1, 1.0)"
    ).close()
    assert {row["unit"]: row["spend"] for row in _rows(model, design)}["d"] == Decimal(big)
    con.raw_sql("INSERT INTO events VALUES ('d', '2025-01-01 02:00:00+00', 0, 1, 1.0)").close()
    with pytest.raises(TerrineError) as error:
        _rows(model, design)
    assert error.value.code == "terrine.temporal.exact_overflow"


def _assert_close(actual, expected):
    """Equal rows, except floats within a tight relative tolerance; PostgreSQL omits
    DuckDB's certified absolute-error columns."""
    assert len(actual) == len(expected)
    for got, want in zip(actual, expected, strict=True):
        extra = set(want) - set(got)
        assert all(name.endswith("_absolute_error") for name in extra), extra
        for name, value in got.items():
            if isinstance(value, float) and want[name] is not None:
                assert math.isclose(value, want[name], rel_tol=1e-12, abs_tol=1e-300), name
            else:
                assert value == want[name], name


def test_averages_and_ratios_match_duckdb_within_tolerance(postgres_scratch, tmp_path):  # noqa: F811
    duckdb = ibis.duckdb.connect()
    _load(duckdb, timestamp_type="TIMESTAMPTZ", amount_type="DECIMAL(12, 2)")
    _load(postgres_scratch.con, timestamp_type="timestamptz", amount_type="numeric(12, 2)")
    # Unit d has no orders: an empty average and a zero ratio denominator.
    design = _design(
        outcomes={"average_after": tr.agg("average_spend", window=("0s", "1d"))},
        average_before=tr.agg("average_spend", window=("-1d", "0s")),
        average_week=tr.agg("average_spend", window=("-7d", "0s")),
        per_order_before=tr.agg("spend_per_order", window=("-3d", "0s")),
    )
    expected = _rows(_model(duckdb, tmp_path, "duckdb"), design)
    actual = _rows(_model(postgres_scratch.con, tmp_path, "postgres"), design)
    _assert_close(actual, expected)
    by_unit = {row["unit"]: row for row in actual}
    assert by_unit["a"]["average_before"] == pytest.approx(7.15 / 2, rel=1e-15)
    assert by_unit["d"]["average_before"] is None
    assert by_unit["d"]["per_order_before"] is None


def test_float_windows_match_duckdb_within_tolerance(postgres_scratch, tmp_path):  # noqa: F811
    duckdb = ibis.duckdb.connect()
    _load(duckdb, timestamp_type="TIMESTAMPTZ", amount_type="DECIMAL(12, 2)")
    _load(postgres_scratch.con, timestamp_type="timestamptz", amount_type="numeric(12, 2)")
    design = _design(
        outcomes={"weight_after": tr.agg("weight", window=("0s", "1d"))},
        weight_before=tr.agg("weight", window=("-1d", "0s")),
        weight_week=tr.agg("weight", window=("-7d", "0s")),
        light_week=tr.agg("light", window=("-7d", "0s")),
    )
    expected = _rows(_model(duckdb, tmp_path, "duckdb"), design)
    actual = _rows(_model(postgres_scratch.con, tmp_path, "postgres"), design)
    _assert_close(actual, expected)
    assert {row["unit"]: row["weight_before"] for row in actual}["a"] == 0.75


def _float_rows(con, rows: list[tuple[str, str]]) -> None:
    cursor = con.raw_sql(
        "INSERT INTO events VALUES "
        + ", ".join(f"('d', '{at}+00', 0, 0, {weight}, 0)" for at, weight in rows)
    )
    if con.name == "postgres":  # DuckDB returns its connection, not a cursor
        cursor.close()


def test_large_float_history_cancels_exactly_outside_the_window(postgres_scratch, tmp_path):  # noqa: F811
    con = postgres_scratch.con
    _load(con, timestamp_type="timestamptz", amount_type="numeric(12, 2)")
    # Float prefix sums would lose the small window entirely beneath this history.
    _float_rows(
        con,
        [("2024-12-20 00:00:00", "1e17"), ("2024-12-21 00:00:00", "-3e16")]
        + [("2025-01-01 12:00:00", "0.1"), ("2025-01-01 13:00:00", "0.2")],
    )
    design = _design(weight_before=tr.agg("weight", window=("-1d", "0s")))
    value = {
        row["unit"]: row["weight_before"]
        for row in _rows(_model(con, tmp_path, "postgres"), design)
    }
    assert math.isclose(value["d"], 0.3, rel_tol=1e-15)


@pytest.mark.parametrize(
    ("rows", "metric", "failure"),
    [
        # A non-finite value before the window still poisons it, as on DuckDB.
        (
            [("2024-12-20 00:00:00", "'NaN'"), ("2025-01-01 12:00:00", "1.0")],
            "weight",
            "nonfinite_input",
        ),
        # Division must not erase it: 0 / Infinity would otherwise be 0.
        ([("2025-01-01 12:00:00", "'Infinity'")], "spend_per_weight", "nonfinite_input"),
        (
            [("2025-01-01 12:00:00", "1e308"), ("2025-01-01 13:00:00", "1e308")],
            "weight",
            "bound_overflow",
        ),
    ],
)
def test_float_window_failures_are_coded_refusals(
    postgres_scratch, tmp_path, rows, metric, failure
):  # noqa: F811
    con = postgres_scratch.con
    _load(con, timestamp_type="timestamptz", amount_type="numeric(12, 2)")
    _float_rows(con, rows)
    design = _design(value=tr.agg(metric, window=("-1d", "0s")))
    with pytest.raises(TerrineError) as error:
        _rows(_model(con, tmp_path, "postgres"), design)
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failure"] == failure


def test_cancelling_float_window_stays_within_the_absolute_sum_bound(postgres_scratch, tmp_path):  # noqa: F811
    """Each float enters as its shortest decimal, within half an ulp, so under
    cancellation the agreement with DuckDB is relative to the window's absolute sum."""
    duckdb = ibis.duckdb.connect()
    _load(duckdb, timestamp_type="TIMESTAMPTZ", amount_type="DECIMAL(12, 2)")
    _load(postgres_scratch.con, timestamp_type="timestamptz", amount_type="numeric(12, 2)")
    rows = [("2025-01-01 12:00:00", "1.000001"), ("2025-01-01 13:00:00", "-1.0")]
    for con in (duckdb, postgres_scratch.con):
        _float_rows(con, rows)
    design = _design(weight_before=tr.agg("weight", window=("-1d", "0s")))

    def value(model):
        return {row["unit"]: row["weight_before"] for row in _rows(model, design)}["d"]

    expected = value(_model(duckdb, tmp_path, "duckdb"))
    actual = value(_model(postgres_scratch.con, tmp_path, "postgres"))
    assert abs(actual - expected) <= 2**-52 * (1.000001 + 1.0)


def test_float_state_ignores_the_callers_float_text_precision(postgres_scratch, tmp_path):  # noqa: F811
    _load(postgres_scratch.con, timestamp_type="timestamptz", amount_type="numeric(12, 2)")
    _float_rows(postgres_scratch.con, [("2025-01-01 12:00:00", str(1 / 3))])
    con = connect_postgres(schema=postgres_scratch.schema, options="-c extra_float_digits=0")
    try:
        design = _design(weight_before=tr.agg("weight", window=("-1d", "0s")))
        rows = _rows(_model(con, tmp_path, "postgres"), design)
    finally:
        con.disconnect()
    assert {row["unit"]: row["weight_before"] for row in rows}["d"] == 1 / 3


def test_empty_window_after_nonfinite_history_is_zero(postgres_scratch, tmp_path):  # noqa: F811
    con = postgres_scratch.con
    _load(con, timestamp_type="timestamptz", amount_type="numeric(12, 2)")
    _float_rows(con, [("2024-12-20 00:00:00", "'Infinity'")])
    design = _design(weight_before=tr.agg("weight", window=("-1d", "0s")))
    rows = _rows(_model(con, tmp_path, "postgres"), design)
    assert {row["unit"]: row["weight_before"] for row in rows}["d"] == 0.0
