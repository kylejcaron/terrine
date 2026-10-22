"""PostgreSQL exact window counts and sums against the DuckDB reference backend."""

from __future__ import annotations

from decimal import Decimal

import ibis
import pytest

import terrine as tr
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
    facts: [order, quantity, amount, weight]
    properties: [heaviness]
    dimensions: []
facts:
  order: {{source: orders, column: null}}
  quantity: {{source: orders, column: qty}}
  amount: {{source: orders, column: amount}}
  weight: {{source: orders, column: weight}}
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
metrics:
  orders: {{kind: mean, entity: user, measure: order_count}}
  heavy_orders: {{kind: mean, entity: user, measure: heavy_order_count}}
  quantity: {{kind: mean, entity: user, measure: quantity_total}}
  spend: {{kind: mean, entity: user, measure: amount_total}}
  average_spend: {{kind: mean, entity: user, measure: amount_average}}
  weight: {{kind: mean, entity: user, measure: weight_total}}
  spend_per_order: {{kind: ratio, entity: user, numerator: amount_total, denominator: order_count}}
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
        f"qty BIGINT, amount {amount_type}, weight DOUBLE PRECISION)"
    )
    con.raw_sql("INSERT INTO users VALUES " + ", ".join(f"('{user}')" for user in _USERS))
    con.raw_sql(
        "INSERT INTO events VALUES "
        + ", ".join(
            f"('{user}', '{at}+00', {qty}, {amount}, {weight})"
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


@pytest.mark.parametrize("metric", ["average_spend", "spend_per_order", "weight"])
def test_quotients_and_floats_refuse_on_postgres(postgres_scratch, tmp_path, metric):  # noqa: F811
    con = postgres_scratch.con
    _load(con, timestamp_type="timestamptz", amount_type="numeric(12, 2)")
    model = _model(con, tmp_path, "postgres")
    with pytest.raises(TerrineError) as error:
        _design(value=tr.agg(metric, window=("-1d", "0s"))).build(model)
    assert error.value.code == "terrine.temporal.float64_unsupported"
