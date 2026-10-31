"""PostgreSQL scheduled, event-anchored and as-of designs against DuckDB."""

from __future__ import annotations

from datetime import UTC, datetime

import ibis
import pytest

import terrine as tr
from integration.warehouse_execution.postgres_fixtures import (
    postgres_scratch as postgres_scratch,  # noqa: F401
)

pytestmark = pytest.mark.postgres

_CATALOG = """schema_version: 2
day_boundary: UTC
dialect: {dialect}
sources:
  orders:
    relation: {{kind: table, name: events}}
    time: occurred_at
    units: {{user: user_id}}
    events: [order, amount]
    properties: [tier]
    dimensions: []
  states:
    relation: {{kind: table, name: states}}
    time: observed_at
    units: {{user: user_id}}
    events: []
    properties: [state]
    dimensions: []
events:
  order: {{source: orders, column: null}}
  amount: {{source: orders, column: amount}}
properties:
  tier: {{source: orders, column: tier, dtype: string, as_of: event_time}}
  state: {{source: states, column: state, dtype: string, as_of: event_time}}
dimensions: {{}}
measures:
  order_count: {{event: order, aggregation: count}}
  amount_total: {{event: amount, aggregation: sum}}
metrics:
  orders: {{kind: mean, unit: user, measure: order_count}}
  spend: {{kind: mean, unit: user, measure: amount_total}}
"""

# Units interleave in time so an as-of lookup must match within the unit, not
# merely pick the globally nearest earlier row.
_EVENTS = [
    ("a", "2025-01-01 08:00:00", "5.00", "bronze"),
    ("b", "2025-01-01 09:00:00", "1.00", "silver"),
    ("a", "2025-01-02 00:00:00", "2.50", "gold"),
    ("b", "2025-01-02 06:00:00", "4.00", None),
    ("a", "2025-01-03 12:00:00", "1.25", "gold"),
    ("c", "2025-01-03 23:59:59.999999", "9.99", "bronze"),
    ("b", "2025-01-04 00:00:00", "3.00", "gold"),
]
_USERS = ["a", "b", "c", "d"]
_STATES = [
    ("a", "2025-01-02 00:00:00", "active"),
    ("a", "2025-01-03 00:00:00", "active"),
    ("a", "2025-01-04 00:00:00", "closed"),
    ("b", "2025-01-02 00:00:00", "active"),
    ("b", "2025-01-03 00:00:00", "closed"),
    ("c", "2025-01-02 00:00:00", "active"),
    ("c", "2025-01-04 00:00:00", "active"),
]


def _load(con, *, timestamp_type: str, amount_type: str) -> None:
    con.raw_sql("CREATE TABLE users(user_id VARCHAR)")
    con.raw_sql(
        f"CREATE TABLE events(user_id VARCHAR, occurred_at {timestamp_type}, "
        f"amount {amount_type}, tier VARCHAR, event_id UUID)"
    )
    con.raw_sql("INSERT INTO users VALUES " + ", ".join(f"('{user}')" for user in _USERS))
    con.raw_sql(
        "INSERT INTO events VALUES "
        + ", ".join(
            f"('{user}', '{at}+00', {amount}, {'NULL' if tier is None else repr(tier)}, "
            f"'00000000-0000-0000-0000-{index:012d}')"
            for index, (user, at, amount, tier) in enumerate(_EVENTS)
        )
    )
    con.raw_sql(
        f"CREATE TABLE states(user_id VARCHAR, observed_at {timestamp_type}, state VARCHAR)"
    )
    con.raw_sql(
        "INSERT INTO states VALUES "
        + ", ".join(f"('{user}', '{at}+00', '{state}')" for user, at, state in _STATES)
    )


def _model(con, tmp_path, dialect: str, event_ids=None):
    path = tmp_path / f"{dialect}.yaml"
    path.write_text(_CATALOG.format(dialect=dialect), encoding="utf-8")
    return tr.Model.from_catalog(
        path,
        con=con,
        units={"user": tr.UnitSource("users", "user_id")},
        readiness={
            "orders": tr.Coverage("2024-12-01", "2025-02-01"),
            "states": tr.Coverage("2024-12-01", "2025-02-01"),
        },
        event_ids=event_ids,
    )


_BUDGET = tr.Budget(max_rows=1_000, max_output_bytes=1_000_000, strict_scan=False)
_DESIGNS = {
    "as_of_interleaved_units": tr.Design(
        "user",
        tr.FixedDate("2025-01-02T03:00:00+00:00"),
        features={
            "tier_now": tr.asof("tier", before="0d"),
            "tier_day_before": tr.asof("tier", before="-1d"),
            "spend_before": tr.agg("spend", window=("-2d", "0s")),
        },
        outcomes={},
    ),
    "last_event_time": tr.Design(
        "user",
        tr.FixedDate("2025-01-04T00:00:00+00:00"),
        features={"last_order_at": tr.event_time("order", window=("-4d", "0s"), occurrence="last")},
    ),
    "schedule": tr.Design(
        "user",
        tr.Schedule("1d", "2025-01-01", "2025-01-04"),
        features={"orders_before": tr.agg("orders", window=("-1d", "0s"))},
        outcomes={"spend_after": tr.agg("spend", window=("0s", "1d"))},
    ),
    "first_event_anchor": tr.Design(
        "user",
        tr.EventAnchor("order", which="first"),
        features={},
        outcomes={
            "orders_next_day": tr.agg("orders", window=("0s", "1d")),
            "spend_next_two_days": tr.agg("spend", window=("0s", "2d")),
        },
    ),
    "time_to_first_order": tr.Design(
        "user",
        tr.FixedDate("2025-01-01T12:00:00+00:00"),
        shape=tr.TimeToEvent("order"),
        administrative_cutoff=datetime(2025, 1, 4, tzinfo=UTC),
    ),
    # Entry and exit coverages are narrowed against the backend's timestamp range.
    "time_to_order_with_entry_and_exit": tr.Design(
        "user",
        tr.FixedDate("2025-01-01T12:00:00+00:00"),
        shape=tr.TimeToEvent("order", entry="amount", delayed_entry=True, exit="amount"),
        administrative_cutoff=datetime(2025, 1, 4, tzinfo=UTC),
    ),
    "observed_state_panel": tr.Design(
        "user",
        tr.Schedule("1d", "2025-01-02", "2025-01-04"),
        shape=tr.StatePanel(
            tr.StateObservations("state", ("active", "closed"), absorbing=("closed",)),
            max_periods=3,
        ),
    ),
}


def _rows(model, design):
    table = design.build(model).materialize(_BUDGET).table
    kept = [name for name in table.column_names if name != "anchor_id"]
    return sorted(table.select(kept).to_pylist(), key=lambda row: (row["unit"], row["t0"]))


def _both(postgres_scratch, tmp_path, *, naive=False, **model_options):
    duckdb = ibis.duckdb.connect()
    _load(
        duckdb, timestamp_type="TIMESTAMP" if naive else "TIMESTAMPTZ", amount_type="DECIMAL(10, 2)"
    )
    _load(
        postgres_scratch.con,
        timestamp_type="timestamp" if naive else "timestamptz",
        amount_type="numeric(10, 2)",
    )
    return (
        _model(duckdb, tmp_path, "duckdb", **model_options),
        _model(postgres_scratch.con, tmp_path, "postgres", **model_options),
    )


# Naive source timestamps denote UTC and take each backend's UTC conversion path.
_NAIVE_DESIGNS = ("first_event_anchor", "observed_state_panel", "schedule")


@pytest.mark.parametrize(
    ("name", "naive"),
    [(name, False) for name in sorted(_DESIGNS)] + [(name, True) for name in _NAIVE_DESIGNS],
)
def test_design_matches_duckdb(postgres_scratch, tmp_path, name, naive):  # noqa: F811
    duckdb, postgres = _both(postgres_scratch, tmp_path, naive=naive)
    design = _DESIGNS[name]
    expected = _rows(duckdb, design)
    actual = _rows(postgres, design)
    assert expected, "the DuckDB reference produced no rows"
    assert actual == expected


@pytest.mark.parametrize("naive", [False, True])
def test_landmark_nth_anchor_matches_original_time_oracle(postgres_scratch, tmp_path, naive):  # noqa: F811
    duckdb, postgres = _both(postgres_scratch, tmp_path, naive=naive)
    design = tr.Design(
        "user",
        tr.EventAnchor("order", which="nth", n=2, offset="1d"),
        anchor_range=tr.AnchorRange("2025-01-02", "2025-01-02"),
        features={"day_one_orders": tr.agg("orders", window=("-1d", "0s"))},
    )
    expected = _rows(duckdb, design)
    actual = _rows(postgres, design)
    assert actual == expected
    assert [(row["unit"], row["t0"], row["day_one_orders"]) for row in actual] == [
        ("a", datetime(2025, 1, 3, tzinfo=UTC), 1),
        ("b", datetime(2025, 1, 3, 6, tzinfo=UTC), 1),
    ]
    assert {row["anchor_provenance"] for row in actual} == {"event:order:nth:offset:86400"}


def test_uuid_event_ids_break_first_event_ties(postgres_scratch, tmp_path):  # noqa: F811
    from terrine.temporal.first import first_occurrence

    duckdb, postgres = _both(postgres_scratch, tmp_path, event_ids={"orders": "event_id"})
    design = _DESIGNS["time_to_first_order"]
    assert _rows(postgres, design) == _rows(duckdb, design)
    for model in (duckdb, postgres):
        con = model.con
        con.raw_sql("ALTER TABLE events ALTER COLUMN user_id SET NOT NULL")
        con.raw_sql(
            "INSERT INTO events VALUES ('a', '2025-01-02 00:00:00+00', 2.50, 'gold', "
            "'00000000-0000-0000-0000-000000000100')"
        )
        candidates = con.sql(
            "SELECT user_id, 'anchor' AS anchor_id, TIMESTAMPTZ '2025-01-02' AS lower, "
            "TIMESTAMPTZ '2025-01-03' AS upper FROM (VALUES ('a'), ('a')) AS r(user_id)"
        )
        result = first_occurrence(
            con.table("events"),
            candidates,
            unit="user_id",
            timestamp="occurred_at",
            lower="lower",
            upper="upper",
            event_id="event_id",
        )
        result.validate(con.to_pyarrow)
        rows = con.to_pyarrow(
            result.table.select(
                "first_timestamp", event_id=result.table.first_event_id.cast("string")
            )
        ).to_pylist()
        assert (
            rows
            == [
                {
                    "first_timestamp": datetime(2025, 1, 2, tzinfo=UTC),
                    "event_id": "00000000-0000-0000-0000-000000000002",
                }
            ]
            * 2
        )
        assert "LATERAL" not in con.compile(result.table).upper()


@pytest.mark.parametrize("statistic", ["total", "mean"])
def test_cohort_summaries_match_duckdb(postgres_scratch, tmp_path, statistic):  # noqa: F811
    duckdb, postgres = _both(postgres_scratch, tmp_path)
    design = tr.Design(
        "user",
        tr.EventAnchor("order", which="first"),
        shape=tr.CohortSpec("order", tr.Trajectory("spend", "1d", periods=2, cumulative=True)),
        administrative_cutoff=datetime(2025, 1, 6, tzinfo=UTC),
    )
    grouping = tr.CohortGrouping("1d", datetime(2025, 1, 1, tzinfo=UTC), statistic=statistic)

    def summary(model):
        cohorts = design.build(model).cohorts(grouping=grouping, value_column="cumulative_value")
        rows = cohorts.to_pyarrow(budget=_BUDGET).to_pylist()
        return sorted(rows, key=lambda row: tuple(str(value) for value in row.values()))

    expected = summary(duckdb)
    assert expected, "the DuckDB reference produced no rows"
    assert summary(postgres) == expected


def test_late_timestamp_outside_the_window_leaves_results_unchanged(postgres_scratch, tmp_path):  # noqa: F811
    con = postgres_scratch.con
    _load(con, timestamp_type="timestamptz", amount_type="numeric(10, 2)")
    model = _model(con, tmp_path, "postgres")
    design = _DESIGNS["as_of_interleaved_units"]
    before = _rows(model, design)
    # Valid in PostgreSQL, but past the BIGINT Unix-microsecond range.
    con.raw_sql(
        "INSERT INTO events VALUES ('a', '294250-01-01 00:00:00+00', 1.00, 'platinum', "
        "'00000000-0000-0000-0000-999999999999')"
    ).close()
    assert _rows(model, design) == before


@pytest.mark.parametrize("recipe,anchor_join_count", [("forecast", 3), ("ltv", 4)])
def test_forecast_and_ltv_use_set_based_lookups_and_anchor_joins(
    postgres_scratch,
    recipe,
    anchor_join_count,  # noqa: F811
):
    con = postgres_scratch.con
    con.raw_sql(
        "CREATE TABLE customers AS SELECT i::bigint AS customer_id "
        "FROM generate_series(0, 999) AS s(i)"
    ).close()
    con.raw_sql(
        "CREATE TABLE orders AS SELECT (i % 1000)::bigint AS customer_id, "
        "TIMESTAMP '2024-01-01' + (i * 1009 % 31536000) * INTERVAL '1 second' AS ordered_at, "
        "(i % 10000)::double precision / 100 AS amount FROM generate_series(0, 29999) AS s(i)"
    ).close()
    con.raw_sql("ALTER TABLE customers ALTER COLUMN customer_id SET NOT NULL").close()
    con.raw_sql("ALTER TABLE orders ALTER COLUMN customer_id SET NOT NULL").close()
    con.raw_sql("ALTER TABLE orders ALTER COLUMN ordered_at SET NOT NULL").close()
    for name in ("customers", "orders"):
        con.raw_sql(f"ANALYZE {name}").close()
    model = tr.Model.from_tables(
        con,
        units={"customer": tr.UnitSource("customers", "customer_id")},
        events={
            "orders": tr.Events(
                "orders",
                time="ordered_at",
                key="customer_id",
                metrics={"orders": tr.count(), "spend": tr.sum("amount")},
                complete=("2024-01-01", "2024-12-31"),
            )
        },
    )
    design = {
        "forecast": tr.recipes.time_series(
            every="28d",
            start="2024-03-01",
            end="2024-12-01",
            features={
                "orders_30d": tr.agg("orders", window=("-30d", "0s")),
                "spend_30d": tr.agg("spend", window=("-30d", "0s")),
            },
            outcomes={"spend_next_30d": tr.agg("spend", window=("0s", "30d"))},
        ),
        "ltv": tr.recipes.ltv(start_event="orders", metric="spend", period="30d", periods=4),
    }[recipe]
    sql = con.compile(design.build(model).expr)
    if recipe == "ltv":
        # Prove equijoin plans are available even when start-event cardinality
        # estimates favor nested loops. The forecast uses default planner settings.
        con.raw_sql("SET enable_nestloop = off").close()
    with con.raw_sql(f"EXPLAIN (FORMAT JSON) {sql}") as cursor:
        plan = cursor.fetchone()[0][0]["Plan"]

    def nodes(node):
        yield node
        for child in node.get("Plans", ()):
            yield from nodes(child)

    anchor_joins = [
        node
        for node in nodes(plan)
        if any(
            "anchor_id" in node.get(condition, "") or "schedule:" in node.get(condition, "")
            for condition in ("Join Filter", "Hash Cond", "Merge Cond")
        )
    ]
    problems = []
    if "LATERAL" in sql.upper():
        problems.append("prefix lookups contain correlated LATERAL subqueries")
    if len(anchor_joins) != anchor_join_count or any(
        node["Node Type"] not in {"Hash Join", "Merge Join"} for node in anchor_joins
    ):
        problems.append(
            f"anchor assembly is not {anchor_join_count} equijoins: "
            f"{[n['Node Type'] for n in anchor_joins]}"
        )
    assert not problems, problems


def test_nullable_array_keys_keep_scalar_distinctness(postgres_scratch):  # noqa: F811
    from terrine.backends.joins import null_safe_equal

    con = postgres_scratch.con
    keys = con.sql(
        "SELECT k FROM (VALUES (NULL::bigint[]), (ARRAY[]::bigint[]), "
        "(ARRAY[0]::bigint[]), (ARRAY[NULL]::bigint[])) AS keys(k)"
    )
    left = keys.select(a=keys.k)
    right = keys.select(b=keys.k).view()
    pairs = left.cross_join(right)
    rows = con.to_pyarrow(
        pairs.select(
            expected=pairs.a.identical_to(pairs.b),
            actual=null_safe_equal(pairs.a, pairs.b),
        )
    ).to_pylist()
    assert len(rows) == 16
    assert all(row["actual"] is row["expected"] for row in rows)


@pytest.mark.parametrize("roster_type,event_type", [("BIGINT", "INTEGER"), ("INTEGER", "BIGINT")])
@pytest.mark.parametrize("recipe", ["forecast", "first_occurrence"])
def test_mixed_integer_unit_widths_preserve_roster_keys(
    postgres_scratch,
    roster_type,
    event_type,
    recipe,  # noqa: F811
):
    empty_unit = 4_294_967_297 if roster_type == "BIGINT" else 2
    designs = {
        "forecast": tr.recipes.time_series(
            every="1d",
            start="2024-01-02",
            end="2024-01-03",
            features={"orders_1d": tr.agg("orders", window=("-1d", "0s"))},
            outcomes={"spend_next_1d": tr.agg("spend", window=("0s", "1d"))},
        ),
        "first_occurrence": tr.Design(
            "customer",
            tr.FixedDate("2024-01-01"),
            shape=tr.TimeToEvent("orders"),
            administrative_cutoff=datetime(2024, 1, 4, tzinfo=UTC),
        ),
    }
    duckdb = ibis.duckdb.connect()
    try:
        actual = []
        for con in (duckdb, postgres_scratch.con):
            con.raw_sql(f"CREATE TABLE customers(customer_id {roster_type})")
            con.raw_sql(
                f"CREATE TABLE orders(customer_id {event_type}, "
                "ordered_at TIMESTAMP, amount DOUBLE PRECISION)"
            )
            con.raw_sql(f"INSERT INTO customers VALUES (1), ({empty_unit})")
            con.raw_sql(
                "INSERT INTO orders VALUES "
                "(1, '2024-01-01 12:00:00', 10), (1, '2024-01-02 12:00:00', 20)"
            )
            if event_type == "BIGINT":
                con.raw_sql("INSERT INTO orders VALUES (4294967297, '2024-01-01 12:00:00', 99)")
            model = tr.Model.from_tables(
                con,
                units={"customer": tr.UnitSource("customers", "customer_id")},
                events={
                    "orders": tr.Events(
                        "orders",
                        key="customer_id",
                        time="ordered_at",
                        metrics={"orders": tr.count(), "spend": tr.sum("amount")},
                        complete=("2024-01-01", "2024-02-01"),
                    )
                },
            )
            frame = designs[recipe].build(model)
            assert frame.expr.unit.type() == con.table("customers").customer_id.type()
            table = frame.to_pyarrow(budget=_BUDGET)
            kept = [name for name in table.column_names if name != "anchor_id"]
            rows = sorted(table.select(kept).to_pylist(), key=lambda row: (row["unit"], row["t0"]))
            assert {row["unit"] for row in rows} == {1, empty_unit}
            if recipe == "forecast":
                assert sorted(
                    (row["orders_1d"], row["spend_next_1d"]) for row in rows if row["unit"] == 1
                ) == [(1, 0), (1, 20)]
            else:
                assert sorted((row["unit"], row["event_time"], row["event"]) for row in rows) == [
                    (1, datetime(2024, 1, 1, 12, tzinfo=UTC), True),
                    (empty_unit, None, False),
                ]
            actual.append(rows)
        assert actual[0] == actual[1]
    finally:
        duckdb.disconnect()
