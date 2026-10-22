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

_CATALOG = """schema_version: 1
day_boundary: UTC
dialect: {dialect}
sources:
  orders:
    relation: {{kind: table, name: events}}
    timestamp: occurred_at
    entities: {{user: user_id}}
    facts: [order, amount]
    properties: [tier]
    dimensions: []
  states:
    relation: {{kind: table, name: states}}
    timestamp: observed_at
    entities: {{user: user_id}}
    facts: []
    properties: [state]
    dimensions: []
facts:
  order: {{source: orders, column: null}}
  amount: {{source: orders, column: amount}}
properties:
  tier: {{source: orders, column: tier, dtype: string, as_of: event_time}}
  state: {{source: states, column: state, dtype: string, as_of: event_time}}
dimensions: {{}}
measures:
  order_count: {{fact: order, aggregation: count}}
  amount_total: {{fact: amount, aggregation: sum}}
metrics:
  orders: {{kind: mean, entity: user, measure: order_count}}
  spend: {{kind: mean, entity: user, measure: amount_total}}
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
    "schedule": tr.Design(
        "user",
        tr.Schedule("1d", "2025-01-01", "2025-01-05"),
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
    "observed_state_panel": tr.Design(
        "user",
        tr.Schedule("1d", "2025-01-02", "2025-01-05"),
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


def _both(postgres_scratch, tmp_path, **model_options):
    duckdb = ibis.duckdb.connect()
    _load(duckdb, timestamp_type="TIMESTAMPTZ", amount_type="DECIMAL(10, 2)")
    _load(postgres_scratch.con, timestamp_type="timestamptz", amount_type="numeric(10, 2)")
    return (
        _model(duckdb, tmp_path, "duckdb", **model_options),
        _model(postgres_scratch.con, tmp_path, "postgres", **model_options),
    )


@pytest.mark.parametrize("name", sorted(_DESIGNS))
def test_design_matches_duckdb(postgres_scratch, tmp_path, name):  # noqa: F811
    duckdb, postgres = _both(postgres_scratch, tmp_path)
    design = _DESIGNS[name]
    expected = _rows(duckdb, design)
    actual = _rows(postgres, design)
    assert expected, "the DuckDB reference produced no rows"
    assert actual == expected


def test_uuid_event_ids_break_first_event_ties(postgres_scratch, tmp_path):  # noqa: F811
    duckdb, postgres = _both(postgres_scratch, tmp_path, event_ids={"orders": "event_id"})
    design = _DESIGNS["time_to_first_order"]
    assert _rows(postgres, design) == _rows(duckdb, design)


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
