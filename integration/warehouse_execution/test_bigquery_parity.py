"""BigQuery window metrics and study shapes against the DuckDB reference backend.

Data, catalogs and designs are shared with the PostgreSQL probes; only loading
differs, so the backends are checked on identical inputs.
"""

from __future__ import annotations

import itertools
from datetime import UTC, datetime
from typing import Literal

import ibis
import pytest

import terrine as tr
from integration.warehouse_execution import test_postgres_shapes as shapes
from integration.warehouse_execution import test_postgres_windows as windows
from integration.warehouse_execution.bigquery_fixtures import (
    bigquery_scratch as bigquery_scratch,  # noqa: F401
)

pytestmark = pytest.mark.bigquery


def _quote(value) -> str:
    return "NULL" if value is None else repr(value)


def _load_windows(scratch) -> None:
    scratch.sql("CREATE TABLE users(user_id STRING)")
    scratch.sql(
        "CREATE TABLE events(user_id STRING, occurred_at TIMESTAMP, qty INT64, "
        "amount NUMERIC(12, 2), weight FLOAT64, light FLOAT64)"
    )
    scratch.sql("INSERT users VALUES " + ", ".join(f"('{user}')" for user in windows._USERS))
    scratch.sql(
        "INSERT events VALUES "
        + ", ".join(
            f"('{user}', TIMESTAMP '{at}+00', {qty}, {amount}, {weight}, NULL)"
            for user, at, qty, amount, weight in windows._EVENTS
        )
    )


def _load_shapes(scratch) -> None:
    scratch.sql("CREATE TABLE users(user_id STRING)")
    scratch.sql(
        "CREATE TABLE events(user_id STRING, occurred_at TIMESTAMP, amount NUMERIC(10, 2), "
        "tier STRING, event_id STRING)"
    )
    scratch.sql("INSERT users VALUES " + ", ".join(f"('{user}')" for user in shapes._USERS))
    scratch.sql(
        "INSERT events VALUES "
        + ", ".join(
            f"('{user}', TIMESTAMP '{at}+00', {amount}, {_quote(tier)}, "
            f"'00000000-0000-0000-0000-{index:012d}')"
            for index, (user, at, amount, tier) in enumerate(shapes._EVENTS)
        )
    )
    scratch.sql("CREATE TABLE states(user_id STRING, observed_at TIMESTAMP, state STRING)")
    scratch.sql(
        "INSERT states VALUES "
        + ", ".join(
            f"('{user}', TIMESTAMP '{at}+00', '{state}')" for user, at, state in shapes._STATES
        )
    )


def _duckdb_windows():
    con = ibis.duckdb.connect()
    windows._load(con, timestamp_type="TIMESTAMPTZ", amount_type="DECIMAL(12, 2)")
    return con


def test_window_metrics_match_duckdb(bigquery_scratch, tmp_path):  # noqa: F811
    _load_windows(bigquery_scratch)
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
    actual = windows._rows(windows._model(bigquery_scratch.con, tmp_path, "bigquery"), design)
    windows._assert_close(actual, expected)


@pytest.mark.parametrize("name", sorted(shapes._DESIGNS))
def test_design_matches_duckdb(bigquery_scratch, tmp_path, name):  # noqa: F811
    _load_shapes(bigquery_scratch)
    duckdb = ibis.duckdb.connect()
    shapes._load(duckdb, timestamp_type="TIMESTAMPTZ", amount_type="DECIMAL(10, 2)")
    design = shapes._DESIGNS[name]
    expected = shapes._rows(shapes._model(duckdb, tmp_path, "duckdb"), design)
    actual = shapes._rows(shapes._model(bigquery_scratch.con, tmp_path, "bigquery"), design)
    assert expected, "the DuckDB reference produced no rows"
    assert actual == expected


def test_cohort_summaries_match_duckdb(bigquery_scratch, tmp_path):  # noqa: F811
    _load_shapes(bigquery_scratch)
    duckdb = ibis.duckdb.connect()
    shapes._load(duckdb, timestamp_type="TIMESTAMPTZ", amount_type="DECIMAL(10, 2)")
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

    for statistic in ("total", "mean"):
        expected = summary(shapes._model(duckdb, tmp_path, "duckdb"), statistic)
        actual = summary(shapes._model(bigquery_scratch.con, tmp_path, "bigquery"), statistic)
        assert expected, "the DuckDB reference produced no rows"
        windows._assert_close(actual, expected)


_INSTANTS = {
    "earliest": "0001-01-01 00:00:00+00",
    "before_epoch": "1969-12-31 23:59:59.999999+00",
    "epoch": "1970-01-01 00:00:00+00",
    "after_epoch": "1970-01-01 00:00:00.000001+00",
    "ordinary": "2025-06-01 12:34:56.789012+00",
    "latest": "9999-12-31 23:59:59.999999+00",
    "missing": None,
}


def test_timestamp_parts_and_comparisons_match_the_reference(bigquery_scratch):  # noqa: F811
    from integration.warehouse_execution import test_postgres_timestamps as reference
    from terrine.backends.dialect import dialect_scope
    from terrine.backends.timestamps import compare_timestamps, timestamp_parts

    scratch = bigquery_scratch
    scratch.sql("CREATE TABLE instants(name STRING, instant TIMESTAMP)")
    scratch.sql(
        "INSERT instants VALUES "
        + ", ".join(
            f"('{name}', {'NULL' if text is None else f'TIMESTAMP {text!r}'})"
            for name, text in _INSTANTS.items()
        )
    )
    table = scratch.con.table("instants")
    with dialect_scope("bigquery"):
        rank, seconds, nanos = timestamp_parts(table.instant)
        parts = {
            row["name"]: None
            if row["rank"] is None
            else (row["rank"], row["seconds"], row["nanos"])
            for row in table.select("name", rank=rank, seconds=seconds, nanos=nanos)
            .to_pyarrow()
            .to_pylist()
        }
        assert parts == {name: reference._reference(text) for name, text in _INSTANTS.items()}

        left = table.select(left_name=table.name, left_at=table.instant)
        right = table.select(right_name=table.name, right_at=table.instant)
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
                reference._reference(_INSTANTS[row["left_name"]]),
                reference._reference(_INSTANTS[row["right_name"]]),
                op,
                offset,
            )
            assert row[f"{op}_{index}"] == expected, (
                op,
                offset,
                row["left_name"],
                row["right_name"],
            )


def test_float_windows_match_duckdb_within_tolerance(bigquery_scratch, tmp_path):  # noqa: F811
    _load_windows(bigquery_scratch)
    design = windows._design(
        outcomes={"weight_after": tr.agg("weight", window=("0s", "1d"))},
        weight_before=tr.agg("weight", window=("-1d", "0s")),
        weight_week=tr.agg("weight", window=("-7d", "0s")),
        spend_per_weight=tr.agg("spend_per_weight", window=("-7d", "0s")),
    )
    expected = windows._rows(windows._model(_duckdb_windows(), tmp_path, "duckdb"), design)
    actual = windows._rows(windows._model(bigquery_scratch.con, tmp_path, "bigquery"), design)
    windows._assert_close(actual, expected)


@pytest.mark.parametrize(
    ("rows", "metric", "failure"),
    [
        # Non-finite history before a non-empty window refuses, as on DuckDB.
        (
            (("2024-12-20 00:00:00", "CAST('nan' AS FLOAT64)"), ("2025-01-01 12:00:00", "1.0")),
            "weight",
            "nonfinite_input",
        ),
        # Beyond BIGNUMERIC's exact range.
        ((("2025-01-01 12:00:00", "1e39"),), "weight", "bound_overflow"),
        # A dropped denominator must not read as a zero denominator.
        ((("2025-01-01 12:00:00", "1e39"),), "spend_per_weight", "bound_overflow"),
        # SAFE_DIVIDE returns NULL when the quotient leaves BIGNUMERIC's range.
        ((("2025-01-01 12:00:00", "1e-38"),), "spend_per_weight", "bound_overflow"),
        # Every prefix fits, but SAFE_SUBTRACT of the window's prefixes overflows.
        (
            (
                ("2024-12-20 00:00:00", "-4e38"),
                ("2024-12-31 12:00:00", "4e38"),
                ("2025-01-01 12:00:00", "4e38"),
            ),
            "weight",
            "bound_overflow",
        ),
    ],
)
def test_float_window_failures_are_coded_refusals(
    bigquery_scratch, tmp_path, rows, metric, failure
):  # noqa: F811
    from terrine.errors import TerrineError

    _load_windows(bigquery_scratch)
    bigquery_scratch.sql(
        "INSERT events VALUES "
        + ", ".join(f"('d', TIMESTAMP '{at}+00', 1, 10, {weight}, NULL)" for at, weight in rows)
    )
    design = windows._design(value=tr.agg(metric, window=("-3d", "0s")))
    with pytest.raises(TerrineError) as error:
        windows._rows(windows._model(bigquery_scratch.con, tmp_path, "bigquery"), design)
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failure"] == failure
