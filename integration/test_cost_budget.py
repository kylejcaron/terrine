"""Generated-SQL size per recipe and agreement with hand-written queries.

Each recipe runs at 10,000 units and 300,000 events, returns the values of a
hand-written query of the same semantics, and compiles within 30,000 characters per
feature. The README design stays within 20,000 characters. The 5-metric by
10-window BigQuery design checks the submitted statements against a UTF-8 byte
ceiling. Query time is measured by ``benchmarks/run_cost.py``, not here.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from statistics import median
from types import MappingProxyType, SimpleNamespace
from typing import Any, NamedTuple

import ibis
import pyarrow as pa
import pytest
import sqlglot
from sqlglot import exp

from benchmarks.compare_sql import _capture_study_sql
from benchmarks.run_overhead import compilations
from benchmarks.workloads import cost_model, load_tables, recipe_cases
from integration.warehouse_execution.postgres_fixtures import connect_postgres
from terrine import preview
from terrine.data import Budget, CoverageSummary, EligibilitySummary, Events, Roles, UnitSource
from terrine.errors import TerrineError
from terrine.metrics import count
from terrine.metrics import sum as sum_metric
from terrine.model import Model
from terrine.simulate.scale import ScaleConfig
from terrine.spec import Design, FixedDate, Schedule, agg, gt
from tests.execution.test_bigquery_session import offline_bigquery
from tests.fixtures.readme import readme_design, readme_model

_CONFIG = ScaleConfig(units=10_000, events_per_unit=30, batch_size=50_000)
_CHARS_PER_FEATURE = 30_000
_README_CHARS = 20_000
_BIGQUERY_BYTES = 1_000_000

# Measured 2026-11-07: recipes above a ceiling remain open work, named with their
# measurements.
_OVER_CHARS = {
    "life_table": "49,345 characters for one follow-up",
}


class Measured(NamedTuple):
    chars: int
    features: int
    equal: bool
    # The error a recipe's statement raised, if any.
    error: str | None = None


def _value(value: Any) -> Any:
    """Compare instants in UTC (naive values denote UTC) and integral decimals as ints."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return int(value)
    return value


def _rows(table, key: tuple[str, ...], values: tuple[str, ...]) -> dict[tuple, tuple]:
    rows = {}
    for row in table.to_pylist():
        rows[tuple(_value(row[name]) for name in key)] = tuple(_value(row[name]) for name in values)
    assert len(rows) == table.num_rows, "row keys must identify rows"
    return rows


def _measure(con: Any, dialect: str) -> Mapping[str, Measured]:
    load_tables(con, _CONFIG)
    model = cost_model(con, _CONFIG, dialect=dialect)
    measured = {}
    for name, case in recipe_cases(_CONFIG).items():
        frame = case.design.build(model)
        chars = len(ibis.to_sql(frame.expr))
        complete = [
            meta.complete_column
            for meta in frame.columns.values()
            if meta.complete_column is not None and meta.complete_column in frame.expr.columns
        ]
        try:
            flags = con.to_pyarrow(frame.expr.select(*complete)).to_pylist() if complete else []
            actual = con.to_pyarrow(frame.expr.select(*case.key, *case.values))
        except Exception as error:  # noqa: BLE001 - recorded for the recipe's own check
            measured[name] = Measured(chars, case.features, False, repr(error))
            continue
        assert all(all(row.values()) for row in flags), "the workload keeps every value complete"
        equal = _rows(actual, case.key, case.values) == _rows(
            con.sql(case.reference).to_pyarrow(), case.key, case.values
        )
        measured[name] = Measured(chars, case.features, equal)
    return MappingProxyType(measured)


def _cases(over: Mapping[str, str], what: str) -> list:
    return [
        pytest.param(
            name,
            marks=pytest.mark.xfail(strict=True, reason=f"measured {over[name]} {what}")
            if name in over
            else (),
        )
        for name in recipe_cases(_CONFIG)
    ]


@pytest.fixture(scope="module")
def duckdb_measured() -> Iterator[Mapping[str, Measured]]:
    con = ibis.duckdb.connect()
    try:
        measured = _measure(con, "duckdb")
    finally:
        con.disconnect()
    yield measured


@pytest.fixture(scope="module")
def postgres_measured(worker_id: str) -> Iterator[Mapping[str, Measured]]:
    schema = f"terrine_{worker_id}_{uuid.uuid4().hex[:12]}"
    admin = connect_postgres()
    admin.raw_sql(f'CREATE SCHEMA "{schema}"').close()
    con = connect_postgres(schema=schema)
    try:
        measured = _measure(con, "postgres")
    finally:
        con.disconnect()
        admin.raw_sql(f'DROP SCHEMA "{schema}" CASCADE').close()
        admin.disconnect()
    yield measured


@pytest.mark.scale
@pytest.mark.parametrize("recipe", _cases(_OVER_CHARS, "against 30,000 per feature"))
def test_recipe_sql_within_characters_per_feature(duckdb_measured, recipe):
    measured = duckdb_measured[recipe]
    assert measured.chars <= _CHARS_PER_FEATURE * measured.features


@pytest.mark.scale
@pytest.mark.parametrize("recipe", list(recipe_cases(_CONFIG)))
def test_recipe_matches_hand_written_query_on_duckdb(duckdb_measured, recipe):
    measured = duckdb_measured[recipe]
    assert measured.error is None and measured.equal


@pytest.mark.scale
@pytest.mark.postgres
@pytest.mark.parametrize("recipe", list(recipe_cases(_CONFIG)))
def test_recipe_matches_hand_written_query_on_postgres(postgres_measured, recipe):
    measured = postgres_measured[recipe]
    assert measured.error is None and measured.equal


@pytest.mark.scale
def test_readme_design_sql_within_budget():
    assert len(ibis.to_sql(readme_design().build(readme_model()).expr)) <= _README_CHARS


@pytest.mark.scale
def test_five_metrics_by_ten_windows_compile_under_the_bigquery_limit(monkeypatch):
    """Measure the guarded public read where Ibis hands SQL to the owned client."""

    class ReachedOutputDownload(Exception):
        pass

    submitted = []

    def has_output_limit(sql):
        return sql.rsplit(None, 2)[-2:] == [b"LIMIT", b"3"]

    def download(**kwargs):
        # The fixture measures SQL, not the numerical results of a warehouse run.
        raise ReachedOutputDownload

    def submit(sql):
        encoded = sql.encode("utf-8")
        submitted.append(encoded)
        if has_output_limit(encoded):
            return SimpleNamespace(to_arrow_iterable=download)
        query = sqlglot.parse_one(sql, read="bigquery")
        assert isinstance(query, exp.Select)
        projection = query.named_selects
        assert set(projection) <= {"invalid_count", "duplicate_count"}
        return SimpleNamespace(to_arrow=lambda **_: pa.table({name: [0] for name in projection}))

    config = ScaleConfig(units=2, events_per_unit=2, batch_size=4)
    con = ibis.duckdb.connect()
    try:
        load_tables(con, config)
        # Only table schemas, not DuckDB rows or its SQL, are used for BigQuery.
        schemas = {name: con.table(name).schema() for name in ("events", "users")}
    finally:
        con.disconnect()

    features = {
        f"{metric}_{days}d": agg(metric, window=(f"-{days}d", "0s"))
        for metric in ("events", "total", "average", "per_event", "active")
        for days in range(1, 11)
    }
    design = Design("user", FixedDate(datetime(2025, 1, 1, tzinfo=UTC) + timedelta(days=14)))
    with offline_bigquery(monkeypatch, schemas, submit) as (owner, admitted, _):
        model = cost_model(owner, config, dialect="bigquery")
        frame = Design(design.unit, design.anchor, features=features).build(model)
        with pytest.raises(TerrineError) as error:
            frame.materialize(budget=Budget(max_scan_bytes=1))
        assert error.value.code == "terrine.budget.scan_unknown"
        assert not admitted and not submitted
        with pytest.raises(ReachedOutputDownload):
            frame.materialize(budget=Budget(max_rows=2))

    sizes = [len(sql) for sql in submitted]
    print(
        "BigQuery UTF-8 bytes; "
        f"admitted: {[len(sql) for sql in admitted]}; submitted: {sizes}"
    )
    assert has_output_limit(submitted[-1])
    assert len(submitted) > 1 and not any(map(has_output_limit, submitted[:-1]))
    assert max(sizes) <= _BIGQUERY_BYTES, sizes
    assert submitted == admitted


def _fact_scans(node):
    details = node["extra_info"]
    if node.get("operator_type") == "TABLE_SCAN" and details.get("Table", "").endswith(".facts"):
        yield (
            node["operator_rows_scanned"],
            node["operator_cardinality"],
            details.get("Filters", ""),
        )
    for child in node["children"]:
        yield from _fact_scans(child)


@pytest.mark.scale
def test_bounded_fact_read_preserves_guarded_rows():
    """Profile the statement actually sent by the guarded public path."""
    con = ibis.duckdb.connect(threads=1)
    try:
        _bounded_fact_case(con)
    finally:
        con.disconnect()


def _bounded_fact_case(con):
    con.raw_sql("""
        CREATE TABLE roster AS SELECT unnest(['a', 'b', 'c']) AS id;
        CREATE TABLE facts AS
        SELECT * FROM (
            SELECT 'a' AS id, TIMESTAMPTZ '2020-01-01' + i * INTERVAL '1 minute' AS ts,
                   1.0::DOUBLE AS amount
            FROM range(600000) AS r(i)
            UNION ALL VALUES
                ('a', TIMESTAMPTZ '2025-01-01', 'nan'::DOUBLE),
                ('a', TIMESTAMPTZ '2025-01-02', 2.0),
                ('b', TIMESTAMPTZ '2025-01-31', NULL),
                ('a', TIMESTAMPTZ '2025-02-01', 3.0),
                ('a', TIMESTAMPTZ '2025-03-01', 4.0),
                ('b', TIMESTAMPTZ '2025-03-31', 'nan'::DOUBLE)
            UNION ALL
            SELECT 'a', TIMESTAMPTZ '2025-04-01' + i * INTERVAL '1 minute', 1.0
            FROM range(600000) AS r(i)
        ) ORDER BY ts;
    """)
    model = Model.from_tables(
        con,
        units={"person": UnitSource("roster", "id")},
        events={
            "purchase": Events(
                "facts",
                time="ts",
                key="id",
                complete=("2025-01-01", "2025-03-31"),
                metrics={"spend": sum_metric("amount"), "purchases": count()},
            )
        },
    )
    past, future = ("-30d", "0s"), ("0s", "30d")
    design = Design(
        "person",
        Schedule("1mo", "2025-02-01", "2025-03-01"),
        features={"past": agg("spend", window=past)},
        outcomes={"future": agg("spend", window=future)},
        eligibility={"bought": gt(agg("purchases", window=past), 0)},
    )
    start = time.perf_counter()
    with _capture_study_sql() as statements, compilations() as translations:
        result = design.build(model).materialize()
    public_seconds = time.perf_counter() - start
    _assert_bounded_rows(result)

    scans = []
    study_seconds = []
    for statement in statements:
        start = time.perf_counter()
        _, profile = con.con.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + statement).fetchone()
        study_seconds.append(round(time.perf_counter() - start, 6))
        scans.extend(_fact_scans(json.loads(profile)))
    print(
        f"public_seconds={public_seconds:.6f} compilations={len(translations)} "
        f"translation_seconds={sum(record['seconds'] for record in translations):.6f} "
        f"study_statements={len(statements)} study_profile_seconds={study_seconds} "
        f"fact_scans={scans}"
    )

    # Both errors apply. A window check on an eligibility-false candidate must
    # fail before the retained row limit, even though c cannot reach the output.
    con.raw_sql("INSERT INTO facts VALUES ('c', TIMESTAMPTZ '2025-03-20', 'nan'::DOUBLE)")
    with pytest.raises(TerrineError) as refused:
        design.build(model).materialize(budget=Budget(max_rows=1))
    assert refused.value.code == "terrine.temporal.float64_precision"
    assert refused.value.context["reason"] == "nonfinite_input"
    assert refused.value.context["failed_count"] == 1
    assert scans and all("2025-01-02" in f and "2025-03-31" in f for _, _, f in scans)


def _assert_bounded_rows(result):
    assert result.roles == Roles(
        unit="unit",
        anchor="anchor_id",
        t0="t0",
        treatment=None,
        features=("past",),
        outcomes=("future",),
    )
    assert result.provenance.eligibility_summary == EligibilitySummary(
        candidate=6,
        excluded_false=2,
        unknown=0,
        retained=4,
    )
    past, future = result.columns["past"], result.columns["future"]
    fields = [
        "unit",
        "anchor_id",
        "t0",
        "past",
        past.complete_column,
        past.reason_column,
        "future",
        future.complete_column,
        future.reason_column,
    ]
    selected = result.table.select(fields).sort_by([("unit", "ascending"), ("t0", "ascending")])
    assert selected.schema.types == [
        pa.string(),
        pa.string(),
        pa.timestamp("us", tz="UTC"),
        pa.float64(),
        pa.bool_(),
        pa.string(),
        pa.float64(),
        pa.bool_(),
        pa.string(),
    ]
    feb = datetime(2025, 2, 1, tzinfo=UTC)
    mar = datetime(2025, 3, 1, tzinfo=UTC)
    assert [tuple(row.values()) for row in selected.to_pylist()] == [
        ("a", "schedule:0", feb, 2.0, True, None, 7.0, True, None),
        ("a", "schedule:1", mar, 3.0, True, None, 4.0, True, None),
        ("b", "schedule:0", feb, None, True, "all_values_null", 0.0, True, None),
        ("b", "schedule:1", mar, None, True, "all_values_null", 0.0, True, None),
    ]


def _parquet_file_reads(profile):
    if "Total Files Read" in profile["extra_info"]:
        yield int(profile["extra_info"]["Total Files Read"])
    for child in profile["children"]:
        yield from _parquet_file_reads(child)


def _partitioned_orders(con, files, predicate=None):
    orders = con.con.read_parquet(files, hive_partitioning=True)
    if predicate is not None:
        orders = orders.filter(predicate)
    orders.create_view("orders", replace=True)


def _utc_month_bounds(span):
    first = span.start.date().replace(day=1)
    end = span.end_exclusive
    last = end.date().replace(day=1)
    if end != datetime(last.year, last.month, 1, tzinfo=UTC):
        last = date(last.year + (last.month == 12), last.month % 12 + 1, 1)
    return first, last


@pytest.mark.scale
def test_certified_event_month_prunes_files_without_changing_guarded_rows(tmp_path):
    """Replay the actual guarded study on immutable, construction-proven partitions."""
    con = ibis.duckdb.connect(threads=1)
    paths = str(tmp_path / "event-month")
    try:
        con.raw_sql("CREATE TABLE customers AS SELECT 'u' || i AS customer_id FROM range(101) t(i)")
        con.con.execute(
            """
            COPY (
                WITH events AS (
                    SELECT 'u' || (i % 100) AS customer_id,
                        month + (date_diff('second', month, month + INTERVAL '1 month')
                            * i // 16000) * INTERVAL '1 second' AS ordered_at,
                        CASE WHEN i % 7 <> 0 THEN (i % 11)::DOUBLE END AS amount
                    FROM range(TIMESTAMP '2022-01-01', TIMESTAMP '2026-01-01', INTERVAL '1 month')
                        AS months(month)
                    CROSS JOIN range(16000) AS events(i)
                )
                SELECT *, date_trunc('month', ordered_at)::DATE AS month_start FROM events
            ) TO ? (FORMAT PARQUET, PARTITION_BY month_start)
            """,
            [paths],
        )
        files = str(tmp_path / "event-month" / "*" / "*.parquet")
        # File reads are not an admitted Frame input. Capture against a native
        # copy of the same rows, then replay only its already-submitted SQL.
        con.con.read_parquet(files, hive_partitioning=True).create("orders")
        model, design = readme_model(con), readme_design()
        report = preview(design, model)
        (use,) = report.source_reads
        span = CoverageSummary(
            datetime(2025, 1, 2, tzinfo=UTC), datetime(2025, 3, 31, tzinfo=UTC)
        )
        assert use.event_span == span and use.physical_key is None
        start, end = _utc_month_bounds(span)
        assert (start, end) == (date(2025, 1, 1), date(2025, 4, 1))

        begin = time.perf_counter()
        with _capture_study_sql() as statements:
            design.build(model).materialize()
        public_seconds = time.perf_counter() - begin
        guarded_sql = statements[-1]
        con.raw_sql("DROP TABLE orders")
        variants = {
            "event_only": None,
            "event_and_month": f"month_start >= DATE '{start}' AND month_start < DATE '{end}'",
        }
        samples = {name: [] for name in variants}
        results = {}
        for iteration in range(4):
            for name in (variants if iteration % 2 == 0 else reversed(variants)):
                _partitioned_orders(con, files, variants[name])
                begin = time.perf_counter()
                result = con.con.execute(guarded_sql).to_arrow_table()
                if iteration:
                    samples[name].append(time.perf_counter() - begin)
                results[name] = result.sort_by([("unit", "ascending"), ("t0", "ascending")])
        baseline = results["event_only"]
        assert (baseline.num_rows, baseline.num_columns) == (202, 33)
        assert results["event_and_month"].equals(baseline)
        feb, mar = (row for row in baseline.to_pylist() if row["unit"] == "u0")
        assert (feb["orders_30d"], feb["orders_30d__reason"]) == (154, None)
        assert (mar["spend_next_30d"], mar["spend_next_30d__reason"]) == (
            None,
            "incomplete_coverage",
        )

        scans = {}
        for name, clause in variants.items():
            _partitioned_orders(con, files, clause)
            _, profile = con.con.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + guarded_sql).fetchone()
            scans[name] = list(_parquet_file_reads(json.loads(profile)))
        medians = {name: median(times) for name, times in samples.items()}
        print(
            f"public_seconds={public_seconds:.6f} "
            f"guarded_median_seconds={medians} "
            f"files_per_scan={scans}"
        )
        assert scans == {"event_only": [48, 48], "event_and_month": [3, 3]}

        # A January event ingested in April still contributes to the February window.
        paths = str(tmp_path / "ingestion-month")
        con.con.execute(
            """
            COPY (
                SELECT * FROM (VALUES
                    ('u0', TIMESTAMP '2025-01-15', 13.0::DOUBLE, DATE '2025-04-01'),
                    ('u0', TIMESTAMP '2025-01-20', NULL::DOUBLE, DATE '2025-01-01')
                ) AS t(customer_id, ordered_at, amount, month_start)
            ) TO ? (FORMAT PARQUET, PARTITION_BY month_start)
            """,
            [paths],
        )
        ingestion = str(tmp_path / "ingestion-month" / "*" / "*.parquet")
        _partitioned_orders(con, ingestion)
        full = con.con.execute(guarded_sql).to_arrow_table()
        _partitioned_orders(con, ingestion, variants["event_and_month"])
        wrong = con.con.execute(guarded_sql).to_arrow_table()
        assert _february_spend(full) == (13.0, None)
        assert _february_spend(wrong) == (None, "all_values_null")
    finally:
        con.disconnect()


def _february_spend(table):
    row = next(
        row for row in table.to_pylist() if row["unit"] == "u0" and row["t0"].month == 2
    )
    return row["spend_30d"], row["spend_30d__reason"]
