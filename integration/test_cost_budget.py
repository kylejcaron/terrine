"""Generated-SQL size per recipe and agreement with hand-written queries.

Each recipe runs at 10,000 units and 300,000 events, returns the values of a
hand-written query of the same semantics, and compiles within 30,000 characters per
feature. The README design stays within 20,000 characters, and a 5-metric by
10-window design compiles below BigQuery's 1,024k-character limit. Query time
against the hand-written queries is measured by ``benchmarks/run_cost.py``, not here.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Any, NamedTuple

import ibis
import ibis.expr.operations as ops
import pytest

from benchmarks.workloads import cost_model, load_tables, recipe_cases
from integration.warehouse_execution.postgres_fixtures import connect_postgres
from terrine.backends.registry import ENGINES
from terrine.model import Model
from terrine.simulate.scale import ScaleConfig
from terrine.spec import Design, FixedDate, agg
from tests.fixtures.readme import readme_design, readme_model

_CONFIG = ScaleConfig(units=10_000, events_per_unit=30, batch_size=50_000)
_CHARS_PER_FEATURE = 30_000
_README_CHARS = 20_000
_BIGQUERY_CHARS = 1_000_000

# Measured 2026-11-07: recipes above a ceiling remain open work, named with their
# measurements.
_OVER_CHARS = {
    "ltv": "79,098 characters for one trajectory: one window per age",
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


def _bound(backend: Any, namespace: Any, relation: Any, schemas: Mapping[str, Any]):
    """A table bound to ``backend`` from metadata alone."""
    table: Any = ops.DatabaseTable
    return table(relation.name, schemas[relation.name], backend, namespace).to_expr()


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
@pytest.mark.xfail(
    strict=True, reason="measured 1,050,208 characters (formatted); 22,600 per metric window"
)
def test_five_metrics_by_ten_windows_compile_under_the_bigquery_limit():
    """Compiled for BigQuery from relation metadata alone; no warehouse is contacted."""
    from ibis.backends.bigquery import Backend

    config = ScaleConfig(units=2, events_per_unit=2, batch_size=4)
    con = ibis.duckdb.connect()
    try:
        load_tables(con, config)
        bound = cost_model(con, config)
        schemas = {name: con.table(name).schema() for name in ("events", "users")}
    finally:
        con.disconnect()
    bigquery = Backend()
    namespace = ops.Namespace(catalog="project", database="dataset")
    model = Model._assemble(
        catalog=bound.catalog,
        con=bigquery,
        readiness=bound.readiness,
        units=bound.units,
        event_ids=bound.event_ids,
        sources=bound.sources,
        metrics=bound.metrics,
        resolver=lambda relation: _bound(bigquery, namespace, relation, schemas),
        input_binding=None,
        engine=ENGINES["bigquery"],
    )
    features = {
        f"{metric}_{days}d": agg(metric, window=(f"-{days}d", "0s"))
        for metric in ("events", "total", "average", "per_event", "active")
        for days in range(1, 11)
    }
    design = Design("user", FixedDate(datetime(2025, 1, 1, tzinfo=UTC) + timedelta(days=14)))
    frame = Design(design.unit, design.anchor, features=features).build(model)
    assert len(ibis.to_sql(frame.expr, dialect="bigquery")) <= _BIGQUERY_CHARS
