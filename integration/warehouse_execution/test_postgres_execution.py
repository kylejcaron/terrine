"""Live PostgreSQL execution probes."""

from __future__ import annotations

import pyarrow as pa
import pytest

from integration.warehouse_execution.postgres_fixtures import (
    postgres_scratch as postgres_scratch,  # noqa: F401
)

pytestmark = pytest.mark.postgres


def test_scratch_schema_is_isolated_and_round_trips_exact_values(postgres_scratch):  # noqa: F811
    con, schema = postgres_scratch.con, postgres_scratch.schema
    assert int(con.version.split(".")[0]) >= 16
    con.raw_sql("CREATE TABLE events (unit text, ts timestamptz, amount numeric(20, 4))").close()
    con.raw_sql(
        "INSERT INTO events VALUES ('a', '2025-01-01 00:00:00.000001+00', 12345678901234.5678)"
    ).close()
    table = con.table("events", database=schema).to_pyarrow()
    assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")
    assert str(table["amount"][0].as_py()) == "12345678901234.5678"
    assert table["ts"][0].as_py().microsecond == 1


_CATALOG = """schema_version: 1
day_boundary: UTC
dialect: postgres
sources:
  sessions:
    relation: {kind: table, name: events}
    timestamp: occurred_at
    entities: {user: user_id}
    facts: [session]
    properties: []
    dimensions: []
facts:
  session: {source: sessions, column: null}
properties: {}
dimensions: {}
measures:
  session_count: {fact: session, aggregation: count}
metrics:
  sessions: {kind: mean, entity: user, measure: session_count}
"""


def test_first_dataset_counts_match_the_duckdb_tutorial(postgres_scratch, tmp_path):  # noqa: F811
    import terrine as tr

    con = postgres_scratch.con
    con.raw_sql("CREATE TABLE users(user_id text)").close()
    con.raw_sql("INSERT INTO users VALUES ('a'), ('b')").close()
    con.raw_sql("CREATE TABLE events(user_id text, occurred_at timestamptz)").close()
    con.raw_sql(
        "INSERT INTO events VALUES ('a', '2025-01-01 12:00:00+00'), "
        "('a', '2025-01-02 00:00:00+00'), ('a', '2025-01-02 12:00:00+00')"
    ).close()
    catalog = tmp_path / "catalog.yaml"
    catalog.write_text(_CATALOG, encoding="utf-8")
    model = tr.Model.from_catalog(
        catalog,
        con=con,
        units={"user": tr.UnitSource("users", "user_id")},
        readiness={"sessions": tr.Coverage("2025-01-01", "2025-01-03")},
    )
    design = tr.Design(
        "user",
        tr.FixedDate("2025-01-02"),
        features={"sessions_before": tr.agg("sessions", window=("-1d", "0s"))},
        outcomes={"sessions_after": tr.agg("sessions", window=("0s", "1d"))},
    )
    result = design.build(model).materialize(
        tr.Budget(max_rows=10, max_output_bytes=1_000_000, strict_scan=False)
    )
    rows = sorted(
        result.table.select(["unit", "sessions_before", "sessions_after"]).to_pylist(),
        key=lambda row: row["unit"],
    )
    assert rows == [
        {"unit": "a", "sessions_before": 1, "sessions_after": 2},
        {"unit": "b", "sessions_before": 0, "sessions_after": 0},
    ]
    assert result.provenance.execution_snapshot.backend == "postgres"
    assert result.provenance.engine_name == "postgres"
    assert result.provenance.dialect == "postgres"
    assert result.provenance.engine_status == "verified"
