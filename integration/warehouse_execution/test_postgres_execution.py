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
    con.raw_sql(
        "CREATE TABLE events (unit text, ts timestamptz, amount numeric(20, 4))"
    ).close()
    con.raw_sql(
        "INSERT INTO events VALUES ('a', '2025-01-01 00:00:00.000001+00', 12345678901234.5678)"
    ).close()
    table = con.table("events", database=schema).to_pyarrow()
    assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")
    assert str(table["amount"][0].as_py()) == "12345678901234.5678"
    assert table["ts"][0].as_py().microsecond == 1
