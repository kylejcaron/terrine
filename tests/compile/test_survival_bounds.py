from datetime import datetime

import ibis
import ibis.expr.datatypes as dt
import pytest

from terrine.compile.shapes.time_to_event import _narrow_bound


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        yield connection
    finally:
        connection.disconnect()


def _bound(con, lower, upper):
    table = con.sql(
        f"SELECT TIMESTAMPTZ '2500-01-01' AS value, "
        f"TIMESTAMP_NS '{lower}' AS lower, TIMESTAMP_NS '{upper}' AS upper"
    )
    result = _narrow_bound(table.value, table.lower, table.upper, dt.Timestamp(scale=9))
    return con.to_pyarrow(table.select(value=result.cast("string"))).to_pylist()


def test_wide_entry_coverage_start_is_intersected_before_narrowing(con):
    assert _bound(con, "-infinity", "infinity") == [{"value": "infinity"}]


def test_wide_exit_coverage_start_is_intersected_before_narrowing(con):
    # Follow-up intersects an out-of-range coverage bound with the finite axis.
    assert _bound(con, "2025-01-01", "2025-01-11") == [{"value": "2025-01-11 00:00:00"}]


def test_wide_roster_observation_end_is_intersected_before_narrowing(con):
    end = datetime(2025, 1, 12)
    assert _bound(con, "2024-12-30", end.isoformat()) == [{"value": "2025-01-12 00:00:00"}]
