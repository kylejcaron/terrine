from __future__ import annotations

import ibis
import pyarrow as pa
import pytest

from terrine.errors import TerrineError
from terrine.temporal.first import first_occurrence


def _relations(con):
    epoch_ns = 1_735_689_600_000_000_000
    events = con.create_table(
        "first_events",
        pa.table(
            {
                "unit": ["u1", "u1", "u1", "u1", "u2"],
                "event_id": ["z", "a", "b", "c", "x"],
                "event_time": pa.array(
                    [epoch_ns + 1, epoch_ns + 1, epoch_ns + 2, epoch_ns + 3, epoch_ns + 4],
                    type=pa.timestamp("ns"),
                ),
            }
        ),
    )
    candidates = con.create_table(
        "first_candidates",
        pa.table(
            {
                "unit": ["u1", "u2", "u3"],
                "anchor_id": ["a1", "a2", "a3"],
                "lower": pa.array([epoch_ns] * 3, type=pa.timestamp("ns")),
                "upper": pa.array(
                    [epoch_ns + 4, epoch_ns + 5, epoch_ns + 5],
                    type=pa.timestamp("ns"),
                ),
            }
        ),
    )
    return events, candidates


def _table(con, result):
    result.validate(con.to_pyarrow)
    return con.to_pyarrow(result.table)


def _lookup(events, candidates):
    return first_occurrence(
        events,
        candidates,
        unit="unit",
        timestamp="event_time",
        lower="lower",
        upper="upper",
        event_id="event_id",
    )


def test_first_occurrence_orders_exact_timestamps_and_tied_ids():
    con = ibis.duckdb.connect()
    try:
        events, candidates = _relations(con)
        result = _table(con, _lookup(events, candidates))
        row = next(row for row in result.to_pylist() if row["anchor_id"] == "a1")
        timestamp_ns = result.column("first_timestamp").cast(pa.int64()).to_pylist()
        row_index = result.column("anchor_id").to_pylist().index("a1")
        assert row["first_event_id"] == "a"
        assert timestamp_ns[row_index] == 1_735_689_600_000_000_001
        assert row["no_event"] is False
    finally:
        con.disconnect()


def test_no_occurrence_retains_unit_with_null_timestamp_and_censor_flag():
    con = ibis.duckdb.connect()
    try:
        events, candidates = _relations(con)
        result = _table(con, _lookup(events, candidates))
        row = next(row for row in result.to_pylist() if row["anchor_id"] == "a3")
        assert row["unit"] == "u3"
        assert row["first_timestamp"] is None
        assert row["no_event"] is True
    finally:
        con.disconnect()


def test_first_occurrence_does_not_floor_time():
    con = ibis.duckdb.connect()
    try:
        events, candidates = _relations(con)
        result = _table(con, _lookup(events, candidates))
        ids = result.column("unit").to_pylist()
        times = result.column("first_timestamp").cast(pa.int64()).to_pylist()
        assert all(
            time == 1_735_689_600_000_000_001
            for unit, time in zip(ids, times, strict=True)
            if unit == "u1"
        )
    finally:
        con.disconnect()


def test_duplicate_event_ids_are_checked_when_result_validates():
    con = ibis.duckdb.connect()
    try:
        events, candidates = _relations(con)
        duplicate_events = events.mutate(event_id=ibis.literal("duplicate"))
        result = _lookup(duplicate_events, candidates)
        with pytest.raises(TerrineError) as error:
            result.validate(con.to_pyarrow)
        assert error.value.code == "terrine.temporal.duplicate_event_id"
    finally:
        con.disconnect()
