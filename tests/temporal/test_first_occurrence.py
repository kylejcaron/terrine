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
                "event_id": ["z", "a", "b", "c", "a"],
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


def test_mixed_timestamp_precisions_preserve_half_open_bounds():
    con = ibis.duckdb.connect()
    try:
        instant_us = 1_735_689_600_000_000
        events = con.create_table(
            "mixed_events",
            pa.table(
                {
                    "unit": ["u"],
                    "event_id": ["e"],
                    "event_time": pa.array([instant_us], type=pa.timestamp("us", tz="UTC")),
                }
            ),
        )
        candidates = con.create_table(
            "mixed_candidates",
            pa.table(
                {
                    "unit": ["u"],
                    "anchor_id": ["a"],
                    "lower": pa.array([instant_us * 1000 - 1], type=pa.timestamp("ns")),
                    "upper": pa.array([instant_us * 1000 + 1], type=pa.timestamp("ns")),
                }
            ),
        )
        result = _table(con, _lookup(events, candidates))
        assert result.column("first_event_id").to_pylist() == ["e"]
    finally:
        con.disconnect()


def test_millisecond_timestamps_promote_losslessly_to_microseconds():
    con = ibis.duckdb.connect()
    try:
        instant_ms = 1_735_689_600_000
        events = con.create_table(
            "millisecond_events",
            pa.table(
                {
                    "unit": ["u", "u"],
                    "event_id": ["a", "b"],
                    "event_time": pa.array(
                        [instant_ms + 1, instant_ms + 2], type=pa.timestamp("ms")
                    ),
                }
            ),
        )
        # A microsecond lower bound between the two events keeps the first one out.
        candidates = con.create_table(
            "millisecond_candidates",
            pa.table(
                {
                    "unit": ["u"],
                    "anchor_id": ["a"],
                    "lower": pa.array([instant_ms * 1000 + 1500], type=pa.timestamp("us")),
                    "upper": pa.array([instant_ms * 1000 + 5000], type=pa.timestamp("us")),
                }
            ),
        )
        result = _table(con, _lookup(events, candidates))
        assert result.column("first_event_id").to_pylist() == ["b"]
        assert result.column("first_timestamp").cast(pa.int64()).to_pylist() == [
            instant_ms * 1000 + 2000
        ]
    finally:
        con.disconnect()


def test_unspecified_timestamp_scale_uses_native_microsecond_precision():
    con = ibis.duckdb.connect()
    try:
        events, _ = _relations(con)
        candidates = con.sql(
            "SELECT 'u1' AS unit, 'a' AS anchor_id, "
            "TIMESTAMPTZ '2025-01-01 00:00:00+00' AS lower, "
            "TIMESTAMPTZ '2025-01-01 00:00:01+00' AS upper"
        )
        result = _table(con, _lookup(events, candidates))
        assert result.num_rows == 1
    finally:
        con.disconnect()


def test_lookup_preserves_fresh_memtable_dependencies():
    con = ibis.duckdb.connect()
    try:
        events, candidates = _relations(con)
        annotation = ibis.memtable(pa.table({"unit": ["u1"], "flag": [True]}))
        decorated = events.join(annotation, events.unit == annotation.unit).select(
            unit=events.unit, event_id=events.event_id, event_time=events.event_time
        )
        result = _table(con, _lookup(decorated, candidates))
        assert result.column("anchor_id").to_pylist()
    finally:
        con.disconnect()


def test_lookup_avoids_candidate_column_collisions_and_null_bounds_are_no_event():
    con = ibis.duckdb.connect()
    try:
        events, candidates = _relations(con)
        candidates = candidates.mutate(
            __terrine_event_id=ibis.literal("candidate metadata"),
            __terrine_first_lower_key=ibis.literal("candidate key"),
        )
        candidates = candidates.mutate(upper=ibis.null().cast(candidates.upper.type()))
        result = _table(con, _lookup(events, candidates)).to_pylist()
        assert len(result) == 3
        assert all(row["first_timestamp"] is None for row in result)
        assert all(row["first_event_id"] is None for row in result)
        assert all(row["no_event"] is True for row in result)
        assert all(row["__terrine_event_id"] == "candidate metadata" for row in result)
    finally:
        con.disconnect()


@pytest.mark.parametrize("binding", ["unbound", "different_connection"])
def test_first_occurrence_refuses_relations_without_one_supported_connection(binding):
    con = ibis.duckdb.connect()
    other = ibis.duckdb.connect()
    try:
        events, candidates = _relations(con)
        if binding == "unbound":
            events = ibis.table(events.schema())
            expected_backend = "unbound"
        else:
            _, candidates = _relations(other)
            expected_backend = "duckdb"
        with pytest.raises(TerrineError) as caught:
            _lookup(events, candidates)
        assert caught.value.code == "terrine.temporal.first_occurrence_unsupported_backend"
        assert dict(caught.value.context) == {"backend": expected_backend}
    finally:
        other.disconnect()
        con.disconnect()
