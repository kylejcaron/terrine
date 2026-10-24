import ibis
import pyarrow as pa
import pytest

from terrine.errors import TerrineError
from terrine.temporal.first import last_event_value


def _relations(con):
    origin = 1_735_689_600_000_000_000
    events = con.create_table(
        "last_events",
        pa.table(
            {
                "unit": ["u", "u", "u"],
                "event_id": ["z", "a", "b"],
                "event_time": pa.array(
                    [origin + 1, origin + 1, origin + 2], type=pa.timestamp("ns")
                ),
                "value": [10, 20, 30],
            }
        ),
    )
    candidates = con.create_table(
        "last_candidates",
        pa.table(
            {
                "unit": ["u", "u", "absent"],
                "anchor_id": ["a1", "a2", "a3"],
                "lower": pa.array([origin, origin, origin], type=pa.timestamp("ns")),
                "upper": pa.array([origin + 2, origin + 3, origin + 3], type=pa.timestamp("ns")),
            }
        ),
    )
    return origin, events, candidates


def test_last_event_value_orders_by_time_then_canonical_event_id():
    con = ibis.duckdb.connect()
    try:
        _, events, candidates = _relations(con)
        result = last_event_value(
            events,
            candidates,
            unit="unit",
            timestamp="event_time",
            value="value",
            lower="lower",
            upper="upper",
            event_id="event_id",
        )
        result.validate(con.to_pyarrow)
        actual = con.to_pyarrow(result.table).sort_by("anchor_id").to_pylist()

        assert [row["last_value"] for row in actual] == [20, 30, None]
        assert [row["last_event_id"] for row in actual] == ["a", "b", None]
        assert [row["no_event"] for row in actual] == [False, False, True]
    finally:
        con.disconnect()


def test_last_event_value_promotes_millisecond_timestamps_losslessly():
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
                    "value": [10, 20],
                }
            ),
        )
        # A microsecond upper bound between the two events keeps the later one out.
        candidates = con.create_table(
            "millisecond_candidates",
            pa.table(
                {
                    "unit": ["u"],
                    "anchor_id": ["a1"],
                    "lower": pa.array([instant_ms * 1000], type=pa.timestamp("us")),
                    "upper": pa.array([instant_ms * 1000 + 1500], type=pa.timestamp("us")),
                }
            ),
        )
        result = last_event_value(
            events,
            candidates,
            unit="unit",
            timestamp="event_time",
            value="value",
            lower="lower",
            upper="upper",
            event_id="event_id",
        )
        result.validate(con.to_pyarrow)
        actual = con.to_pyarrow(result.table).to_pylist()
        assert [row["last_value"] for row in actual] == [10]
        assert actual[0]["last_timestamp"].microsecond == 1000
    finally:
        con.disconnect()


def test_last_event_value_preserves_value_named_like_internal_rank():
    con = ibis.duckdb.connect()
    try:
        _, events, candidates = _relations(con)
        events = events.mutate(__terrine_last_rank=events.value).drop("value")
        result = last_event_value(
            events,
            candidates,
            unit="unit",
            timestamp="event_time",
            value="__terrine_last_rank",
            lower="lower",
            upper="upper",
            event_id="event_id",
        )
        result.validate(con.to_pyarrow)
        actual = con.to_pyarrow(result.table).sort_by("anchor_id").to_pylist()
        assert [row["last_value"] for row in actual] == [20, 30, None]
    finally:
        con.disconnect()


def test_last_event_allows_output_named_like_event_value():
    con = ibis.duckdb.connect()
    try:
        _, events, candidates = _relations(con)
        events = events.mutate(last_value=events.value).drop("value")
        result = last_event_value(
            events,
            candidates,
            unit="unit",
            timestamp="event_time",
            value="last_value",
            lower="lower",
            upper="upper",
            event_id="event_id",
        )
        result.validate(con.to_pyarrow)
        actual = con.to_pyarrow(result.table).sort_by("anchor_id").to_pylist()
        assert [row["last_value"] for row in actual] == [20, 30, None]
    finally:
        con.disconnect()


def test_last_event_lookup_rejects_ambiguous_event_ids():
    con = ibis.duckdb.connect()
    try:
        _, events, candidates = _relations(con)
        duplicate_events = events.mutate(event_id=ibis.literal("duplicate"))
        result = last_event_value(
            duplicate_events,
            candidates,
            unit="unit",
            timestamp="event_time",
            value="value",
            lower="lower",
            upper="upper",
            event_id="event_id",
        )
        with pytest.raises(TerrineError) as error:
            result.validate(con.to_pyarrow)
        assert error.value.code == "terrine.temporal.duplicate_event_id"
    finally:
        con.disconnect()
