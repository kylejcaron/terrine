from datetime import UTC, datetime

import ibis
import pyarrow as pa

from terrine.temporal.retention import retention_threshold_bands


def test_retention_band_uses_distinct_occurrences_and_finite_followup():
    con = ibis.duckdb.connect()
    try:
        origin = datetime(2025, 1, 1, tzinfo=UTC)
        anchors = con.create_table(
            "retention_anchors",
            pa.table(
                {
                    "unit": ["u1", "u2"],
                    "anchor_id": ["a1", "a2"],
                    "anchor_time": pa.array([origin, origin], type=pa.timestamp("us", tz="UTC")),
                    "observed_until": pa.array(
                        [datetime(2025, 1, 11, tzinfo=UTC), datetime(2025, 1, 5, tzinfo=UTC)],
                        type=pa.timestamp("us", tz="UTC"),
                    ),
                }
            ),
        )
        events = con.create_table(
            "retention_events",
            pa.table(
                {
                    "unit": ["u1", "u1", "u1", "u1", "u2"],
                    "event_id": ["e1", "e1", "e2", "wrong", "e3"],
                    "event_name": ["target", "target", "target", "other", "target"],
                    "event_time": pa.array(
                        [
                            datetime(2025, 1, 2, tzinfo=UTC),
                            datetime(2025, 1, 2, tzinfo=UTC),
                            datetime(2025, 1, 3, tzinfo=UTC),
                            datetime(2025, 1, 2, tzinfo=UTC),
                            datetime(2025, 1, 2, tzinfo=UTC),
                        ],
                        type=pa.timestamp("us", tz="UTC"),
                    ),
                }
            ),
        )
        result = retention_threshold_bands(
            events,
            anchors,
            unit="unit",
            event_id="event_id",
            event_name="event_name",
            target_event="target",
            timestamp="event_time",
            anchor_id="anchor_id",
            anchor_timestamp="anchor_time",
            observed_until="observed_until",
            finite_horizon_seconds=10 * 86400,
            count_thresholds=(1, 2),
        )
        actual = con.to_pyarrow(result).sort_by("anchor_id").to_pylist()
        assert [
            (row["occurrence_count"], row["retention_band"], row["complete"], row["reason"])
            for row in actual
        ] == [(2, 2, True, None), (None, None, False, "insufficient_followup")]
    finally:
        con.disconnect()


def test_open_ended_retention_requires_complete_source_cutoff():
    con = ibis.duckdb.connect()
    try:
        origin = datetime(2025, 1, 1, tzinfo=UTC)
        anchors = con.create_table(
            "open_anchors",
            pa.table(
                {
                    "unit": ["u"],
                    "anchor_id": ["a"],
                    "anchor_time": pa.array([origin], type=pa.timestamp("us", tz="UTC")),
                    "observed_until": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                }
            ),
        )
        events = con.create_table(
            "open_events",
            pa.table(
                {
                    "unit": ["u"],
                    "event_id": ["e"],
                    "event_name": ["target"],
                    "event_time": pa.array(
                        [datetime(2025, 1, 2, tzinfo=UTC)], type=pa.timestamp("us", tz="UTC")
                    ),
                }
            ),
        )
        result = retention_threshold_bands(
            events,
            anchors,
            unit="unit",
            event_id="event_id",
            event_name="event_name",
            target_event="target",
            timestamp="event_time",
            anchor_id="anchor_id",
            anchor_timestamp="anchor_time",
            observed_until="observed_until",
            source_complete_through=datetime(2025, 1, 3, tzinfo=UTC),
            count_thresholds=(1,),
        )
        row = con.to_pyarrow(result).to_pylist()[0]
        assert (
            row["occurrence_count"],
            row["retention_band"],
            row["complete"],
            row["reason"],
        ) == (1, 1, True, None)
        unavailable = retention_threshold_bands(
            events,
            anchors,
            unit="unit",
            event_id="event_id",
            event_name="event_name",
            target_event="target",
            timestamp="event_time",
            anchor_id="anchor_id",
            anchor_timestamp="anchor_time",
            observed_until="observed_until",
            source_complete_through=datetime(2024, 12, 31, tzinfo=UTC),
            count_thresholds=(1,),
        )
        unavailable_row = con.to_pyarrow(unavailable).to_pylist()[0]
        assert (
            unavailable_row["occurrence_count"],
            unavailable_row["retention_band"],
            unavailable_row["complete"],
            unavailable_row["reason"],
        ) == (None, None, False, "horizon_unavailable")
    finally:
        con.disconnect()


def test_naive_retention_timestamps_mean_utc_under_non_utc_session_timezone():
    con = ibis.duckdb.connect()
    try:
        con.raw_sql("SET TimeZone = 'America/Los_Angeles'")
        anchors = con.create_table(
            "naive_anchors",
            pa.table(
                {
                    "unit": ["u"],
                    "anchor_id": ["a"],
                    "anchor_time": pa.array([datetime(2025, 1, 1)], type=pa.timestamp("us")),
                    "observed_until": pa.array([None], type=pa.timestamp("us")),
                }
            ),
        )
        events = con.create_table(
            "naive_events",
            pa.table(
                {
                    "unit": ["u"],
                    "event_id": ["e"],
                    "event_name": ["target"],
                    "event_time": pa.array([datetime(2025, 1, 2, 20)], type=pa.timestamp("us")),
                }
            ),
        )
        result = retention_threshold_bands(
            events,
            anchors,
            unit="unit",
            event_id="event_id",
            event_name="event_name",
            target_event="target",
            timestamp="event_time",
            anchor_id="anchor_id",
            anchor_timestamp="anchor_time",
            observed_until="observed_until",
            source_complete_through=datetime(2025, 1, 3, tzinfo=UTC),
            count_thresholds=(1,),
        )
        row = con.to_pyarrow(result).to_pylist()[0]
        assert (row["occurrence_count"], row["complete"], row["reason"]) == (1, True, None)
    finally:
        con.disconnect()


def test_retention_rejects_unsupported_timestamp_dtype_with_coded_error():
    import pytest

    from terrine.errors import TerrineError

    con = ibis.duckdb.connect()
    try:
        anchors = con.create_table(
            "bad_anchors",
            pa.table(
                {
                    "unit": ["u"],
                    "anchor_id": ["a"],
                    "anchor_time": [1],
                    "observed_until": pa.array([None], type=pa.timestamp("us")),
                }
            ),
        )
        events = con.create_table(
            "bad_events",
            pa.table(
                {
                    "unit": ["u"],
                    "event_id": ["e"],
                    "event_name": ["target"],
                    "event_time": pa.array([datetime(2025, 1, 2)], type=pa.timestamp("us")),
                }
            ),
        )
        with pytest.raises(TerrineError) as error:
            retention_threshold_bands(
                events,
                anchors,
                unit="unit",
                event_id="event_id",
                event_name="event_name",
                target_event="target",
                timestamp="event_time",
                anchor_id="anchor_id",
                anchor_timestamp="anchor_time",
                observed_until="observed_until",
                source_complete_through=datetime(2025, 1, 3, tzinfo=UTC),
                count_thresholds=(1,),
            )
        assert error.value.code == "terrine.temporal.retention_invalid"
    finally:
        con.disconnect()
