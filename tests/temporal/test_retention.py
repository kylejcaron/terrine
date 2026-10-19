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
            (row["occurrence_count"], row["retention_band"], row["complete"]) for row in actual
        ] == [
            (2, 2, True),
            (None, None, False),
        ]
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
                    "observed_until": pa.array(
                        [datetime(2025, 1, 4, tzinfo=UTC)], type=pa.timestamp("us", tz="UTC")
                    ),
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
        assert (row["occurrence_count"], row["retention_band"], row["complete"]) == (1, 1, True)
    finally:
        con.disconnect()
