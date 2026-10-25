from datetime import UTC, datetime

import ibis
import pyarrow as pa
import pytest

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
                        [datetime(2025, 1, 11, tzinfo=UTC), None],
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


@pytest.mark.parametrize("failure", ["missing_start", "wrong_width"])
def test_bounded_open_retention_refuses_unknown_span_before_execution(failure):
    from terrine.errors import TerrineError
    from terrine.temporal.ranges import RangePlan

    anchors = ibis.table(
        {
            "unit": "string",
            "anchor_id": "string",
            "t0": "timestamp('UTC')",
            "observed_until": "timestamp('UTC')",
        }
    )
    events = ibis.table(
        {
            "unit": "string",
            "event_id": "string",
            "name": "string",
            "timestamp": "timestamp('UTC')",
        }
    )
    with pytest.raises(TerrineError) as error:
        retention_threshold_bands(
            events,
            anchors,
            unit="unit",
            event_id="event_id",
            event_name="name",
            target_event="target",
            timestamp="timestamp",
            anchor_id="anchor_id",
            anchor_timestamp="t0",
            observed_until="observed_until",
            count_thresholds=(1,),
            source_complete_through=datetime(2025, 1, 6, tzinfo=UTC),
            source_complete_from=(
                None if failure == "missing_start" else datetime(2025, 1, 2, tzinfo=UTC)
            ),
            range_plan=RangePlan(5 * 86_400, 1, "fixed"),
        )
    assert error.value.code == "terrine.temporal.range_bound_unavailable"
    assert error.value.context["route"]


def test_open_retention_bound_uses_elapsed_utc_span_across_clock_change():
    from zoneinfo import ZoneInfo

    from terrine.temporal.ranges import RangePlan

    anchors = ibis.table(
        {
            "unit": "string",
            "anchor_id": "string",
            "t0": "timestamp('UTC')",
            "observed_until": "timestamp('UTC')",
        }
    )
    events = ibis.table(
        {
            "unit": "string",
            "event_id": "string",
            "name": "string",
            "timestamp": "timestamp('UTC')",
        }
    )
    zone = ZoneInfo("America/New_York")
    result = retention_threshold_bands(
        events,
        anchors,
        unit="unit",
        event_id="event_id",
        event_name="name",
        target_event="target",
        timestamp="timestamp",
        anchor_id="anchor_id",
        anchor_timestamp="t0",
        observed_until="observed_until",
        count_thresholds=(1,),
        source_complete_from=datetime(2025, 3, 8, tzinfo=zone),
        source_complete_through=datetime(2025, 3, 10, tzinfo=zone),
        range_plan=RangePlan(47 * 3600, 1, "fixed"),
    )
    assert result.schema()["occurrence_count"].is_integer()


@pytest.mark.parametrize("bounded", [False, True])
@pytest.mark.parametrize("anchor_ns,end_ns", [(1, 1_000_000_001), (-1, 999_999_999)])
def test_finite_retention_preserves_nanosecond_end_and_followup(bounded, anchor_ns, end_ns):
    from terrine.temporal.ranges import RangePlan

    con = ibis.duckdb.connect()
    try:
        anchors = con.create_table(
            "precise_end_anchors",
            pa.table(
                {
                    "unit": ["u", "u"],
                    "anchor_id": ["exact", "short"],
                    "t0": pa.array([anchor_ns, anchor_ns], type=pa.timestamp("ns")),
                    "observed_until": pa.array([end_ns, end_ns - 1], type=pa.timestamp("ns")),
                }
            ),
        )
        events = con.create_table(
            "precise_end_events",
            pa.table(
                {
                    "unit": ["u"] * 4,
                    "event_id": ["before", "start", "inside", "end"],
                    "name": ["target"] * 4,
                    "timestamp": pa.array(
                        [anchor_ns - 1, anchor_ns, end_ns - 1, end_ns], type=pa.timestamp("ns")
                    ),
                }
            ),
        )
        result = retention_threshold_bands(
            events,
            anchors,
            unit="unit",
            event_id="event_id",
            event_name="name",
            target_event="target",
            timestamp="timestamp",
            anchor_id="anchor_id",
            anchor_timestamp="t0",
            observed_until="observed_until",
            count_thresholds=(1, 2),
            finite_horizon_seconds=1,
            range_plan=RangePlan(1, 2, "declared_anchor_count") if bounded else None,
        )
        actual = con.to_pyarrow(result).sort_by("anchor_id").to_pylist()
        assert [
            (
                row["anchor_id"],
                row["occurrence_count"],
                row["retention_band"],
                row["complete"],
                row["reason"],
            )
            for row in actual
        ] == [
            ("exact", 2, 2, True, None),
            ("short", None, None, False, "insufficient_followup"),
        ]
    finally:
        con.disconnect()
