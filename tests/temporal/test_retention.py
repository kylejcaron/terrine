from datetime import UTC, datetime, timedelta

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


@pytest.mark.parametrize("finite", [True, False])
def test_bounded_retention_matches_exact_path_and_independent_pandas(finite):
    import pandas as pd

    from terrine.temporal.ranges import RangePlan
    from tests.reference.retention import retention_rows

    con = ibis.duckdb.connect()
    try:
        origin = datetime(2025, 1, 1, tzinfo=UTC)
        cutoff = origin + timedelta(days=5)
        anchor_rows = [
            {
                "unit": unit,
                "anchor_id": str(day),
                "t0": origin + timedelta(days=day),
                "observed_until": (
                    None
                    if unit == "u" and day == 2
                    else origin + timedelta(days=1, hours=12)
                    if unit == "u" and day == 1
                    else cutoff
                ),
            }
            for unit in ("u", "empty")
            for day in range(-1, 4)
        ]
        event_rows = [
            {
                "unit": "u",
                "event_id": identity,
                "name": name,
                "timestamp": origin + timedelta(days=day),
            }
            for day, identity, name in (
                (0, "e1", "target"),
                (0, "e1", "target"),
                (1, "e1", "target"),
                (2, "e2", "target"),
                (3, "e3", "target"),
                (5, "at_end", "target"),
                (1, "wrong", "other"),
            )
        ]
        anchors = con.create_table("band_anchors", pa.Table.from_pylist(anchor_rows))
        events = con.create_table("band_events", pa.Table.from_pylist(event_rows))
        horizon = 2 * 86_400 if finite else None

        def build(range_plan=None, *, reverse=False):
            return retention_threshold_bands(
                events.order_by(ibis.desc("timestamp")) if reverse else events,
                anchors,
                unit="unit",
                event_id="event_id",
                event_name="name",
                target_event="target",
                timestamp="timestamp",
                anchor_id="anchor_id",
                anchor_timestamp="t0",
                observed_until="observed_until",
                count_thresholds=(1, 2, 3),
                finite_horizon_seconds=horizon,
                source_complete_from=origin,
                source_complete_through=None if finite else cutoff,
                range_plan=range_plan,
            )

        exact = build()
        plan = RangePlan(
            horizon if horizon is not None else (cutoff - origin).days * 86_400,
            max(sum(row["unit"] == unit for row in anchor_rows) for unit in ("u", "empty")),
            "declared_anchor_count",
        )
        bounded = build(plan, reverse=True)
        expected = retention_rows(
            pd.DataFrame(event_rows),
            pd.DataFrame(anchor_rows),
            thresholds=(1, 2, 3),
            horizon=horizon,
            coverage_start=origin,
            cutoff=cutoff,
        )

        def key(row):
            return row["unit"], row["anchor_id"]

        assert sorted(con.to_pyarrow(exact).to_pylist(), key=key) == sorted(expected, key=key)
        assert sorted(con.to_pyarrow(bounded).to_pylist(), key=key) == sorted(expected, key=key)
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


@pytest.mark.parametrize("finite", [True, False])
def test_bounded_retention_preserves_submicrosecond_anchor_lower_bound(finite):
    from terrine.temporal.ranges import RangePlan

    con = ibis.duckdb.connect()
    try:
        anchors = con.create_table(
            "precise_anchors",
            pa.table(
                {
                    "unit": ["u"],
                    "anchor_id": ["a"],
                    "t0": pa.array([1], type=pa.timestamp("ns")),
                    "observed_until": pa.array([2_000_000], type=pa.timestamp("us")),
                }
            ),
        )
        events = con.create_table(
            "precise_events",
            pa.table(
                {
                    "unit": ["u", "u"],
                    "event_id": ["before", "at_start"],
                    "name": ["target", "target"],
                    "timestamp": pa.array([0, 1], type=pa.timestamp("ns")),
                }
            ),
        )

        def build(plan=None):
            return retention_threshold_bands(
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
                finite_horizon_seconds=1 if finite else None,
                source_complete_through=None
                if finite
                else datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC),
                source_complete_from=datetime(1970, 1, 1, tzinfo=UTC),
                range_plan=plan,
            )

        exact = con.to_pyarrow(build()).to_pylist()
        bounded = con.to_pyarrow(build(RangePlan(1, 1, "fixed"))).to_pylist()
        assert exact[0]["occurrence_count"] == 1
        assert exact[0]["retention_band"] == 1
        assert bounded == exact
    finally:
        con.disconnect()
