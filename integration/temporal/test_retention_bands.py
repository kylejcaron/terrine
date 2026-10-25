from datetime import UTC, datetime, timedelta

import ibis
import pyarrow as pa
import pytest

from terrine.temporal.retention import retention_threshold_bands


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
