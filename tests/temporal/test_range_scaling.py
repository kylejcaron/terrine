from datetime import UTC, datetime, timedelta

import ibis
import pandas as pd
import pyarrow as pa
import pytest

from terrine.windows import Window
from tests.reference.ranges import window_values


@pytest.mark.parametrize("span,step", [(3, 1), (4000, 1000)])
def test_range_join_intermediate_bound_is_blocked(span, step):
    from terrine.temporal.ranges import RangePlan, blocked_range_join

    con = ibis.duckdb.connect()
    try:
        origin = datetime(1960, 1, 1, tzinfo=UTC)
        anchor_rows = [
            {"unit": unit, "anchor_id": str(index), "t0": origin + timedelta(days=index * step)}
            for unit in ("a", "empty")
            for index in range(9)
        ]
        event_rows = [
            {
                "unit": "a",
                "timestamp": origin + timedelta(days=day, microseconds=delta),
                "value": value,
            }
            for day, delta, value in (
                (-span, -1, 99),
                (-span, 0, 1),
                (-span, 1, 2),
                (-1, 0, 2),
                (0, -1, None),
                (0, 0, 8),
                (0, 0, 8),
                (step, 0, 3),
                (step, 1, None),
                (8 * step, 0, 10),
            )
        ]
        anchors = con.create_table("anchors", pa.Table.from_pylist(anchor_rows))
        events = con.create_table("events", pa.Table.from_pylist(event_rows))
        width = span * 86_400
        plan = RangePlan(width, min(9, 2 * ((span + step - 1) // step)), "schedule")
        joined = blocked_range_join(
            anchors,
            events,
            plan=plan,
            window=Window(-width, 0),
            event_unit="unit",
            event_timestamp="timestamp",
        )
        requests = con.to_pyarrow(joined.requests)
        candidates = con.to_pyarrow(joined.candidates)
        matched = con.to_pyarrow(joined.matched)
        assert requests.num_rows <= 2 * len(anchor_rows)
        assert candidates.num_rows <= plan.max_candidates_per_event * len(event_rows)
        assert matched.num_rows == sum(
            row["event_count"]
            for row in window_values(pd.DataFrame(event_rows), pd.DataFrame(anchor_rows), -width, 0)
        )
        assert matched.num_rows < len(anchor_rows) * len(event_rows)
        actual = con.to_pyarrow(
            joined.matched.group_by("unit", "anchor_id").aggregate(
                minimum=ibis._.value.min(),
                maximum=ibis._.value.max(),
                distinct=ibis._.value.nunique(),
                event_count=ibis._.count(),
            )
        ).to_pylist()
        oracle = window_values(pd.DataFrame(event_rows), pd.DataFrame(anchor_rows), -width, 0)
        expected = {(row["unit"], row["anchor_id"]): row for row in oracle}
        for row in actual:
            assert all(row[key] == expected[row["unit"], row["anchor_id"]][key] for key in row)
    finally:
        con.disconnect()


def test_bucket_join_keeps_pre_epoch_nanosecond_edges():
    from terrine.temporal.ranges import RangePlan, blocked_range_join

    con = ibis.duckdb.connect()
    try:
        anchors = con.create_table(
            "ns_anchors",
            pa.table(
                {
                    "unit": ["u", "u"],
                    "anchor_id": ["a", "b"],
                    "t0": pa.array([0, 500_000], type=pa.timestamp("us", tz="UTC")),
                }
            ),
        )
        coefficients = [-1_000_000_001, -1_000_000_000, -999_999_999, -1, 0, 999_999_999]
        events = con.create_table(
            "ns_events",
            pa.table(
                {
                    "event_unit": ["u"] * len(coefficients),
                    "timestamp": pa.array(coefficients, type=pa.timestamp("ns")),
                    "coefficient": coefficients,
                }
            ),
        )
        joined = blocked_range_join(
            anchors,
            events,
            plan=RangePlan(1, 2, "declared_anchor_count"),
            window=Window(-1, 0),
            event_unit="event_unit",
            event_timestamp="timestamp",
        )
        actual = con.to_pyarrow(joined.matched).to_pylist()
        expected = {
            (identity, value)
            for identity, end in (("a", 0), ("b", 500_000_000))
            for value in coefficients
            if end - 1_000_000_000 <= value < end
        }
        assert {(row["anchor_id"], row["coefficient"]) for row in actual} == expected
        assert con.to_pyarrow(joined.candidates).num_rows <= 2 * len(coefficients)
    finally:
        con.disconnect()


@pytest.mark.parametrize(
    "anchor_ns,start,end,coefficients,want",
    [
        (1, 0, 1, [0, 1, 1_000_000_000, 1_000_000_001], [1, 1_000_000_000]),
        (1, -1, 0, [-1_000_000_000, -999_999_999, 0, 1], [-999_999_999, 0]),
        (-1, 0, 1, [-2, -1, 999_999_998, 999_999_999], [-1, 999_999_998]),
        (-1, -1, 0, [-1_000_000_002, -1_000_000_001, -2, -1], [-1_000_000_001, -2]),
    ],
)
def test_bucket_join_preserves_shifted_nanosecond_endpoints(
    anchor_ns, start, end, coefficients, want
):
    from terrine.temporal.ranges import RangePlan, blocked_range_join

    con = ibis.duckdb.connect()
    try:
        anchors = con.create_table(
            "shifted_anchors",
            pa.table({"unit": ["u"], "t0": pa.array([anchor_ns], type=pa.timestamp("ns"))}),
        )
        events = con.create_table(
            "shifted_events",
            pa.table(
                {
                    "event_unit": ["u"] * len(coefficients),
                    "timestamp": pa.array(coefficients, type=pa.timestamp("ns")),
                    "coefficient": coefficients,
                }
            ),
        )
        joined = blocked_range_join(
            anchors,
            events,
            plan=RangePlan(1, 1, "fixed"),
            window=Window(start, end),
            event_unit="event_unit",
            event_timestamp="timestamp",
        )
        actual = con.to_pyarrow(joined.matched.select("coefficient")).column(0).to_pylist()
        assert sorted(actual) == want
        assert con.to_pyarrow(joined.requests).num_rows <= 2
        assert con.to_pyarrow(joined.candidates).num_rows <= len(coefficients)
    finally:
        con.disconnect()


@pytest.mark.parametrize(
    "anchor_us,start,end",
    [
        (2**63 - 2, 2**63 - 2, 2**63 - 1),
        (-(2**63) + 1, -(2**63), -(2**63) + 1),
    ],
)
def test_bucket_join_virtual_endpoints_do_not_overflow_native_timestamp_domain(
    anchor_us, start, end
):
    from terrine.temporal.ranges import RangePlan, blocked_range_join

    con = ibis.duckdb.connect()
    try:
        anchors = con.create_table(
            "wide_anchors",
            pa.table({"unit": ["u"], "t0": pa.array([anchor_us], type=pa.timestamp("us"))}),
        )
        events = con.create_table(
            "wide_events",
            pa.table(
                {
                    "event_unit": ["u"],
                    "timestamp": pa.array([anchor_us], type=pa.timestamp("us")),
                }
            ),
        )
        joined = blocked_range_join(
            anchors,
            events,
            plan=RangePlan(1, 1, "fixed"),
            window=Window(start, end),
            event_unit="event_unit",
            event_timestamp="timestamp",
        )
        assert con.to_pyarrow(joined.requests).num_rows <= 2
        assert con.to_pyarrow(joined.candidates).num_rows == 0
        assert con.to_pyarrow(joined.matched).num_rows == 0
    finally:
        con.disconnect()


def test_shifted_timestamp_comparisons_preserve_null_predicates():
    from terrine.backends.timestamps import compare_timestamps

    con = ibis.duckdb.connect()
    try:
        timestamps = con.create_table(
            "nullable_timestamps",
            pa.table(
                {
                    "left": pa.array([1, None], type=pa.timestamp("ns")),
                    "right": pa.array([None, 1], type=pa.timestamp("ns")),
                }
            ),
        )
        comparisons = timestamps.select(
            **{
                op: compare_timestamps(
                    timestamps.left, timestamps.right, op, right_offset_seconds=1
                )
                for op in ("lt", "le", "gt", "ge", "eq")
            }
        )
        assert (
            con.to_pyarrow(comparisons).to_pylist()
            == [{op: None for op in ("lt", "le", "gt", "ge", "eq")}] * 2
        )
    finally:
        con.disconnect()
