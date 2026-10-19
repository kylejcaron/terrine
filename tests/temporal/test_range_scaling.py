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
