"""Throwaway known-answer probes for the direct-window experiment; not a test suite."""

import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import ibis
import pyarrow as pa

from benchmarks.query_strategy_spike import direct_query


def instant(day, hour=0):
    return datetime(2025, 1, day, hour, tzinfo=UTC)


def run(*, bucketed=False):
    con = ibis.duckdb.connect(threads=1)
    try:
        units = ["empty", "nulls", "normal", "partial", "unknown", "old_bad", "new_bad"]
        anchors = con.create_table(
            "probe_anchors",
            pa.table(
                {
                    "unit": units,
                    "t0": [instant(15)] * len(units),
                    "_observed_from": [
                        instant(14, 12) if u == "partial" else instant(1) for u in units
                    ],
                    "_observed_until": [instant(16)] * len(units),
                }
            ),
        )
        rows = [
            ("nulls", 14, 0, None, "yes"),
            ("normal", 1, 0, 1e16, "yes"),
            ("normal", 14, 0, 1.0, "yes"),
            ("normal", 14, 12, 2.0, "no"),
            ("normal", 15, 0, 999.0, "yes"),
            ("partial", 14, 13, 7.0, "yes"),
            ("unknown", 14, 0, 5.0, None),
            ("old_bad", 1, 0, math.inf, "yes"),
            ("old_bad", 14, 0, 4.0, "yes"),
            ("new_bad", 14, 0, math.inf, "yes"),
        ]
        events = con.create_table(
            "probe_events",
            pa.table(
                {
                    "unit_id": [r[0] for r in rows],
                    "event_id": list(range(len(rows))),
                    "event_time": [instant(r[1], r[2]) for r in rows],
                    "value": pa.array([r[3] for r in rows], type=pa.float64()),
                    "label": [r[4] for r in rows],
                }
            ),
        )
        coverage = SimpleNamespace(start=instant(13), end_exclusive=instant(15))
        query, _ = direct_query(
            anchors,
            events,
            coverage,
            (1, 3),
            native=True,
            predicate=events.label == "yes",
            bucketed=bucketed,
        )
        result = {row["unit"]: row for row in query.to_pyarrow().to_pylist()}
        checks = {}

        def require(name, condition):
            assert condition, name
            checks[name] = True

        require(
            "empty count and sum are zero",
            result["empty"]["events_1d"] == 0 and result["empty"]["total_1d"] == 0,
        )
        require(
            "empty average is unavailable",
            result["empty"]["average_1d"] is None
            and result["empty"]["average_1d__reason"] == "no_observations",
        )
        require(
            "zero denominator is unavailable",
            result["empty"]["per_event_1d"] is None
            and result["empty"]["per_event_1d__reason"] == "zero_denominator",
        )
        require(
            "all null is not zero",
            result["nulls"]["total_1d"] is None
            and result["nulls"]["total_1d__reason"] == "all_values_null"
            and result["nulls"]["events_1d"] == 1,
        )
        require(
            "half open bounds and selected rows",
            result["normal"]["events_1d"] == 1 and result["normal"]["total_1d"] == 1.0,
        )
        require(
            "source completeness stays per window",
            result["normal"]["total_1d__complete"] is True
            and result["normal"]["total_3d"] is None
            and result["normal"]["total_3d__reason"] == "incomplete_coverage",
        )
        require(
            "observation boundary withdraws value",
            result["partial"]["total_1d"] is None
            and result["partial"]["total_1d__reason"] == "incomplete_observation",
        )
        require(
            "unknown predicate gates complete output",
            result["unknown"]["check_unknown_1"] is True
            and result["unknown"]["check_unknown_3"] is False,
        )
        require(
            "excluded nonfinite history does not poison window",
            result["old_bad"]["total_1d"] == 4.0 and result["old_bad"]["check_numeric_1"] is False,
        )
        require(
            "selected nonfinite input gates output", result["new_bad"]["check_numeric_1"] is True
        )
        return {
            "known_answers": checks,
            "intentional_contract_changes": [
                "excluded float history does not affect value",
                "excluded nonfinite history does not refuse",
            ],
            "limits": [
                "one source, UTC days, native float",
                "no dimension-history unavailable-label distinction",
                "not a full Frame execution or persisted implementation",
            ],
        }
    finally:
        con.disconnect()


if __name__ == "__main__":
    result = {"raw": run(), "daily": run(bucketed=True)}
    print(json.dumps(result, indent=2))
    if len(sys.argv) > 1:
        Path(sys.argv[1]).write_text(json.dumps(result, indent=2))
