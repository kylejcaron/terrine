"""Independent Python reference for the spike's bounded, fully observed cost workload."""

import math
from collections import defaultdict
from datetime import timedelta


def assert_values(con, table, mode, coverage):
    events = con.table("events_native" if mode == "native" else "events")
    grouped = defaultdict(list)
    for row in events.select("unit_id", "event_time", "value").to_pyarrow().to_pylist():
        grouped[row["unit_id"]].append((row["event_time"], row["value"]))
    checked = 0
    for row in table.to_pylist():
        for days in range(1, 11):
            start, end = row["t0"] - timedelta(days=days), row["t0"]
            values = [value for instant, value in grouped[row["unit"]] if start <= instant < end]
            present = [value for value in values if value is not None]
            n, nn = len(values), len(present)
            total = (
                (math.fsum(present) if mode == "native" else sum(present))
                if present
                else (None if n else 0)
            )
            expected = {
                "events": (n, None),
                "total": (total, "all_values_null" if n and not nn else None),
                "average": (
                    float(total) / nn if nn and total is not None else None,
                    "all_values_null" if n and not nn else "no_observations" if not n else None,
                ),
                "per_event": (
                    float(total) / n if n and total is not None else None,
                    "all_values_null" if n and not nn else "zero_denominator" if not n else None,
                ),
                "active": (bool(n), None),
            }
            for metric, (wanted, reason) in expected.items():
                name = f"{metric}_{days}d"
                actual = row[name]
                if isinstance(actual, float) or isinstance(wanted, float):
                    assert (
                        actual is not None
                        and wanted is not None
                        and math.isclose(float(actual), float(wanted), rel_tol=1e-10, abs_tol=1e-9)
                    ), (row["unit"], name, actual, wanted)
                else:
                    assert actual == wanted, (row["unit"], name, actual, wanted)
                assert row[f"{name}__reason"] == reason
                assert row[f"{name}__complete"] is True
                assert row[f"{name}__observed_from"] == coverage.start
                assert row[f"{name}__observed_until"] == coverage.end_exclusive
                checked += 1
    return checked
