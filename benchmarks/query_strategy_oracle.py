"""Independent Python reference for the spike's bounded, fully observed cost workload."""

import math
from bisect import bisect_left
from collections import Counter, defaultdict
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


def count_inputs(con, *, irregular, first, anchors):
    """Expected origins and per-unit timestamps from physical input rows."""
    units = set(con.table("users").to_pyarrow()["unit_id"].to_pylist())
    grouped = defaultdict(list)
    events = con.table("events").select("unit_id", "event_time").to_pyarrow()
    for row in events.to_pylist():
        if row["unit_id"] in units:
            grouped[row["unit_id"]].append(row["event_time"])
    origins = Counter(
        (unit, instant)
        for unit in units
        for instant in (
            grouped[unit] if irregular else (first + timedelta(days=i) for i in range(anchors))
        )
    )
    return origins, {unit: sorted(times) for unit, times in grouped.items()}


def assert_count_values(result, timestamps, windows, coverage):
    """Check selected count, presence and provenance without a SQL reference."""
    names = ("unit", "t0", *result.roles.features)
    fields = {}
    for name in result.roles.features:
        meta = result.columns[name]
        fields[name] = (
            meta.complete_column,
            meta.reason_column,
            meta.observed_from_column,
            meta.observed_until_column,
        )
    table = result.table.select(list(dict.fromkeys((*names, *sum(fields.values(), ())))))
    for row in table.to_pylist():
        times = timestamps.get(row["unit"], ())
        end = row["t0"]
        stop = bisect_left(times, end)
        for days in windows:
            start = end - timedelta(days=days)
            n = stop - bisect_left(times, start)
            complete = coverage.start <= start and end <= coverage.end_exclusive
            for name, expected in ((f"events_{days}d", n), (f"active_{days}d", n != 0)):
                wanted = expected if complete else None
                assert type(row[name]) is type(wanted) and row[name] == wanted, (
                    row["unit"],
                    end,
                    name,
                    row[name],
                    wanted,
                )
                assert tuple(row[key] for key in fields[name]) == (
                    complete,
                    None if complete else "incomplete_coverage",
                    coverage.start,
                    coverage.end_exclusive,
                ), (row["unit"], end, name)
    return len(table) * len(result.roles.features)
