"""Small raw-row baseline reference, independent of production resolution."""

from datetime import UTC, datetime, time, timedelta
from math import fsum

import pyarrow as pa

from terrine.simulate.world import World


def _histories(world: World):
    histories = {}
    for row in world.attributes.to_pylist():
        histories.setdefault(row["unit_id"], []).append(row)
    for rows in histories.values():
        if any(row["valid_from"] is None for row in rows):
            raise ValueError("history requires valid_from")
        rows.sort(key=lambda row: row["valid_from"])
        previous = None
        for row in rows:
            start, end = row["valid_from"], row["valid_to"]
            if end is not None and start >= end:
                raise ValueError("history validity must have positive duration")
            if previous is not None and (
                previous["valid_to"] is None or previous["valid_to"] > start
            ):
                raise ValueError("history intervals overlap or duplicate")
            previous = row
    return histories


def _measurement(coverage, roster, start, end, *, strict_asof=False):
    source_start = datetime.combine(coverage.start, time.min, UTC)
    source_end = datetime.combine(coverage.through + timedelta(days=1), time.min, UTC)
    unit_start, unit_end = roster["observed_from"], roster["observed_until"]
    observed_start = max(source_start, unit_start) if unit_start is not None else source_start
    observed_end = min(source_end, unit_end) if unit_end is not None else source_end
    source_complete = source_start < start if strict_asof else source_start <= start
    source_complete = source_complete and end <= source_end
    unit_complete = (
        unit_start is None or (unit_start < start if strict_asof else unit_start <= start)
    ) and (unit_end is None or end <= unit_end)
    reason = (
        "incomplete_coverage"
        if not source_complete
        else "incomplete_observation"
        if not unit_complete
        else None
    )
    return {
        "observed_from": observed_start,
        "observed_until": observed_end,
        "complete": reason is None,
        "reason": reason,
    }


def reference_rows(world: World) -> pa.Table:
    """Scan raw half-open events and strict-left-limit history for every roster unit.

    Source coverage and unit observation are separate requirements. Source
    incompleteness takes precedence when both fail; neither drops a candidate.
    A complete empty sum is zero, while a nonempty all-null sum stays null.
    """
    t0 = world.config.t0
    end = t0 + timedelta(seconds=world.config.horizon_seconds)
    coverage = world.readiness["world_events"]
    histories = _histories(world)
    events = world.events.to_pylist()
    result = []
    for roster in world.roster.to_pylist():
        unit = roster["unit_id"]
        versions = [
            row
            for row in histories.get(unit, [])
            if row["valid_from"] < t0 and (row["valid_to"] is None or row["valid_to"] >= t0)
        ]
        baseline_meta = _measurement(coverage, roster, t0, t0, strict_asof=True)
        version = versions[0] if versions else None
        if baseline_meta["complete"] and version is None:
            baseline_meta = {**baseline_meta, "complete": False, "reason": "missing_history"}
        values = {
            name: version[name] if version is not None and baseline_meta["complete"] else None
            for name in ("x", "plan_tier", "baseline_enabled")
        }
        outcome_meta = _measurement(coverage, roster, t0, end)
        postings = [
            row["value"]
            for row in events
            if row["unit_id"] == unit and row["kind"] == "revenue" and t0 <= row["ts"] < end
        ]
        outcome = None
        if outcome_meta["complete"]:
            present = [value for value in postings if value is not None]
            if postings and not present:
                outcome_meta = {**outcome_meta, "reason": "all_null"}
            else:
                outcome = fsum(present)
        row = {
            "unit_id": unit,
            "anchor_id": f"fixed:{t0.isoformat()}",
            "t0": t0,
            **values,
            "net_revenue": outcome,
            "target_weight": (
                1 + world.config.target_weight_slope * values["x"]
                if values["x"] is not None
                else None
            ),
        }
        for name in ("x", "plan_tier", "baseline_enabled", "net_revenue"):
            meta = outcome_meta if name == "net_revenue" else baseline_meta
            row.update({f"{name}__{key}": value for key, value in meta.items()})
        result.append(row)
    timestamp = pa.timestamp("us", tz="UTC")
    fields = [
        ("unit_id", pa.int64()),
        ("anchor_id", pa.string()),
        ("t0", timestamp),
        ("x", pa.float64()),
        ("plan_tier", pa.string()),
        ("baseline_enabled", pa.bool_()),
        ("net_revenue", pa.float64()),
        ("target_weight", pa.float64()),
    ]
    for name in ("x", "plan_tier", "baseline_enabled", "net_revenue"):
        fields.extend(
            [
                (f"{name}__observed_from", timestamp),
                (f"{name}__observed_until", timestamp),
                (f"{name}__complete", pa.bool_()),
                (f"{name}__reason", pa.string()),
            ]
        )
    return pa.Table.from_pylist(result, schema=pa.schema(fields))
