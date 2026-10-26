"""Arrow aggregation for descriptive risk-set tables."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any, TypedDict

import pyarrow as pa

from terrine.errors import RefusalSpec

__all__ = ["aggregate_life_table"]


class _BinCounts(TypedDict):
    at_risk: int
    events: int
    censored: int
    delayed_entries: int
    reasons: dict[str, int]


_ROWS_EXCEEDED = RefusalSpec(
    "terrine.budget.rows_exceeded", "Output exceeds the configured row limit {limit}."
)


_NS_PER_UNIT = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}


def _nanoseconds(table: pa.Table, name: str) -> list[int | None]:
    column = table[name]
    dtype = column.type
    if not pa.types.is_timestamp(dtype):
        raise TypeError(f"{name} must be a timestamp")
    multiplier = _NS_PER_UNIT[dtype.unit]
    values = column.cast(pa.int64()).to_pylist()
    return [None if value is None else value * multiplier for value in values]


def _duration_nanoseconds(table: pa.Table, name: str) -> list[int | None]:
    column = table[name]
    dtype = column.type
    if not pa.types.is_duration(dtype):
        raise TypeError(f"{name} must be a duration")
    multiplier = _NS_PER_UNIT[dtype.unit]
    values = column.cast(pa.int64()).to_pylist()
    return [None if value is None else value * multiplier for value in values]

def aggregate_life_table(
    records: pa.Table,
    *,
    interval_seconds: int,
    group_by: Sequence[str],
    group_supports: Mapping[str, str] | None = None,
    max_rows: int | None = None,
) -> pa.Table:
    """Aggregate time-to-event rows by elapsed-time bin without curve inference."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if not set(("t0", "duration", "entry_time", "event", "censor_reason")).issubset(
        records.column_names
    ):
        raise ValueError("time-to-event records are missing life-table columns")
    if any(name not in records.column_names for name in group_by):
        raise ValueError("life-table grouping column is missing")

    t0 = _nanoseconds(records, "t0")
    duration = _duration_nanoseconds(records, "duration")
    entry = _nanoseconds(records, "entry_time")
    events = records["event"].to_pylist()
    complete = (
        records["complete"].to_pylist()
        if "complete" in records.column_names
        else [True] * records.num_rows
    )
    record_reasons = (
        records["reason"].to_pylist()
        if "reason" in records.column_names
        else [None] * records.num_rows
    )
    censor_reasons = records["censor_reason"].to_pylist()
    group_supports = group_supports or {}
    if any(name not in records.column_names for name in group_supports):
        raise ValueError("life-table grouping support column is missing")
    if any(policy not in {"min", "max", "all", "reasons"} for policy in group_supports.values()):
        raise ValueError("unsupported life-table grouping support policy")
    groups = [records[name].to_pylist() for name in group_by]
    support_values = {name: records[name].to_pylist() for name in group_supports}
    support_by_group: dict[tuple[object, ...], dict[str, list[Any]]] = defaultdict(
        lambda: defaultdict(list)
    )
    width = interval_seconds * 1_000_000_000
    incomplete_by_group: dict[tuple[object, ...], set[str]] = defaultdict(set)
    bins_by_group: dict[tuple[object, ...], dict[int, _BinCounts]] = defaultdict(dict)
    output_bins = 0

    for index, origin in enumerate(t0):
        key = tuple(column[index] for column in groups)
        if complete[index] is not True:
            incomplete_by_group[key].add(record_reasons[index] or "reason_unavailable")
        terminal_elapsed = duration[index]
        entered = entry[index]
        if origin is None or terminal_elapsed is None or entered is None:
            continue
        entry_elapsed = entered - origin
        if terminal_elapsed < 0 or entry_elapsed < 0:
            continue
        for name, values in support_values.items():
            support_by_group[key][name].append(values[index])
        terminal_bin = terminal_elapsed // width
        delayed_bin = entry_elapsed // width if entry_elapsed > 0 else None
        group_bins = bins_by_group[key]
        for time_bin in range(terminal_bin + 1):
            current = group_bins.get(time_bin)
            if current is None:
                output_bins += 1
                if max_rows is not None and output_bins > max_rows:
                    raise _ROWS_EXCEEDED.error(limit=max_rows)
                current: _BinCounts = {
                    "at_risk": 0,
                    "events": 0,
                    "censored": 0,
                    "delayed_entries": 0,
                    "reasons": {},
                }
                group_bins[time_bin] = current
            bin_start = time_bin * width
            bin_end = bin_start + width
            if entry_elapsed < bin_end and terminal_elapsed >= bin_start:
                current["at_risk"] += 1
            if delayed_bin == time_bin:
                current["delayed_entries"] += 1
        terminal_row = group_bins[terminal_bin]
        is_complete = complete[index] is True
        if events[index] and is_complete:
            terminal_row["events"] += 1
        else:
            terminal_row["censored"] += 1
            reason = (
                record_reasons[index] if not is_complete else censor_reasons[index]
            ) or "unknown"
            reason_counts = terminal_row["reasons"]
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    reduced_support_by_group = {}
    for group_key in bins_by_group:
        support = support_by_group.get(group_key, {})
        reduced = {}
        for name, policy in group_supports.items():
            values = support[name]
            present = [value for value in values if value is not None]
            if policy == "min":
                reduced[name] = min(present) if present else None
            elif policy == "max":
                reduced[name] = max(present) if present else None
            elif policy == "all":
                reduced[name] = all(value is True for value in values)
            else:
                reduced[name] = ";".join(sorted(set(present))) or None
        reduced_support_by_group[group_key] = reduced

    schema_fields = [records.schema.field(name) for name in group_by]
    schema_fields.extend(records.schema.field(name) for name in group_supports)
    schema_fields.extend(
        [
            pa.field("unit", pa.int64(), nullable=False),
            pa.field("anchor_id", pa.int64(), nullable=False),
            pa.field("t0", pa.int64(), nullable=False),
            pa.field("time_bin", pa.int64(), nullable=False),
            pa.field("at_risk", pa.int64(), nullable=False),
            pa.field("events", pa.int64(), nullable=False),
            pa.field("censored", pa.int64(), nullable=False),
            pa.field("censored_by_reason", pa.map_(pa.string(), pa.int64()), nullable=False),
            pa.field("delayed_entries", pa.int64(), nullable=False),
            pa.field("complete", pa.bool_(), nullable=False),
            pa.field("reason", pa.list_(pa.string()), nullable=False),
            pa.field("event_rate", pa.float64()),
            pa.field("censor_rate", pa.float64()),
        ]
    )
    schema = pa.schema(schema_fields)
    rows = []
    for unit, group_key in enumerate(sorted(bins_by_group, key=repr)):
        for time_bin, counts in sorted(bins_by_group[group_key].items()):
            at_risk = counts["at_risk"]
            events_count = counts["events"]
            censored_count = counts["censored"]
            row = dict(zip(group_by, group_key, strict=True))
            row.update(reduced_support_by_group[group_key])
            row.update(
                unit=unit,
                anchor_id=time_bin,
                t0=time_bin,
                time_bin=time_bin,
                at_risk=at_risk,
                events=events_count,
                censored=censored_count,
                complete=not incomplete_by_group[group_key],
                reason=sorted(incomplete_by_group[group_key]),
                censored_by_reason=sorted(counts["reasons"].items()),
                delayed_entries=counts["delayed_entries"],
                event_rate=events_count / at_risk if at_risk else None,
                censor_rate=censored_count / at_risk if at_risk else None,
            )
            rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema)
