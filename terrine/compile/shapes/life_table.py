"""Arrow aggregation for descriptive risk-set tables."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from itertools import repeat
from typing import Any

import pyarrow as pa

import terrine.reasons as reason_codes
from terrine.errors import RefusalSpec
from terrine.spec._columns import IDENTITY, LIFE_TABLE_VALUES

__all__ = ["aggregate_life_table"]


@dataclass(slots=True)
class _BinChanges:
    risk_delta: int = 0
    events: int = 0
    censored: int = 0
    delayed_entries: int = 0
    reasons: dict[str, int] | None = None


@dataclass(slots=True)
class _GroupBins:
    terminal_bin: int = -1
    changes: defaultdict[int, _BinChanges] = field(
        default_factory=lambda: defaultdict(_BinChanges)
    )
    support: dict[str, Any] = field(default_factory=dict)
    incomplete: set[str] = field(default_factory=set)


_ROWS_EXCEEDED = RefusalSpec(
    "terrine.budget.rows_exceeded", "Output exceeds the configured row limit {limit}."
)


_NS_PER_UNIT = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}


def _tick_multiplier(schema: pa.Schema, name: str, *, is_duration: bool = False) -> int:
    dtype = schema.field(name).type
    if is_duration:
        if not pa.types.is_duration(dtype):
            raise TypeError(f"{name} must be a duration")
    elif not pa.types.is_timestamp(dtype):
        raise TypeError(f"{name} must be a timestamp")
    return _NS_PER_UNIT[dtype.unit]


class _LifeTableAccumulator:
    """Retain only each group's support, distinct reasons and sparse bin changes."""

    def __init__(
        self,
        schema: pa.Schema,
        *,
        interval_seconds: int,
        group_by: Sequence[str],
        group_supports: Mapping[str, str] | None = None,
        max_rows: int | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        names = set(schema.names)
        if not {"t0", "duration", "entry_time", "event", "censor_reason"} <= names:
            raise ValueError("time-to-event records are missing life-table columns")
        if any(name not in names for name in group_by):
            raise ValueError("life-table grouping column is missing")
        self._origin_scale = _tick_multiplier(schema, "t0")
        self._duration_scale = _tick_multiplier(schema, "duration", is_duration=True)
        self._entry_scale = _tick_multiplier(schema, "entry_time")
        group_supports = dict(group_supports or {})
        if any(name not in names for name in group_supports):
            raise ValueError("life-table grouping support column is missing")
        if not {"min", "max", "all", "reasons"}.issuperset(group_supports.values()):
            raise ValueError("unsupported life-table grouping support policy")
        self._schema = schema
        self._group_by = tuple(group_by)
        self._group_supports = group_supports
        self._has_complete = "duration__complete" in names
        self._has_reason = "duration__reason" in names
        self._width = interval_seconds * 1_000_000_000
        self._max_rows = max_rows
        self._output_bins = 0
        self._groups: defaultdict[tuple[object, ...], _GroupBins] = defaultdict(_GroupBins)

    def consume(self, batch: pa.RecordBatch) -> None:
        if not batch.num_rows:
            return
        origins = batch["t0"].cast(pa.int64()).to_pylist()
        durations = batch["duration"].cast(pa.int64()).to_pylist()
        entries = batch["entry_time"].cast(pa.int64()).to_pylist()
        events = batch["event"].to_pylist()
        complete = (
            batch["duration__complete"].to_pylist()
            if self._has_complete
            else repeat(True, batch.num_rows)
        )
        record_reasons = (
            batch["duration__reason"].to_pylist()
            if self._has_reason
            else repeat(None, batch.num_rows)
        )
        censor_reasons = batch["censor_reason"].to_pylist()
        keys = (
            zip(*(batch[name].to_pylist() for name in self._group_by), strict=True)
            if self._group_by
            else repeat((), batch.num_rows)
        )
        supports = [
            (name, policy, batch[name].to_pylist()) for name, policy in self._group_supports.items()
        ]

        for index, (key, origin, duration, entry, valid, record_reason) in enumerate(
            zip(keys, origins, durations, entries, complete, record_reasons, strict=True)
        ):
            is_complete = valid is True
            if not is_complete:
                self._groups[key].incomplete.add(record_reason or reason_codes.REASON_UNAVAILABLE)
            if origin is None or duration is None or entry is None:
                continue
            entry_elapsed = entry * self._entry_scale - origin * self._origin_scale
            if duration < 0 or entry_elapsed < 0:
                continue
            group = self._groups[key]
            support = group.support
            for name, policy, values in supports:
                value = values[index]
                if policy == "all":
                    if value is not True:
                        support[name] = False
                elif value is not None:
                    previous = support.get(name)
                    if policy == "reasons":
                        if previous is None:
                            support[name] = {value}
                        else:
                            previous.add(value)
                    elif (
                        previous is None
                        or (policy == "min" and value < previous)
                        or (policy == "max" and value > previous)
                    ):
                        support[name] = value

            terminal_bin = duration * self._duration_scale // self._width
            if terminal_bin > group.terminal_bin:
                self._output_bins += terminal_bin - group.terminal_bin
                if self._max_rows is not None and self._output_bins > self._max_rows:
                    raise _ROWS_EXCEEDED.error(limit=self._max_rows)
                group.terminal_bin = terminal_bin
            entry_bin = entry_elapsed // self._width
            # Both endpoint bins are at risk; an entry beyond the terminal bin contributes none.
            if entry_bin <= terminal_bin:
                entry_row = group.changes[entry_bin]
                entry_row.risk_delta += 1
                group.changes[terminal_bin + 1].risk_delta -= 1
                if entry_elapsed:
                    entry_row.delayed_entries += 1
            terminal_row = group.changes[terminal_bin]
            if events[index] and is_complete:
                terminal_row.events += 1
            else:
                terminal_row.censored += 1
                reason = (
                    censor_reasons[index] if is_complete else record_reason
                ) or reason_codes.UNKNOWN
                if terminal_row.reasons is None:
                    terminal_row.reasons = {}
                terminal_row.reasons[reason] = terminal_row.reasons.get(reason, 0) + 1

    def finish(self) -> pa.Table:
        return _life_table(self._schema, self._group_by, self._group_supports, self._groups)


def aggregate_life_table(
    records: pa.Table,
    *,
    interval_seconds: int,
    group_by: Sequence[str],
    group_supports: Mapping[str, str] | None = None,
    max_rows: int | None = None,
) -> pa.Table:
    """Aggregate time-to-event rows by elapsed-time bin without curve inference."""
    accumulator = _LifeTableAccumulator(
        records.schema,
        interval_seconds=interval_seconds,
        group_by=group_by,
        group_supports=group_supports,
        max_rows=max_rows,
    )
    for batch in records.to_batches(max_chunksize=65_536):
        accumulator.consume(batch)
    return accumulator.finish()


def _life_table(
    input_schema: pa.Schema,
    group_by: Sequence[str],
    group_supports: Mapping[str, str],
    groups: Mapping[tuple[object, ...], _GroupBins],
) -> pa.Table:
    schema_fields = [input_schema.field(name) for name in group_by]
    schema_fields.extend(input_schema.field(name) for name in group_supports)
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
    empty_bin = _BinChanges()
    keys = (key for key, group in groups.items() if group.terminal_bin >= 0)
    for unit, group_key in enumerate(sorted(keys, key=repr)):
        group = groups[group_key]
        reasons = sorted(group.incomplete)
        group_columns = dict(zip(group_by, group_key, strict=True))
        for name, policy in group_supports.items():
            if policy == "all":
                group_columns[name] = group.support.get(name, True)
            elif policy == "reasons":
                group_columns[name] = ";".join(sorted(group.support.get(name, ()))) or None
            else:
                group_columns[name] = group.support.get(name)
        at_risk = 0
        for time_bin in range(group.terminal_bin + 1):
            changes = group.changes.get(time_bin, empty_bin)
            at_risk += changes.risk_delta
            events_count = changes.events
            censored_count = changes.censored
            row = group_columns.copy()
            row.update(
                unit=unit,
                anchor_id=time_bin,
                t0=time_bin,
                time_bin=time_bin,
                at_risk=at_risk,
                events=events_count,
                censored=censored_count,
                complete=not reasons,
                reason=reasons,
                censored_by_reason=sorted(changes.reasons.items()) if changes.reasons else [],
                delayed_entries=changes.delayed_entries,
                event_rate=events_count / at_risk if at_risk else None,
                censor_rate=censored_count / at_risk if at_risk else None,
            )
            rows.append(row)
    table = pa.Table.from_pylist(rows, schema=schema)
    values = LIFE_TABLE_VALUES
    projection = []
    names = []
    for name in (
        *IDENTITY,
        *(
            item
            for value in group_by
            for item in (value, f"{value}__complete", f"{value}__reason")
            if item in table.column_names
        ),
        *(
            item
            for item in table.column_names
            if item not in group_supports and item not in group_by
        ),
        *group_supports,
    ):
        if name in names or name in ("complete", "reason"):
            continue
        names.append(name)
        projection.append(table[name])
        if name in values:
            names.extend((f"{name}__complete", f"{name}__reason"))
            projection.extend((table["complete"], table["reason"]))
    return pa.Table.from_arrays(projection, names=names)
