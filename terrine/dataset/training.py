"""Assign splits from retained label support without changing row membership."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pyarrow as pa

from terrine.dataset.splits import (
    GroupSplit,
    SplitAssignment,
    TimeSplit,
    assign_group_splits,
    assign_splits,
    reason_counts,
)


def assign_training_splits(
    table: pa.Table,
    *,
    compiled: Any,
    labels: tuple[str, ...],
    policy: TimeSplit | GroupSplit,
    binned_state: bool,
) -> tuple[pa.Table, dict[str, int]]:
    records: list[dict[str, Any]] = []
    for index, row in enumerate(table.to_pylist()):
        origin = row[compiled.roles.t0]
        support, reasons = {}, {}
        for label in labels:
            metadata = compiled.columns[label]
            timing = compiled.expression_timings.get(label)
            point = False
            if label == "to_state" and "to_time" in row:
                end = row["to_time"]
                point = not binned_state
            elif timing is not None and timing.end_bound_column is not None:
                end = row[timing.end_bound_column]
            elif (
                timing is not None
                and timing.kind == "event_time"
                and timing.relative_offsets is None
                and timing.point_time_column is not None
            ):
                end = row[timing.point_time_column]
                point = True
            elif metadata.window is not None:
                end = origin + timedelta(seconds=metadata.window.end)
            else:
                end = None
            available = (
                row.get(metadata.complete_column) is True
                and row[label] is not None
                and end is not None
            )
            support[label] = {"available": available, "end": end, "point": point}
            reasons[label] = row.get(metadata.reason_column) or "label_unavailable"
        records.append(
            {
                **row,
                "id": index,
                "origin": origin,
                "label_support": support,
                "label_reasons": reasons,
            }
        )
    if isinstance(policy, TimeSplit):
        assignments = assign_splits(records, policy)
    else:
        assignments = assign_group_splits(records, policy)
        for row in records:
            for label, item in row["label_support"].items():
                if not item["available"]:
                    assignments[row["id"]] = SplitAssignment(
                        "unassigned", row["label_reasons"][label]
                    )
                    break
    return (
        table.append_column(
            "__terrine_split", pa.array([item.split for item in assignments.values()], pa.string())
        ).append_column(
            "__terrine_split_reason",
            pa.array([item.reason for item in assignments.values()], pa.string()),
        ),
        reason_counts(assignments),
    )
