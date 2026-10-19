"""Sparse range joins with declared, input-derived cardinality bounds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.temporal._timestamps import (
    _quotient,
    _remainder,
    compare_timestamps,
    timestamp_order_key,
)
from terrine.windows import Window


@dataclass(frozen=True, slots=True)
class RangePlan:
    """Bound bucket candidates by a multiple of the selected event count.

    For A anchors and E events, requests <= 2A and candidates <= K E, where
    K is ``max_candidates_per_event``. This does not bound source scan bytes.
    """

    block_seconds: int
    max_candidates_per_event: int
    basis: str

    def __post_init__(self) -> None:
        for value in (self.block_seconds, self.max_candidates_per_event):
            if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < 2**63:
                raise ValueError("range bounds must be positive signed Int64 integers")
        if not self.basis:
            raise ValueError("range bounds require a derivation basis")


@dataclass(frozen=True, slots=True)
class RangeJoin:
    requests: ibis.Table
    candidates: ibis.Table
    matched: ibis.Table


def _bucket(timestamp: ir.TimestampValue, width: int) -> ir.NumericValue:
    key = timestamp_order_key(timestamp)
    zero = ibis.literal(0)
    seconds = cast(ir.IntegerValue, key["seconds"]) - cast(
        ir.IntegerValue, (cast(ir.IntegerValue, key["nanos"]) < zero).cast(dt.int64)
    )
    quotient = cast(ir.IntegerValue, _quotient(seconds, width))
    remainder = cast(ir.IntegerValue, _remainder(seconds, width))
    return quotient - cast(ir.IntegerValue, (remainder < zero).cast(dt.int64))


def blocked_range_join(
    anchors: ibis.Table,
    events: ibis.Table,
    *,
    plan: RangePlan,
    window: Window,
    event_unit: str,
    event_timestamp: str,
    unit_column: str = "unit",
) -> RangeJoin:
    """Match two sparse time buckets per interval, then apply exact endpoints.

    The caller derives the candidate multiplicity from the anchor declaration;
    no relation cardinality is guessed or measured by this operation.
    """
    if plan.block_seconds != window.end - window.start:
        raise ValueError("range block width must equal the window width")
    reserved = set(anchors.columns) | set(events.columns)
    bucket = "__terrine_range_bucket"
    while bucket in reserved:
        bucket += "_"
    start = anchors.t0 + ibis.interval(seconds=window.start)
    end = anchors.t0 + ibis.interval(seconds=window.end)
    first = anchors.mutate(**{bucket: _bucket(start, plan.block_seconds)})
    last = anchors.mutate(**{bucket: _bucket(end, plan.block_seconds)}).filter(
        _bucket(start, plan.block_seconds) != _bucket(end, plan.block_seconds)
    )
    requests = first.union(last, distinct=False)
    selected = events.mutate(**{bucket: _bucket(events[event_timestamp], plan.block_seconds)})
    candidates = requests.inner_join(
        selected,
        [requests[unit_column] == selected[event_unit], requests[bucket] == selected[bucket]],
    )
    # Timestamp predicates refer to the joined relation so they remain below
    # the equality join even on backends without inequality-join support.
    event_time = event_timestamp
    if event_time in anchors.columns:
        event_time += "_right"
    matched = candidates.filter(
        compare_timestamps(
            candidates[event_time], candidates.t0 + ibis.interval(seconds=window.start), "ge"
        )
        & compare_timestamps(
            candidates[event_time], candidates.t0 + ibis.interval(seconds=window.end), "lt"
        )
    )
    return RangeJoin(requests, candidates, matched)
