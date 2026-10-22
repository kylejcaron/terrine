"""Sparse range joins with declared, input-derived cardinality bounds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from terrine.errors import RefusalSpec
from terrine.temporal._timestamps import (
    _remainder,
    compare_timestamps,
    integer_quotient,
    timestamp_parts,
)
from terrine.windows import Window

_RANGE_BOUND = RefusalSpec(
    "terrine.temporal.range_bound_unavailable",
    "A bounded range strategy is unavailable for {expression}: {reason}. "
    "The candidate multiplier limit is {limit}; {route}.",
)
_MAX_CANDIDATES = 128


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


def _bucket(
    timestamp: ir.TimestampValue, width: int, *, offset_seconds: int = 0
) -> ir.NumericValue:
    _rank, key_seconds, key_nanos = timestamp_parts(timestamp)
    zero = ibis.literal(0)
    seconds = cast(
        ir.IntegerValue, key_seconds - cast(ir.IntegerValue, (key_nanos < zero).cast(dt.int64))
    )
    quotient = integer_quotient(seconds, width, like=timestamp)
    remainder = cast(ir.IntegerValue, _remainder(seconds, width))
    negative = cast(ir.IntegerValue, (remainder < zero).cast(dt.int64))
    wide = dt.Decimal(38, 0)
    base = cast(ir.DecimalValue, (quotient - negative).cast(wide))
    if offset_seconds == 0:
        return base
    # Split before addition so bucket division stays integral at wide offsets.
    shift, residue = divmod(offset_seconds, width)
    shifted = base + cast(ir.DecimalValue, ibis.literal(shift, type=wide))
    if residue:
        normalized = cast(ir.IntegerValue, remainder + negative * width)
        shifted += cast(
            ir.IntegerValue, (normalized >= ibis.literal(width - residue)).cast(dt.int64)
        )
    return shifted


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
    first_bucket = _bucket(anchors.t0, plan.block_seconds, offset_seconds=window.start)
    last_bucket = _bucket(anchors.t0, plan.block_seconds, offset_seconds=window.end)
    first = anchors.mutate(**{bucket: first_bucket})
    last = anchors.mutate(**{bucket: last_bucket}).filter(first_bucket != last_bucket)
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
            candidates[event_time], candidates.t0, "ge", right_offset_seconds=window.start
        )
        & compare_timestamps(
            candidates[event_time], candidates.t0, "lt", right_offset_seconds=window.end
        )
    )
    return RangeJoin(requests, candidates, matched)
