"""Temporal dependencies of individual additive fact uses, independent of storage."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


def _utc(value: datetime | None) -> datetime | None:
    try:
        if value is not None and value.utcoffset() is not None:
            return value.astimezone(UTC)
    except (OverflowError, ValueError):
        pass
    return None


@dataclass(frozen=True, slots=True)
class WindowUse:
    """One source's elapsed-time support at a study unit and origin.

    Membership is ``anchor + lower <= instant < anchor + upper``. This describes
    candidate fact rows, not independent integrity checks or a physical read.
    Unknown coordinates conservatively intersect a change.
    """

    source: str
    unit: str
    lower_seconds: int
    upper_seconds: int

    def __post_init__(self) -> None:
        if self.lower_seconds >= self.upper_seconds:
            raise ValueError("window support requires lower_seconds < upper_seconds")

    def input_span(
        self, first_anchor: datetime | None, last_anchor: datetime | None
    ) -> tuple[datetime, datetime] | None:
        """Return the UTC half-open hull for these inclusive anchor extrema, or unknown."""
        first, last = _utc(first_anchor), _utc(last_anchor)
        if first is None or last is None or first > last:
            return None
        try:
            return first + timedelta(seconds=self.lower_seconds), last + timedelta(
                seconds=self.upper_seconds
            )
        except OverflowError:
            return None

    def contains_change(self, anchor: datetime | None, *, instant: datetime | None) -> bool:
        """Test a point change; affected origins satisfy ``x - upper < a <= x - lower``."""
        span = self.input_span(anchor, anchor)
        instant = _utc(instant)
        return span is None or instant is None or span[0] <= instant < span[1]

    def intersects_change(
        self, anchor: datetime | None, *, start: datetime | None, end: datetime | None
    ) -> bool:
        """Test a half-open interval change; known empty intervals affect nothing."""
        start, end = _utc(start), _utc(end)
        if start is None or end is None or start > end:
            return True
        if start == end:
            return False
        span = self.input_span(anchor, anchor)
        return span is None or (start < span[1] and span[0] < end)


def _shared_input_span(
    uses: Iterable[WindowUse], origin_bounds: tuple[datetime, datetime]
) -> tuple[datetime, datetime] | None:
    """Hull for consumers of a shared read; one unknown use prevents pruning."""
    hull = None
    for use in uses:
        span = use.input_span(*origin_bounds)
        if span is None:
            return None
        hull = span if hull is None else (min(hull[0], span[0]), max(hull[1], span[1]))
    return hull
