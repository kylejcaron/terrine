"""Pure semantic capability rules shared across binding and execution."""

from datetime import datetime, timedelta
from typing import Literal

from terrine.errors import RefusalSpec
from terrine.semantic.time import boundary_timezone
from terrine.windows import Window

__all__ = [
    "UNALIGNED_CALENDAR_AVERAGE",
    "UNSUPPORTED_TEMPORAL_KIND",
    "UNSUPPORTED_TRANSFORM",
    "calendar_average_alignment",
]

UNALIGNED_CALENDAR_AVERAGE = RefusalSpec(
    "terrine.metric.unaligned_calendar_average",
    "avg_calendar_day requires a positive whole number of local days with both endpoints "
    "at local midnight; use sum or avg_event for arbitrary instants, or choose genuinely "
    "aligned anchors and endpoints.",
)
UNSUPPORTED_TEMPORAL_KIND = RefusalSpec(
    "terrine.metric.unsupported_temporal_kind",
    "Metric kind {kind!r} is not supported by the temporal engine.",
)
UNSUPPORTED_TRANSFORM = RefusalSpec(
    "terrine.metric.unsupported_transform",
    "Metric transform {transform!r} is declared but cannot be compiled by the temporal engine.",
)


def _midnight(value: datetime) -> bool:
    return value.hour == value.minute == value.second == value.microsecond == 0


def calendar_average_alignment(
    window: Window, *, anchor: datetime | None, day_boundary: str
) -> Literal["aligned", "unaligned", "runtime_check"]:
    """Classify whether a calendar-day average has positive aligned local-day endpoints."""
    if not isinstance(window, Window):
        raise TypeError("window must be a Window")
    zone = boundary_timezone(day_boundary)
    duration = window.end - window.start
    if duration <= 0 or duration % 86400:
        return "unaligned"
    if anchor is None:
        return "runtime_check"
    if anchor.tzinfo is None or anchor.utcoffset() is None:
        raise ValueError("anchor must be timezone-aware")
    local = anchor.astimezone(zone)
    start = local + timedelta(seconds=window.start)
    end = local + timedelta(seconds=window.end)
    return "aligned" if _midnight(start) and _midnight(end) else "unaligned"
