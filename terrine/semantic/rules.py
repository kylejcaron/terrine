"""Pure semantic capability rules shared across binding and execution."""

from datetime import datetime
from typing import Literal

from terrine.backends.registry import engine_supports
from terrine.errors import RefusalSpec
from terrine.semantic.time import boundary_timezone
from terrine.windows import Window

__all__ = [
    "UNALIGNED_CALENDAR_AVERAGE",
    "UNSUPPORTED_TEMPORAL_KIND",
    "UNSUPPORTED_TRANSFORM",
    "UNSUPPORTED_TEMPORAL_AGGREGATION",
    "UNSUPPORTED_EVENT_RESOLUTION",
    "calendar_average_alignment",
    "cutoff_alignment",
    "supports_event_resolution",
    "supports_temporal_aggregation",
]

UNALIGNED_CALENDAR_AVERAGE = RefusalSpec(
    "terrine.metric.unaligned_calendar_average",
    "A per-day mean needs a window of whole local days with both window bounds "
    "at local midnight. Choose matching times, or use an event sum or per-event mean "
    "for other window bounds.",
)
UNSUPPORTED_TEMPORAL_KIND = RefusalSpec(
    "terrine.metric.unsupported_temporal_kind",
    "Metric kind {kind!r} cannot be calculated over a time window. "
    "Use a mean, count, sum or ratio metric over the window.",
)
UNSUPPORTED_TRANSFORM = RefusalSpec(
    "terrine.metric.unsupported_transform",
    "Metric transform {transform!r} cannot be applied over a time window. "
    "Use the metric without this transform.",
)

UNKNOWN_FILTER = RefusalSpec(
    "terrine.filter.unknown",
    "Metric filters are unknown for {failed_count} requested rows. "
    "Provide the missing label history and resolve null or non-finite label values "
    "and filter values before exporting.",
)

SUPPORTED_TEMPORAL_AGGREGATIONS = frozenset({"sum", "count", "avg_event", "avg_calendar_day"})
UNSUPPORTED_TEMPORAL_AGGREGATION = RefusalSpec(
    "terrine.metric.unsupported_aggregation",
    "Metric aggregation {aggregation!r} cannot be calculated over a time window. "
    "Use count, sum, a per-event mean or a per-day mean.",
)


def supports_temporal_aggregation(aggregation: str) -> bool:
    """Whether the shared additive temporal engine implements this aggregation."""
    return aggregation in SUPPORTED_TEMPORAL_AGGREGATIONS


UNSUPPORTED_EVENT_RESOLUTION = RefusalSpec(
    "terrine.temporal.unsupported_event_resolution",
    "Exact event-time windows are unavailable for {dialect!r}. "
    "Use local-midnight t0 and window bounds, or use DuckDB.",
)


def supports_event_resolution(dialect: str) -> bool:
    """Whether a bound backend profile supports exact event-time resolution.

    PostgreSQL and BigQuery resolve exactly at their native microsecond
    precision, Snowflake at its nanosecond precision.
    """
    return engine_supports(dialect, "event_resolution")


def _wall_clock_aligned(anchor: datetime, offset_seconds: int, day_boundary: str) -> bool:
    zone = boundary_timezone(day_boundary)
    source_offset = anchor.utcoffset()
    target_offset = zone.utcoffset(None)
    if anchor.tzinfo is None or source_offset is None:
        raise ValueError("anchor must be timezone-aware")
    assert target_offset is not None
    shift = target_offset - source_offset
    ticks = (
        (anchor.hour * 3600 + anchor.minute * 60 + anchor.second) * 1_000_000
        + anchor.microsecond
        + (shift.days * 86400 + shift.seconds) * 1_000_000
        + shift.microseconds
        + offset_seconds * 1_000_000
    )
    return ticks % (86400 * 1_000_000) == 0


def calendar_average_alignment(
    window: Window, *, anchor: datetime | None, day_boundary: str
) -> Literal["aligned", "unaligned", "runtime_check"]:
    """Classify whether a calendar-day average has positive aligned local-day endpoints."""
    if not isinstance(window, Window):
        raise TypeError("window must be a Window")
    duration = window.end - window.start
    if duration <= 0 or duration % 86400:
        return "unaligned"
    if anchor is None:
        return "runtime_check"
    result = _wall_clock_aligned(anchor, window.start, day_boundary)
    return "aligned" if result else "unaligned"


def cutoff_alignment(
    cutoff: int, *, anchor: datetime | None, day_boundary: str
) -> Literal["aligned", "unaligned", "runtime_check"]:
    """Classify whether an elapsed cutoff lands on a local day boundary."""
    if isinstance(cutoff, bool) or not isinstance(cutoff, int):
        raise TypeError("cutoff must be integer seconds")
    if anchor is None:
        return "runtime_check"
    result = _wall_clock_aligned(anchor, cutoff, day_boundary)
    return "aligned" if result else "unaligned"
