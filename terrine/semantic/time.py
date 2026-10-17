"""Pure fixed-boundary date and instant conversion helpers."""

import re
from datetime import UTC, date, datetime, timedelta, timezone

from terrine._time import utc_instant

__all__ = ["boundary_timezone", "instant"]


def boundary_timezone(boundary: str):
    """Return a fixed timezone for UTC, signed HH:MM, or UTC-prefixed offsets."""
    if boundary == "UTC":
        return UTC
    match = (
        re.fullmatch(r"(?:UTC)?([+-])([0-9]{2}):([0-9]{2})", boundary)
        if isinstance(boundary, str)
        else None
    )
    if match is None:
        raise ValueError("day_boundary must be UTC, ±HH:MM, or UTC±HH:MM")
    hours, minutes = int(match[2]), int(match[3])
    if hours > 14 or minutes > 59 or (hours == 14 and minutes != 0):
        raise ValueError("day_boundary offset is outside the fixed timezone range")
    delta = timedelta(hours=hours, minutes=minutes)
    if match[1] == "-":
        delta = -delta
    return timezone(delta)


def instant(value: date | datetime | str, boundary: str) -> datetime:
    """Normalize a date or timestamp to a UTC-aware instant under a fixed boundary."""
    zone = boundary_timezone(boundary)
    if isinstance(value, str):
        if len(value) != 10:
            return utc_instant(value)
        value = date.fromisoformat(value)
    if isinstance(value, datetime):
        return utc_instant(value)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), zone).astimezone(UTC)
    raise TypeError("expected a date, aware datetime, or ISO string")
