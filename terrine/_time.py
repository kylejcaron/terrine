"""Exact microsecond UTC instants for portable definitions."""

import re
from datetime import UTC, datetime

_FRACTION = re.compile(r"[.,]([0-9]+)")


def utc_instant(value: datetime | str) -> datetime:
    """Normalize an aware instant without silently discarding submicroseconds."""
    if isinstance(value, str):
        for match in _FRACTION.finditer(value):
            if match.group(1)[6:].strip("0"):
                raise ValueError("instant must be exactly representable at microsecond precision")
        value = datetime.fromisoformat(value)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("instant must be timezone-aware")
    value = value.astimezone(UTC)
    if type(value) is not datetime:
        return utc_instant(value.isoformat())
    return value
