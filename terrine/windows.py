"""Exact elapsed-second windows; endpoints are half-open and never bucketed."""

import re
from dataclasses import dataclass

from terrine.errors import RefusalSpec

__all__ = ["Window"]

_INVALID_DURATION = RefusalSpec(
    "terrine.window.invalid_duration",
    "Expected integral elapsed seconds or a signed s/m/h/d/w duration.",
)
_INVALID_WINDOW = RefusalSpec(
    "terrine.window.invalid_window", "Window start must precede its exclusive end."
)
_PATTERN = re.compile(r"([+-]?[0-9]+)([smhdw])", re.ASCII)
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
_MIN = -(2**63)
_MAX = 2**63 - 1


def seconds(value: str | int) -> int:
    """Normalize an elapsed duration without coercion or calendar arithmetic."""
    if isinstance(value, bool):
        raise _INVALID_DURATION.error()
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and (match := _PATTERN.fullmatch(value)):
        # Bound digit count before int conversion to reject hostile giant values.
        digits = match[1].lstrip("+-").lstrip("0") or "0"
        if len(digits) > 19:
            raise _INVALID_DURATION.error()
        result = int(digits) * (-1 if match[1].startswith("-") else 1) * _UNITS[match[2]]
    else:
        raise _INVALID_DURATION.error()
    if not _MIN <= result <= _MAX:
        raise _INVALID_DURATION.error()
    return result


@dataclass(frozen=True, slots=True, init=False)
class Window:
    """An elapsed [start, end) interval relative to an exact anchor instant."""

    start: int
    end: int

    def __init__(self, start: str | int, end: str | int) -> None:
        start_seconds, end_seconds = seconds(start), seconds(end)
        if start_seconds >= end_seconds:
            raise _INVALID_WINDOW.error()
        object.__setattr__(self, "start", start_seconds)
        object.__setattr__(self, "end", end_seconds)
