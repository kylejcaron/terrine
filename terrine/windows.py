"""Exact elapsed-second windows; endpoints are half-open and never bucketed."""

import re
from dataclasses import dataclass

from terrine.errors import RefusalSpec

__all__ = ["Window"]

_INVALID_DURATION = RefusalSpec(
    "terrine.window.invalid_duration",
    "{field}={value!r} is not an elapsed duration: use integral seconds or a signed "
    's/m/h/d/w string such as "30d" or "4w". Calendar months ("1mo", "1y") are '
    "accepted only by a Schedule's every.",
)
_INVALID_WINDOW = RefusalSpec(
    "terrine.window.invalid_window",
    "The window start is not before its end. "
    "Use window=(earlier_time, later_time); the start is included and the end is excluded.",
)
_PATTERN = re.compile(r"([+-]?[0-9]+)([smhdw])", re.ASCII)
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
_MIN = -(2**63)
_MAX = 2**63 - 1


def seconds(value: str | int, *, field: str) -> int:
    """Normalize an elapsed duration without coercion or calendar arithmetic.

    ``field`` names the caller's parameter in the refusal context.
    """
    if isinstance(value, bool):
        raise _INVALID_DURATION.error(field=field, value=str(value))
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and (match := _PATTERN.fullmatch(value)):
        # Bound digit count before int conversion to reject hostile giant values.
        digits = match[1].lstrip("+-").lstrip("0") or "0"
        if len(digits) > 19:
            raise _INVALID_DURATION.error(field=field, value=value)
        result = int(digits) * (-1 if match[1].startswith("-") else 1) * _UNITS[match[2]]
    else:
        raise _INVALID_DURATION.error(field=field, value=str(value))
    if not _MIN <= result <= _MAX:
        raise _INVALID_DURATION.error(field=field, value=str(value))
    return result


@dataclass(frozen=True, slots=True, init=False)
class Window:
    """An elapsed [start, end) interval relative to an exact anchor instant."""

    start: int
    end: int

    def __init__(self, start: str | int, end: str | int) -> None:
        start_seconds, end_seconds = seconds(start, field="window"), seconds(end, field="window")
        if start_seconds >= end_seconds:
            raise _INVALID_WINDOW.error()
        object.__setattr__(self, "start", start_seconds)
        object.__setattr__(self, "end", end_seconds)
