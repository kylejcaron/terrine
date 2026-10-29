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
    "Window start {start_offset!r} is not before end {end_offset!r}. "
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


def _duration(value: int, *, weeks: bool = False, signed: bool = False) -> str:
    """Use the largest exact elapsed unit, with optional weeks and an explicit sign."""
    magnitude = abs(value)
    if not magnitude:
        return "0s"
    sign = "-" if signed and value < 0 else "+" if signed else ""
    if weeks and magnitude % 604_800 == 0:
        return f"{sign}{magnitude // 604_800}w"
    for unit in ("d", "h", "m", "s"):
        size = _UNITS[unit]
        if magnitude % size == 0:
            return f"{sign}{magnitude // size}{unit}"
    raise AssertionError("every integer is a multiple of one second")


@dataclass(frozen=True, slots=True, init=False)
class Window:
    """An elapsed [start, end) interval relative to an exact anchor instant."""

    start: int
    end: int

    def __init__(self, start: str | int, end: str | int) -> None:
        start_seconds, end_seconds = seconds(start, field="window"), seconds(end, field="window")
        if start_seconds >= end_seconds:
            raise _INVALID_WINDOW.error(
                start=start_seconds,
                end=end_seconds,
                start_offset=_duration(start_seconds, signed=True),
                end_offset=_duration(end_seconds, signed=True),
            )
        object.__setattr__(self, "start", start_seconds)
        object.__setattr__(self, "end", end_seconds)
