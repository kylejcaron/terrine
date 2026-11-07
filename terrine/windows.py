"""Exact elapsed-second windows; endpoints are half-open and never bucketed.

Public elapsed durations use signed integer units ``s``, ``min``, ``h``, ``d``
or ``w`` for seconds, minutes, hours, days or weeks. An optional ``+`` or ``-``
sign is accepted. Bare integer ``0`` is allowed; nonzero bare integers,
decimals (``"1.5d"``), noncanonical compounds and unitless strings are refused.
Minutes are spelled ``min``; ``m`` is reserved for calendar months spelled ``mo``.
Offsets are exact integer seconds within the signed 64-bit range, held as
`Duration` values whose text uses the largest exact unit or canonical hour/minute parts.

Calendar months (``"1mo"``) and years (``"1y"``) are `Months` values, accepted only
by ``Schedule.every`` and recipe ``every=`` parameters, not by elapsed windows.
"""

import re
from dataclasses import dataclass
from typing import Literal

from terrine.errors import RefusalSpec, parameter_names

__all__ = ["Window", "Duration", "Months", "duration"]

_INVALID_DURATION = RefusalSpec(
    "terrine.window.invalid_duration",
    "{field}={value!r} is not an elapsed duration: use a signed integer with a unit "
    'such as "-30s", "-30min", or "4w"; bare 0 is allowed. Calendar months ("1mo", "1y") '
    "are accepted only by a Schedule's every.",
)
_INVALID_MONTHS = RefusalSpec(
    "terrine.window.invalid_duration",
    "{field}={value!r} is not a calendar step: use a whole number of months such as "
    'Months(3), or text such as "3mo" or "1y".',
)
_INVALID_STEP = RefusalSpec("terrine.design.invalid", "Design input {field} is invalid: {reason}.")
_INVALID_WINDOW = RefusalSpec(
    "terrine.window.invalid_window",
    "Window start {start_offset!r} is not before end {end_offset!r}. "
    "Use window=(earlier_time, later_time); the start is included and the end is excluded.",
)
_PATTERN = re.compile(r"([+-]?[0-9]+)(min|[shdw])", re.ASCII)
_LEGACY_PATTERN = re.compile(r"([+-]?[0-9]+)(min|m|[shdw])", re.ASCII)
_CALENDAR_STEP = re.compile(r"([0-9]{1,19})(mo|y)", re.ASCII)
_UNITS = {"s": 1, "min": 60, "m": 60, "h": 3600, "d": 86400, "w": 604800}
_MIN = -(2**63)
_MAX = 2**63 - 1
_MAX_MONTHS = 10**19 - 1


@dataclass(frozen=True, slots=True, init=False)
class Duration:
    """Hold an exact signed elapsed duration in whole seconds.

    Values compare and hash by seconds; ``str()`` is canonical text using the largest exact
    unit, or hour and minute parts, which `duration` parses back to an equal value.

    Args:
        seconds: Signed whole seconds within the 64-bit integer range; booleans are refused.

    Attributes:
        seconds: Signed whole seconds within the 64-bit integer range.

    Example: from terrine.windows import Duration; str(Duration(604_800))
    """

    seconds: int

    def __init__(self, seconds: int) -> None:
        if isinstance(seconds, bool) or not isinstance(seconds, int) or not _MIN <= seconds <= _MAX:
            raise _INVALID_DURATION.error(field="seconds", value=str(seconds))
        object.__setattr__(self, "seconds", seconds)
    def __str__(self) -> str:
        text = _duration(abs(self.seconds), weeks=True)
        return f"-{text}" if self.seconds < 0 else text

    def __repr__(self) -> str:
        return f"Duration({str(self)!r})"


@dataclass(frozen=True, slots=True, init=False)
class Months:
    """Hold a positive calendar step in whole months.

    Each scheduled date clamps independently to its month's last day. ``str()`` is
    ``"<n>mo"``, or ``"<n/12>y"`` for whole years.

    Args:
        months: Positive whole number of calendar months, at most 19 decimal digits.

    Attributes:
        months: Positive whole number of calendar months, at most 19 decimal digits.

    Example: from terrine.windows import Months; str(Months(12))
    """

    months: int

    def __init__(self, months: int) -> None:
        if isinstance(months, bool) or not isinstance(months, int):
            raise _INVALID_MONTHS.error(field="months", value=str(months))
        if months <= 0:
            raise _INVALID_STEP.error(reason="not_positive", field="months")
        if months > _MAX_MONTHS:
            raise _INVALID_STEP.error(reason="out_of_range", field="months")
        object.__setattr__(self, "months", months)

    def __str__(self) -> str:
        years, months = divmod(self.months, 12)
        return f"{years}y" if months == 0 else f"{self.months}mo"

    def __repr__(self) -> str:
        return f"Months({self.months})"


def duration(value: str | Duration | Literal[0], *, field: str) -> Duration:
    """Normalize a public elapsed-duration input; only bare zero is accepted.

    Args:
        value: Duration string (see `terrine.windows`), Duration value, or bare 0.
        field: Input name reported by a refusal.

    Returns:
        The exact Duration; malformed, calendar and nonzero bare integer inputs refuse.

    Example: from terrine.windows import duration; duration("-30d", field="window")
    """
    if isinstance(value, Duration):
        return value
    if isinstance(value, bool) or (isinstance(value, int) and value != 0):
        raise _INVALID_DURATION.error(field=field, value=str(value), reason="bare_integer")
    return Duration(_seconds(value, field=field))


def calendar_or_duration(
    value: str | Duration | Months | Literal[0], *, field: str
) -> Duration | Months:
    """Normalize a step that may be a calendar month count or an elapsed duration."""
    if isinstance(value, Months):
        return value
    if isinstance(value, str) and (match := _CALENDAR_STEP.fullmatch(value)):
        with parameter_names(months=field):
            return Months(int(match[1]) * (12 if match[2] == "y" else 1))
    return duration(value, field=field)


def _seconds(value: str | int, *, field: str, legacy_minutes: bool = False) -> int:
    """Normalize elapsed seconds, including trusted normalized integer metadata."""
    if isinstance(value, bool):
        raise _INVALID_DURATION.error(field=field, value=str(value))
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and (
        match := (_LEGACY_PATTERN if legacy_minutes else _PATTERN).fullmatch(value)
    ):
        digits = match[1].lstrip("+-").lstrip("0") or "0"
        if len(digits) > 19:
            raise _INVALID_DURATION.error(field=field, value=value)
        result = int(digits) * (-1 if match[1].startswith("-") else 1) * _UNITS[match[2]]
    elif isinstance(value, str) and (
        match := re.fullmatch(r"([+-]?)([0-9]+)h([0-9]+)min", value, re.ASCII)
    ):
        if len(match[2]) > 19 or len(match[3]) > 19:
            raise _INVALID_DURATION.error(field=field, value=value)
        result = (int(match[2]) * 3600 + int(match[3]) * 60) * (
            -1 if match[1] == "-" else 1
        )
    elif isinstance(value, str) and re.fullmatch(r"[+-]?[0-9]+m", value, re.ASCII):
        raise _INVALID_DURATION.error(
            field=field,
            value=value,
            hint="use 'min' for minutes or 'mo' for months",
        )
    else:
        raise _INVALID_DURATION.error(field=field, value=str(value))
    if not _MIN <= result <= _MAX:
        raise _INVALID_DURATION.error(field=field, value=str(value))
    return result


def _elapsed(value: int, *, field: str) -> Duration:
    """Hold computed whole seconds; a value outside int64 refuses under the caller's field."""
    if isinstance(value, bool) or not isinstance(value, int) or not _MIN <= value <= _MAX:
        raise _INVALID_DURATION.error(field=field, value=f"{value}s")
    return Duration(value)


def _duration(value: int, *, weeks: bool = False, signed: bool = False) -> str:
    """Use the largest exact elapsed unit, with optional weeks and an explicit sign."""
    magnitude = abs(value)
    if not magnitude:
        return "0s"
    sign = "-" if signed and value < 0 else "+" if signed else ""
    if weeks and magnitude % 604_800 == 0:
        return f"{sign}{magnitude // 604_800}w"
    if magnitude % 86_400 == 0:
        return f"{sign}{magnitude // 86_400}d"
    if magnitude % 3_600 == 0:
        return f"{sign}{magnitude // 3_600}h"
    if magnitude >= 3_600 and magnitude % 60 == 0:
        hours, remainder = divmod(magnitude, 3_600)
        return f"{sign}{hours}h{remainder // 60}min"
    if magnitude % 60 == 0:
        return f"{sign}{magnitude // 60}min"
    return f"{sign}{magnitude}s"


@dataclass(frozen=True, slots=True, init=False)
class Window:
    """Describe an elapsed [start, end) window relative to t0 or a selected event time.


    Offsets use signed integers with s, min, h, d or w units, Duration values, or bare 0.
    Decimals, noncanonical compounds and calendar mo/y offsets are refused; calendar
    steps are accepted only by Schedule.every and recipe every= parameters.

    Args:
        start: Earlier offset as a duration string (see `terrine.windows`), Duration or bare 0;
            included.
        end: Later offset as a duration string (see `terrine.windows`), Duration or bare 0;
            excluded.

    Attributes:
        start: Inclusive elapsed offset as a Duration.
        end: Exclusive elapsed offset as a Duration.

    Example: from terrine.windows import Window; Window("-30d", "0s")
    """

    start: Duration
    end: Duration

    def __init__(
        self, start: str | Duration | Literal[0], end: str | Duration | Literal[0]
    ) -> None:
        start_value, end_value = duration(start, field="window"), duration(end, field="window")
        if start_value.seconds >= end_value.seconds:
            raise _INVALID_WINDOW.error(
                start=start_value.seconds,
                end=end_value.seconds,
                start_offset=_duration(start_value.seconds, signed=True),
                end_offset=_duration(end_value.seconds, signed=True),
            )
        object.__setattr__(self, "start", start_value)
        object.__setattr__(self, "end", end_value)

    def __repr__(self) -> str:
        return f"Window({str(self.start)!r}, {str(self.end)!r})"
