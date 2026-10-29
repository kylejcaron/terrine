"""Pure immutable anchor definitions for study designs."""

from __future__ import annotations

import calendar
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from terrine._time import utc_instant
from terrine.errors import RefusalSpec
from terrine.windows import seconds

__all__ = ["FixedDate", "EventAnchor", "Schedule", "AnchorRange"]

_INVALID = RefusalSpec("terrine.design.invalid", "Design input {field} is invalid: {reason}.")
_CHOICE = RefusalSpec("terrine.design.invalid", "Design input {field} must be one of {choices}.")
_DATE_END_OVERFLOW = RefusalSpec(
    "terrine.design.invalid",
    "End day {end!r} leaves no following day to include the whole end day. "
    "Choose an earlier end day.",
)
_REVERSED_DAYS = RefusalSpec(
    "terrine.design.invalid",
    "End day {end!r} is before start day {start!r}. "
    "The end is the last included day; choose an end on or after the start.",
)
_REVERSED_INSTANTS = RefusalSpec(
    "terrine.design.invalid",
    "End time {end!r} is not after start time {start!r}. "
    "Choose a later end time; the end time itself is excluded.",
)
_CALENDAR_STEP = re.compile(r"([0-9]{1,19})(mo|y)", re.ASCII)


def _date(value: date | str, field: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, (date, str)):
        raise _INVALID.error(reason="invalid_date", field=field)
    try:
        return date.fromisoformat(value) if isinstance(value, str) else value
    except ValueError as error:
        raise _INVALID.error(reason="invalid_date", field=field) from error


def _aware_instant(value: object, field: str) -> datetime:
    """An aware datetime or ISO instant as exact UTC, or a coded design refusal."""
    if isinstance(value, datetime) and (value.tzinfo is None or value.utcoffset() is None):
        raise _INVALID.error(reason="naive_datetime", field=field)
    if not isinstance(value, (datetime, str)):
        raise _INVALID.error(reason="wrong_type", field=field)
    try:
        return utc_instant(value)
    except (ValueError, OverflowError) as error:
        raise _INVALID.error(reason="invalid_instant", field=field) from error


def _range_bound(value: date | datetime | str | None, field: str) -> date | datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        if "T" in value or " " in value:
            return _aware_instant(value, field)
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise _INVALID.error(reason="invalid_date", field=field) from error
    if isinstance(value, datetime):
        return _aware_instant(value, field)
    if isinstance(value, date):
        return value
    raise _INVALID.error(reason="wrong_type", field=field)


@dataclass(frozen=True, slots=True, init=False)
class FixedDate:
    date: date | datetime

    def __init__(self, date: date | datetime | str) -> None:
        bound = _range_bound(date, "date")
        if bound is None:
            raise _INVALID.error(reason="wrong_type", field="date")
        object.__setattr__(self, "date", bound)


@dataclass(frozen=True, slots=True, init=False)
class EventAnchor:
    """Select an original event, then shift time zero by a non-negative elapsed duration."""

    event: str
    which: str
    n: int | None
    offset: int

    def __init__(
        self, event: str, which: str = "first", n: int | None = None, offset: str | int = 0
    ) -> None:
        if not isinstance(event, str) or not event.strip():
            raise _INVALID.error(reason="empty_name", field="event")
        if which not in ("first", "each", "nth"):
            raise _CHOICE.error(
                reason="unsupported_value", field="which", choices=("first", "each", "nth")
            )
        if which == "nth":
            if isinstance(n, bool) or not isinstance(n, int) or n < 1:
                raise _INVALID.error(reason="not_positive_integer", field="n")
        elif n is not None:
            raise _INVALID.error(reason="n_requires_nth", field="n")
        elapsed = seconds(offset, field="offset")
        if elapsed < 0:
            raise _INVALID.error(reason="negative", field="offset")
        object.__setattr__(self, "event", event)
        object.__setattr__(self, "which", which)
        object.__setattr__(self, "n", n)
        object.__setattr__(self, "offset", elapsed)


def _inclusive_end(start: date | None, end: date, reason: str) -> None:
    """Refuse a calendar end that cannot be widened by one day or precedes its start."""
    if end == date.max:
        raise _DATE_END_OVERFLOW.error(reason="date_overflow", field="end", end=end.isoformat())
    if start is not None and start > end:
        raise _REVERSED_DAYS.error(
            reason=reason, field="end", start=start.isoformat(), end=end.isoformat()
        )


def add_months(day: date, months: int) -> date:
    """Advance ``day`` by whole calendar months, clamping to the target month's last day.

    ``2025-01-31 + 1`` is ``2025-02-28``, ``+ 2`` is ``2025-03-31``; ``2024-02-29 + 12`` is
    ``2025-02-28``. The target month must stay within the calendar's year range.
    """
    index = day.year * 12 + day.month - 1 + months
    year, month = divmod(index, 12)
    month += 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


@dataclass(frozen=True, slots=True, init=False)
class Schedule:
    """Anchors every ``every`` from ``start`` through ``end``, both days included.

    ``every`` is an elapsed duration (``"7d"``, ``"4w"``, integral seconds) or a calendar step
    (``"1mo"``, ``"3mo"``, ``"1y"``); exactly one of ``every`` and ``months`` is set. Calendar
    anchors are ``start + k * months``, each clamped to its own month's last day, never chained
    from the previous anchor.
    """

    every: int | None
    months: int | None
    start: date
    end: date

    def __init__(self, every: str | int, start: date | str, end: date | str) -> None:
        if isinstance(every, str) and (match := _CALENDAR_STEP.fullmatch(every)):
            step = int(match[1]) * (12 if match[2] == "y" else 1)
            interval, months = None, step
        else:
            step = seconds(every, field="every")
            interval, months = step, None
        if step <= 0:
            raise _INVALID.error(reason="not_positive", field="every")
        start_date, end_date = _date(start, "start"), _date(end, "end")
        _inclusive_end(start_date, end_date, "schedule_end_before_start")
        object.__setattr__(self, "every", interval)
        object.__setattr__(self, "months", months)
        object.__setattr__(self, "start", start_date)
        object.__setattr__(self, "end", end_date)

    @property
    def end_exclusive(self) -> date:
        """The day after the last scheduled day; anchors satisfy ``t0 < day(end_exclusive)``."""
        return self.end + timedelta(days=1)

    def count(self, day_boundary: Callable[[date], datetime]) -> int:
        """Number of anchors: arithmetic for an elapsed step, one clamp check for months.

        A month schedule bounds ``k`` by the year-month difference and checks the clamped
        day once, so no date beyond ``end``'s month is constructed.
        """
        if self.months is None:
            assert self.every is not None
            span = day_boundary(self.end_exclusive) - day_boundary(self.start)
            return (int(span.total_seconds()) + self.every - 1) // self.every
        whole_months = (self.end.year - self.start.year) * 12 + self.end.month - self.start.month
        last = whole_months // self.months
        return last + 1 if add_months(self.start, last * self.months) <= self.end else last

    def instant(self, k: int, day_boundary: Callable[[date], datetime]) -> datetime:
        """t0 of anchor ``k``: ``start + k * every`` seconds, or local midnight of its month."""
        if self.months is None:
            assert self.every is not None
            return day_boundary(self.start) + timedelta(seconds=k * self.every)
        return day_boundary(add_months(self.start, k * self.months))

    def instants(self, day_boundary: Callable[[date], datetime]) -> tuple[datetime, ...]:
        """Every t0 of a calendar-month schedule in order; an elapsed schedule is arithmetic."""
        if self.months is None:
            raise _INVALID.error(reason="elapsed_schedule", field="every")
        return tuple(self.instant(k, day_boundary) for k in range(self.count(day_boundary)))


@dataclass(frozen=True, slots=True, init=False)
class AnchorRange:
    """Keep anchors from ``start`` through ``end``; a calendar ``end`` includes its whole day."""

    start: date | datetime | None
    end: date | datetime | None

    def __init__(
        self, start: date | datetime | str | None = None, end: date | datetime | str | None = None
    ) -> None:
        start_bound, end_bound = _range_bound(start, "start"), _range_bound(end, "end")
        if start_bound is None and end_bound is None:
            raise _INVALID.error(reason="no_bound", field="start")
        if isinstance(end_bound, datetime):
            if isinstance(start_bound, datetime) and start_bound >= end_bound:
                raise _REVERSED_INSTANTS.error(
                    reason="range_end_before_start",
                    field="end",
                    start=start_bound.isoformat(),
                    end=end_bound.isoformat(),
                )
        elif end_bound is not None:
            same_kind = start_bound if not isinstance(start_bound, datetime) else None
            _inclusive_end(same_kind, end_bound, "range_end_before_start")
        object.__setattr__(self, "start", start_bound)
        object.__setattr__(self, "end", end_bound)

    @property
    def end_exclusive(self) -> date | datetime | None:
        """The bound anchors stay below: an instant unchanged, a calendar day widened by one."""
        if self.end is None or isinstance(self.end, datetime):
            return self.end
        return self.end + timedelta(days=1)
