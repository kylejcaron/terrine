"""Pure immutable anchor definitions for study designs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from terrine._time import utc_instant
from terrine.errors import RefusalSpec
from terrine.windows import seconds

__all__ = ["FixedDate", "EventAnchor", "Schedule", "AnchorRange"]

_INVALID = RefusalSpec("terrine.design.invalid", "Design validation failed: {reason}.")


def _date(value: date | str) -> date:
    if isinstance(value, datetime) or not isinstance(value, (date, str)):
        raise TypeError("expected a date, not a timestamp")
    try:
        parsed = date.fromisoformat(value) if isinstance(value, str) else value
    except ValueError as error:
        raise ValueError("date must be an ISO calendar date") from error
    return parsed


def _range_bound(value: date | datetime | str | None) -> date | datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        if "T" in value or " " in value:
            return utc_instant(value)
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise ValueError("range bounds must be ISO dates or timestamps") from error
    if isinstance(value, datetime):
        return utc_instant(value)
    if isinstance(value, date):
        return value
    raise TypeError("range bounds must be date, datetime, ISO string, or None")


@dataclass(frozen=True, slots=True, init=False)
class FixedDate:
    date: date | datetime

    def __init__(self, date: date | datetime | str) -> None:
        bound = _range_bound(date)
        if bound is None:
            raise TypeError("fixed date cannot be None")
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
            raise ValueError("event must be a non-empty string")
        if which not in ("first", "each", "nth"):
            raise ValueError("which must be first, each, or nth")
        if which == "nth":
            if isinstance(n, bool) or not isinstance(n, int) or n < 1:
                raise ValueError("nth anchors require a positive one-based n")
        elif n is not None:
            raise ValueError("n is only valid for nth anchors")
        elapsed = seconds(offset)
        if elapsed < 0:
            raise _INVALID.error(reason="event anchor offset must be non-negative")
        object.__setattr__(self, "event", event)
        object.__setattr__(self, "which", which)
        object.__setattr__(self, "n", n)
        object.__setattr__(self, "offset", elapsed)


@dataclass(frozen=True, slots=True, init=False)
class Schedule:
    every: int
    start: date
    end: date

    def __init__(self, every: str | int, start: date | str, end: date | str) -> None:
        interval = seconds(every)
        if interval <= 0:
            raise ValueError("schedule interval must be positive")
        start_date, end_date = _date(start), _date(end)
        if start_date >= end_date:
            raise ValueError("schedule end is exclusive and must follow start")
        object.__setattr__(self, "every", interval)
        object.__setattr__(self, "start", start_date)
        object.__setattr__(self, "end", end_date)


@dataclass(frozen=True, slots=True, init=False)
class AnchorRange:
    start: date | datetime | None
    end: date | datetime | None

    def __init__(
        self, start: date | datetime | str | None = None, end: date | datetime | str | None = None
    ) -> None:
        start_bound, end_bound = _range_bound(start), _range_bound(end)
        if start_bound is None and end_bound is None:
            raise ValueError("at least one range bound is required")
        if (
            start_bound is not None
            and end_bound is not None
            and isinstance(start_bound, datetime) == isinstance(end_bound, datetime)
            and start_bound >= end_bound
        ):
            raise ValueError("range start must precede its exclusive end")
        object.__setattr__(self, "start", start_bound)
        object.__setattr__(self, "end", end_bound)
