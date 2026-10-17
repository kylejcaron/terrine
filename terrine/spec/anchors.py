"""Pure immutable anchor definitions for study designs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time

from terrine.windows import seconds

__all__ = ["FixedDate", "EventAnchor", "Schedule", "AnchorRange"]


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
        try:
            value = (
                datetime.fromisoformat(value)
                if "T" in value or " " in value
                else date.fromisoformat(value)
            )
        except ValueError as error:
            raise ValueError("range bounds must be ISO dates or timestamps") from error
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime range bounds must be timezone-aware")
        return value.astimezone(UTC)
    if isinstance(value, date):
        return value
    raise TypeError("range bounds must be date, datetime, ISO string, or None")


def _ordinal(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    return datetime.combine(value, time.min, UTC)


@dataclass(frozen=True, slots=True, init=False)
class FixedDate:
    date: date

    def __init__(self, date: date | str) -> None:
        object.__setattr__(self, "date", _date(date))


@dataclass(frozen=True, slots=True)
class EventAnchor:
    fact: str
    which: str = "first"
    n: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.fact, str) or not self.fact.strip():
            raise ValueError("fact must be a non-empty string")
        if self.which not in ("first", "each", "nth"):
            raise ValueError("which must be first, each, or nth")
        if self.which == "nth":
            if isinstance(self.n, bool) or not isinstance(self.n, int) or self.n < 1:
                raise ValueError("nth anchors require a positive one-based n")
        elif self.n is not None:
            raise ValueError("n is only valid for nth anchors")


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
            and _ordinal(start_bound) >= _ordinal(end_bound)
        ):
            raise ValueError("range start must precede its exclusive end")
        object.__setattr__(self, "start", start_bound)
        object.__setattr__(self, "end", end_bound)
