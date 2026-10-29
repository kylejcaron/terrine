"""Serializable specifications for period trajectories and cohort summaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from terrine._time import utc_instant
from terrine.windows import seconds

__all__ = ["Trajectory", "CohortSpec", "CohortGrouping", "TimeToEvent", "LifeTable"]


@dataclass(frozen=True, slots=True, init=False)
class Trajectory:
    metric: str
    period: int
    periods: int
    cumulative: bool

    def __init__(
        self, metric: str, period: str | int, periods: int, cumulative: bool = False
    ) -> None:
        if not isinstance(metric, str) or not metric.strip():
            raise ValueError("metric must be a non-empty name")
        duration = seconds(period, field="period")
        if duration <= 0:
            raise ValueError("period must be positive")
        if isinstance(periods, bool) or not isinstance(periods, int) or periods <= 0:
            raise ValueError("periods must be a positive finite integer")
        if not isinstance(cumulative, bool):
            raise TypeError("cumulative must be bool")
        object.__setattr__(self, "metric", metric)
        object.__setattr__(self, "period", duration)
        object.__setattr__(self, "periods", periods)
        object.__setattr__(self, "cumulative", cumulative)


@dataclass(frozen=True, slots=True)
class CohortSpec:
    origin: str
    trajectory: Trajectory

    def __post_init__(self) -> None:
        if not isinstance(self.origin, str) or not self.origin.strip():
            raise ValueError("origin must be a non-empty event name")
        if not isinstance(self.trajectory, Trajectory):
            raise TypeError("trajectory must be Trajectory")


@dataclass(frozen=True, slots=True, init=False)
class CohortGrouping:
    bucket: int
    bucket_origin: datetime
    statistic: Literal["total", "mean"]

    def __init__(
        self,
        bucket: str | int,
        bucket_origin: datetime,
        statistic: Literal["total", "mean"] = "mean",
    ) -> None:
        duration = seconds(bucket, field="bucket")
        if duration <= 0:
            raise ValueError("bucket must be positive")
        if (
            not isinstance(bucket_origin, datetime)
            or bucket_origin.tzinfo is None
            or bucket_origin.utcoffset() is None
        ):
            raise ValueError("bucket_origin must be timezone-aware")
        if statistic not in ("total", "mean"):
            raise ValueError("statistic must be 'total' or 'mean'")
        object.__setattr__(self, "bucket", duration)
        object.__setattr__(self, "bucket_origin", utc_instant(bucket_origin))
        object.__setattr__(self, "statistic", statistic)


@dataclass(frozen=True, slots=True)
class TimeToEvent:
    event: str
    exit: str | None = None
    entry: str | None = None
    delayed_entry: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.event, str) or not self.event.strip():
            raise ValueError("event must be a non-empty event name")
        for name in ("exit", "entry"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be a non-empty event name or None")
        if not isinstance(self.delayed_entry, bool):
            raise TypeError("delayed_entry must be bool")
        if self.delayed_entry and self.entry is None:
            raise ValueError("delayed_entry requires an entry event")


@dataclass(frozen=True, slots=True, init=False)
class LifeTable:
    """Bin observed time-to-event records into descriptive risk-set summaries."""

    time_to_event: TimeToEvent
    interval: int
    group_by: tuple[str, ...]

    def __init__(
        self,
        time_to_event: TimeToEvent,
        interval: str | int,
        group_by: tuple[str, ...] = (),
    ) -> None:
        if not isinstance(time_to_event, TimeToEvent):
            raise TypeError("time_to_event must be TimeToEvent")
        width = seconds(interval, field="bin")
        if width <= 0:
            raise ValueError("interval must be positive")
        if (
            isinstance(group_by, (str, bytes))
            or not isinstance(group_by, tuple)
            or any(not isinstance(name, str) or not name.strip() for name in group_by)
        ):
            raise TypeError("group_by must be a tuple of non-empty feature names")
        if len(set(group_by)) != len(group_by):
            raise ValueError("group_by names must be unique")
        reserved = {
            "unit",
            "anchor_id",
            "t0",
            "time_bin",
            "at_risk",
            "events",
            "censored",
            "censored_by_reason",
            "delayed_entries",
            "event_rate",
            "censor_rate",
        }
        if reserved.intersection(group_by):
            raise ValueError("group_by names cannot collide with life-table output columns")
        object.__setattr__(self, "time_to_event", time_to_event)
        object.__setattr__(self, "interval", width)
        object.__setattr__(self, "group_by", group_by)
