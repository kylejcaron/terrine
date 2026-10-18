"""Serializable specifications for period trajectories and cohort summaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from terrine._time import utc_instant
from terrine.windows import seconds

__all__ = ["Trajectory", "CohortSpec", "CohortGrouping", "TimeToEvent"]


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
        duration = seconds(period)
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
            raise ValueError("origin must be a non-empty fact name")
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
        duration = seconds(bucket)
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
            raise ValueError("event must be a non-empty fact name")
        for name in ("exit", "entry"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be a non-empty fact name or None")
        if not isinstance(self.delayed_entry, bool):
            raise TypeError("delayed_entry must be bool")
        if self.delayed_entry and self.entry is None:
            raise ValueError("delayed_entry requires an entry fact")
