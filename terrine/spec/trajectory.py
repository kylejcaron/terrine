"""Serializable specifications for period trajectories and cohort summaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from terrine.windows import seconds

__all__ = ["Trajectory", "CohortSpec", "CohortGrouping"]


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
        object.__setattr__(self, "bucket_origin", bucket_origin.astimezone(UTC))
        object.__setattr__(self, "statistic", statistic)
