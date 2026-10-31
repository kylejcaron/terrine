"""Serializable specifications for period trajectories and cohort summaries."""

from __future__ import annotations

from dataclasses import KW_ONLY, dataclass
from datetime import datetime
from typing import Literal

from terrine.errors import RefusalSpec
from terrine.spec.anchors import _aware_instant
from terrine.windows import duration

__all__ = ["Trajectory", "CohortSpec", "CohortGrouping", "TimeToEvent", "LifeTable"]

_INVALID = RefusalSpec("terrine.design.invalid", "Design input {field} is invalid: {reason}.")
_CHOICE = RefusalSpec("terrine.design.invalid", "Design input {field} must be one of {choices}.")


def _name(value: object, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise _INVALID.error(reason="empty_name", field=field)


@dataclass(frozen=True, slots=True, init=False)
class Trajectory:
    """Describe a metric over elapsed periods after t0, optionally accumulated from t0.

    Example: tr.Trajectory("spend", period="30d", periods=2, cumulative=True)
    """

    metric: str
    period: int
    periods: int
    cumulative: bool

    def __init__(
        self, metric: str, period: str | Literal[0], periods: int, cumulative: bool = False
    ) -> None:
        _name(metric, "metric")
        period_seconds = duration(period, field="period")
        if period_seconds <= 0:
            raise _INVALID.error(reason="not_positive", field="period")
        if isinstance(periods, bool) or not isinstance(periods, int) or periods <= 0:
            raise _INVALID.error(reason="not_positive_integer", field="periods")
        if not isinstance(cumulative, bool):
            raise _INVALID.error(reason="wrong_type", field="cumulative")
        object.__setattr__(self, "metric", metric)
        object.__setattr__(self, "period", period_seconds)
        object.__setattr__(self, "periods", periods)
        object.__setattr__(self, "cumulative", cumulative)


@dataclass(frozen=True, slots=True)
class CohortSpec:
    """Pair a unit's origin event with a metric trajectory.

    Example: tr.CohortSpec("orders", tr.Trajectory("spend", "30d", 2))
    """

    origin: str
    trajectory: Trajectory

    def __post_init__(self) -> None:
        _name(self.origin, "origin")
        if not isinstance(self.trajectory, Trajectory):
            raise _INVALID.error(reason="wrong_type", field="trajectory")


@dataclass(frozen=True, slots=True, init=False)
class CohortGrouping:
    """Group units by origin-time buckets and summarize their trajectory values.

    Example: tr.CohortGrouping("30d", datetime(2025, 1, 1, tzinfo=UTC))
    """

    bucket: int
    bucket_origin: datetime
    statistic: Literal["total", "mean"]

    def __init__(
        self,
        bucket: str | Literal[0],
        bucket_origin: datetime,
        statistic: Literal["total", "mean"] = "mean",
    ) -> None:
        bucket_seconds = duration(bucket, field="bucket")
        if bucket_seconds <= 0:
            raise _INVALID.error(reason="not_positive", field="bucket")
        if not isinstance(bucket_origin, datetime):
            raise _INVALID.error(reason="wrong_type", field="bucket_origin")
        origin = _aware_instant(bucket_origin, "bucket_origin")
        if statistic not in ("total", "mean"):
            raise _CHOICE.error(
                reason="unsupported_value", field="statistic", choices=("total", "mean")
            )
        object.__setattr__(self, "bucket", bucket_seconds)
        object.__setattr__(self, "bucket_origin", origin)
        object.__setattr__(self, "statistic", statistic)


@dataclass(frozen=True, slots=True)
class TimeToEvent:
    """Describe follow-up from t0 until an event, exit or the end of complete history.

    Example: tr.TimeToEvent("orders")
    """

    end_event: str
    _: KW_ONLY
    exit_event: str | None = None
    entry_event: str | None = None
    delayed_entry: bool = False

    def __post_init__(self) -> None:
        _name(self.end_event, "end_event")
        for name in ("exit_event", "entry_event"):
            value = getattr(self, name)
            if value is not None:
                _name(value, name)
        if not isinstance(self.delayed_entry, bool):
            raise _INVALID.error(reason="wrong_type", field="delayed_entry")
        if self.delayed_entry and self.entry_event is None:
            raise _INVALID.error(reason="entry_required", field="entry_event")


@dataclass(frozen=True, slots=True, init=False)
class LifeTable:
    """Summarize time-to-event follow-up in elapsed bins with risk and censor counts.

    Example: tr.LifeTable(tr.TimeToEvent("orders"), bin="7d")
    """

    time_to_event: TimeToEvent
    bin: int
    group_by: tuple[str, ...]

    def __init__(
        self,
        time_to_event: TimeToEvent,
        bin: str | Literal[0],
        group_by: tuple[str, ...] = (),
    ) -> None:
        if not isinstance(time_to_event, TimeToEvent):
            raise _INVALID.error(reason="wrong_type", field="time_to_event")
        width = duration(bin, field="bin")
        if width <= 0:
            raise _INVALID.error(reason="not_positive", field="bin")
        if isinstance(group_by, (str, bytes)) or not isinstance(group_by, tuple):
            raise _INVALID.error(reason="wrong_type", field="group_by")
        for name in group_by:
            _name(name, "group_by")
        if len(set(group_by)) != len(group_by):
            raise _INVALID.error(reason="duplicate_names", field="group_by")
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
            raise _INVALID.error(
                reason="reserved_name",
                field="group_by",
                names=tuple(sorted(reserved.intersection(group_by))),
            )
        object.__setattr__(self, "time_to_event", time_to_event)
        object.__setattr__(self, "bin", width)
        object.__setattr__(self, "group_by", group_by)
