"""Canonical data-only specifications for sequential and event-time panels."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Literal

from terrine.errors import RefusalSpec
from terrine.spec.anchors import EventAnchor
from terrine.spec.design import Design
from terrine.spec.state_panel import StatePanel
from terrine.windows import seconds

__all__ = [
    "Initiation",
    "PersistentAdoption",
    "TrialWeight",
    "SequentialSpec",
    "EventPanelSpec",
    "Sequential",
    "CalendarPanel",
    "EventPanel",
]

_INVALID_HISTORY = RefusalSpec(
    "terrine.design.invalid_treatment_history", "Treatment history is invalid: {reason}."
)
_CONFLICT = RefusalSpec(
    "terrine.design.conflicting_treatment", "Sequential treatment conflicts with Design.treatment."
)


def _selector(source: str, property: str) -> None:
    if (
        not isinstance(source, str)
        or not source.strip()
        or not isinstance(property, str)
        or not property.strip()
    ):
        raise ValueError("source and property must be non-empty strings")


def _history(value: datetime) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise _INVALID_HISTORY.error(reason="history_start must be timezone-aware")


@dataclass(frozen=True, slots=True)
class Initiation:
    source: str
    property: str
    history_start: datetime
    knowledge: Literal["recorded_at", "retrospective_effective_time"] = "recorded_at"

    def __post_init__(self) -> None:
        _selector(self.source, self.property)
        _history(self.history_start)
        if self.knowledge not in ("recorded_at", "retrospective_effective_time"):
            raise ValueError("unsupported treatment knowledge policy")


@dataclass(frozen=True, slots=True)
class PersistentAdoption:
    source: str
    property: str
    adopted_value: str
    history_start: datetime
    persistence: Literal["absorbing"]
    knowledge: Literal["recorded_at", "retrospective_effective_time"] = "recorded_at"

    def __post_init__(self) -> None:
        _selector(self.source, self.property)
        if not isinstance(self.adopted_value, str) or not self.adopted_value:
            raise ValueError("adopted_value must be a non-empty string")
        _history(self.history_start)
        if self.persistence != "absorbing":
            raise ValueError("persistence must be 'absorbing'")
        if self.knowledge not in ("recorded_at", "retrospective_effective_time"):
            raise ValueError("unsupported treatment knowledge policy")


@dataclass(frozen=True, slots=True, init=False)
class TrialWeight:
    kind: Literal["equal_trial", "explicit"]
    values: Mapping[str, float] | None

    def __init__(
        self, kind: Literal["equal_trial", "explicit"], values: Mapping[str, float] | None = None
    ):
        if kind not in ("equal_trial", "explicit"):
            raise ValueError("kind must be equal_trial or explicit")
        if kind == "explicit":
            if not isinstance(values, Mapping) or not values:
                raise ValueError("explicit trial weights require values")
            copied = dict(values)
            if any(not isinstance(key, str) or not key for key in copied):
                raise ValueError("trial weight keys must be non-empty strings")
            if any(
                isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0
                for value in copied.values()
            ):
                raise ValueError("trial weights must be positive numbers")
            total = sum(copied.values())
            if total <= 0:
                raise ValueError("trial weights must have positive total")
            values = MappingProxyType({key: value / total for key, value in copied.items()})
        elif values is not None:
            raise ValueError("equal_trial does not accept values")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "values", values)


@dataclass(frozen=True, slots=True)
class SequentialSpec:
    treatment: Initiation | PersistentAdoption
    trial_weight: TrialWeight | None = None
    anticipation_seconds: int = 0
    reference_period_seconds: int = -86_400

    def __post_init__(self) -> None:
        if not isinstance(self.treatment, (Initiation, PersistentAdoption)):
            raise TypeError("treatment must be Initiation or PersistentAdoption")
        if self.trial_weight is not None and not isinstance(self.trial_weight, TrialWeight):
            raise TypeError("trial_weight must be TrialWeight or None")
        if isinstance(self.anticipation_seconds, bool) or self.anticipation_seconds < 0:
            raise ValueError("anticipation_seconds must be nonnegative")


@dataclass(frozen=True, slots=True)
class EventPanelSpec:
    periods: tuple[int, ...]
    period_seconds: int
    reference_period: int

    def __post_init__(self) -> None:
        periods = tuple(self.periods)
        if not periods or any(
            isinstance(period, bool) or not isinstance(period, int) for period in periods
        ):
            raise ValueError("periods must be a non-empty tuple of integers")
        if len(set(periods)) != len(periods):
            raise ValueError("periods must be unique")
        if (
            isinstance(self.period_seconds, bool)
            or not isinstance(self.period_seconds, int)
            or self.period_seconds <= 0
        ):
            raise ValueError("period_seconds must be a positive integer")
        if self.reference_period not in periods:
            raise ValueError("reference_period must be declared")
        object.__setattr__(self, "periods", periods)


def _copy_design(base: Design, shape: str | StatePanel | SequentialSpec | EventPanelSpec) -> Design:
    return Design(
        base.unit,
        base.anchor,
        features=base.features,
        outcomes=base.outcomes,
        eligibility=base.eligibility,
        gap=base.gap,
        shape=shape,
        anchor_range=base.anchor_range,
    )


def Sequential(
    base: Design,
    *,
    treatment: Initiation | PersistentAdoption,
    trial_weight: TrialWeight | None = None,
    anticipation: str = "0d",
    reference_period: str = "-1d",
) -> Design:
    if not isinstance(base, Design):
        raise TypeError("base must be Design")
    if base.treatment is not None:
        raise _CONFLICT.error()
    return _copy_design(
        base,
        SequentialSpec(treatment, trial_weight, seconds(anticipation), seconds(reference_period)),
    )


def CalendarPanel(sequential: Design) -> Design:
    if not isinstance(sequential, Design) or not isinstance(sequential.shape, SequentialSpec):
        raise TypeError("CalendarPanel requires a Sequential Design")
    return sequential


def EventPanel(
    base: Design,
    *,
    periods: tuple[int, ...],
    period_seconds: int,
    reference_period: int = -1,
) -> Design:
    if not isinstance(base, Design):
        raise TypeError("base must be Design")
    if not isinstance(base.anchor, EventAnchor):
        raise TypeError("EventPanel requires an EventAnchor Design")
    if base.treatment is not None:
        raise _CONFLICT.error()
    spec = EventPanelSpec(periods, period_seconds, reference_period)
    return _copy_design(base, spec)
