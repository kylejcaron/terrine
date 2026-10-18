"""Canonical data-only specifications for sequential and event-time panels."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

from terrine._time import utc_instant
from terrine.errors import RefusalSpec
from terrine.spec.anchors import EventAnchor
from terrine.spec.design import Design
from terrine.spec.state_panel import StatePanel
from terrine.windows import seconds

if TYPE_CHECKING:
    from terrine.frame import Frame
    from terrine.model import Model

__all__ = [
    "Initiation",
    "PersistentAdoption",
    "TrialWeight",
    "MatchedT0Sample",
    "SequentialSpec",
    "EventPanelSpec",
    "Sequential",
    "CalendarPanel",
    "EventPanel",
    "matched_t0",
]

_INVALID_HISTORY = RefusalSpec(
    "terrine.design.invalid_treatment_history", "Treatment history is invalid: {reason}."
)
_CONFLICT = RefusalSpec(
    "terrine.design.conflicting_treatment", "Sequential treatment conflicts with Design.treatment."
)
_INVALID_SAMPLE = RefusalSpec(
    "terrine.design.invalid_matched_t0_sample", "Matched-t0 sample policy is invalid: {reason}."
)


def _selector(source: str, property: str) -> None:
    if (
        not isinstance(source, str)
        or not source.strip()
        or not isinstance(property, str)
        or not property.strip()
    ):
        raise ValueError("source and property must be non-empty strings")


def _history(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise _INVALID_HISTORY.error(reason="history_start must be timezone-aware")
    try:
        return utc_instant(value)
    except ValueError as error:
        raise _INVALID_HISTORY.error(reason="history_start must be timezone-aware") from error


@dataclass(frozen=True, slots=True)
class Initiation:
    source: str
    property: str
    history_start: datetime
    knowledge: Literal["recorded_at", "retrospective_effective_time"] = "recorded_at"

    def __post_init__(self) -> None:
        _selector(self.source, self.property)
        object.__setattr__(self, "history_start", _history(self.history_start))
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
        object.__setattr__(self, "history_start", _history(self.history_start))
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
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
                for value in copied.values()
            ):
                raise ValueError("trial weights must be finite positive numbers")
            maximum = max(copied.values())
            if maximum <= 1 and math.isclose(
                math.fsum(copied.values()), 1.0, rel_tol=1e-15, abs_tol=1e-15
            ):
                normalized = {key: float(value) for key, value in copied.items()}
            else:
                scaled = {key: value / maximum for key, value in copied.items()}
                total = math.fsum(scaled.values())
                normalized = {key: value / total for key, value in scaled.items()}
            if any(not math.isfinite(value) or value <= 0 for value in normalized.values()):
                raise ValueError("normalized trial weights must remain finite and positive")
            values = MappingProxyType(normalized)
        elif values is not None:
            raise ValueError("equal_trial does not accept values")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "values", values)


@dataclass(frozen=True, slots=True)
class MatchedT0Sample:
    """Canonical matched-t0 sampling policy, retained on its Design."""

    strata: tuple[str, ...]
    controls_per_treated: int
    seed: int

    def __post_init__(self) -> None:
        strata = tuple(self.strata)
        if any(not isinstance(name, str) or not name.strip() for name in strata):
            raise ValueError("strata must contain non-empty names")
        if len(set(strata)) != len(strata):
            raise ValueError("strata must be unique")
        if (
            isinstance(self.controls_per_treated, bool)
            or not isinstance(self.controls_per_treated, int)
            or self.controls_per_treated <= 0
        ):
            raise ValueError("controls_per_treated must be a positive integer")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int) or self.seed < 0:
            raise ValueError("seed must be a nonnegative integer")
        object.__setattr__(self, "strata", strata)


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
        sample=base.sample,
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


def matched_t0(base: Design, model: Model, *, sample: MatchedT0Sample) -> Frame:
    """Build a sampled sequential Frame with its policy serialized on the Design."""
    if not isinstance(base, Design) or not isinstance(base.shape, SequentialSpec):
        raise TypeError("matched_t0 requires a Sequential Design")
    if not isinstance(sample, MatchedT0Sample):
        raise TypeError("sample must be MatchedT0Sample")
    if base.sample is not None:
        raise _INVALID_SAMPLE.error(reason="the base design already declares a sample policy")
    sampled = Design(
        base.unit,
        base.anchor,
        features=base.features,
        outcomes=base.outcomes,
        eligibility=base.eligibility,
        gap=base.gap,
        shape=base.shape,
        anchor_range=base.anchor_range,
        sample=sample,
    )
    return sampled.build(model)
