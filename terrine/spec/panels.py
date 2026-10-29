"""Canonical data-only specifications for sequential and event-time panels."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import KW_ONLY, dataclass
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

from terrine._time import utc_instant
from terrine.errors import RefusalSpec, parameter_names
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
    "CalendarComparisonSpec",
    "Sequential",
    "CalendarPanel",
    "EventPanel",
    "CalendarComparisonPanel",
    "matched_t0",
]

_INVALID_HISTORY = RefusalSpec(
    "terrine.design.invalid_treatment_history",
    "Treatment history input {field} is invalid: {reason}.",
)
_CONFLICT = RefusalSpec(
    "terrine.design.conflicting_treatment", "Sequential treatment conflicts with Design.treatment."
)
_INVALID_SAMPLE = RefusalSpec(
    "terrine.design.invalid_matched_t0_sample", "Matched-t0 sample policy is invalid: {reason}."
)
_INVALID_COMPARISON = RefusalSpec(
    "terrine.design.invalid_calendar_comparison",
    "Calendar comparison specification is invalid: {reason}.",
)
_INVALID = RefusalSpec("terrine.design.invalid", "Design input {field} is invalid: {reason}.")
_CHOICE = RefusalSpec("terrine.design.invalid", "Design input {field} must be one of {choices}.")
_KNOWLEDGE = ("recorded_at", "retrospective_effective_time")


def _event_name(value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise _INVALID.error(reason="empty_name", field="event")


def _knowledge(value: str) -> None:
    if value not in _KNOWLEDGE:
        raise _CHOICE.error(reason="unsupported_value", field="knowledge", choices=_KNOWLEDGE)


def _history(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise _INVALID_HISTORY.error(reason="wrong_type", field="history_start")
    if value.tzinfo is None or value.utcoffset() is None:
        raise _INVALID_HISTORY.error(reason="naive_datetime", field="history_start")
    try:
        return utc_instant(value)
    except (ValueError, OverflowError) as error:
        raise _INVALID_HISTORY.error(reason="invalid_instant", field="history_start") from error


@dataclass(frozen=True, slots=True)
class Initiation:
    """Treatment begins once the named event or label has been observed by t0."""

    event: str
    _: KW_ONLY
    history_start: datetime
    knowledge: Literal["recorded_at", "retrospective_effective_time"] = "recorded_at"

    def __post_init__(self) -> None:
        _event_name(self.event)
        object.__setattr__(self, "history_start", _history(self.history_start))
        _knowledge(self.knowledge)


@dataclass(frozen=True, slots=True)
class PersistentAdoption:
    """Treatment is the absorbing state in which a label holds ``adopted_value``."""

    event: str
    _: KW_ONLY
    adopted_value: str
    history_start: datetime
    persistence: Literal["absorbing"] = "absorbing"
    knowledge: Literal["recorded_at", "retrospective_effective_time"] = "recorded_at"

    def __post_init__(self) -> None:
        _event_name(self.event)
        if not isinstance(self.adopted_value, str) or not self.adopted_value:
            raise _INVALID.error(reason="empty_name", field="adopted_value")
        object.__setattr__(self, "history_start", _history(self.history_start))
        if self.persistence != "absorbing":
            raise _CHOICE.error(
                reason="unsupported_value", field="persistence", choices=("absorbing",)
            )
        _knowledge(self.knowledge)


@dataclass(frozen=True, slots=True, init=False)
class TrialWeight:
    kind: Literal["equal_trial", "explicit"]
    values: Mapping[str, float] | None

    def __init__(
        self, kind: Literal["equal_trial", "explicit"], values: Mapping[str, float] | None = None
    ):
        if kind not in ("equal_trial", "explicit"):
            raise _CHOICE.error(
                reason="unsupported_value", field="kind", choices=("equal_trial", "explicit")
            )
        if kind == "explicit":
            if not isinstance(values, Mapping) or not values:
                raise _INVALID.error(reason="empty", field="values")
            copied = dict(values)
            if any(not isinstance(key, str) or not key for key in copied):
                raise _INVALID.error(reason="empty_name", field="values")
            if any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
                for value in copied.values()
            ):
                raise _INVALID.error(reason="not_positive", field="values")
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
                raise _INVALID.error(reason="not_positive", field="values")
            values = MappingProxyType(normalized)
        elif values is not None:
            raise _INVALID.error(reason="unexpected_values", field="values")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "values", values)


@dataclass(frozen=True, slots=True)
class MatchedT0Sample:
    """Canonical matched-t0 sampling policy, retained on its Design."""

    strata: tuple[str, ...]
    controls_per_treated: int
    seed: int

    def __post_init__(self) -> None:
        if not isinstance(self.strata, Iterable):
            raise _INVALID.error(reason="wrong_type", field="strata")
        strata = tuple(self.strata)
        if any(not isinstance(name, str) or not name.strip() for name in strata):
            raise _INVALID.error(reason="empty_name", field="strata")
        if len(set(strata)) != len(strata):
            raise _INVALID.error(reason="duplicate_names", field="strata")
        if (
            isinstance(self.controls_per_treated, bool)
            or not isinstance(self.controls_per_treated, int)
            or self.controls_per_treated <= 0
        ):
            raise _INVALID.error(reason="not_positive_integer", field="controls_per_treated")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int) or self.seed < 0:
            raise _INVALID.error(reason="not_nonnegative_integer", field="seed")
        object.__setattr__(self, "strata", strata)


@dataclass(frozen=True, slots=True)
class SequentialSpec:
    treatment: Initiation | PersistentAdoption
    trial_weight: TrialWeight | None = None
    anticipation_seconds: int = 0
    reference_period_seconds: int = -86_400

    def __post_init__(self) -> None:
        if not isinstance(self.treatment, (Initiation, PersistentAdoption)):
            raise _INVALID.error(reason="wrong_type", field="treatment")
        if self.trial_weight is not None and not isinstance(self.trial_weight, TrialWeight):
            raise _INVALID.error(reason="wrong_type", field="trial_weight")
        if (
            isinstance(self.anticipation_seconds, bool)
            or not isinstance(self.anticipation_seconds, int)
            or self.anticipation_seconds < 0
        ):
            raise _INVALID.error(reason="negative", field="anticipation_seconds")


@dataclass(frozen=True, slots=True)
class EventPanelSpec:
    periods: tuple[int, ...]
    period_seconds: int
    reference_period: int

    def __post_init__(self) -> None:
        if not isinstance(self.periods, Iterable):
            raise _INVALID.error(reason="wrong_type", field="periods")
        periods = tuple(self.periods)
        if not periods:
            raise _INVALID.error(reason="empty", field="periods")
        if any(isinstance(period, bool) or not isinstance(period, int) for period in periods):
            raise _INVALID.error(reason="wrong_type", field="periods")
        if len(set(periods)) != len(periods):
            raise _INVALID.error(reason="duplicate_names", field="periods")
        if (
            isinstance(self.period_seconds, bool)
            or not isinstance(self.period_seconds, int)
            or self.period_seconds <= 0
        ):
            raise _INVALID.error(reason="not_positive_integer", field="period_seconds")
        if self.reference_period not in periods:
            raise _INVALID.error(reason="undeclared_name", field="reference_period")
        object.__setattr__(self, "periods", periods)


@dataclass(frozen=True, slots=True)
class CalendarComparisonSpec:
    cohort: Initiation
    control: Literal["never_treated", "not_yet_treated"]
    reference_period: int
    comparison_periods: tuple[int, ...]
    anticipation_periods: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.cohort, Initiation):
            raise _INVALID.error(reason="wrong_type", field="cohort")
        if self.control not in ("never_treated", "not_yet_treated"):
            raise _CHOICE.error(
                reason="unsupported_value",
                field="control",
                choices=("never_treated", "not_yet_treated"),
            )
        for field, values in (
            ("reference_period", (self.reference_period,)),
            ("comparison_periods", self.comparison_periods),
            ("anticipation_periods", self.anticipation_periods),
        ):
            if not isinstance(values, Iterable) or any(
                isinstance(value, bool) or not isinstance(value, int) for value in tuple(values)
            ):
                raise _INVALID.error(reason="wrong_type", field=field)
        comparisons = tuple(self.comparison_periods)
        anticipations = tuple(self.anticipation_periods)
        if not comparisons:
            raise _INVALID_COMPARISON.error(reason="comparison periods must be nonempty")
        if len(set(comparisons)) != len(comparisons):
            raise _INVALID_COMPARISON.error(reason="comparison periods must be unique")
        if any(value >= 0 for value in anticipations) or len(set(anticipations)) != len(
            anticipations
        ):
            raise _INVALID_COMPARISON.error(
                reason="anticipation periods must be unique negative leads"
            )
        boundary = min(anticipations) if anticipations else 0
        if self.reference_period >= boundary:
            raise _INVALID_COMPARISON.error(
                reason="reference period must precede the anticipation window"
            )
        if any(value < 0 and value >= boundary for value in comparisons):
            raise _INVALID_COMPARISON.error(
                reason="pre-treatment comparisons must precede the anticipation window"
            )
        if self.reference_period in comparisons:
            raise _INVALID_COMPARISON.error(
                reason="reference period conflicts with a comparison period"
            )
        object.__setattr__(self, "comparison_periods", comparisons)
        object.__setattr__(self, "anticipation_periods", anticipations)


def _copy_design(
    base: Design,
    shape: str | StatePanel | SequentialSpec | EventPanelSpec | CalendarComparisonSpec,
) -> Design:
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
        raise _INVALID.error(reason="wrong_type", field="base")
    if base.treatment is not None:
        raise _CONFLICT.error()
    with parameter_names(anticipation_seconds="anticipation"):
        spec = SequentialSpec(
            treatment,
            trial_weight,
            seconds(anticipation, field="anticipation"),
            seconds(reference_period, field="reference_period"),
        )
    return _copy_design(base, spec)


def CalendarPanel(sequential: Design) -> Design:
    if not isinstance(sequential, Design) or not isinstance(sequential.shape, SequentialSpec):
        raise _INVALID.error(reason="requires_sequential", field="sequential")
    return sequential


def CalendarComparisonPanel(
    base: Design,
    model: Model,
    *,
    cohort: Initiation,
    control: Literal["never_treated", "not_yet_treated"],
    reference_period: int,
    comparison_periods: tuple[int, ...],
    anticipation_periods: tuple[int, ...] = (),
) -> Frame:
    """Build a canonical roster-backed calendar comparison Frame."""
    if not isinstance(base, Design):
        raise _INVALID.error(reason="wrong_type", field="base")
    if base.sample is not None:
        raise _INVALID_SAMPLE.error(reason="calendar comparisons refuse sampled populations")
    spec = CalendarComparisonSpec(
        cohort, control, reference_period, comparison_periods, anticipation_periods
    )
    if base.treatment is not None:
        raise _CONFLICT.error()
    return _copy_design(base, spec).build(model)


def EventPanel(
    base: Design,
    *,
    periods: tuple[int, ...],
    period_seconds: int,
    reference_period: int = -1,
) -> Design:
    if not isinstance(base, Design):
        raise _INVALID.error(reason="wrong_type", field="base")
    if not isinstance(base.anchor, EventAnchor):
        raise _INVALID.error(reason="requires_event_anchor", field="base")
    if base.treatment is not None:
        raise _CONFLICT.error()
    spec = EventPanelSpec(periods, period_seconds, reference_period)
    return _copy_design(base, spec)


def matched_t0(base: Design, model: Model, *, sample: MatchedT0Sample) -> Frame:
    """Build a sampled sequential Frame with its policy serialized on the Design."""
    if not isinstance(base, Design) or not isinstance(base.shape, SequentialSpec):
        raise _INVALID.error(reason="requires_sequential", field="base")
    if not isinstance(sample, MatchedT0Sample):
        raise _INVALID.error(reason="wrong_type", field="sample")
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
