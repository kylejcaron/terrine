"""Build common dataset designs from units, event times and metric windows.

Example: tr.recipes.time_series(every="1mo", start="2025-02-01", end="2025-03-01")
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Literal

from terrine.errors import RefusalSpec, parameter_names
from terrine.spec import (
    AnchorRange,
    BinnedMetricState,
    CohortSpec,
    Design,
    EventAnchor,
    EventPanel,
    EventSequence,
    Initiation,
    LifeTable,
    PersistentAdoption,
    Schedule,
    Sequential,
    SessionSpec,
    StateObservations,
    StatePanel,
    TimeToEvent,
    Trajectory,
    Treatment,
    TrialWeight,
)
from terrine.spec.expressions import Expr, Predicate
from terrine.spec.state_panel import schedule_periods
from terrine.windows import Window, seconds

__all__ = [
    "time_series",
    "trial_panel",
    "event_panel",
    "ltv",
    "survival",
    "life_table",
    "state_panel",
    "event_sequence",
]

_CHOICE = RefusalSpec("terrine.design.invalid", "Design input {field} must be one of {choices}.")
_INVALID = RefusalSpec("terrine.design.invalid", "Design input {field} is invalid: {reason}.")


def _event_anchor(event: str, occurrence: str, n: int | None) -> EventAnchor:
    """EventAnchor's own validation order, reported under the recipe's ``occurrence``."""
    with parameter_names(which="occurrence"):
        return EventAnchor(event, which=occurrence, n=n)


def _entry_anchor(start_event: str, occurrence: str, n: int | None) -> EventAnchor:
    if occurrence not in ("first", "nth"):
        raise _CHOICE.error(
            reason="single_entry_required", field="occurrence", choices=("first", "nth")
        )
    with parameter_names(event="start_event"):
        return EventAnchor(start_event, which=occurrence, n=n)


def time_series(
    unit: str | None = None,
    *,
    every: str | int,
    start: date | str,
    end: date | str,
    features: Mapping[str, Expr | Predicate] | None = None,
    outcomes: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    treatment: Treatment | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | int = "0d",
) -> Design:
    """Build one row per unit and scheduled t0, including the schedule's end day.

    ``every`` accepts elapsed durations or calendar steps such as ``"1mo"``.

    Example: tr.recipes.time_series(every="1mo", start="2025-02-01", end="2025-03-01")
    """
    return Design(
        unit,
        Schedule(every, start, end),
        features=features,
        outcomes=outcomes,
        eligibility=eligibility,
        treatment=treatment,
        anchor_range=anchor_range,
        gap=gap,
    )


def trial_panel(
    unit: str | None = None,
    *,
    every: str | int,
    start: date | str,
    end: date | str,
    treatment: Initiation | PersistentAdoption,
    trial_weight: TrialWeight | None = None,
    anticipation: str = "0d",
    reference_period: str = "-1d",
    features: Mapping[str, Expr | Predicate] | None = None,
    outcomes: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | int = "0d",
) -> Design:
    """Build repeated trials at scheduled t0 times with an event or adoption treatment.

    Example: tr.recipes.trial_panel(treatment=initiation, **schedule)
    """
    base = Design(
        unit,
        Schedule(every, start, end),
        features=features,
        outcomes=outcomes,
        eligibility=eligibility,
        anchor_range=anchor_range,
        gap=gap,
    )
    return Sequential(
        base,
        treatment=treatment,
        trial_weight=trial_weight,
        anticipation=anticipation,
        reference_period=reference_period,
    )


def event_panel(
    unit: str | None = None,
    *,
    event: str,
    periods: tuple[int, ...],
    period: str | int,
    occurrence: Literal["first", "each", "nth"] = "first",
    n: int | None = None,
    reference_period: int = -1,
    features: Mapping[str, Expr | Predicate] | None = None,
    outcomes: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | int = "0d",
) -> Design:
    """Build rows at elapsed periods relative to each unit's selected event time.

    Example: tr.recipes.event_panel(event="orders", periods=(-1, 0, 1), period="7d")
    """
    base = Design(
        unit,
        _event_anchor(event, occurrence, n),
        features=features,
        outcomes=outcomes,
        eligibility=eligibility,
        anchor_range=anchor_range,
        gap=gap,
    )
    with parameter_names(period_seconds="period"):
        return EventPanel(
            base,
            periods=periods,
            period_seconds=seconds(period, field="period"),
            reference_period=reference_period,
        )


def ltv(
    unit: str | None = None,
    *,
    start_event: str,
    metric: str,
    period: str | int,
    periods: int,
    features: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    treatment: Treatment | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | int = "0d",
    administrative_cutoff: datetime | None = None,
) -> Design:
    """Build a cumulative metric trajectory starting at each unit's first origin event.

    The metric must be an untransformed sum, and ``period`` must be an elapsed duration.

    Example: tr.recipes.ltv(start_event="orders", metric="spend", period="30d", periods=2)
    """
    with parameter_names(event="start_event"):
        anchor = EventAnchor(start_event)
    trajectory = Trajectory(metric, period, periods, cumulative=True)
    with parameter_names(origin="start_event"):
        shape = CohortSpec(start_event, trajectory)
    return Design(
        unit,
        anchor,
        shape=shape,
        features=features,
        eligibility=eligibility,
        treatment=treatment,
        anchor_range=anchor_range,
        gap=gap,
        administrative_cutoff=administrative_cutoff,
    )


def survival(
    unit: str | None = None,
    *,
    start_event: str,
    end_event: str,
    occurrence: Literal["first", "nth"] = "first",
    n: int | None = None,
    features: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    treatment: Treatment | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | int = "0d",
    administrative_cutoff: datetime | None = None,
) -> Design:
    """Build follow-up from a unit's first or nth start event until an end event or cutoff.

    Example: tr.recipes.survival(start_event="orders", end_event="orders")
    """
    anchor = _entry_anchor(start_event, occurrence, n)
    with parameter_names(event="end_event"):
        shape = TimeToEvent(end_event)
    return Design(
        unit,
        anchor,
        shape=shape,
        features=features,
        eligibility=eligibility,
        treatment=treatment,
        anchor_range=anchor_range,
        gap=gap,
        administrative_cutoff=administrative_cutoff,
    )


def life_table(
    unit: str | None = None,
    *,
    start_event: str,
    end_event: str,
    bin: str | int,
    group_by: Mapping[str, Expr | Predicate] | None = None,
    occurrence: Literal["first", "nth"] = "first",
    n: int | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | int = "0d",
    administrative_cutoff: datetime | None = None,
) -> Design:
    """Build elapsed-bin risk and censor counts, optionally grouped by named expressions.

    Example: tr.recipes.life_table(start_event="orders", end_event="orders", bin="7d")
    """
    if group_by is not None and not isinstance(group_by, Mapping):
        raise _INVALID.error(reason="wrong_type", field="group_by")
    groups = {} if group_by is None else dict(group_by)
    anchor = _entry_anchor(start_event, occurrence, n)
    with parameter_names(event="end_event"):
        time_to_event = TimeToEvent(end_event)
    with parameter_names(interval="bin"):
        shape = LifeTable(time_to_event, bin, group_by=tuple(groups))
    return Design(
        unit,
        anchor,
        shape=shape,
        features=groups,
        eligibility=eligibility,
        anchor_range=anchor_range,
        gap=gap,
        administrative_cutoff=administrative_cutoff,
    )


def state_panel(
    unit: str | None = None,
    *,
    state_source: StateObservations | BinnedMetricState,
    every: str | int,
    start: date | str,
    end: date | str,
    periods: int | None = None,
    features: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | int = "0d",
) -> Design:
    """Build repeated unit states over whole elapsed periods from start through end.

    ``periods`` bounds the schedule's period count and defaults to that count.

    Example: tr.recipes.state_panel(state_source=state, **schedule)
    """
    schedule = Schedule(every, start, end)
    whole_periods = schedule_periods(schedule)
    max_periods = whole_periods if periods is None else periods
    with parameter_names(state="state_source", max_periods="periods"):
        shape = StatePanel(state_source, max_periods=max_periods)
    return Design(
        unit,
        schedule,
        shape=shape,
        features=features,
        eligibility=eligibility,
        anchor_range=anchor_range,
        gap=gap,
    )


def event_sequence(
    unit: str | None = None,
    *,
    event: str,
    horizon: str | int,
    max_events: int,
    occurrence: Literal["first", "each", "nth"] = "first",
    n: int | None = None,
    session_gap: str | int | None = None,
    features: Mapping[str, Expr | Predicate] | None = None,
    outcomes: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | int = "0d",
) -> Design:
    """Build each unit's event sequence over [t0, t0 + horizon), with optional sessions.

    Example: tr.recipes.event_sequence(event="orders", horizon="30d", max_events=10)
    """
    return Design(
        unit,
        _event_anchor(event, occurrence, n),
        shape=EventSequence(
            event,
            Window(0, seconds(horizon, field="horizon")),
            max_events=max_events,
            sessions=None if session_gap is None else SessionSpec(session_gap),
        ),
        features=features,
        outcomes=outcomes,
        eligibility=eligibility,
        anchor_range=anchor_range,
        gap=gap,
    )
