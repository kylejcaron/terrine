"""Common dataset row shapes, expressed as ordinary low-level Designs."""

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
    """Build a Design on a calendar-date Schedule whose ``end`` day is included.

    ``unit`` defaults to the model's only unit; a model with several needs it named.
    ``every`` is an elapsed duration (``"7d"``, ``"4w"``) or a calendar step (``"1mo"``,
    ``"3mo"``, ``"1y"``) whose anchors clamp to each month's last day.
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
    """Apply Sequential to a Design whose Schedule dates define the trials.

    ``every`` is an elapsed duration or a calendar step, as for ``time_series``.
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
    """Apply EventPanel to an event-anchored Design for explicit relative periods."""
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
    """Build an origin-inclusive cumulative trajectory from a native SUM metric.

    The metric must be untransformed and backed by exactly one SUM measure:
    with Model.from_tables, a tr.sum(column) metric; counts, means and ratios
    are not accepted by this shape.
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
    """Build TimeToEvent follow-up from a first or nth start event, bounded by coverage/cutoff."""
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
    """Build a LifeTable over TimeToEvent whose rows are grouped by the ``group_by`` expressions.

    Each ``group_by`` entry is a named feature expression or predicate; the names are
    the life table's grouping columns, so nothing else can be declared as a feature.
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
    """Build a Schedule Design with a StatePanel over the whole periods from start through end.

    ``every`` is an elapsed duration: a state transition's period is a fixed width, so a
    calendar step refuses with a route to its elapsed equivalent. ``periods`` defaults to
    the schedule's own period count; an explicit value is an upper bound that validation
    refuses to exceed.
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
    """Build an EventSequence over [anchor, anchor + horizon), optionally grouped into sessions."""
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
