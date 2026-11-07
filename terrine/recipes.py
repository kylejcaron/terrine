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
from terrine.windows import Duration, Months, Window, duration

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


def _entry_anchor(
    start_event: str, occurrence: Literal["first", "nth"], n: int | None
) -> EventAnchor:
    if occurrence not in ("first", "nth"):
        raise _CHOICE.error(
            reason="single_entry_required", field="occurrence", choices=("first", "nth")
        )
    with parameter_names(event="start_event"):
        return EventAnchor(start_event, occurrence=occurrence, n=n)


def time_series(
    unit: str | None = None,
    *,
    every: str | Duration | Months | Literal[0],
    start: date | str,
    end: date | str,
    features: Mapping[str, Expr | Predicate] | None = None,
    outcomes: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    treatment: Treatment | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | Duration | Literal[0] = "0d",
) -> Design:
    """Build one row per unit and scheduled t0, including the schedule's end day.

    ``every`` accepts elapsed durations or calendar steps such as ``"1mo"``.


    Args:
        unit: Unit name declared in the model; None infers the sole compatible unit.
        every: Positive duration string (see `terrine.windows`) or calendar step such as "1mo" or
            "1y".
        start: First scheduled calendar day, as a date or ISO YYYY-MM-DD string, included.
        end: Last scheduled calendar day, as a date or ISO YYYY-MM-DD string, included in full.
        features: Mapping of output names to expressions or predicates; None declares no features.
        outcomes: Mapping of output names to future expressions or predicates; None declares no
            outcomes.
        eligibility: Mapping of rule names to predicates; None applies no eligibility rules.
        treatment: Treatment declaration; None produces no treatment column.
        anchor_range: AnchorRange restricting row t0 values; None keeps all generated anchors.
        gap: Nonnegative duration string (see `terrine.windows`); "0d" leaves no feature-to-t0 gap.

    Returns:
        An ordinary immutable Design; call describe(model) or build(model) to use it.

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
    every: str | Duration | Months | Literal[0],
    start: date | str,
    end: date | str,
    treatment: Initiation | PersistentAdoption,
    trial_weight: TrialWeight | None = None,
    anticipation: str | Duration | Literal[0] = "0d",
    reference_period: str | Duration | Literal[0] = "-1d",
    features: Mapping[str, Expr | Predicate] | None = None,
    outcomes: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | Duration | Literal[0] = "0d",
) -> Design:
    """Build repeated trials at scheduled t0 times with an event or adoption treatment.


    Args:
        unit: Unit name declared in the model; None infers the sole compatible unit.
        every: Positive duration string (see `terrine.windows`) or calendar step such as "1mo" or
            "1y".
        start: First scheduled calendar day, as a date or ISO YYYY-MM-DD string, included.
        end: Last scheduled calendar day, as a date or ISO YYYY-MM-DD string, included in full.
        treatment: Initiation or PersistentAdoption declaration supplying treatment history.
        trial_weight: TrialWeight declaration; None gives each qualifying trial its ordinary weight.
        anticipation: Nonnegative duration string (see `terrine.windows`); "0d" excludes no
            anticipatory time.
        reference_period: Nonpositive duration string (see `terrine.windows`); "-1d" compares one
            day before t0.
        features: Mapping of output names to expressions or predicates; None declares no features.
        outcomes: Mapping of output names to future expressions or predicates; None declares no
            outcomes.
        eligibility: Mapping of rule names to predicates; None applies no eligibility rules.
        anchor_range: AnchorRange restricting row t0 values; None keeps all generated anchors.
        gap: Nonnegative duration string (see `terrine.windows`); "0d" leaves no feature-to-t0 gap.

    Returns:
        An ordinary immutable Design; call describe(model) or build(model) to use it.

    Example:
        from datetime import datetime, UTC

        tr.recipes.trial_panel(
            every="1d",
            start="2025-02-01",
            end="2025-02-03",
            treatment=spec.Initiation("orders", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
        )
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
    period: str | Duration | Literal[0],
    occurrence: Literal["first", "each", "nth"] = "first",
    n: int | None = None,
    reference_index: int = -1,
    features: Mapping[str, Expr | Predicate] | None = None,
    outcomes: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | Duration | Literal[0] = "0d",
) -> Design:
    """Build rows at elapsed periods relative to each unit's selected event time.


    Args:
        unit: Unit name declared in the model; None infers the sole compatible unit.
        event: Name of an event declared in the model.
        periods: Tuple of integer event-relative period indexes to emit.
        period: Positive elapsed duration string (see `terrine.windows`) defining one period.
        occurrence: "first", "each" or "nth"; "first" uses the earliest event as the panel origin.
        n: Positive one-based occurrence index for "nth"; None is required for other rules.
        reference_index: Integer reference index included in periods; -1 uses the period before the
            event.
        features: Mapping of output names to expressions or predicates; None declares no features.
        outcomes: Mapping of output names to future expressions or predicates; None declares no
            outcomes.
        eligibility: Mapping of rule names to predicates; None applies no eligibility rules.
        anchor_range: AnchorRange restricting row t0 values; None keeps all generated anchors.
        gap: Nonnegative duration string (see `terrine.windows`); "0d" leaves no feature-to-t0 gap.

    Returns:
        An ordinary immutable Design; call describe(model) or build(model) to use it.

    Example: tr.recipes.event_panel(event="orders", periods=(-1, 0, 1), period="7d")
    """
    base = Design(
        unit,
        EventAnchor(event, occurrence=occurrence, n=n),
        features=features,
        outcomes=outcomes,
        eligibility=eligibility,
        anchor_range=anchor_range,
        gap=gap,
    )
    with parameter_names(reference_period="reference_index"):
        return EventPanel(base, periods=periods, period=period, reference_period=reference_index)


def ltv(
    unit: str | None = None,
    *,
    start_event: str,
    metric: str,
    period: str | Duration | Literal[0],
    max_periods: int,
    features: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    treatment: Treatment | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | Duration | Literal[0] = "0d",
    administrative_cutoff: datetime | None = None,
) -> Design:
    """Build a cumulative metric trajectory starting at each unit's first origin event.

    The metric must be an untransformed sum, and ``period`` must be an elapsed duration.


    Args:
        unit: Unit name declared in the model; None infers the sole compatible unit.
        start_event: Declared event name whose selected occurrence starts follow-up.
        metric: Name of a metric declared in the model.
        period: Positive elapsed duration string (see `terrine.windows`) defining one period.
        max_periods: Positive integer number of periods to produce.
        features: Mapping of output names to expressions or predicates; None declares no features.
        eligibility: Mapping of rule names to predicates; None applies no eligibility rules.
        treatment: Treatment declaration; None produces no treatment column.
        anchor_range: AnchorRange restricting row t0 values; None keeps all generated anchors.
        gap: Nonnegative duration string (see `terrine.windows`); "0d" leaves no feature-to-t0 gap.
        administrative_cutoff: Timezone-aware datetime ending follow-up; None uses declared
            completeness.

    Returns:
        An ordinary immutable Design; call describe(model) or build(model) to use it.

    Example: tr.recipes.ltv(start_event="orders", metric="spend", period="30d", max_periods=2)
    """
    with parameter_names(event="start_event"):
        anchor = EventAnchor(start_event)
    with parameter_names(periods="max_periods"):
        trajectory = Trajectory(metric, period, max_periods, cumulative=True)
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
    gap: str | Duration | Literal[0] = "0d",
    administrative_cutoff: datetime | None = None,
) -> Design:
    """Build follow-up from a unit's first or nth start event until an end event or cutoff.


    Args:
        unit: Unit name declared in the model; None infers the sole compatible unit.
        start_event: Declared event name whose selected occurrence starts follow-up.
        end_event: Declared event name whose first occurrence at or after t0 ends follow-up.
        occurrence: "first" or "nth" start event; "first" begins at the earliest occurrence.
        n: Positive one-based occurrence index for "nth"; None is required for other rules.
        features: Mapping of output names to expressions or predicates; None declares no features.
        eligibility: Mapping of rule names to predicates; None applies no eligibility rules.
        treatment: Treatment declaration; None produces no treatment column.
        anchor_range: AnchorRange restricting row t0 values; None keeps all generated anchors.
        gap: Nonnegative duration string (see `terrine.windows`); "0d" leaves no feature-to-t0 gap.
        administrative_cutoff: Timezone-aware datetime ending follow-up; None uses declared
            completeness.

    Returns:
        An ordinary immutable Design; call describe(model) or build(model) to use it.

    Example: tr.recipes.survival(start_event="orders", end_event="orders")
    """
    anchor = _entry_anchor(start_event, occurrence, n)
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
    bin: str | Duration | Literal[0],
    group_by: Mapping[str, Expr | Predicate] | None = None,
    occurrence: Literal["first", "nth"] = "first",
    n: int | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | Duration | Literal[0] = "0d",
    administrative_cutoff: datetime | None = None,
) -> Design:
    """Build elapsed-bin risk and censor counts, optionally grouped by named expressions.


    Args:
        unit: Unit name declared in the model; None infers the sole compatible unit.
        start_event: Declared event name whose selected occurrence starts follow-up.
        end_event: Declared event name whose first occurrence at or after t0 ends follow-up.
        bin: Positive elapsed duration string (see `terrine.windows`) defining risk-count bins.
        group_by: Mapping of grouping names to expressions or predicates; None yields an ungrouped
            life table.
        occurrence: "first" or "nth" start event; "first" begins at the earliest occurrence.
        n: Positive one-based occurrence index for "nth"; None is required for other rules.
        eligibility: Mapping of rule names to predicates; None applies no eligibility rules.
        anchor_range: AnchorRange restricting row t0 values; None keeps all generated anchors.
        gap: Nonnegative duration string (see `terrine.windows`); "0d" leaves no feature-to-t0 gap.
        administrative_cutoff: Timezone-aware datetime ending follow-up; None uses declared
            completeness.

    Returns:
        An ordinary immutable Design; call describe(model) or build(model) to use it.

    Example: tr.recipes.life_table(start_event="orders", end_event="orders", bin="7d")
    """
    if group_by is not None and not isinstance(group_by, Mapping):
        raise _INVALID.error(reason="wrong_type", field="group_by")
    groups = {} if group_by is None else dict(group_by)
    anchor = _entry_anchor(start_event, occurrence, n)
    shape = LifeTable(TimeToEvent(end_event), bin, group_by=tuple(groups))
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
    every: str | Duration | Literal[0],
    start: date | str,
    end: date | str,
    max_periods: int | None = None,
    features: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | Duration | Literal[0] = "0d",
) -> Design:
    """Build repeated unit states over whole elapsed periods from start through end.

    ``max_periods`` bounds the schedule's period count and defaults to that count.


    Args:
        unit: Unit name declared in the model; None infers the sole compatible unit.
        state_source: StateObservations or BinnedMetricState declaration defining each period's
            state.
        every: Positive elapsed duration string (see `terrine.windows`); calendar steps cannot
            define state periods.
        start: First scheduled calendar day, as a date or ISO YYYY-MM-DD string, included.
        end: Last scheduled calendar day, as a date or ISO YYYY-MM-DD string, included in full.
        max_periods: Positive integer bound on schedule periods; None uses the whole finite
            schedule.
        features: Mapping of output names to expressions or predicates; None declares no features.
        eligibility: Mapping of rule names to predicates; None applies no eligibility rules.
        anchor_range: AnchorRange restricting row t0 values; None keeps all generated anchors.
        gap: Nonnegative duration string (see `terrine.windows`); "0d" leaves no feature-to-t0 gap.

    Returns:
        An ordinary immutable Design; call describe(model) or build(model) to use it.

    Example:
        tr.recipes.state_panel(
            every="7d",
            start="2025-02-01",
            end="2025-02-14",
            state_source=spec.BinnedMetricState("spend", (50,), ("low", "high")),
        )
    """
    schedule = Schedule(every, start, end)
    whole_periods = schedule_periods(schedule)
    max_periods = whole_periods if max_periods is None else max_periods
    with parameter_names(state="state_source", periods="max_periods"):
        shape = StatePanel(state_source, periods=max_periods)
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
    horizon: str | Duration | Literal[0],
    max_events: int,
    occurrence: Literal["first", "each", "nth"] = "first",
    n: int | None = None,
    session_gap: str | Duration | Literal[0] | None = None,
    features: Mapping[str, Expr | Predicate] | None = None,
    outcomes: Mapping[str, Expr | Predicate] | None = None,
    eligibility: Mapping[str, Predicate] | None = None,
    anchor_range: AnchorRange | None = None,
    gap: str | Duration | Literal[0] = "0d",
) -> Design:
    """Build each unit's event sequence over [t0, t0 + horizon), with optional sessions.


    Args:
        unit: Unit name declared in the model; None infers the sole compatible unit.
        event: Name of an event declared in the model.
        horizon: Positive elapsed duration string (see `terrine.windows`) bounding follow-up after
            t0.
        max_events: Positive integer maximum number of events per anchor.
        occurrence: "first", "each" or "nth"; "first" starts at the earliest event.
        n: Positive one-based occurrence index for "nth"; None is required for other rules.
        session_gap: Nonnegative duration string (see `terrine.windows`); "0s" groups only
            simultaneous events. None does not group events into sessions.
        features: Mapping of output names to expressions or predicates; None declares no features.
        outcomes: Mapping of output names to future expressions or predicates; None declares no
            outcomes.
        eligibility: Mapping of rule names to predicates; None applies no eligibility rules.
        anchor_range: AnchorRange restricting row t0 values; None keeps all generated anchors.
        gap: Nonnegative duration string (see `terrine.windows`); "0d" leaves no feature-to-t0 gap.

    Returns:
        An ordinary immutable Design; call describe(model) or build(model) to use it.

    Example: tr.recipes.event_sequence(event="orders", horizon="30d", max_events=10)
    """
    return Design(
        unit,
        EventAnchor(event, occurrence=occurrence, n=n),
        shape=EventSequence(
            event,
            Window(0, duration(horizon, field="horizon")),
            max_events=max_events,
            sessions=None if session_gap is None else SessionSpec(session_gap),
        ),
        features=features,
        outcomes=outcomes,
        eligibility=eligibility,
        anchor_range=anchor_range,
        gap=gap,
    )


def __dir__() -> list[str]:
    return sorted(__all__)
