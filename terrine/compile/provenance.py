from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from datetime import UTC
from typing import TYPE_CHECKING

from terrine.data import (
    CalendarComparisonMeta,
    CalendarSupportCell,
    PanelMeta,
    StatePanelMeta,
    TreatmentHistoryMeta,
    TrialWeightMeta,
)
from terrine.spec.state_panel import BinnedMetricState, StatePanel, encode_cutpoint
from terrine.windows import Duration

EXECUTION_REVISION = 2

if TYPE_CHECKING:
    from datetime import datetime

    import pyarrow as pa

    from terrine.model import Model
    from terrine.spec.design import Design


def state_panel_metadata(design: Design, model: Model) -> StatePanelMeta | None:
    from terrine.spec.anchors import Schedule

    if not isinstance(design.shape, StatePanel):
        return None
    schedule = design.anchor
    assert isinstance(schedule, Schedule)
    state = design.shape.state
    if isinstance(state, BinnedMetricState):
        source = {
            "kind": "binned_metric",
            "metric": state.metric,
            "cutpoints": [encode_cutpoint(value) for value in state.cutpoints],
        }
        levels, ordered, absorbing = state.levels, True, ()
        timing = "strict_period"
    else:
        source = {
            "kind": "observations",
            "property": state.property,
            "point_time_columns": {"from_state": "from_time", "to_state": "to_time"},
        }
        levels, ordered, absorbing = state.levels, state.ordered, state.absorbing
        timing = "at_origin"
    assert isinstance(schedule.every, Duration)
    return StatePanelMeta(
        state_source=source,
        levels=levels,
        ordered=ordered,
        absorbing=absorbing,
        panel_start=model.day_boundary(schedule.start).astimezone(UTC),
        administrative_end=model.day_boundary(schedule.end_exclusive).astimezone(UTC),
        period_seconds=schedule.every.seconds,
        baseline_features=tuple(design.shape.baseline_features),
        time_varying_features=tuple(design.features),
        prior_state_timing=timing,
        period_column="period_index",
        from_time_column="from_time",
        to_time_column="to_time",
        scheduled_to_column="scheduled_to",
        gap_column="gap",
        terminal_column="terminal",
    )


def panel_metadata(design: Design, model: Model) -> PanelMeta | None:
    from terrine.spec.anchors import Schedule
    from terrine.spec.panels import EventPanelSpec, Initiation, PersistentAdoption, SequentialSpec
    from terrine.spec.validate import resolve_selector_source

    if isinstance(design.shape, SequentialSpec):
        treatment = design.shape.treatment
        history = TreatmentHistoryMeta(
            kind="initiation" if isinstance(treatment, Initiation) else "persistent_adoption",
            source=resolve_selector_source(model, treatment.event),
            property=treatment.event,
            history_start=treatment.history_start,
            knowledge=treatment.knowledge,
            adopted_value=treatment.adopted_value
            if isinstance(treatment, PersistentAdoption)
            else None,
            persistence=treatment.persistence
            if isinstance(treatment, PersistentAdoption)
            else None,
        )
        assert isinstance(design.anchor, Schedule)
        weight = design.shape.trial_weight
        trial_weight = (
            None if weight is None else TrialWeightMeta(kind=weight.kind, values=weight.values)
        )
        count = design.anchor.count(model.day_boundary)
        trials = tuple(f"schedule:{index}" for index in range(count))
        return PanelMeta(
            shape="sequential",
            treatment=history,
            trial_weight=trial_weight,
            trials=trials,
            anticipation_seconds=design.shape.anticipation.seconds,
        )
    if isinstance(design.shape, EventPanelSpec):
        return PanelMeta(
            shape="event_panel",
            periods=design.shape.periods,
            period_seconds=design.shape.period.seconds,
            reference_period=design.shape.reference_period,
        )
    return None


def calendar_comparison_metadata(design: Design, model: Model) -> CalendarComparisonMeta | None:
    from terrine.spec.anchors import Schedule
    from terrine.spec.panels import CalendarComparisonSpec
    from terrine.spec.validate import resolve_selector_source

    if not isinstance(design.shape, CalendarComparisonSpec):
        return None
    assert isinstance(design.anchor, Schedule)
    assert isinstance(design.anchor.every, Duration)
    cohort = design.shape.cohort
    return CalendarComparisonMeta(
        control=design.shape.control,
        cohort_source=resolve_selector_source(model, cohort.event),
        cohort_property=cohort.event,
        history_start=cohort.history_start,
        knowledge=cohort.knowledge,
        reference_period=design.shape.reference_period,
        comparison_periods=design.shape.comparison_periods,
        anticipation_periods=design.shape.anticipation_periods,
        period_seconds=design.anchor.every.seconds,
    )


def with_calendar_support(
    metadata: CalendarComparisonMeta, cohorts: pa.ChunkedArray, rows: pa.Table
) -> CalendarComparisonMeta:
    """Count distinct emitted units over the original, independently observed cohort grid."""
    units: dict[tuple[datetime, int, bool], set[pa.Scalar]] = defaultdict(set)
    for cohort, period, unit, treated, control in zip(
        rows["cohort"].to_pylist(),
        rows["event_period"].to_pylist(),
        rows["unit"],
        rows["treatment"].to_pylist(),
        rows["control_eligible"].to_pylist(),
        strict=True,
    ):
        if unit.is_valid and (treated is True or (treated is False and control is True)):
            units[cohort, period, treated].add(unit)
    periods = metadata._period_kinds()
    return replace(
        metadata,
        support=tuple(
            CalendarSupportCell(
                cohort=cohort,
                period=period,
                kind=kind,
                treated=len(units.get((cohort, period, True), ())),
                control=len(units.get((cohort, period, False), ())),
            )
            for cohort in cohorts.to_pylist()
            for period, kind in periods.items()
        ),
    )
