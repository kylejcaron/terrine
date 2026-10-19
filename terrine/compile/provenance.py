from __future__ import annotations

from datetime import UTC
from typing import TYPE_CHECKING

from terrine.data import (
    CalendarComparisonMeta,
    PanelMeta,
    StatePanelMeta,
    TreatmentHistoryMeta,
    TrialWeightMeta,
)
from terrine.spec.state_panel import BinnedMetricState, StatePanel, encode_cutpoint

if TYPE_CHECKING:
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
    return StatePanelMeta(
        state_source=source,
        levels=levels,
        ordered=ordered,
        absorbing=absorbing,
        panel_start=model.day_boundary(schedule.start).astimezone(UTC),
        administrative_end=model.day_boundary(schedule.end).astimezone(UTC),
        period_seconds=schedule.every,
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


def panel_metadata(design: Design) -> PanelMeta | None:
    from terrine.spec.anchors import Schedule
    from terrine.spec.panels import EventPanelSpec, Initiation, PersistentAdoption, SequentialSpec

    if isinstance(design.shape, SequentialSpec):
        treatment = design.shape.treatment
        history = TreatmentHistoryMeta(
            "initiation" if isinstance(treatment, Initiation) else "persistent_adoption",
            treatment.source,
            treatment.property,
            treatment.history_start,
            treatment.knowledge,
            treatment.adopted_value if isinstance(treatment, PersistentAdoption) else None,
            treatment.persistence if isinstance(treatment, PersistentAdoption) else None,
        )
        assert isinstance(design.anchor, Schedule)
        weight = design.shape.trial_weight
        trial_weight = None if weight is None else TrialWeightMeta(weight.kind, weight.values)
        span = (design.anchor.end - design.anchor.start).days * 86_400
        count = (span - 1) // design.anchor.every + 1
        trials = tuple(f"schedule:{index}" for index in range(count))
        return PanelMeta(
            "sequential",
            history,
            trial_weight,
            trials=trials,
            anticipation_seconds=design.shape.anticipation_seconds,
        )
    if isinstance(design.shape, EventPanelSpec):
        return PanelMeta(
            "event_panel",
            periods=design.shape.periods,
            period_seconds=design.shape.period_seconds,
            reference_period=design.shape.reference_period,
        )
    return None


def calendar_comparison_metadata(design: Design) -> CalendarComparisonMeta | None:
    from terrine.spec.anchors import Schedule
    from terrine.spec.panels import CalendarComparisonSpec

    if not isinstance(design.shape, CalendarComparisonSpec):
        return None
    assert isinstance(design.anchor, Schedule)
    cohort = design.shape.cohort
    return CalendarComparisonMeta(
        design.shape.control,
        cohort.source,
        cohort.property,
        cohort.history_start,
        cohort.knowledge,
        design.shape.reference_period,
        design.shape.comparison_periods,
        design.shape.anticipation_periods,
        design.anchor.every,
    )
