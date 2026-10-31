"""Declared shape columns shared by metadata descriptions and compilation."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from terrine.spec.panels import CalendarComparisonSpec, EventPanelSpec, SequentialSpec
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import StateObservations, StatePanel
from terrine.spec.trajectory import CohortSpec, LifeTable, TimeToEvent

if TYPE_CHECKING:
    from terrine.spec.design import Design

IDENTITY = ("unit", "anchor_id", "t0")
EVENT_PANEL = ("parent_event_id", "parent_t0", "event_period")
TRIAL_PANEL = ("treatment", "trial_id")
CALENDAR_COMPARISON = (
    "cohort",
    "period",
    "event_period",
    "treatment",
    "control_eligible",
    "__terrine_treatment_initiation_time",
)
SURVIVAL = (
    "event_time",
    "censor_time",
    "duration",
    "event",
    "entry_time",
    "censor_reason",
)
SURVIVAL_VALUES = ("event_time", "duration", "event", "censor_reason")
TRAJECTORY = ("unit", "anchor_id", "cohort_time", "age", "period_value", "period_value_mature")
LIFE_TABLE_VALUES = (
    "at_risk",
    "events",
    "censored",
    "censored_by_reason",
    "delayed_entries",
    "event_rate",
    "censor_rate",
)
STATE_PANEL = (
    "from_time",
    "to_time",
    "scheduled_to",
    "period_index",
    "gap",
    "from_state",
    "to_state",
    "terminal",
)
STATE_VALUES = ("from_state", "to_state")
SEQUENCE = ("is_event", "sequence_position", "event_id", "event_time", "event_count")
SESSION_VALUES = (
    "session_index",
    "session_event_count",
    "session_start_time",
    "session_end_time",
    "previous_event_id",
    "previous_event_time",
    "transition",
)
TRANSITION_LABELS = ("transition_from", "transition_to")


def value_columns(names: Iterable[str]) -> tuple[str, ...]:
    """Expand values into their published value, completeness and reason triples."""
    return tuple(
        column for name in names for column in (name, f"{name}__complete", f"{name}__reason")
    )


def design_columns(design: Design) -> tuple[str, ...]:
    """Return the default dataframe column order without compiling or reading rows."""
    shape = design.shape
    named = (*design.features, *design.outcomes, *design.eligibility)
    treatment = ("treatment",) if design.treatment is not None else ()
    if isinstance(shape, LifeTable):
        return (
            *IDENTITY,
            *value_columns(shape.group_by),
            "time_bin",
            *value_columns(LIFE_TABLE_VALUES),
        )
    if isinstance(shape, TimeToEvent):
        columns = [*IDENTITY, *value_columns((*design.features, *design.eligibility, *treatment))]
        for name in SURVIVAL:
            columns.extend(value_columns((name,)) if name in SURVIVAL_VALUES else (name,))
        return tuple(columns)
    if isinstance(shape, CohortSpec):
        columns = []
        for name in TRAJECTORY:
            columns.extend(value_columns((name,)) if name == "period_value" else (name,))
        columns.extend(value_columns((*design.features, *design.eligibility, *treatment)))
        if shape.trajectory.cumulative:
            columns.extend(value_columns(("cumulative_value",)))
        return tuple(columns)
    if isinstance(shape, StatePanel):
        columns = list(IDENTITY)
        for name in STATE_PANEL:
            columns.extend(value_columns((name,)) if name in STATE_VALUES else (name,))
        features = (*design.features, *shape.baseline_features)
        named = (
            (*design.eligibility, *features)
            if isinstance(shape.state, StateObservations)
            else (*features, *design.eligibility)
        )
        return (*columns, *value_columns(named))
    if isinstance(shape, EventSequence):
        columns = [*IDENTITY, *SEQUENCE[:-1], *value_columns((SEQUENCE[-1],))]
        values = list(shape.properties) if shape.representation == "positions" else ["event_values"]
        if shape.sessions is not None:
            if shape.representation == "positions":
                values.extend(SESSION_VALUES)
                if shape.label_property is not None:
                    values.extend(TRANSITION_LABELS)
            values.append("sessions")
        if shape.funnel is not None:
            values.append("funnel_attempts")
        return (*columns, *value_columns((*values, *named)))
    if isinstance(shape, EventPanelSpec):
        return (*IDENTITY, *EVENT_PANEL, *value_columns((*named, *treatment)))
    if isinstance(shape, CalendarComparisonSpec):
        return (*IDENTITY, *value_columns((*CALENDAR_COMPARISON, *named)))
    if isinstance(shape, SequentialSpec):
        extra = (*TRIAL_PANEL, *(("trial_weight",) if shape.trial_weight is not None else ()))
        if design.sample is not None:
            extra = (*extra, "assignment_probability")
        return (*IDENTITY, *value_columns((*named, *extra)))
    return (*IDENTITY, *value_columns((*named, *treatment)))
