"""Select the existing compiler path for a design shape."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import ibis

from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.spec.design import Design
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import StatePanel
from terrine.spec.trajectory import CohortSpec, TimeToEvent

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["compile_design"]


def compile_design(
    design: Design,
    model: Model,
    *,
    persisted_prefixes: Mapping[tuple[str, str, str, str], ibis.Table] | None = None,
    persisted_anchors: ibis.Table | None = None,
) -> CompiledStudy:
    """Compile through the dispatcher, substituting verified cross-section inputs."""
    if isinstance(design.shape, EventSequence):
        from terrine.compile.shapes.event_sequence import compile_event_sequence

        return compile_event_sequence(design, model)
    if isinstance(design.shape, StatePanel):
        from terrine.compile.shapes.state_panel import compile_state_panel

        return compile_state_panel(design, model)
    from terrine.spec.panels import EventPanelSpec, SequentialSpec

    if isinstance(design.shape, SequentialSpec):
        from terrine.compile.shapes.sequential import compile_sequential

        return compile_sequential(design, model)

    if isinstance(design.shape, EventPanelSpec):
        from terrine.compile.shapes.event_panel import compile_event_panel

        return compile_event_panel(design, model)
    from terrine.spec.panels import CalendarComparisonSpec

    if isinstance(design.shape, CalendarComparisonSpec):
        from terrine.compile.shapes.calendar_comparison import compile_calendar_comparison

        return compile_calendar_comparison(design, model)
    if isinstance(design.shape, (CohortSpec, TimeToEvent)):
        if isinstance(design.shape, CohortSpec):
            from terrine.compile.shapes.ltv import compile_ltv

            return compile_ltv(design, model)
        from terrine.compile.shapes.time_to_event import compile_time_to_event

        return compile_time_to_event(design, model)
    return compile_columns(
        design,
        model,
        persisted_prefixes=persisted_prefixes,
        persisted_anchors=persisted_anchors,
    )


def compile_cohort_summary(study: CompiledStudy, *, grouping, value_column: str) -> CompiledStudy:
    """Apply a cohort summary to a compiled trajectory relation."""
    from terrine.compile.shapes.cohorts import compile_cohorts

    return compile_cohorts(study, grouping=grouping, value_column=value_column)
