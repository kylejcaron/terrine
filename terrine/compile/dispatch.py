"""Select the existing compiler path for a design shape."""

from __future__ import annotations

from typing import TYPE_CHECKING

from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.spec.design import Design
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import StatePanel

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["compile_design"]


def compile_design(design: Design, model: Model) -> CompiledStudy:
    """Compile a supported Design through its single shape dispatcher."""
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
    return compile_columns(design, model)
