"""Select the existing compiler path for a design shape."""

from __future__ import annotations

from typing import TYPE_CHECKING

from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.spec.design import Design
from terrine.spec.state_panel import StatePanel
from terrine.spec.trajectory import CohortSpec, TimeToEvent

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["compile_design"]


def compile_design(design: Design, model: Model) -> CompiledStudy:
    """Compile a supported Design through its single shape dispatcher."""
    if isinstance(design.shape, StatePanel):
        from terrine.compile.shapes.state_panel import compile_state_panel

        return compile_state_panel(design, model)
    if isinstance(design.shape, (CohortSpec, TimeToEvent)):
        if isinstance(design.shape, CohortSpec):
            from terrine.compile.shapes.ltv import compile_ltv

            return compile_ltv(design, model)
        from terrine.compile.shapes.time_to_event import compile_time_to_event

        return compile_time_to_event(design, model)
    return compile_columns(design, model)


def compile_cohort_summary(study: CompiledStudy, *, grouping, value_column: str) -> CompiledStudy:
    """Apply a cohort summary to a compiled trajectory relation."""
    from terrine.compile.shapes.cohorts import compile_cohorts

    return compile_cohorts(study, grouping=grouping, value_column=value_column)
