"""Select the existing compiler path for a design shape."""

from __future__ import annotations

from typing import TYPE_CHECKING

from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.spec.design import Design
from terrine.spec.state_panel import StatePanel

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["compile_design"]


def compile_design(design: Design, model: Model) -> CompiledStudy:
    """Compile a supported Design through its single shape dispatcher."""
    if isinstance(design.shape, StatePanel):
        from terrine.compile.shapes.state_panel import compile_state_panel

        return compile_state_panel(design, model)
    return compile_columns(design, model)
