"""Compiler for schedule-anchored sequential trials."""

from __future__ import annotations

from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.spec.design import Design
from terrine.spec.panels import SequentialSpec


def compile_sequential(design: Design, model) -> CompiledStudy:
    if not isinstance(design.shape, SequentialSpec):
        raise TypeError("compile_sequential requires a Sequential Design")
    return compile_columns(design, model)
