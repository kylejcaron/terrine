"""Select the existing compiler path for a design shape."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

import ibis
import pyarrow as pa

from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.execution._metadata import metadata_operation
from terrine.spec.design import Design
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import StatePanel
from terrine.spec.trajectory import CohortSpec, LifeTable, TimeToEvent
from terrine.spec.validate import ensure_runtime_shape_supported

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["compile_design", "compile_cohort_summary", "aggregate_life_table"]


@metadata_operation()
def compile_design(
    design: Design,
    model: Model,
    *,
    persisted_prefixes: Mapping[tuple[str, str, str, str], ibis.Table] | None = None,
    persisted_anchors: ibis.Table | None = None,
) -> CompiledStudy:
    """Compile a shape, propagating supported verified logical substitutions."""
    from terrine.compile.columns import exact_scope

    with exact_scope(model):
        return _compile_design(
            design,
            model,
            persisted_prefixes=persisted_prefixes,
            persisted_anchors=persisted_anchors,
        )


def _compile_design(
    design: Design,
    model: Model,
    *,
    persisted_prefixes: Mapping[tuple[str, str, str, str], ibis.Table] | None = None,
    persisted_anchors: ibis.Table | None = None,
) -> CompiledStudy:
    if persisted_prefixes is not None or persisted_anchors is not None:
        ensure_runtime_shape_supported(design.shape, anchor=design.anchor)
    if isinstance(design.shape, EventSequence):
        from terrine.compile.shapes.event_sequence import compile_event_sequence

        return compile_event_sequence(design, model)
    if isinstance(design.shape, StatePanel):
        from terrine.compile.shapes.state_panel import compile_state_panel

        return compile_state_panel(
            design,
            model,
            persisted_prefixes=persisted_prefixes,
            persisted_anchors=persisted_anchors,
        )
    from terrine.spec.panels import EventPanelSpec, SequentialSpec

    if isinstance(design.shape, SequentialSpec):
        from terrine.compile.shapes.sequential import compile_sequential

        return compile_sequential(
            design,
            model,
            persisted_prefixes=persisted_prefixes,
            persisted_anchors=persisted_anchors,
        )

    if isinstance(design.shape, EventPanelSpec):
        from terrine.compile.shapes.event_panel import compile_event_panel

        return compile_event_panel(
            design,
            model,
            persisted_prefixes=persisted_prefixes,
            persisted_anchors=persisted_anchors,
        )
    from terrine.spec.panels import CalendarComparisonSpec

    if isinstance(design.shape, CalendarComparisonSpec):
        from terrine.compile.shapes.calendar_comparison import compile_calendar_comparison

        return compile_calendar_comparison(design, model)
    if isinstance(design.shape, (CohortSpec, TimeToEvent, LifeTable)):
        if isinstance(design.shape, CohortSpec):
            from terrine.compile.shapes.ltv import compile_ltv

            return compile_ltv(design, model)
        if isinstance(design.shape, LifeTable):
            shape = design.shape.time_to_event
            design = Design(
                design.unit,
                design.anchor,
                features=design.features,
                eligibility=design.eligibility,
                treatment=design.treatment,
                gap=design.gap,
                shape=shape,
                anchor_range=design.anchor_range,
                sample=design.sample,
                administrative_cutoff=design.administrative_cutoff,
            )
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


def aggregate_life_table(
    records: pa.Table,
    *,
    interval_seconds: int,
    group_by: Sequence[str],
    group_supports: Mapping[str, str],
    max_rows: int | None = None,
) -> pa.Table:
    """Aggregate compiled life-table records through the shape dispatcher."""
    from terrine.compile.shapes.life_table import aggregate_life_table as aggregate

    return aggregate(
        records,
        interval_seconds=interval_seconds,
        group_by=group_by,
        group_supports=group_supports,
        max_rows=max_rows,
    )
