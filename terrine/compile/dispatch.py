"""Select the existing compiler path for a design shape."""

from __future__ import annotations

from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import ibis
import pyarrow as pa

from terrine.compile.columns import (
    CompiledStudy,
    _column_resolver,
    _ColumnRequirements,
    _resolve_columns,
    compile_columns,
)
from terrine.compile.prune import prune_columns
from terrine.execution._metadata import metadata_operation
from terrine.spec.design import Design
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import StatePanel
from terrine.spec.trajectory import CohortSpec, LifeTable, TimeToEvent
from terrine.spec.validate import (
    ValidationReport,
    _validate_declarations,
    _validation_resolver,
    ensure_runtime_shape_supported,
    resolve_unit,
    validate_design,
)

if TYPE_CHECKING:
    from terrine.compile.shapes.life_table import _LifeTableAccumulator
    from terrine.model import Model

__all__ = ["compile_design", "compile_cohort_summary", "aggregate_life_table"]


@dataclass(frozen=True, slots=True)
class _StudyRequirements:
    """Declaration-only preparation for one Design and its derived shape designs."""

    design: Design
    required_sources: tuple[str, ...]
    validations: tuple[tuple[Design, ValidationReport], ...]
    columns: tuple[tuple[Design, _ColumnRequirements], ...]


@dataclass(slots=True)
class _DeclarationPass:
    model: Model
    validations: list[tuple[Design, ValidationReport]] = field(default_factory=list)
    columns: list[tuple[Design, _ColumnRequirements]] = field(default_factory=list)

    def validate(self, design: Design, model: Model) -> ValidationReport:
        if model is not self.model:
            return _validate_declarations(design, model)
        for previous, report in self.validations:
            if previous == design:
                return report
        report = _validate_declarations(design, model)
        self.validations.append((design, report))
        return report

    def resolve_columns(self, design: Design, model: Model) -> _ColumnRequirements:
        if model is not self.model:
            return _resolve_columns(design, model)
        for previous, requirements in self.columns:
            if previous == design:
                return requirements
        requirements = _resolve_columns(design, model)
        self.columns.append((design, requirements))
        return requirements

    @contextmanager
    def active(self) -> Generator[None]:
        validation = _validation_resolver.set(self.validate)
        columns = _column_resolver.set(self.resolve_columns)
        try:
            yield
        finally:
            _column_resolver.reset(columns)
            _validation_resolver.reset(validation)


def _prepare_study(design: Design, model: Model) -> tuple[_StudyRequirements, CompiledStudy]:
    design = resolve_unit(design, model)
    declarations = _DeclarationPass(model)
    with declarations.active():
        compiled = compile_design(design, model)
        # Shapes may lower a different design (e.g. LifeTable -> TimeToEvent).
        # Resolve the outer dependencies as well, without moving its inspection
        # validation ahead of any of the shape compiler's existing refusals.
        report = validate_design(design, model)
    return (
        _StudyRequirements(
            design,
            report.required_sources,
            tuple(declarations.validations),
            tuple(declarations.columns),
        ),
        compiled,
    )


def _bind_study(
    study: _StudyRequirements, model: Model, bound: Model, *, prune_output: bool
) -> CompiledStudy:
    design = study.design
    # _for_execution has already checked authority and schema and normalized
    # metadata in the new view. Reuse declaration conclusions only when the
    # fresh relevant metadata (including capability profiles) matches.
    same = (
        bound.catalog is model.catalog
        and bound.readiness is model.readiness
        and bound._engine == model._engine
        and (
            design.unit is None
            or bound.units.get(design.unit) == model.units.get(design.unit)
        )
        and all(
            bound.sources.get(name) == model.sources[name]
            and bound.event_ids.get(name) == model.event_ids.get(name)
            for name in study.required_sources
        )
    )
    declarations = (
        _DeclarationPass(bound, list(study.validations), list(study.columns))
        if same
        else _DeclarationPass(bound)
    )
    with declarations.active():
        if not same:
            validate_design(design, bound)
        return compile_design(design, bound, _prune_output=prune_output)


@metadata_operation()
def compile_design(
    design: Design,
    model: Model,
    *,
    persisted_anchors: ibis.Table | None = None,
    _prune_output: bool = True,
) -> CompiledStudy:
    """Compile a shape with optional verified logical substitutions."""
    from terrine.compile.columns import exact_scope

    design = resolve_unit(design, model)
    with exact_scope(model):
        compiled = _compile_design(
            design,
            model,
            persisted_anchors=persisted_anchors,
        )
    if _prune_output:
        compiled = compiled.with_output(prune_columns(compiled.table))
    return compiled


def _compile_design(
    design: Design,
    model: Model,
    *,
    persisted_anchors: ibis.Table | None = None,
) -> CompiledStudy:
    if persisted_anchors is not None:
        ensure_runtime_shape_supported(design.shape, anchor=design.anchor)
    if isinstance(design.shape, EventSequence):
        from terrine.compile.shapes.event_sequence import compile_event_sequence

        return compile_event_sequence(design, model)
    if isinstance(design.shape, StatePanel):
        from terrine.compile.shapes.state_panel import compile_state_panel

        return compile_state_panel(
            design,
            model,
            persisted_anchors=persisted_anchors,
        )
    from terrine.spec.panels import EventPanelSpec, SequentialSpec

    if isinstance(design.shape, SequentialSpec):
        from terrine.compile.shapes.sequential import compile_sequential

        return compile_sequential(
            design,
            model,
            persisted_anchors=persisted_anchors,
        )

    if isinstance(design.shape, EventPanelSpec):
        from terrine.compile.shapes.event_panel import compile_event_panel

        return compile_event_panel(
            design,
            model,
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
        group_by = ()
        if isinstance(design.shape, LifeTable):
            group_by = design.shape.group_by
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

        return compile_time_to_event(design, model, group_by=group_by)
    return compile_columns(
        design,
        model,
        persisted_anchors=persisted_anchors,
    )


def compile_cohort_summary(
    study: CompiledStudy, *, grouping, value_column: str, dialect: str | None = None
) -> CompiledStudy:
    """Apply a cohort summary to a compiled trajectory relation.

    ``dialect`` is the admitted expression policy of the Model that compiled
    ``study``; without it the relation's own backend selects the policy.
    """
    from terrine.compile.shapes.cohorts import compile_cohorts

    return compile_cohorts(study, grouping=grouping, value_column=value_column, dialect=dialect)


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


def _life_table_accumulator(
    schema: pa.Schema,
    *,
    interval_seconds: int,
    group_by: Sequence[str],
    group_supports: Mapping[str, str],
    max_rows: int | None,
) -> _LifeTableAccumulator:
    from terrine.compile.shapes.life_table import _LifeTableAccumulator

    return _LifeTableAccumulator(
        schema,
        interval_seconds=interval_seconds,
        group_by=group_by,
        group_supports=group_supports,
        max_rows=max_rows,
    )
