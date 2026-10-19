"""Lazy study frames with one verified execution lifetime per export."""

from __future__ import annotations

import hashlib
import json
import warnings
from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING

import pyarrow as pa

import terrine.compile.provenance as _provenance
from terrine.compile.cost import _OUTPUT_BYTES_EXCEEDED, _ROWS_EXCEEDED, _scan_policy
from terrine.data import (
    Budget,
    CohortOperationMeta,
    ColumnMeta,
    MaterializedFrame,
    Provenance,
    Roles,
    SamplingMeta,
)
from terrine.errors import RefusalSpec
from terrine.execution.gates import enforce_shape_budget, validate_compiled_output
from terrine.spec.anchors import FixedDate
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import StatePanel
from terrine.spec.trajectory import CohortGrouping, CohortSpec, LifeTable, TimeToEvent

if TYPE_CHECKING:
    from terrine.compile.columns import CompiledStudy
    from terrine.model import Model
    from terrine.spec.design import Design

__all__ = ["Frame", "CohortTable"]

_MISSING_DEPENDENCY = RefusalSpec(
    "terrine.convert.missing_dependency",
    "Conversion requires the optional {extra!r} dependency; install terrine[{extra}].",
)


def _optional_dependency(name: str):
    try:
        return import_module(name)
    except ImportError as exc:
        raise _MISSING_DEPENDENCY.error(extra=name) from exc


@dataclass(frozen=True, slots=True, init=False, repr=False, eq=False)
class Frame:
    """A compiled study, not executed data.

    ``expr`` is an unsafe Ibis escape hatch. Only materialization/conversion
    methods hold execution authority and enforce integrity and output budgets.
    Output limits do not bound upstream scan, query work or peak memory.
    """

    _design: Design
    _model: Model
    _compiled: CompiledStudy

    def __init__(self, design: Design, model: Model) -> None:
        from terrine.compile.dispatch import compile_design

        object.__setattr__(self, "_design", design)
        object.__setattr__(self, "_model", model)
        object.__setattr__(self, "_compiled", compile_design(design, model))

    @property
    def expr(self):
        """Return the Ibis graph; direct execution bypasses all export safeguards."""
        return self._compiled.table

    @property
    def roles(self):
        return self._compiled.roles

    @property
    def columns(self):
        return self._compiled.columns

    def materialize(self, budget: Budget | None = None) -> MaterializedFrame:
        return self._materialize(budget)

    def _materialize(
        self,
        budget: Budget | None = None,
        *,
        transform=None,
        shape: str | None = None,
        cohort_operation: CohortOperationMeta | None = None,
    ) -> MaterializedFrame:
        """Verify, audit and consume one bounded Arrow output stream atomically.

        No partial output is returned on limits, invalid history or stream errors.
        Concurrent writes after the execution view is established are excluded.
        """
        enforce_shape_budget(self._design.shape, budget)
        for warning in _scan_policy(budget):
            warnings.warn(warning, RuntimeWarning, stacklevel=2)
        from terrine.compile.dispatch import compile_design
        from terrine.execution.inputs import open_execution

        model = self._model
        with open_execution(
            model.catalog, model.con, units=model.units, input_binding=model.input_binding
        ) as context:
            from terrine.spec.validate import validate_design

            execution_model = model._for_execution(
                context,
                source_names=set(validate_design(self._design, model).required_sources),
                unit_name=self._design.unit,
            )
            compiled = compile_design(self._design, execution_model)
            if transform is not None:
                compiled = transform(compiled)
            summary = validate_compiled_output(compiled, context.execute)
            query = compiled.table
            chunk_size = 65_536
            is_life_table = isinstance(self._design.shape, LifeTable)
            if budget is not None and budget.max_rows is not None and not is_life_table:
                limit = min(budget.max_rows + 1, 2**63 - 1)
                query = query.limit(limit)
                chunk_size = min(chunk_size, limit)
            batches = []
            row_count = byte_count = 0
            context._check_query(query)
            reader = context.con.to_pyarrow_batches(query, chunk_size=chunk_size)
            try:
                schema = reader.schema
                for batch in reader:
                    if batch.schema != schema:
                        batch = batch.cast(schema)
                    row_count += batch.num_rows
                    byte_count += batch.nbytes
                    if budget is not None and not is_life_table:
                        if budget.max_rows is not None and row_count > budget.max_rows:
                            raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
                        if (
                            budget.max_output_bytes is not None
                            and byte_count > budget.max_output_bytes
                        ):
                            raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
                    batches.append(batch)
                table = pa.Table.from_batches(batches, schema=schema)
            finally:
                reader.close()
            roles = compiled.roles
            columns = compiled.columns
            if isinstance(self._design.shape, LifeTable):
                from terrine.compile.dispatch import aggregate_life_table

                life_table = self._design.shape
                group_supports: dict[str, str] = {}
                for name in life_table.group_by:
                    meta = compiled.columns[name]
                    for column, policy in (
                        (meta.observed_from_column, "min"),
                        (meta.observed_until_column, "max"),
                        (meta.complete_column, "all"),
                        (meta.reason_column, "reasons"),
                        (meta.absolute_error_column, "max"),
                    ):
                        if column is not None:
                            group_supports.setdefault(column, policy)
                table = aggregate_life_table(
                    table,
                    interval_seconds=life_table.interval,
                    group_by=life_table.group_by,
                    group_supports=group_supports,
                    max_rows=None if budget is None else budget.max_rows,
                )
                if budget is not None:
                    if budget.max_rows is not None and table.num_rows > budget.max_rows:
                        raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
                    if (
                        budget.max_output_bytes is not None
                        and table.nbytes > budget.max_output_bytes
                    ):
                        raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
                roles = Roles(
                    "unit",
                    "anchor_id",
                    "t0",
                    None,
                    (*life_table.group_by, "time_bin"),
                    (
                        "at_risk",
                        "events",
                        "censored",
                        "delayed_entries",
                        "event_rate",
                        "censor_rate",
                    ),
                )
                columns = {name: compiled.columns[name] for name in life_table.group_by}
                columns["time_bin"] = ColumnMeta(None, "feature", None, None, None, None)
                columns.update(
                    {
                        name: ColumnMeta(None, "outcome", None, None, "complete", "reason")
                        for name in roles.outcomes
                    }
                )
            if isinstance(self._design.shape, TimeToEvent):
                for name in ("event_time", "censor_time", "duration", "entry_time"):
                    if name not in table.column_names:
                        continue
                    dtype = table.schema.field(name).type
                    if pa.types.is_timestamp(dtype) and dtype.tz is None:
                        timestamp = table[name].cast(pa.timestamp(dtype.unit, tz="UTC"))
                        table = table.set_column(
                            table.schema.get_field_index(name), name, timestamp
                        )
            if isinstance(self._design.shape, StatePanel):
                gap_index = table.schema.get_field_index("gap")
                gap = table.column(gap_index).cast(pa.duration("us"))
                table = table.set_column(gap_index, "gap", gap)
            treatment = self._design.treatment
            treatment_column = compiled.roles.treatment
            baseline_binary = (
                treatment is not None
                and treatment_column is not None
                and isinstance(self._design.anchor, FixedDate)
                and self._design.shape == "cross_section"
                and treatment.at == 0
                and treatment.known_at_t0
                and pa.types.is_boolean(table.schema.field(treatment_column).type)
            )
            calendar_comparison = _provenance.calendar_comparison_metadata(self._design)
            panel = _provenance.panel_metadata(self._design)

            if isinstance(self._design.shape, StatePanel):
                shape_name = "state_panel"
            elif isinstance(self._design.shape, EventSequence):
                shape_name = "event_sequence"
            elif calendar_comparison is not None:
                shape_name = "calendar_comparison"
            elif panel is not None:
                shape_name = panel.shape
            elif isinstance(self._design.shape, CohortSpec):
                shape_name = "ltv"
            elif isinstance(self._design.shape, LifeTable):
                shape_name = "life_table"
            elif isinstance(self._design.shape, TimeToEvent):
                shape_name = "time_to_event"
            else:
                shape_name = (
                    self._design.shape if isinstance(self._design.shape, str) else "cross_section"
                )
            feature_cutoffs = compiled.feature_cutoffs
            expression_timings = compiled.expression_timings
            if isinstance(self._design.shape, LifeTable):
                feature_cutoffs = {
                    name: cutoff
                    for name, cutoff in compiled.feature_cutoffs.items()
                    if name in self._design.shape.group_by
                }
                expression_timings = {
                    name: timing
                    for name, timing in compiled.expression_timings.items()
                    if name in self._design.shape.group_by
                }
            provenance = Provenance(
                feature_cutoffs=feature_cutoffs,
                eligibility_cutoffs=compiled.eligibility_cutoffs,
                design_hash=self._design.spec_hash,
                model_hash=model.model_hash,
                source_generations=context.snapshot.source_generations,
                execution_snapshot=context.snapshot,
                shape=shape or shape_name,
                treatment_design="baseline_binary" if baseline_binary else None,
                eligibility_summary=summary,
                treatment_knowledge="retrospective_effective_time"
                if treatment is not None
                else None,
                treatment_known_at_t0=treatment.known_at_t0 if treatment is not None else False,
                state_panel=_provenance.state_panel_metadata(self._design, model),
                expression_timings=expression_timings,
                sequence_meta=compiled.sequence_meta,
                panel=panel,
                sampling=(
                    SamplingMeta(
                        self._design.sample.strata,
                        self._design.sample.controls_per_treated,
                        self._design.sample.seed,
                    )
                    if self._design.sample is not None
                    else None
                ),
                calendar_comparison=calendar_comparison,
                administrative_cutoff=self._design.administrative_cutoff,
                cohort_operation=cohort_operation,
            )
            return MaterializedFrame(table, roles, columns, provenance)

    def to_pyarrow(self, *, budget: Budget | None = None) -> pa.Table:
        """Execute once and attach portable study metadata to the Arrow schema."""
        return self.materialize(budget).to_arrow()

    def to_pandas(self, *, budget: Budget | None = None):
        """Convert using nullable Arrow-backed dtypes; import pandas only on demand."""
        pandas = _optional_dependency("pandas")
        return self.to_pyarrow(budget=budget).to_pandas(types_mapper=pandas.ArrowDtype)

    def to_polars(self, *, budget: Budget | None = None):
        """Convert using the optional native Polars Arrow conversion."""
        polars = _optional_dependency("polars")
        return polars.from_arrow(self.to_pyarrow(budget=budget))

    def cohorts(self, *, grouping: CohortGrouping, value_column: str) -> CohortTable:
        if not isinstance(self._design.shape, CohortSpec):
            raise TypeError("cohorts requires a trajectory Frame")
        if not isinstance(grouping, CohortGrouping):
            raise TypeError("grouping must be CohortGrouping")
        if not isinstance(value_column, str) or value_column not in {
            "period_value",
            "cumulative_value",
        }:
            raise ValueError("value_column must name a trajectory value")
        if value_column == "cumulative_value" and not self._design.shape.trajectory.cumulative:
            raise ValueError("the trajectory did not request cumulative values")
        return CohortTable(self, grouping, value_column)


@dataclass(frozen=True, slots=True)
class CohortTable:
    parent_frame: Frame
    grouping: CohortGrouping
    value_column: str

    def materialize(self, *, budget: Budget | None = None) -> MaterializedFrame:
        parent_design_hash = self.parent_frame._design.spec_hash
        payload = {
            "bucket_seconds": self.grouping.bucket,
            "bucket_origin": self.grouping.bucket_origin.isoformat(),
            "statistic": self.grouping.statistic,
            "value_column": self.value_column,
            "parent_design_hash": parent_design_hash,
        }
        operation_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        operation = CohortOperationMeta(
            self.grouping.bucket,
            self.grouping.bucket_origin,
            self.grouping.statistic,
            self.value_column,
            parent_design_hash,
            operation_hash,
        )
        from terrine.compile.dispatch import compile_cohort_summary

        return self.parent_frame._materialize(
            budget,
            transform=lambda study: compile_cohort_summary(
                study, grouping=self.grouping, value_column=self.value_column
            ),
            shape="cohort_table",
            cohort_operation=operation,
        )

    def to_pyarrow(self, *, budget: Budget | None = None) -> pa.Table:
        return self.materialize(budget=budget).to_arrow()
