"""Lazy study frames with one verified execution lifetime per export."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING

import pyarrow as pa

from terrine.compile.cost import (
    _OUTPUT_BYTES_EXCEEDED,
    _ROWS_EXCEEDED,
    _eligibility_summary,
    _scan_policy,
)
from terrine.data import Budget, MaterializedFrame, Provenance
from terrine.errors import RefusalSpec
from terrine.spec.anchors import FixedDate

if TYPE_CHECKING:
    from terrine.compile.columns import CompiledStudy
    from terrine.model import Model
    from terrine.spec.design import Design

__all__ = ["Frame"]

_ELIGIBILITY_UNKNOWN = RefusalSpec(
    "terrine.study.eligibility_unknown",
    "Eligibility is unresolved for {unknown} of {candidate} candidates; "
    "complete the required history before exporting rows.",
)
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
        from terrine.compile.columns import compile_columns

        object.__setattr__(self, "_design", design)
        object.__setattr__(self, "_model", model)
        object.__setattr__(self, "_compiled", compile_columns(design, model))

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
        """Verify, audit and consume one bounded Arrow output stream atomically.

        No partial output is returned on limits, invalid history or stream errors.
        Concurrent writes after the execution view is established are excluded.
        """
        for warning in _scan_policy(budget):
            warnings.warn(warning, RuntimeWarning, stacklevel=2)
        from terrine.compile.columns import compile_columns
        from terrine.execution.inputs import open_execution

        model = self._model
        with open_execution(
            model.catalog, model.con, units=model.units, input_binding=model.input_binding
        ) as context:
            compiled = compile_columns(self._design, model._for_execution(context))
            compiled.validate(execute=context.execute)
            summary = _eligibility_summary(compiled.audit_table, context.execute)
            if summary.unknown:
                raise _ELIGIBILITY_UNKNOWN.error(
                    candidate=summary.candidate,
                    excluded_false=summary.excluded_false,
                    unknown=summary.unknown,
                    retained=summary.retained,
                )
            query = compiled.table
            chunk_size = 65_536
            if budget is not None and budget.max_rows is not None:
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
                    if budget is not None:
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
            provenance = Provenance(
                feature_cutoffs=compiled.feature_cutoffs,
                eligibility_cutoffs=compiled.eligibility_cutoffs,
                design_hash=self._design.spec_hash,
                model_hash=model.model_hash,
                source_generations=context.snapshot.source_generations,
                execution_snapshot=context.snapshot,
                shape=self._design.shape,
                treatment_design="baseline_binary" if baseline_binary else None,
                eligibility_summary=summary,
                treatment_knowledge="retrospective_effective_time"
                if treatment is not None
                else None,
                treatment_known_at_t0=treatment.known_at_t0 if treatment is not None else False,
            )
            return MaterializedFrame(table, compiled.roles, compiled.columns, provenance)

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
