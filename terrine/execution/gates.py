"""Execution-owned safeguards shared by ordinary Frames and runtime staging."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pyarrow as pa

from terrine.data import Budget, EligibilitySummary
from terrine.errors import RefusalSpec, TerrineError

_STATE_PANEL_BUDGET = RefusalSpec(
    "terrine.state_panel.budget_required",
    "State-panel materialization requires positive max_rows and max_output_bytes budgets.",
)
_SEQUENCE_BUDGET = RefusalSpec(
    "terrine.sequence.budget_required",
    "Event-sequence materialization requires positive max_rows and max_output_bytes budgets.",
)
_ELIGIBILITY_UNKNOWN = RefusalSpec(
    "terrine.study.eligibility_unknown",
    "Eligibility is unresolved for {unknown} of {candidate} candidates; "
    "complete the required history before exporting rows.",
)


def enforce_shape_budget(shape: object, budget: Budget | None) -> None:
    """Refuse unbounded exports for shapes whose output size must be bounded."""
    shape_name = type(shape).__name__
    refusal = (
        _STATE_PANEL_BUDGET
        if shape_name == "StatePanel"
        else _SEQUENCE_BUDGET
        if shape_name == "EventSequence"
        else None
    )
    if refusal is not None and (
        budget is None
        or budget.max_rows is None
        or budget.max_output_bytes is None
        or budget.max_rows <= 0
        or budget.max_output_bytes <= 0
    ):
        raise refusal.error()


def _eligibility_summary(
    table: Any,
    execute: Callable[[Any], pa.Table],
    *,
    cap: int | None = None,
) -> EligibilitySummary:
    if cap is not None:
        table = table.limit(min(cap + 1, 2**63 - 1))
    eligible = table["__terrine_eligible"]
    result = execute(
        table.aggregate(
            candidate=table.count(),
            excluded_false=(~eligible).sum().fill_null(0),
            unknown=eligible.isnull().sum().fill_null(0),
            retained=eligible.sum().fill_null(0),
        )
    )
    names = ("candidate", "excluded_false", "unknown", "retained")
    if (
        not isinstance(result, pa.Table)
        or result.num_rows != 1
        or set(result.column_names) != set(names)
    ):
        raise TerrineError(
            "Eligibility audit did not return one aggregate count record",
            code="terrine.metadata.invalid_eligibility_summary",
            context={},
        )
    return EligibilitySummary(**{name: result[name][0].as_py() for name in names})


def validate_compiled_output(compiled: Any, execute: Callable[[Any], pa.Table]):
    """Apply integrity and unknown-eligibility gates before output can be staged."""
    compiled.validate(execute=execute)
    summary = _eligibility_summary(compiled.audit_table, execute)
    if summary.unknown:
        raise _ELIGIBILITY_UNKNOWN.error(
            candidate=summary.candidate,
            excluded_false=summary.excluded_false,
            unknown=summary.unknown,
            retained=summary.retained,
        )
    return summary
