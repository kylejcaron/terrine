"""Row-free planning and execution-budget policy for study frames."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from terrine.data import (
    _OUTPUT_BYTES_EXCEEDED,
    _ROWS_EXCEEDED,
    Budget,
    CoverageSummary,
    Preview,
)
from terrine.errors import RefusalSpec
from terrine.execution.gates import _eligibility_summary

if TYPE_CHECKING:
    from terrine.model import Model
    from terrine.spec.design import Design

__all__ = ["preview", "_OUTPUT_BYTES_EXCEEDED", "_ROWS_EXCEEDED"]

_SCAN_UNKNOWN = RefusalSpec(
    "terrine.budget.scan_unknown",
    "A strict scan-byte budget requires a verified bound or enforceable cap "
    "for every source-read phase. "
    "Configure a capable owner or explicitly choose non-strict policy after reviewing capture, "
    "validation, execution and retries.",
)
_SCAN_EXCEEDED = RefusalSpec(
    "terrine.budget.scan_exceeded", "The verified scan bound exceeds the configured limit {limit}."
)
_SCAN_REASON = "backend_scan_bound_unavailable"
_SCAN_WARNING = "Source scan bytes are unknown; output limits do not bound upstream scan or work."


def _scan_policy(budget: Budget | None, scan_bytes: int | None = None) -> tuple[str, ...]:
    if budget is not None and not isinstance(budget, Budget):
        raise TypeError("budget must be Budget or None")
    if budget is None or budget.max_scan_bytes is None:
        return ()
    if scan_bytes is None:
        if budget.strict_scan:
            raise _SCAN_UNKNOWN.error()
        return (_SCAN_WARNING,)
    if scan_bytes > budget.max_scan_bytes:
        raise _SCAN_EXCEEDED.error(limit=budget.max_scan_bytes)
    return ()


def preview(
    design: Design,
    model: Model,
    budget: Budget | None = None,
    *,
    bounded_count: int = 0,
) -> Preview:
    """Compile without reading rows, or explicitly count at most cap+1 candidates.

    Counts and integrity checks can scan upstream data. Strict unknown scan-byte
    budgets refuse that explicit execution; output caps are not scan bounds.
    """
    if isinstance(bounded_count, bool) or not isinstance(bounded_count, int) or bounded_count < 0:
        raise ValueError("bounded_count must be a nonnegative integer")
    if budget is not None and not isinstance(budget, Budget):
        raise TypeError("budget must be Budget or None")
    from terrine.compile.dispatch import compile_design

    planned = compile_design(design, model)
    warnings = (_SCAN_WARNING,)
    counts: dict[str, Any] = {}
    if bounded_count:
        _scan_policy(budget)
        from terrine.execution.inputs import open_execution

        with open_execution(
            model.catalog, model.con, units=model.units, input_binding=model.input_binding
        ) as context:
            from terrine.spec.validate import validate_design

            execution_model = model._for_execution(
                context,
                source_names=set(validate_design(design, model).required_sources),
                unit_name=design.unit,
            )
            compiled = compile_design(design, execution_model)
            compiled.validate(execute=context.execute)
            summary = _eligibility_summary(compiled.audit_table, context.execute, cap=bounded_count)
        capped = summary.candidate > bounded_count
        counts = {"counted_rows": min(summary.candidate, bounded_count), "count_capped": capped}
        if not capped:
            counts.update(
                candidate_rows=summary.candidate,
                known_false_rows=summary.excluded_false,
                unknown_rows=summary.unknown,
                eligible_rows=summary.retained,
            )
    return Preview(
        planned_rows=None,
        output_bytes=None,
        scan_bytes=None,
        scan_reason=_SCAN_REASON,
        coverage={
            name: CoverageSummary(coverage.start, coverage.end_exclusive)
            for name, coverage in model.readiness.sources.items()
        },
        strategy=planned.strategy,
        budget_warnings=warnings,
        **counts,
    )
