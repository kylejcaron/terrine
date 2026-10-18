"""Row-free planning and execution-budget policy for study frames."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from terrine.data import Budget, CoverageSummary, EligibilitySummary, Preview
from terrine.errors import RefusalSpec, TerrineError

if TYPE_CHECKING:
    from terrine.model import Model
    from terrine.spec.design import Design

__all__ = ["preview"]

_ROWS_EXCEEDED = RefusalSpec(
    "terrine.budget.rows_exceeded", "Output exceeds the configured row limit {limit}."
)
_OUTPUT_BYTES_EXCEEDED = RefusalSpec(
    "terrine.budget.output_bytes_exceeded", "Output exceeds the configured byte limit {limit}."
)
_SCAN_UNKNOWN = RefusalSpec(
    "terrine.budget.scan_unknown",
    "A strict scan-byte budget requires a verified scan bound; this backend has none. "
    "Explicitly choose non-strict scan policy after reviewing upstream work.",
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


def _eligibility_summary(
    table: Any,
    execute: Callable[[Any], pa.Table],
    *,
    cap: int | None = None,
) -> EligibilitySummary:
    if cap is not None:
        table = table.limit(min(cap + 1, 2**63 - 1))
    eligible = table["__terrine_eligible"]
    query = table.aggregate(
        candidate=table.count(),
        excluded_false=(~eligible).sum().fill_null(0),
        unknown=eligible.isnull().sum().fill_null(0),
        retained=eligible.sum().fill_null(0),
    )
    result = execute(query)
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
            compiled = compile_design(design, model._for_execution(context))
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
