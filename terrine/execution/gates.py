"""Execution-owned safeguards shared by ordinary Frames and runtime staging."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, Literal, overload

import pyarrow as pa

from terrine.data import Budget, EligibilitySummary, RowCheckGroup
from terrine.errors import RefusalSpec
from terrine.execution.inputs import ArrowReader

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
_INVALID_ELIGIBILITY_RESULT = RefusalSpec(
    "terrine.metadata.invalid_eligibility_summary",
    "Eligibility audit did not return one aggregate count record",
)


def enforce_shape_budget(shape: object, budget: Budget | None) -> None:
    """Refuse unbounded exports for shapes whose output size must be bounded."""
    shape_types = {base.__name__ for base in type(shape).__mro__}
    refusal = (
        _STATE_PANEL_BUDGET
        if "StatePanel" in shape_types
        else _SEQUENCE_BUDGET
        if "EventSequence" in shape_types
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


GATE_PREFIX = "__terrine_gate_"
_SUMMARY_FIELDS = ("candidate", "excluded_false", "unknown", "retained")
_STRING_COUNT = re.compile(r"0|[1-9][0-9]*")


def foldable(check: Any) -> bool:
    """A ``RowCheckGroup``, or a one-row aggregate check (``table`` and ``failures``)."""
    return isinstance(check, RowCheckGroup) or (
        hasattr(check, "table") and hasattr(check, "failures")
    )


def fold(checks: Sequence[Any]) -> list[list[Any] | Any]:
    """The schedule as maximal runs of foldable checks interleaved with the rest.

    Runs are lists, unfoldable checks stand alone, order is preserved.  The
    last element is always a run, possibly empty, so the eligibility summary
    has a statement to ride on.
    """
    schedule: list[list[Any] | Any] = []
    for check in checks:
        if not foldable(check):
            schedule.append(check)
        elif schedule and isinstance(schedule[-1], list):
            schedule[-1].append(check)
        else:
            schedule.append([check])
    if not schedule or not isinstance(schedule[-1], list):
        schedule.append([])
    return schedule


def _gate_name(index: int) -> str:
    return f"{GATE_PREFIX}check_{index}"


def _failed_rows(table: Any, column: str) -> Any:
    """The number of TRUE rows of a flag column; NULL rows pass."""
    return table[column].ifelse(1, 0).sum().fill_null(0).cast("int64")


def gate_totals(audit: Any | None, run: Sequence[Any]) -> Any:
    """One row holding every count a run's checks need, plus the eligibility summary.

    With ``audit``, one aggregate over it yields the summary counts and the
    failed-row sums of every ``RowCheckGroup`` bound to it.  Every other check
    is cross-joined in schedule order: a group on another relation as its own
    aggregate, a one-row aggregate check as the selection of its string counts.
    Columns are ``__terrine_gate_check_{k}`` with ``k`` numbering the run's
    checks in schedule order; every row sum is an ``int64``.
    """
    base: dict[str, Any] = {}
    if audit is not None:
        eligible = audit["__terrine_eligible"]
        base = {
            f"{GATE_PREFIX}candidate": audit.count().cast("int64"),
            f"{GATE_PREFIX}excluded_false": (~eligible).sum().fill_null(0).cast("int64"),
            f"{GATE_PREFIX}unknown": eligible.isnull().sum().fill_null(0).cast("int64"),
            f"{GATE_PREFIX}retained": eligible.sum().fill_null(0).cast("int64"),
        }
    parts = []
    index = 0
    for item in run:
        if isinstance(item, RowCheckGroup):
            sums = {}
            for check in item.checks:
                sums[_gate_name(index)] = _failed_rows(item.table, check.column)
                index += 1
            if audit is not None and item.table.equals(audit):
                base.update(sums)
            else:
                parts.append(item.table.aggregate(**sums))
        else:
            counts = {}
            for failure in item.failures:
                counts[_gate_name(index)] = item.table[failure.column]
                index += 1
            parts.append(item.table.select(**counts))
    if audit is not None:
        parts.insert(0, audit.aggregate(**base))
    if not parts:
        raise ValueError("a gate needs an audit relation or at least one check")
    first, *rest = parts
    return first.cross_join(*rest) if rest else first


def gate_record(result: Any) -> dict[str, Any]:
    """The one row of an executed gate relation, by column name."""
    if not isinstance(result, pa.Table) or result.num_rows != 1:
        raise _INVALID_ELIGIBILITY_RESULT.error()
    return {name: result[name][0].as_py() for name in result.column_names}


def _count(record: Mapping[str, Any], index: int, *, string: bool) -> int:
    name = _gate_name(index)
    if name not in record:
        raise _INVALID_ELIGIBILITY_RESULT.error()
    value = record[name]
    if string:
        if not isinstance(value, str) or _STRING_COUNT.fullmatch(value) is None:
            raise _INVALID_ELIGIBILITY_RESULT.error()
        return int(value)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _INVALID_ELIGIBILITY_RESULT.error()
    return value


@overload
def gate_result(
    record: Mapping[str, Any], run: Sequence[Any], *, summary: Literal[True]
) -> EligibilitySummary: ...


@overload
def gate_result(
    record: Mapping[str, Any], run: Sequence[Any], *, summary: Literal[False]
) -> None: ...


def gate_result(
    record: Mapping[str, Any], run: Sequence[Any], *, summary: bool
) -> EligibilitySummary | None:
    """Refuse on the first flagged check of ``run``, else summarise eligibility.

    Items are decoded and refused one at a time in schedule order, so an
    earlier item's refusal precedes anything wrong with a later one; the
    eligibility summary is read only once every item has passed.  Row-check
    sums are ``int64``; one-row aggregate checks carry decimal strings.  A
    missing gate column, a malformed count or an inconsistent summary is
    refused as an invalid eligibility result.
    """
    index = 0
    for item in run:
        if isinstance(item, RowCheckGroup):
            checks, string = item.checks, False
        else:
            checks, string = item.failures, True
        counts = [_count(record, index + offset, string=string) for offset in range(len(checks))]
        index += len(checks)
        for check, count in zip(checks, counts, strict=True):
            if count:
                raise check.refusal.error(**check.context, failed_count=count)
    if not summary:
        return None
    names = [f"{GATE_PREFIX}{field}" for field in _SUMMARY_FIELDS]
    if any(name not in record for name in names):
        raise _INVALID_ELIGIBILITY_RESULT.error()
    return EligibilitySummary(*(record[name] for name in names))


def require_resolved(summary: EligibilitySummary) -> EligibilitySummary:
    """Refuse an export while any candidate's eligibility is still unknown."""
    if summary.unknown:
        raise _ELIGIBILITY_UNKNOWN.error(
            candidate=summary.candidate,
            excluded_false=summary.excluded_false,
            unknown=summary.unknown,
            retained=summary.retained,
        )
    return summary


def run_schedule(compiled: Any, execute: Callable[[Any], pa.Table]) -> list[Any]:
    """Run every check before the final run, in schedule order; return that run.

    Anchor checks go first; then each run of foldable checks is one gate
    statement and each unfoldable check validates itself in place.
    """
    compiled._anchors.validate(execute=execute)
    *earlier, final = fold(compiled._checks)
    for item in earlier:
        if isinstance(item, list):
            gate_result(gate_record(execute(gate_totals(None, item))), item, summary=False)
        else:
            item.validate(execute)
    return final


def validate_schedule(compiled: Any, execute: Callable[[Any], pa.Table]) -> None:
    """Run every check of the schedule, the final run included; no summary is read."""
    final = run_schedule(compiled, execute)
    if final:
        gate_result(gate_record(execute(gate_totals(None, final))), final, summary=False)


def gate_summary(compiled: Any, execute: Callable[[Any], pa.Table]) -> EligibilitySummary:
    """Run the whole schedule; the final gate statement also yields the summary.

    Unresolved eligibility is reported, not refused: ``require_resolved``
    decides that before any row leaves.
    """
    final = run_schedule(compiled, execute)
    record = gate_record(execute(gate_totals(compiled.audit_table, final)))
    return gate_result(record, final, summary=True)


def audit_summary(audit: Any, execute: Callable[[Any], pa.Table]) -> EligibilitySummary:
    """The eligibility summary of ``audit`` in one aggregate, with no checks.

    For an audit whose schedule has already run, or for a capped diagnostic
    read of ``audit.limit(...)``.
    """
    return gate_result(gate_record(execute(gate_totals(audit, []))), [], summary=True)


def validate_compiled_output(
    compiled: Any, execute: Callable[[Any], pa.Table]
) -> EligibilitySummary:
    """Apply integrity and unknown-eligibility gates before output can be staged."""
    return require_resolved(gate_summary(compiled, execute))


def gated_output(output: Any, audit: Any, run: Sequence[Any]) -> Any:
    """The output rows, each carrying the final run's gate row.

    One statement: the engine shares the candidate pipeline between the rows
    and their gate instead of running it once per check.
    """
    return output.cross_join(gate_totals(audit, run))


class GatedStream:
    """Output batches of a gated statement, without their gate columns.

    The gate columns of the first non-empty batch decide before that batch is
    yielded: every check of ``run`` in order, then unresolved eligibility.  An
    empty stream carries no gate row, so ``resolve_empty`` executes the gate
    relation alone and a refusal on excluded candidates still fires.
    """

    def __init__(
        self,
        reader: ArrowReader,
        run: Sequence[Any],
        columns: Sequence[str],
        resolve_empty: Callable[[], pa.Table],
    ) -> None:
        self._reader = reader
        self._run = tuple(run)
        self._columns = list(columns)
        self._resolve_empty = resolve_empty
        source = reader.schema
        self.schema = pa.schema(
            [source.field(name) for name in self._columns], metadata=source.metadata
        )
        gate = [name for name in source.names if name.startswith(GATE_PREFIX)]
        self._gate = pa.schema([source.field(name) for name in gate])
        self.summary: EligibilitySummary | None = None
        self._held: pa.RecordBatch | None = None
        self._exhausted = False

    def _resolve(self, record: Mapping[str, Any]) -> EligibilitySummary:
        self.summary = require_resolved(gate_result(record, self._run, summary=True))
        return self.summary

    def resolve(self) -> EligibilitySummary:
        """Read the gate now; the batch that carried it is held for iteration."""
        if self.summary is not None:
            return self.summary
        for batch in self._reader:
            if batch.num_rows == 0:
                continue
            # Raw batches carry the engine's aggregate types (a wide integer
            # for a DuckDB sum); the statement's declared schema is int64.
            gate = batch.slice(0, 1).select(self._gate.names)
            if gate.schema != self._gate:
                gate = gate.cast(self._gate)
            summary = self._resolve(
                {name: gate.column(name)[0].as_py() for name in self._gate.names}
            )
            self._held = batch
            return summary
        self._exhausted = True
        return self._resolve(gate_record(self._resolve_empty()))

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        self.resolve()
        if self._held is not None:
            held, self._held = self._held, None
            yield held.select(self._columns)
        if not self._exhausted:
            for batch in self._reader:
                yield batch.select(self._columns)
            self._exhausted = True

    def close(self) -> None:
        self._reader.close()
