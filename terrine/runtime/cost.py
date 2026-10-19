"""Metadata-only scan evidence and owner authorization for runtime plans."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from terrine.compile.cost import _SCAN_WARNING, _scan_policy
from terrine.errors import RefusalSpec

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from terrine.data import Budget
    from terrine.runtime.backends.base import CostProvider
    from terrine.runtime.planner import RefreshPlan, TaskSpec

__all__ = ["ScanAuthorization", "ScanEvidence", "ScanPhase"]

_PHASES = ("capture", "validation", "execution", "retry")
_BOUND_STATUSES = {"verified_upper_bound", "backend_enforced_cap"}
_MISMATCH = RefusalSpec(
    "terrine.runtime.cost_estimate_mismatch",
    "Cost evidence does not match the task, definition or snapshot; "
    "replan with the owner's metadata provider.",
)
_AUTHORITY = RefusalSpec(
    "terrine.budget.scan_authority_unavailable",
    "Strict scan execution requires owner authorization of the saved all-phase bounds or caps; "
    "configure a capable owner executor or explicitly choose non-strict scan policy.",
)
_ATTEMPTS = RefusalSpec(
    "terrine.budget.scan_attempts_exhausted",
    "Task scan allowance exhausted after {limit} attempts; "
    "create a newly budgeted run before reading sources again.",
)


@dataclass(frozen=True, slots=True)
class ScanPhase:
    phase: Literal["capture", "validation", "execution", "retry"]
    status: Literal["unknown", "forecast", "verified_upper_bound", "backend_enforced_cap"]
    scan_bytes: int | None
    basis: str

    def __post_init__(self) -> None:
        if self.phase not in _PHASES or self.status not in {
            *_BOUND_STATUSES,
            "unknown",
            "forecast",
        }:
            raise ValueError("unsupported scan phase or evidence status")
        if not isinstance(self.basis, str) or not self.basis.strip():
            raise ValueError("scan evidence requires a nonempty basis")
        if self.status == "unknown":
            if self.scan_bytes is not None:
                raise ValueError("unknown scan bytes must be None")
        elif type(self.scan_bytes) is not int or self.scan_bytes < 0:
            raise ValueError("known scan bytes must be a nonnegative integer")

    def to_dict(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "status": self.status,
            "scan_bytes": self.scan_bytes,
            "basis": self.basis,
        }


@dataclass(frozen=True, slots=True)
class ScanEvidence:
    """Per-task read work; retry bytes cover all additional allowed attempts together."""

    definition_hash: str
    snapshot_id: str
    max_attempts: int | None
    phases: tuple[ScanPhase, ...]

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, str) and value for value in (self.definition_hash, self.snapshot_id)
        ):
            raise ValueError("scan evidence requires definition and snapshot identities")
        if self.max_attempts is not None and (
            type(self.max_attempts) is not int or not 0 < self.max_attempts < 2**63
        ):
            raise ValueError("max_attempts must be a positive Int64 or None")
        if not isinstance(self.phases, tuple) or not all(
            isinstance(item, ScanPhase) for item in self.phases
        ):
            raise TypeError("scan phases must be an immutable tuple of ScanPhase")
        names = tuple(item.phase for item in self.phases)
        if names != tuple(name for name in _PHASES if name in names):
            raise ValueError("scan phases must be distinct and in canonical phase order")

    @classmethod
    def unknown(cls, definition_hash: str, snapshot_id: str) -> ScanEvidence:
        return cls(
            definition_hash,
            snapshot_id,
            None,
            tuple(
                ScanPhase(phase, "unknown", None, "backend_scan_bound_unavailable")
                for phase in _PHASES
            ),
        )

    def total(self, *, verified: bool = False) -> int | None:
        if tuple(item.phase for item in self.phases) != _PHASES:
            return None
        if verified and self.max_attempts is None:
            return None
        total = 0
        for item in self.phases:
            if item.scan_bytes is None or (verified and item.status not in _BOUND_STATUSES):
                return None
            total += item.scan_bytes
        return total

    def to_dict(self) -> dict[str, Any]:
        return {
            "definition_hash": self.definition_hash,
            "snapshot_id": self.snapshot_id,
            "max_attempts": self.max_attempts,
            "phases": [item.to_dict() for item in self.phases],
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ScanEvidence:
        return cls(
            value["definition_hash"],
            value["snapshot_id"],
            value["max_attempts"],
            tuple(ScanPhase(**item) for item in value["phases"]),
        )


@dataclass(frozen=True, slots=True)
class ScanAuthorization:
    """Owner attestation that the exact plan's read bounds/caps are enforceable."""

    fingerprint: str


def estimate_task_cost(
    task: TaskSpec, *, provider: CostProvider | None, definition_hash: str, snapshot_id: str
) -> TaskSpec:
    if provider is None or provider.capabilities.scan_estimate is not True:
        return task.with_estimate(
            scan=ScanEvidence.unknown(definition_hash, snapshot_id),
            basis="backend_scan_bound_unavailable",
        )
    estimated = provider.estimate_task(
        task, definition_hash=definition_hash, snapshot_id=snapshot_id
    )
    if type(estimated) is not type(task) or any(
        getattr(task, name) != getattr(estimated, name)
        for name in ("task_id", "layer", "source", "partition", "dependencies")
    ):
        raise _MISMATCH.error()
    _check_context(estimated, definition_hash, snapshot_id)
    return estimated


def _check_context(task: TaskSpec, definition_hash: str, snapshot_id: str) -> None:
    evidence = task.scan_evidence
    if evidence is not None and (
        evidence.definition_hash != definition_hash or evidence.snapshot_id != snapshot_id
    ):
        raise _MISMATCH.error()


def evaluate_costs(
    tasks: Sequence[TaskSpec], budget: Budget | None, *, definition_hash: str, snapshot_id: str
) -> tuple[int | None, tuple[str, ...]]:
    estimate: int | None = 0
    bound: int | None = 0
    for task in tasks:
        _check_context(task, definition_hash, snapshot_id)
        amount = task.estimated_scan_bytes
        estimate = None if estimate is None or amount is None else estimate + amount
        amount = task.scan_evidence.total(verified=True) if task.scan_evidence is not None else None
        bound = None if bound is None or amount is None else bound + amount
    warnings = _scan_policy(budget, scan_bytes=bound)
    if bound is None and not warnings:
        warnings = (_SCAN_WARNING,)
    return estimate, warnings


def authorize_plan_scan(
    plan: RefreshPlan, authorize: Callable[[RefreshPlan], ScanAuthorization] | None
) -> None:
    evaluate_costs(
        plan.tasks, plan.budget, definition_hash=plan.definition_hash, snapshot_id=plan.snapshot_id
    )
    from terrine.runtime.planner import RefreshPlan

    if RefreshPlan.deserialize(plan.serialize()) != plan:
        raise ValueError("runtime plan fields differ from its canonical recipe")
    if (
        not plan.tasks
        or plan.budget is None
        or plan.budget.max_scan_bytes is None
        or not plan.budget.strict_scan
    ):
        return
    if authorize is None:
        raise _AUTHORITY.error()
    grant = authorize(plan)
    if not isinstance(grant, ScanAuthorization) or grant.fingerprint != plan.recipe.fingerprint:
        raise _AUTHORITY.error()
