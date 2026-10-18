from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from terrine.compile.cost import _scan_policy
from terrine.data import Budget
from terrine.runtime.changes import ChangeSet
from terrine.runtime.graph import DependencyGraph
from terrine.runtime.state import LayerNode

__all__ = ["Invalidation", "PartitionSpec", "PlanRecipe", "RefreshPlan", "TaskSpec", "plan_refresh"]


@dataclass(frozen=True, slots=True)
class PartitionSpec:
    source: str
    start: datetime | None = None
    end_exclusive: datetime | None = None
    bucket: int | None = None
    bucket_count: int | None = None


@dataclass(frozen=True, slots=True)
class Invalidation:
    nodes: tuple[LayerNode, ...]
    partitions: tuple[PartitionSpec, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PlanRecipe:
    schema_version: int
    namespace: str
    dataset: str
    definition_hash: str
    snapshot_id: str
    tasks: tuple[dict[str, Any], ...]
    budget: dict[str, int | bool | None] | None
    fingerprint: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "schema_version": self.schema_version,
                "namespace": self.namespace,
                "dataset": self.dataset,
                "definition_hash": self.definition_hash,
                "snapshot_id": self.snapshot_id,
                "tasks": self.tasks,
                "budget": self.budget,
                "fingerprint": self.fingerprint,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True, slots=True)
class TaskSpec:
    task_id: str
    layer: str
    source: str
    partition: str
    dependencies: tuple[str, ...]
    estimated_scan_bytes: int | None = None
    estimated_output_bytes: int | None = None
    estimated_rows: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "layer": self.layer,
            "source": self.source,
            "partition": self.partition,
            "dependencies": list(self.dependencies),
            "estimated_scan_bytes": self.estimated_scan_bytes,
            "estimated_output_bytes": self.estimated_output_bytes,
            "estimated_rows": self.estimated_rows,
        }


@dataclass(frozen=True, slots=True)
class RefreshPlan:
    namespace: str
    dataset: str
    definition_hash: str
    snapshot_id: str
    tasks: tuple[TaskSpec, ...]
    recipe: PlanRecipe
    budget: Budget | None = None
    scan_bytes: int | None = None

    def serialize(self) -> bytes:
        return self.recipe.to_json().encode("utf-8")


def plan_refresh(
    *,
    namespace: str,
    dataset: str,
    definition_hash: str,
    snapshot_id: str,
    graph: DependencyGraph,
    changes: ChangeSet | None = None,
    budget: Budget | None = None,
    max_tasks: int = 4096,
) -> RefreshPlan:
    """Create a deterministic metadata-only plan with explicit unknown costs."""
    if not all((namespace, dataset, definition_hash, snapshot_id)):
        raise ValueError("namespace, dataset, definition_hash, and snapshot_id are required")
    if isinstance(max_tasks, bool) or max_tasks < 1:
        raise ValueError("max_tasks must be positive")
    ordered = graph.topological_order()
    if len(ordered) > max_tasks:
        raise ValueError("planned task count exceeds max_tasks")
    if budget is not None:
        if not isinstance(budget, Budget):
            raise TypeError("budget must be Budget or None")
        _scan_policy(budget, scan_bytes=None)
    source_revisions = {} if changes is None else {c.source: c.revision for c in changes.changes}
    tasks: list[TaskSpec] = []
    ids: dict[LayerNode, str] = {}
    for node in ordered:
        key = json.dumps(
            [node.layer, node.source, node.partition, node.definition_hash, node.generation],
            separators=(",", ":"),
        )
        task_id = hashlib.sha256(key.encode()).hexdigest()[:24]
        ids[node] = task_id
        dependencies = tuple(ids[parent] for parent in graph.dependencies(node))
        tasks.append(TaskSpec(task_id, node.layer, node.source, node.partition, dependencies))
    bodies = tuple(task.to_dict() for task in tasks)
    budget_record = (
        {
            "max_rows": budget.max_rows,
            "max_output_bytes": budget.max_output_bytes,
            "max_scan_bytes": budget.max_scan_bytes,
            "strict_scan": budget.strict_scan,
        }
        if budget is not None
        else None
    )
    fingerprint_payload = {
        "namespace": namespace,
        "dataset": dataset,
        "definition_hash": definition_hash,
        "snapshot_id": snapshot_id,
        "tasks": bodies,
        "revisions": source_revisions,
        "budget": budget_record,
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    recipe = PlanRecipe(
        1, namespace, dataset, definition_hash, snapshot_id, bodies, budget_record, fingerprint
    )
    return RefreshPlan(
        namespace, dataset, definition_hash, snapshot_id, tuple(tasks), recipe, budget
    )
