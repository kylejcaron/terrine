from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime
from typing import TYPE_CHECKING, Any

from terrine.data import Budget
from terrine.runtime.changes import ChangeSet
from terrine.runtime.cost import ScanEvidence, estimate_task_cost, evaluate_costs
from terrine.runtime.graph import DependencyGraph
from terrine.runtime.state import LayerNode

if TYPE_CHECKING:
    from terrine.runtime.backends.base import CostProvider

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
    change_evidence: dict[str, Any] | None
    fingerprint: str
    expected_base_manifest: str | None = None
    expected_base_manifest_bound: bool = False
    reconstruction_hash: str | None = None

    def to_json(self) -> str:
        payload = {
            "schema_version": self.schema_version,
            "namespace": self.namespace,
            "dataset": self.dataset,
            "definition_hash": self.definition_hash,
            "snapshot_id": self.snapshot_id,
            "tasks": self.tasks,
            "budget": self.budget,
            "change_evidence": self.change_evidence,
            "expected_base_manifest": self.expected_base_manifest,
            "expected_base_manifest_bound": self.expected_base_manifest_bound,
            "fingerprint": self.fingerprint,
        }
        if self.reconstruction_hash is not None:
            payload["reconstruction_hash"] = self.reconstruction_hash
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))


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
    scan_evidence: ScanEvidence | None = None
    estimate_basis: str = "estimate_unavailable"

    def __post_init__(self) -> None:
        for value in (self.estimated_scan_bytes, self.estimated_output_bytes, self.estimated_rows):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("cost estimates must be nonnegative integers or None")
        if not isinstance(self.estimate_basis, str) or not self.estimate_basis.strip():
            raise ValueError("cost estimates require a nonempty basis")
        if self.scan_evidence is not None and (
            not isinstance(self.scan_evidence, ScanEvidence)
            or self.estimated_scan_bytes != self.scan_evidence.total()
        ):
            raise ValueError("scan estimate must agree with its phase evidence")

    def with_estimate(
        self,
        *,
        scan: ScanEvidence,
        output_bytes: int | None = None,
        rows: int | None = None,
        basis: str,
    ) -> TaskSpec:
        """Attach metadata forecasts without promoting them to execution bounds."""
        return replace(
            self,
            estimated_scan_bytes=scan.total(),
            estimated_output_bytes=output_bytes,
            estimated_rows=rows,
            scan_evidence=scan,
            estimate_basis=basis,
        )

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
            "scan_evidence": self.scan_evidence.to_dict()
            if self.scan_evidence is not None
            else None,
            "estimate_basis": self.estimate_basis,
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
    expected_base_manifest: str | None = None
    expected_base_manifest_bound: bool = False
    budget_warnings: tuple[str, ...] = ()

    @classmethod
    def deserialize(cls, serialized: bytes | str) -> RefreshPlan:
        """Reconstruct a plan only after verifying its versioned fingerprint."""
        try:
            data = json.loads(serialized)
            schema_version = data["schema_version"]
            namespace = data["namespace"]
            dataset = data["dataset"]
            definition_hash = data["definition_hash"]
            snapshot_id = data["snapshot_id"]
            raw_tasks = data["tasks"]
            budget_record = data["budget"]
            change_evidence = data.get("change_evidence")
            expected_base_manifest = data.get("expected_base_manifest")
            expected_base_manifest_bound = data.get("expected_base_manifest_bound", False)
            reconstruction_hash = data.get("reconstruction_hash")
            fingerprint = data["fingerprint"]
            if (
                type(schema_version) is not int
                or schema_version != 2
                or not isinstance(raw_tasks, list)
            ):
                raise ValueError("unsupported serialized plan")
            tasks = tuple(
                TaskSpec(
                    item["task_id"],
                    item["layer"],
                    item["source"],
                    item["partition"],
                    tuple(item["dependencies"]),
                    item["estimated_scan_bytes"],
                    item["estimated_output_bytes"],
                    item["estimated_rows"],
                    ScanEvidence.from_dict(item["scan_evidence"])
                    if item["scan_evidence"] is not None
                    else None,
                    item["estimate_basis"],
                )
                for item in raw_tasks
            )
            bodies = tuple(task.to_dict() for task in tasks)
            if bodies != tuple(raw_tasks):
                raise ValueError("serialized plan task fields are not canonical")
            budget = Budget(**budget_record) if budget_record is not None else None
            payload = {
                "schema_version": schema_version,
                "namespace": namespace,
                "dataset": dataset,
                "definition_hash": definition_hash,
                "snapshot_id": snapshot_id,
                "tasks": bodies,
                "change_evidence": change_evidence,
                "budget": budget_record,
                "expected_base_manifest": expected_base_manifest,
                "expected_base_manifest_bound": expected_base_manifest_bound,
            }
            if reconstruction_hash is not None:
                if not isinstance(reconstruction_hash, str) or len(reconstruction_hash) != 64:
                    raise ValueError("serialized reconstruction binding is invalid")
                payload["reconstruction_hash"] = reconstruction_hash
            expected = hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            if not isinstance(fingerprint, str) or fingerprint != expected:
                raise ValueError("serialized plan fingerprint does not match")
        except (ValueError, TypeError, KeyError) as exc:
            raise ValueError("serialized plan is malformed or unverifiable") from exc
        scan_bytes, warnings = evaluate_costs(
            tasks, budget, definition_hash=definition_hash, snapshot_id=snapshot_id
        )
        if not isinstance(expected_base_manifest_bound, bool) or (
            expected_base_manifest is not None and not isinstance(expected_base_manifest, str)
        ):
            raise ValueError("serialized base-manifest binding is invalid")
        plan_recipe = PlanRecipe(
            schema_version,
            namespace,
            dataset,
            definition_hash,
            snapshot_id,
            bodies,
            budget_record,
            change_evidence,
            fingerprint,
            expected_base_manifest,
            expected_base_manifest_bound,
            reconstruction_hash,
        )
        return cls(
            namespace,
            dataset,
            definition_hash,
            snapshot_id,
            tasks,
            plan_recipe,
            budget,
            scan_bytes=scan_bytes,
            expected_base_manifest=expected_base_manifest,
            expected_base_manifest_bound=expected_base_manifest_bound,
            budget_warnings=warnings,
        )

    def serialize(self) -> bytes:
        return self.recipe.to_json().encode("utf-8")

    def bind_base_manifest(
        self, generation: str | None, *, reconstruction_hash: str | None = None
    ) -> RefreshPlan:
        """Bind publication preconditions into the canonical plan fingerprint."""
        if generation is not None and (not isinstance(generation, str) or not generation):
            raise ValueError("expected base manifest must be a nonempty generation or None")
        if RefreshPlan.deserialize(self.serialize()) != self:
            raise ValueError("runtime plan fields differ from its canonical recipe")
        recipe = replace(
            self.recipe,
            expected_base_manifest=generation,
            expected_base_manifest_bound=True,
            reconstruction_hash=(
                self.recipe.reconstruction_hash
                if reconstruction_hash is None
                else reconstruction_hash
            ),
        )
        payload = json.loads(recipe.to_json())
        del payload["fingerprint"]
        fingerprint = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return replace(
            self,
            recipe=replace(recipe, fingerprint=fingerprint),
            expected_base_manifest=generation,
            expected_base_manifest_bound=True,
        )


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
    cost_provider: CostProvider | None = None,
) -> RefreshPlan:
    """Create a deterministic metadata-only plan with explicit unknown costs."""
    if not all((namespace, dataset, definition_hash, snapshot_id)):
        raise ValueError("namespace, dataset, definition_hash, and snapshot_id are required")
    if isinstance(max_tasks, bool) or max_tasks < 1:
        raise ValueError("max_tasks must be positive")
    ordered = graph.topological_order()
    has_state_panel = any(node.layer in {"state_point", "state_period"} for node in ordered)
    if changes is not None and changes.evidence != "conservative_rebuild" and not has_state_panel:
        if any(change.kind == "definition" for change in changes.changes):
            pass
        elif not changes.changes:
            ordered = ()
        else:
            changed_sources = {change.source for change in changes.changes}
            readiness_only = all(change.kind == "readiness" for change in changes.changes)
            auxiliary_only = all(
                change.kind in {"readiness", "roster"} for change in changes.changes
            )
            selected = {
                node
                for node in ordered
                if node.source in changed_sources
                or (readiness_only and node.layer == "frame")
                or (auxiliary_only and node.layer == "frame")
            }
            if any(source not in {node.source for node in ordered} for source in changed_sources):
                selected.update(node for node in ordered if node.layer == "frame")
            pending = list(selected)
            while pending:
                node = pending.pop()
                for parent, child in graph.edges:
                    if parent == node and child not in selected:
                        selected.add(child)
                        pending.append(child)
            pending = list(selected)
            while pending:
                node = pending.pop()
                for parent in graph.dependencies(node):
                    if parent not in selected:
                        selected.add(parent)
                        pending.append(parent)
            ordered = tuple(node for node in ordered if node in selected)
    if len(ordered) > max_tasks:
        raise ValueError("planned task count exceeds max_tasks")
    if budget is not None and not isinstance(budget, Budget):
        raise TypeError("budget must be Budget or None")
    change_evidence = (
        None
        if changes is None
        else {
            "evidence": changes.evidence,
            "changes": sorted(
                (_change_dict(change) for change in changes.changes),
                key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
            ),
        }
    )
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
        tasks.append(
            estimate_task_cost(
                TaskSpec(task_id, node.layer, node.source, node.partition, dependencies),
                provider=cost_provider,
                definition_hash=definition_hash,
                snapshot_id=snapshot_id,
            )
        )
    bodies = tuple(task.to_dict() for task in tasks)
    scan_bytes, warnings = evaluate_costs(
        tasks, budget, definition_hash=definition_hash, snapshot_id=snapshot_id
    )
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
        "schema_version": 2,
        "namespace": namespace,
        "dataset": dataset,
        "definition_hash": definition_hash,
        "snapshot_id": snapshot_id,
        "tasks": bodies,
        "change_evidence": change_evidence,
        "budget": budget_record,
        "expected_base_manifest": None,
        "expected_base_manifest_bound": False,
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    recipe = PlanRecipe(
        2,
        namespace,
        dataset,
        definition_hash,
        snapshot_id,
        bodies,
        budget_record,
        change_evidence,
        fingerprint,
    )
    return RefreshPlan(
        namespace,
        dataset,
        definition_hash,
        snapshot_id,
        tuple(tasks),
        recipe,
        budget,
        scan_bytes=scan_bytes,
        budget_warnings=warnings,
    )


def _change_dict(change: Any) -> dict[str, Any]:
    def time_range(value: tuple[datetime, datetime] | None) -> list[str] | None:
        return [item.isoformat() for item in value] if value is not None else None

    return {
        "kind": change.kind,
        "source": change.source,
        "before_key_range": list(change.before_key_range) if change.before_key_range else None,
        "after_key_range": list(change.after_key_range) if change.after_key_range else None,
        "before_time_range": time_range(change.before_time_range),
        "after_time_range": time_range(change.after_time_range),
        "revision": change.revision,
        "tombstone": change.tombstone,
    }
