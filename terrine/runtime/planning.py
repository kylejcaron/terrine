from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from terrine.data import Budget
from terrine.errors import RefusalSpec, TerrineError
from terrine.execution.gates import enforce_shape_budget, validate_compiled_output
from terrine.model import Model
from terrine.runtime.changes import Change, ChangeSet
from terrine.runtime.graph import dependency_graph
from terrine.runtime.materialize import ensure_runtime_expressions_supported
from terrine.runtime.planner import RefreshPlan, plan_refresh
from terrine.runtime.saved_plan import (
    DestinationRef,
    ModelReference,
    ModelResolver,
    SavedPlan,
)
from terrine.runtime.snapshot import SnapshotAuthority, _design_root_names
from terrine.runtime.state import RuntimeStore
from terrine.spec.dataset import INVALID_DEFINITION, DatasetDefinition
from terrine.spec.design import Design
from terrine.spec.io import from_json, to_json
from terrine.spec.validate import ensure_runtime_shape_supported

__all__ = ["PlanRequest", "PlanInvocation", "OwnerPlanner"]

_INVALID_REQUEST = RefusalSpec(
    "terrine.runtime.plan_request_invalid", "Owner plan request is invalid ({reason})."
)
_INVOCATION_CONFLICT = RefusalSpec(
    "terrine.runtime.plan_invocation_conflict",
    "Invocation ID is already bound to a different planning request.",
)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class PlanRequest:
    invocation_id: str
    model: ModelReference
    design: Design
    destination: DestinationRef
    changes: ChangeSet | None = None
    budget: Budget | None = None
    requested_scope: str | None = None
    schema_version: int = 1
    dataset_definition: DatasetDefinition | None = None
    expected_base_manifest: str | None = None
    bind_base_manifest: bool = False

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported plan request version")
        if not isinstance(self.invocation_id, str) or not re.fullmatch(
            r"[A-Za-z0-9_.-]+", self.invocation_id
        ):
            raise ValueError("invocation_id must be a non-empty safe identifier")
        if not isinstance(self.model, ModelReference) or not isinstance(
            self.destination, DestinationRef
        ):
            raise TypeError("model and destination must be verified references")
        if not isinstance(self.design, Design):
            raise TypeError("design must be Design")
        if self.changes is not None and not isinstance(self.changes, ChangeSet):
            raise TypeError("changes must be ChangeSet or None")
        if self.budget is not None and not isinstance(self.budget, Budget):
            raise TypeError("budget must be Budget or None")
        if self.requested_scope is not None and not self.requested_scope.strip():
            raise ValueError("requested_scope must be non-empty when supplied")
        if self.dataset_definition is not None and (
            not isinstance(self.dataset_definition, DatasetDefinition)
            or self.dataset_definition.base_design.spec_hash != self.design.spec_hash
        ):
            raise ValueError("dataset_definition must match the base Design")
        if not isinstance(self.bind_base_manifest, bool) or (
            self.expected_base_manifest is not None
            and not isinstance(self.expected_base_manifest, str)
        ):
            raise ValueError("invalid expected base manifest")

    def to_dict(self) -> dict[str, Any]:
        changes = None
        if self.changes is not None:
            changes = {
                "evidence": self.changes.evidence,
                "changes": [
                    {
                        **asdict(change),
                        "before_time_range": _range_json(change.before_time_range),
                        "after_time_range": _range_json(change.after_time_range),
                    }
                    for change in self.changes.changes
                ],
            }
        return {
            "schema_version": self.schema_version,
            "invocation_id": self.invocation_id,
            "model": self.model.to_dict(),
            "design": to_json(self.design),
            "destination": self.destination.to_dict(),
            "changes": changes,
            "budget": asdict(self.budget) if self.budget is not None else None,
            "requested_scope": self.requested_scope,
            "dataset_definition": (
                self.dataset_definition.canonical_json().decode()
                if self.dataset_definition is not None
                else None
            ),
            "expected_base_manifest": self.expected_base_manifest,
            "bind_base_manifest": self.bind_base_manifest,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PlanRequest:
        try:
            if value["schema_version"] != 1:
                raise ValueError("unsupported request version")
            changes_data = value["changes"]
            changes = None
            if changes_data is not None:
                change_items = []
                for raw in changes_data["changes"]:
                    item = dict(raw)
                    for field in ("before_time_range", "after_time_range"):
                        if item[field] is not None:
                            item[field] = tuple(
                                datetime.fromisoformat(part) for part in item[field]
                            )
                    for field in ("before_key_range", "after_key_range"):
                        if item[field] is not None:
                            item[field] = tuple(item[field])
                    change_items.append(Change(**item))
                changes = ChangeSet(tuple(change_items), changes_data["evidence"])
            budget = Budget(**value["budget"]) if value["budget"] is not None else None
            return cls(
                value["invocation_id"],
                ModelReference(**value["model"]),
                from_json(value["design"]),
                DestinationRef(**value["destination"]),
                changes,
                budget,
                value["requested_scope"],
                value["schema_version"],
                DatasetDefinition.from_json(value["dataset_definition"])
                if value.get("dataset_definition") is not None
                else None,
                value.get("expected_base_manifest"),
                value.get("bind_base_manifest", False),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise _INVALID_REQUEST.error(reason="malformed or unsupported schema") from error


@dataclass(frozen=True, slots=True)
class PlanInvocation:
    invocation_id: str
    serialized_plan: str
    request_key: str
    destination: DestinationRef
    snapshot_id: str
    expected_base_manifest: str | None
    saved_plan_json: str = ""
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.schema_version != 1 or not self.invocation_id or not self.request_key:
            raise ValueError("invalid plan invocation")
        plan = RefreshPlan.deserialize(self.serialized_plan)
        if plan.dataset != self.destination.dataset or plan.snapshot_id != self.snapshot_id:
            raise ValueError("plan invocation identity does not match its plan")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "invocation_id": self.invocation_id,
            "serialized_plan": self.serialized_plan,
            "request_key": self.request_key,
            "destination": self.destination.to_dict(),
            "snapshot_id": self.snapshot_id,
            "expected_base_manifest": self.expected_base_manifest,
            "saved_plan_json": self.saved_plan_json,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PlanInvocation:
        try:
            return cls(
                value["invocation_id"],
                value["serialized_plan"],
                value["request_key"],
                DestinationRef(**value["destination"]),
                value["snapshot_id"],
                value["expected_base_manifest"],
                value.get("saved_plan_json", ""),
                value["schema_version"],
            )
        except (KeyError, TypeError, ValueError) as error:
            raise _INVALID_REQUEST.error(reason="malformed invocation receipt") from error


class OwnerPlanner:
    """Resolve, preflight and durably pin a fresh plan on the owner's connection."""

    def __init__(self, *, resolver: ModelResolver, snapshot_authority: SnapshotAuthority) -> None:
        self.resolver = resolver
        self.snapshot_authority = snapshot_authority
        self._lock = threading.RLock()

    def plan(self, request: PlanRequest, *, con: Any, namespace: str) -> PlanInvocation:
        with self._lock:
            return self._plan(request, con=con, namespace=namespace)

    def _plan(self, request: PlanRequest, *, con: Any, namespace: str) -> PlanInvocation:
        store = RuntimeStore(con, namespace=namespace)
        store.initialize()
        _initialize_invocations(con, store._schema)
        request_json = _canonical(request.to_dict())
        request_hash = hashlib.sha256(request_json.encode()).hexdigest()
        prior = con.execute(
            f"SELECT request_hash, invocation_json FROM {store._schema}.runtime_plan_invocations "
            "WHERE invocation_id=?",
            [request.invocation_id],
        ).fetchone()
        if prior is not None:
            if prior[0] != request_hash:
                raise _INVOCATION_CONFLICT.error()
            return PlanInvocation.from_dict(json.loads(prior[1]))

        manifest_table = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE lower(table_schema)=lower(?) AND lower(table_name)='runtime_manifests'",
            [namespace],
        ).fetchone()
        expected = (
            con.execute(
                f"SELECT generation FROM {store._schema}.runtime_manifests WHERE dataset=?",
                [request.destination.dataset],
            ).fetchone()
            if manifest_table is not None
            else None
        )
        expected_manifest = expected[0] if expected is not None else None
        if request.bind_base_manifest and expected_manifest != request.expected_base_manifest:
            raise _INVALID_REQUEST.error(reason="pinned base generation is no longer current")
        ensure_runtime_shape_supported(request.design.shape)
        ensure_runtime_expressions_supported(request.design)
        model = self.resolver.resolve(request.model)
        if (
            not isinstance(model, Model)
            or ModelReference.from_model(model, owner_key=request.model.owner_key) != request.model
        ):
            raise _INVALID_REQUEST.error(reason="owner Model reference does not match")
        from terrine.compile.cost import _scan_policy
        from terrine.compile.dispatch import compile_design

        budget = request.budget or Budget()
        _scan_policy(request.budget)
        enforce_shape_budget(request.design.shape, budget)
        design = request.design
        if request.dataset_definition is not None:
            try:
                frame = request.dataset_definition.build(model)
            except TerrineError:
                raise
            except (KeyError, TypeError, ValueError, OverflowError) as error:
                raise INVALID_DEFINITION.error(
                    reason="dataset view cannot be constructed"
                ) from error
            design, compiled = frame._design, frame._active_compiled()
        else:
            compiled = compile_design(design, model)
        source_names, _, _ = _design_root_names(model, design)
        graph = dependency_graph(
            model,
            design,
            sources=source_names,
            definition_hash=(
                request.design.spec_hash
                if request.dataset_definition is None
                else hashlib.sha256(
                    (request.dataset_definition.digest + model.model_hash).encode()
                ).hexdigest()
            ),
            partitions=(request.requested_scope or "all",),
            expression_timings=compiled.expression_timings,
        )
        snapshot = self.snapshot_authority.capture(
            model,
            design=design,
            con=con,
            namespace=namespace,
            run_id=request.invocation_id,
            budget=budget,
        )
        context = self.snapshot_authority.reopen(snapshot, con=con, namespace=namespace)
        try:
            execution_model = model._for_execution(
                context,
                source_names=set(source_names),
                unit_name=design.unit,
            )
            snapshot_compiled = (
                compile_design(design, execution_model)
                if request.dataset_definition is None
                else request.dataset_definition.build(execution_model)._active_compiled()
            )
            validate_compiled_output(snapshot_compiled, execution_model.con.to_pyarrow)
        finally:
            context.close()
        plan = plan_refresh(
            namespace=namespace,
            dataset=request.destination.dataset,
            definition_hash=request.design.spec_hash,
            snapshot_id=snapshot.snapshot_id,
            graph=graph,
            changes=request.changes,
            budget=request.budget,
        )
        saved_plan = SavedPlan.create(
            plan_json=plan.serialize(),
            design=request.design,
            model=request.model,
            destination=request.destination,
            namespace=namespace,
            expected_base_manifest=expected_manifest,
            dataset_definition=request.dataset_definition,
        )
        request_key = hashlib.sha256(
            _canonical([namespace, request.invocation_id, request.destination.to_dict()]).encode()
        ).hexdigest()
        invocation = PlanInvocation(
            request.invocation_id,
            plan.serialize().decode("utf-8"),
            request_key,
            request.destination,
            snapshot.snapshot_id,
            expected_manifest,
            saved_plan.to_json(),
        )
        con.execute("BEGIN TRANSACTION")
        try:
            saved_plan.pin_snapshot(store, reference=request.invocation_id, _in_transaction=True)
            con.execute(
                f"INSERT INTO {store._schema}.runtime_plan_invocations VALUES (?, ?, ?, ?)",
                [
                    request.invocation_id,
                    request_hash,
                    invocation.snapshot_id,
                    _canonical(invocation.to_dict()),
                ],
            )
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
        return invocation


def _range_json(value: tuple[datetime, datetime] | None) -> list[str] | None:
    return [item.isoformat() for item in value] if value is not None else None


def _initialize_invocations(con: Any, schema: str) -> None:
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_plan_invocations ("
        "invocation_id VARCHAR PRIMARY KEY, request_hash VARCHAR NOT NULL, "
        "snapshot_id VARCHAR NOT NULL, invocation_json VARCHAR NOT NULL)"
    )
