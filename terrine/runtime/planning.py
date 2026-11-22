from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from terrine.compile.cost import _scan_policy
from terrine.compile.provenance import EXECUTION_REVISION
from terrine.data import _OUTPUT_BYTES_EXCEEDED, _ROWS_EXCEEDED, Budget
from terrine.errors import RefusalSpec, TerrineError
from terrine.execution.backend import backend_provider
from terrine.execution.gates import enforce_shape_budget, validate_compiled_output
from terrine.model import Model
from terrine.runtime.changes import Change, ChangeSet
from terrine.runtime.graph import DependencyGraph, dependency_graph
from terrine.runtime.materialize import ensure_runtime_expressions_supported
from terrine.runtime.planner import ExecutionMode, RefreshPlan, _plan_refresh
from terrine.runtime.saved_plan import _INVALID as _INVALID_BASE
from terrine.runtime.saved_plan import (
    DestinationRef,
    ModelReference,
    ModelResolver,
    SavedPlan,
    _bind_saved_plan,
)
from terrine.runtime.snapshot import (
    ReopenableSnapshot,
    SnapshotAuthority,
    _assert_owner_connection,
    _design_root_names,
)
from terrine.runtime.state import RuntimeStore
from terrine.spec.dataset import INVALID_DEFINITION, DatasetDefinition
from terrine.spec.design import Design
from terrine.spec.io import from_json, to_json
from terrine.spec.state_panel import StatePanel
from terrine.spec.validate import ensure_runtime_shape_supported, resolve_unit

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
            optional = {"dataset_definition", "expected_base_manifest", "bind_base_manifest"}
            required = {
                "schema_version",
                "invocation_id",
                "model",
                "design",
                "destination",
                "changes",
                "budget",
                "requested_scope",
            }
            if (
                not isinstance(value, dict)
                or set(value) - optional != required
                or type(value["schema_version"]) is not int
                or value["schema_version"] != 1
            ):
                raise _INVALID_REQUEST.error(reason="unsupported_request_version")
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
        except TerrineError:
            raise
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
        except TerrineError:
            raise
        except (KeyError, TypeError, ValueError) as error:
            raise _INVALID_REQUEST.error(reason="malformed invocation receipt") from error


def _claims_no_work(changes: ChangeSet | None, design: Design) -> bool:
    # Legacy revision, checksum and feed labels do not carry a verified input identity.
    return (
        changes is not None
        and changes.evidence == "operator"
        and not changes.changes
        and not isinstance(design.shape, StatePanel)
    )


def _plan_no_work(
    store: RuntimeStore,
    authority: SnapshotAuthority,
    *,
    model: Model,
    design: Design,
    definition: DatasetDefinition | None,
    dataset: str,
    generation: str | None,
    changes: ChangeSet | None,
    budget: Budget | None,
    scope: str = "all",
    destination: DestinationRef | None = None,
    model_reference: ModelReference | None = None,
    execution_mode: ExecutionMode = "design",
) -> RefreshPlan | None:
    """Reuse only a committed recipe and retained snapshot on the asserted base."""
    if generation is None or not _claims_no_work(changes, design):
        return None
    con, schema, namespace = store.con, store._schema, store.namespace
    record = con.execute(
        f"SELECT d.recipe_json, d.manifest_json, r.fingerprint, r.destination_json, "
        f"r.run_id, m.snapshot_id FROM {schema}.runtime_manifests m "
        f"JOIN {schema}.runtime_snapshot_manifests s USING(dataset, generation, snapshot_id) "
        f"JOIN {schema}.runtime_dataset_recipes d USING(dataset, generation) "
        f"JOIN {schema}.runtime_runs r ON r.request_key=d.request_key "
        "AND r.dataset=d.dataset AND r.published_generation=d.generation "
        "AND r.snapshot_id=m.snapshot_id AND r.state='succeeded' "
        f"JOIN {schema}.runtime_run_publications p ON p.run_id=r.run_id "
        "AND p.dataset=m.dataset AND p.generation=m.generation AND p.version=m.version "
        "WHERE m.dataset=? AND m.generation=?",
        [dataset, generation],
    ).fetchone()
    if record is None:
        return None
    recipe_json, manifest_json, fingerprint, destination_json, run_id, snapshot_id = record
    recipe = SavedPlan.from_json(recipe_json)
    if recipe.schema_version != 2:
        return None
    if namespace.lower() != recipe.namespace.lower():
        raise _INVALID_BASE.error(reason="committed refresh recipe differs from publication")
    base_plan = _bind_saved_plan(recipe, namespace=recipe.namespace)
    if (
        base_plan.definition.fingerprint != fingerprint
        or destination_json != _canonical(recipe.destination.to_dict())
        or base_plan.dataset != dataset
        or base_plan.snapshot_id != snapshot_id
    ):
        raise _INVALID_BASE.error(reason="committed refresh recipe differs from publication")
    if (
        base_plan.definition.execution_revision != EXECUTION_REVISION
        or (destination is not None and destination != recipe.destination)
        or Path(recipe.destination.database).resolve() != authority.owner.state_path
        or recipe.design_json != to_json(design)
        or recipe.dataset_definition_json
        != (definition.canonical_json().decode() if definition is not None else None)
        or recipe.model
        != (model_reference or ModelReference.from_model(model, owner_key=recipe.model.owner_key))
        or {task.partition for task in base_plan.tasks if task.layer == "frame"} != {scope}
    ):
        return None

    from terrine.runtime.backends.duckdb import DuckDBBackend

    try:
        revision = DuckDBBackend(con, namespace=namespace).generation_execution_revision(
            dataset, generation
        )
    except TerrineError:
        return None
    if revision != EXECUTION_REVISION:
        return None

    source_identity = _reuse_source_identity(store, manifest_json, base_plan, generation)
    if source_identity is None:
        return None
    receipt_hash, owner_reference, rows, output_partitions = source_identity
    if owner_reference != authority.owner.reference:
        return None
    backend_provider(model.con, engine=model._engine).capabilities.require(
        "retained_snapshot",
        backend=model.catalog.dialect,
        route="Materialize a Frame without saving it, or use verified retained local inputs.",
    )
    _assert_owner_connection(authority.owner, con)
    if model.con.con is not con:
        _assert_owner_connection(authority.owner, model.con)
    if store.snapshot_marked_for_gc(snapshot_id):
        return None
    try:
        receipt = authority.find_retained(snapshot_id, namespace=recipe.namespace)
    except TerrineError:
        # No valid retained receipt was found for this owner and snapshot.
        return None
    if receipt.receipt_hash != receipt_hash or receipt.model_hash != recipe.model.model_hash:
        raise _INVALID_BASE.error(reason="retained refresh receipt differs from publication")
    if not Path(receipt.artifact_path).is_file():
        return None
    if not _admit_reused_output(
        store, run_id=run_id, rows=rows, partitions=output_partitions, budget=budget
    ):
        return None
    return _plan_refresh(
        namespace=namespace,
        dataset=dataset,
        definition_hash=design.spec_hash,
        snapshot_id=snapshot_id,
        graph=DependencyGraph((), ()),
        changes=changes,
        budget=budget,
        reuse_base_generation=generation,
        reuse_base_revision=revision,
        execution_mode=execution_mode,
    )


def _reuse_source_identity(
    store: RuntimeStore, manifest_json: str, base_plan: RefreshPlan, generation: str
) -> tuple[str, str, int, tuple[str, ...]] | None:
    # Only the committed generation and snapshot identity fields are needed here.
    try:
        manifest = json.loads(manifest_json)
        published = manifest["generation"]
        source = manifest["source_snapshots"]
        rows = published["row_count"]
        if (
            published["dataset_id"] != base_plan.dataset
            or published["generation_id"] != generation
            or source["snapshot_id"] != base_plan.snapshot_id
            or type(rows) is not int
            or rows < 0
        ):
            raise ValueError
        partitions = dict(zip(published["output_refs"], published["checksums"], strict=True))
        receipt, owner = source.get("receipt_hash"), source.get("owner")
    except (KeyError, TypeError, ValueError) as error:
        raise _INVALID_BASE.error(reason="malformed committed refresh identity") from error
    if receipt is None or owner is None:
        return None
    if not isinstance(receipt, str) or not isinstance(owner, str):
        raise _INVALID_BASE.error(reason="malformed retained refresh identity")
    stored = dict(
        store.con.execute(
            f"SELECT partition,digest FROM {store._schema}.runtime_partitions "
            "WHERE dataset=? AND generation=?",
            [base_plan.dataset, generation],
        ).fetchall()
    )
    task_ids = {task.task_id for task in base_plan.tasks}
    dependencies = {key for task in base_plan.tasks for key in task.dependencies}
    if (
        stored.keys() != task_ids
        or partitions.keys() != task_ids - dependencies
        or any(stored[key] != digest for key, digest in partitions.items())
    ):
        return None
    return receipt, owner, rows, tuple(partitions)


def _admit_reused_output(
    store: RuntimeStore,
    *,
    run_id: str,
    rows: int,
    partitions: tuple[str, ...],
    budget: Budget | None,
) -> bool:
    if budget is None:
        return True
    if budget.max_rows is not None and rows > budget.max_rows:
        raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
    if budget.max_output_bytes is None:
        return True
    recorded, size = store.con.execute(
        f"SELECT count(output_bytes),sum(output_bytes) FROM {store._schema}.runtime_tasks "
        "WHERE run_id=? AND status='complete' AND task_id IN (SELECT unnest(?))",
        [run_id, list(partitions)],
    ).fetchone()
    if not partitions or recorded != len(partitions):
        return False
    if size > budget.max_output_bytes:
        raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
    return True


class OwnerPlanner:
    """Preflight and pin physical Design jobs, or logical tasks for a custom executor."""

    def __init__(
        self,
        *,
        resolver: ModelResolver,
        snapshot_authority: SnapshotAuthority,
        execution_mode: ExecutionMode = "design",
    ) -> None:
        if execution_mode not in ("design", "custom"):
            raise _INVALID_REQUEST.error(reason="unsupported execution mode")
        self._execution_mode = execution_mode
        self.resolver = resolver
        self.snapshot_authority = snapshot_authority
        self._lock = threading.RLock()

    def plan(
        self,
        request: PlanRequest,
        *,
        con: Any,
        namespace: str,
        snapshot: ReopenableSnapshot | None = None,
    ) -> PlanInvocation:
        """Save a plan and pin its inputs before returning an invocation.

        Omit `snapshot` to let the owner select inputs. Pass an existing receipt
        from this owner to plan against exactly that snapshot without recapturing.
        Repeat the same request and receipt to replay the saved invocation.
        """
        with self._lock:
            return self._plan(request, con=con, namespace=namespace, snapshot=snapshot)

    def _plan(
        self,
        request: PlanRequest,
        *,
        con: Any,
        namespace: str,
        snapshot: ReopenableSnapshot | None,
    ) -> PlanInvocation:
        request_json = _canonical(request.to_dict())
        if snapshot is not None:
            if not isinstance(snapshot, ReopenableSnapshot):
                raise TypeError("snapshot must be ReopenableSnapshot")
            _assert_owner_connection(self.snapshot_authority.owner, con)
            # Preserve the request identity for plans without an explicit receipt.
            request_json = _canonical([request_json, snapshot.to_json()])
        store = RuntimeStore(con, namespace=namespace)
        store.initialize()
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

        expected = con.execute(
            f"SELECT generation FROM {store._schema}.runtime_manifests WHERE dataset=?",
            [request.destination.dataset],
        ).fetchone()
        expected_manifest = expected[0] if expected is not None else None
        if request.bind_base_manifest and expected_manifest != request.expected_base_manifest:
            raise _INVALID_REQUEST.error(reason="pinned base generation is no longer current")
        ensure_runtime_shape_supported(request.design.shape, anchor=request.design.anchor)
        ensure_runtime_expressions_supported(request.design)
        model = self.resolver.resolve(request.model)
        if (
            not isinstance(model, Model)
            or ModelReference.from_model(model, owner_key=request.model.owner_key) != request.model
        ):
            raise _INVALID_REQUEST.error(reason="owner Model reference does not match")
        could_reuse = (
            snapshot is None
            and request.bind_base_manifest
            and expected_manifest is not None
            and _claims_no_work(request.changes, request.design)
        )
        if not could_reuse:
            _scan_policy(request.budget)
        enforce_shape_budget(request.design.shape, request.budget)
        design = resolve_unit(request.design, model)
        definition = request.dataset_definition
        if definition is not None:
            definition = definition.resolve_unit(model)
        plan = (
            _plan_no_work(
                store,
                self.snapshot_authority,
                model=model,
                design=design,
                definition=definition,
                dataset=request.destination.dataset,
                generation=expected_manifest,
                changes=request.changes,
                budget=request.budget,
                scope=request.requested_scope or "all",
                destination=request.destination,
                model_reference=request.model,
                execution_mode=self._execution_mode,
            )
            if could_reuse
            else None
        )
        reused = plan is not None
        if plan is None:
            if could_reuse:
                _scan_policy(request.budget)
            plan = self._plan_inputs(
                request, store, model, design, definition, expected_manifest, snapshot
            )
        saved_plan = SavedPlan.create(
            plan_json=plan.serialize(),
            design=design,
            model=request.model,
            destination=request.destination,
            namespace=namespace,
            expected_base_manifest=expected_manifest,
            dataset_definition=definition,
        )
        request_key = hashlib.sha256(
            _canonical([namespace, request.invocation_id, request.destination.to_dict()]).encode()
        ).hexdigest()
        invocation = PlanInvocation(
            request.invocation_id,
            saved_plan.plan_json,
            request_key,
            request.destination,
            plan.snapshot_id,
            expected_manifest,
            saved_plan.to_json(),
        )
        con.execute("BEGIN TRANSACTION")
        try:
            if reused:
                current = con.execute(
                    f"SELECT generation,snapshot_id FROM {store._schema}.runtime_manifests "
                    "WHERE dataset=?",
                    [plan.dataset],
                ).fetchone()
                if current != (expected_manifest, plan.snapshot_id):
                    raise _INVALID_REQUEST.error(
                        reason="pinned base generation is no longer current"
                    )
            # Pinning writes the snapshot guard that both publication and GC must advance.
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

    def _plan_inputs(
        self,
        request: PlanRequest,
        store: RuntimeStore,
        model: Model,
        design: Design,
        definition: DatasetDefinition | None,
        expected_manifest: str | None,
        snapshot: ReopenableSnapshot | None,
    ) -> RefreshPlan:
        from terrine.compile.dispatch import compile_design
        from terrine.runtime.backends.duckdb import DuckDBBackend

        con, namespace = store.con, store.namespace
        budget = request.budget or Budget()
        base_revision = None
        # A failed empty-change proof must not erase the fallback graph.
        if expected_manifest is not None and (request.changes is None or request.changes.changes):
            try:
                backend = DuckDBBackend(con, namespace=namespace)
                base_revision = backend.generation_execution_revision(
                    request.destination.dataset, expected_manifest
                )
            except TerrineError:
                pass
        if definition is not None:
            try:
                compiled = definition.build(model)._active_compiled()
            except TerrineError:
                raise
            except (KeyError, TypeError, ValueError, OverflowError) as error:
                raise INVALID_DEFINITION.error(
                    reason="dataset view cannot be constructed"
                ) from error
        else:
            compiled = compile_design(design, model)
        source_names, _, _ = _design_root_names(model, design)
        graph = dependency_graph(
            model,
            design,
            sources=source_names,
            definition_hash=(
                design.spec_hash
                if definition is None
                else hashlib.sha256((definition.digest + model.model_hash).encode()).hexdigest()
            ),
            partitions=(request.requested_scope or "all",),
            expression_timings=compiled.expression_timings,
        )
        if snapshot is None:
            snapshot = self.snapshot_authority.capture(
                model,
                design=design,
                con=con,
                namespace=namespace,
                run_id=request.invocation_id,
                budget=budget,
            )
        elif snapshot.model_hash != model.model_hash:
            raise _INVALID_BASE.error(reason="retained snapshot Model declaration differs")
        context = self.snapshot_authority.reopen(snapshot, con=con, namespace=namespace)
        try:
            execution_model = model._for_execution(
                context,
                source_names=set(source_names),
                unit_name=design.unit,
            )
            snapshot_compiled = (
                compile_design(design, execution_model)
                if definition is None
                else definition.build(execution_model)._active_compiled()
            )
            context.admit(*snapshot_compiled.reads())
            validate_compiled_output(snapshot_compiled, execution_model.con.to_pyarrow)
        finally:
            context.close()
        return _plan_refresh(
            namespace=namespace,
            dataset=request.destination.dataset,
            definition_hash=design.spec_hash,
            snapshot_id=snapshot.snapshot_id,
            graph=graph,
            changes=request.changes,
            budget=request.budget,
            reuse_base_generation=expected_manifest,
            reuse_base_revision=base_revision,
            execution_mode=self._execution_mode,
        )


def _range_json(value: tuple[datetime, datetime] | None) -> list[str] | None:
    return [item.isoformat() for item in value] if value is not None else None
