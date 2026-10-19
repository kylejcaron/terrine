from __future__ import annotations

import json
import uuid
from typing import Any, Protocol

import ibis

from terrine.errors import RefusalSpec
from terrine.runtime.backends.base import PublicationReceipt
from terrine.runtime.events import read_run_events
from terrine.runtime.execute import RunReceipt, execute_plan
from terrine.runtime.planner import RefreshPlan, plan_refresh
from terrine.runtime.recipe import (
    DestinationRef,
    ModelResolver,
    SavedPlanRecipe,
    _bind_recipe_plan,
    resolve_plan,
)
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.runtime.status import cancel_run, get_run

__all__ = [
    "DesignExecutor",
    "Executor",
    "RunReceipt",
    "cancel_run",
    "find_run_by_request_key",
    "get_run",
    "open_published_frame",
    "open_published_generation",
    "plan_refresh",
    "read_run_events",
    "run_plan",
    "run_serialized_plan",
]

_EXECUTOR_UNAVAILABLE = RefusalSpec(
    "terrine.runtime.executor_unavailable",
    "No owner execution callbacks are configured for this runtime.",
)


class Executor(Protocol):
    """Owner-provided callbacks that execute bounded tasks and publish a generation."""

    def run_task(self, task: Any, dependencies: Any) -> Any: ...

    def publish(self, plan: RefreshPlan, outputs: Any, lease: Any, run_id: str) -> Any: ...


def run_plan(
    plan: RefreshPlan,
    *,
    con: Any,
    namespace: str,
    request_key: str | None = None,
    destination: DestinationRef | None = None,
    executor: Executor | None = None,
) -> RunReceipt:
    """Run a plan under owner execution callbacks, replaying destination-scoped keys."""
    if request_key is not None:
        if destination is None:
            raise ValueError("destination is required with a request_key")
        existing = find_run_by_request_key(
            request_key, destination=destination, con=con, namespace=namespace
        )
        if existing is not None:
            store = RuntimeStore(con, namespace=namespace)
            store.initialize()
            fingerprint = con.execute(
                f"SELECT fingerprint FROM {store._schema}.runtime_runs WHERE run_id=?",
                [existing.run_id],
            ).fetchone()
            if fingerprint is None or fingerprint[0] != plan.recipe.fingerprint:
                from terrine.errors import RefusalSpec

                raise RefusalSpec(
                    "terrine.runtime.request_key_conflict",
                    "Request key is already bound to a different plan.",
                ).error()
            if existing.state in {"running", "cancel_requested", "succeeded", "cancelled"}:
                if existing.state in {"succeeded", "cancelled"}:
                    from terrine.runtime.execute import _release_run_snapshot_refs

                    _release_run_snapshot_refs(store, plan.snapshot_id, existing.run_id)
                _record_result_partitions(plan, existing, con=con, namespace=namespace)
                return existing
            run_id = existing.run_id
        else:
            run_id = str(uuid.uuid4())
    else:
        if destination is not None:
            raise ValueError("destination is only used with a request_key")
        run_id = str(uuid.uuid4())
    if executor is None:
        raise _EXECUTOR_UNAVAILABLE.error()
    receipt = execute_plan(
        plan,
        con=con,
        namespace=namespace,
        run_id=run_id,
        request_key=request_key,
        destination=destination,
        run_task=executor.run_task,
        publish=lambda current_plan, outputs, lease: executor.publish(
            current_plan, outputs, lease, run_id
        ),
    )
    _record_result_partitions(plan, receipt, con=con, namespace=namespace)
    return receipt


def run_serialized_plan(
    serialized: bytes | str,
    *,
    con: Any,
    namespace: str,
    request_key: str | None = None,
    destination: DestinationRef | None = None,
    executor: Any | None = None,
    resolver: ModelResolver | None = None,
    snapshot_authority: SnapshotAuthority | None = None,
) -> RunReceipt:
    """Verify and execute a plan, reopening owner-retained inputs for saved recipes."""
    document = json.loads(serialized)
    if isinstance(document, dict) and "plan_json" in document:
        recipe = SavedPlanRecipe.from_json(serialized)
        embedded_plan = _bind_recipe_plan(recipe, namespace=namespace)
        recipe_destination = recipe.destination
        if destination is not None and destination != recipe_destination:
            raise ValueError("destination differs from saved recipe")
        if request_key is not None:
            destination = recipe_destination
            existing = find_run_by_request_key(
                request_key, destination=destination, con=con, namespace=namespace
            )
            if existing is not None:
                store = RuntimeStore(con, namespace=namespace)
                store.initialize()
                fingerprint = con.execute(
                    f"SELECT fingerprint FROM {store._schema}.runtime_runs WHERE run_id=?",
                    [existing.run_id],
                ).fetchone()
                if fingerprint is None or fingerprint[0] != embedded_plan.recipe.fingerprint:
                    raise RefusalSpec(
                        "terrine.runtime.request_key_conflict",
                        "Request key is already bound to a different plan.",
                    ).error()
            if existing is not None and existing.state in {
                "running",
                "cancel_requested",
                "succeeded",
                "cancelled",
            }:
                return run_plan(
                    embedded_plan,
                    con=con,
                    namespace=namespace,
                    request_key=request_key,
                    destination=destination,
                )
        elif destination is not None:
            raise ValueError("destination is only used with a request_key")
        if resolver is None or snapshot_authority is None:
            raise ValueError("saved recipes require an owner resolver and snapshot authority")
        plan, model, design, _recipe_destination = resolve_plan(
            recipe, resolver=resolver, namespace=namespace
        )
        receipt = snapshot_authority.find_retained(plan.snapshot_id, namespace=namespace)
        if receipt.model_hash != model.model_hash:
            raise RefusalSpec(
                "terrine.runtime.plan_recipe_mismatch",
                "Saved plan reconstruction failed verification "
                "(retained snapshot Model declaration differs).",
            ).error()
        from terrine.execution.gates import enforce_shape_budget

        enforce_shape_budget(design.shape, plan.budget)
        if executor is None:
            preflight_runtime_shape(design, plan.budget)
            executor = DesignExecutor(model, design, plan, con, namespace=namespace)
        if not callable(getattr(executor, "run_snapshot_task", None)):
            raise ValueError("saved recipes require an executor with run_snapshot_task")
        executor = _SnapshotBoundExecutor(executor, snapshot_authority, receipt, con, namespace)
    else:
        plan = RefreshPlan.deserialize(serialized)
    return run_plan(
        plan,
        con=con,
        namespace=namespace,
        request_key=request_key,
        destination=destination,
        executor=executor,
    )


class _SnapshotBoundExecutor:
    def __init__(
        self, executor: Any, authority: SnapshotAuthority, receipt: Any, con: Any, namespace: str
    ) -> None:
        self.executor = executor
        self.authority = authority
        self.receipt = receipt
        self.con = con
        self.namespace = namespace

    def run_task(self, task: Any, dependencies: Any) -> Any:
        context = self.authority.reopen(self.receipt, con=self.con, namespace=self.namespace)
        try:
            return self.executor.run_snapshot_task(context, task, dependencies)
        finally:
            context.close()

    def publish(self, plan: RefreshPlan, outputs: Any, lease: Any, run_id: str) -> Any:
        return self.executor.publish(plan, outputs, lease, run_id)


def find_run_by_request_key(
    request_key: str,
    *,
    destination: DestinationRef,
    con: Any,
    namespace: str,
) -> RunReceipt | None:
    if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 256:
        raise ValueError("request_key must be a non-empty string of at most 256 characters")
    if not isinstance(destination, DestinationRef):
        raise TypeError("destination must be DestinationRef")
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    table = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE lower(table_schema)=lower(?) AND table_name='runtime_runs'",
        [namespace],
    ).fetchone()
    if table is None:
        return None
    con.execute(f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS request_key VARCHAR")
    con.execute(
        f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS destination_json VARCHAR"
    )
    destination_json = json.dumps(destination.to_dict(), sort_keys=True, separators=(",", ":"))
    row = con.execute(
        f"SELECT run_id,state,published_generation FROM {schema}.runtime_runs "
        "WHERE request_key=? AND destination_json=?",
        [request_key, destination_json],
    ).fetchone()
    return RunReceipt(*row) if row is not None else None


def _record_result_partitions(
    plan: RefreshPlan, receipt: RunReceipt, *, con: Any, namespace: str
) -> None:
    if receipt.state != "succeeded":
        return
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_run_results "
        "(run_id VARCHAR NOT NULL, partition VARCHAR NOT NULL, PRIMARY KEY(run_id,partition))"
    )
    dependencies = {dependency for task in plan.tasks for dependency in task.dependencies}
    results = sorted(task.task_id for task in plan.tasks if task.task_id not in dependencies)
    if not results:
        return
    for partition in results:
        con.execute(
            f"INSERT INTO {schema}.runtime_run_results VALUES (?, ?) ON CONFLICT DO NOTHING",
            [receipt.run_id, partition],
        )


def open_published_generation(receipt: RunReceipt, *, con: Any, namespace: str) -> ibis.Table:
    """Open only partitions recorded for the exact run publication receipt."""
    if not isinstance(receipt, RunReceipt) or receipt.state != "succeeded":
        raise ValueError("a succeeded run receipt is required")
    if receipt.published_generation is None:
        raise ValueError("run receipt has no published generation")
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    row = con.execute(
        f"SELECT dataset,state,published_generation FROM {schema}.runtime_runs WHERE run_id=?",
        [receipt.run_id],
    ).fetchone()
    if row is None or row[1] != "succeeded" or row[2] != receipt.published_generation:
        raise ValueError("run receipt does not match authoritative runtime state")
    from terrine.runtime.backends.duckdb import DuckDBBackend

    backend = DuckDBBackend(con, namespace=namespace)
    publication = con.execute(
        f"SELECT dataset,generation,version FROM {schema}.runtime_run_publications WHERE run_id=?",
        [receipt.run_id],
    ).fetchone()
    if (
        publication is None
        or publication[0] != row[0]
        or publication[1] != receipt.published_generation
    ):
        raise ValueError("run has no matching durable publication receipt")
    partitions = con.execute(
        f"SELECT p.partition FROM {schema}.runtime_run_results p "
        f"JOIN {schema}.runtime_partitions r ON r.partition=p.partition "
        "WHERE p.run_id=? AND r.dataset=? AND r.generation=? ORDER BY p.partition",
        [receipt.run_id, publication[0], publication[1]],
    ).fetchall()
    if not partitions:
        raise ValueError("published generation has no retained partitions")
    published = PublicationReceipt(
        publication[0], publication[1], publication[2], tuple(item[0] for item in partitions)
    )
    relations = [
        backend.read_relation(published, partition, ibis_backend=None)
        for (partition,) in partitions
    ]
    return relations[0] if len(relations) == 1 else ibis.union(*relations, distinct=False)


def preflight_runtime_shape(design: Any, budget: Any = None) -> None:
    from terrine.compile.materialize import preflight_materialization_shape
    from terrine.execution.gates import enforce_shape_budget

    enforce_shape_budget(design.shape, budget)
    preflight_materialization_shape(design, budget)


class DesignExecutor:
    """Execute a Design through snapshot-bound compiler layers and atomic publication."""

    def __init__(self, model: Any, design: Any, plan: Any, con: Any, *, namespace: str) -> None:
        from threading import RLock

        from terrine.compile.layers import plan_layers
        from terrine.runtime.backends.duckdb import DuckDBBackend

        self.model = model
        self.design = design
        self._plan = plan
        self._keys = plan_layers(design, model).keys
        self._backend = DuckDBBackend(con, namespace=namespace)
        self._layers: dict[str, dict[Any, Any]] = {}
        self._lock = RLock()

    def run_snapshot_task(self, context: Any, task: Any, dependencies: Any):
        import uuid

        import pyarrow as pa

        from terrine.compile.layers import build_layer
        from terrine.data import MaterializedFrame, Provenance

        if task.layer in {
            "daily",
            "prefix_certificate",
            "selector",
            "state_point",
            "state_period",
            "baseline",
        }:
            return pa.table({"_terrine_empty": pa.array([], type=pa.int8())})
        execution_model = self._execution_model(context)
        if task.layer == "prefix":
            keys = [
                self._with_snapshot(key, self._plan.snapshot_id)
                for key in self._keys
                if key.layer == "prefix" and key.source == task.source
            ]
            results = {
                key: context.execute(
                    build_layer(
                        self.design, execution_model, key, relation_resolver=context.resolve
                    )
                )
                for key in keys
            }
            with self._lock:
                self._layers.setdefault(task.partition, {}).update(results)
            return (
                next(iter(results.values()))
                if results
                else pa.table({"_terrine_empty": pa.array([], type=pa.int8())})
            )
        if task.layer == "anchor":
            key = self._with_snapshot(self._keys[0], self._plan.snapshot_id)
            result = context.execute(
                build_layer(self.design, execution_model, key, relation_resolver=context.resolve)
            )
            with self._lock:
                self._layers.setdefault(task.partition, {})[key] = result
            return result
        if task.layer != "frame":
            raise ValueError(f"unsupported runtime task layer: {task.layer}")

        from terrine.runtime.materialize import compose_runtime_frame

        persisted = {}
        registered = []
        with self._lock:
            layer_outputs = dict(self._layers.get(task.partition, {}))
        for key, arrow in layer_outputs.items():
            name = f"_terrine_runtime_layer_{uuid.uuid4().hex}"
            relation = context.con.create_table(name, obj=arrow, temp=True)
            registered.append(name)
            persisted[key] = relation
        try:
            compiled = compose_runtime_frame(
                self.design, execution_model, persisted=persisted, budget=self._plan.budget
            )
            table = context.execute(compiled.table)
            shape = (
                "state_panel"
                if type(self.design.shape).__name__ == "StatePanel"
                else str(self.design.shape)
            )
            from terrine.execution.gates import _eligibility_summary

            return MaterializedFrame(
                table,
                compiled.roles,
                compiled.columns,
                Provenance(
                    feature_cutoffs=compiled.feature_cutoffs,
                    eligibility_cutoffs=compiled.eligibility_cutoffs,
                    eligibility_summary=_eligibility_summary(compiled.audit_table, context.execute),
                    design_hash=self.design.spec_hash,
                    model_hash=execution_model.model_hash,
                    source_generations=context.snapshot.source_generations,
                    execution_snapshot=context.snapshot,
                    shape=shape,
                    treatment_design=None,
                    expression_timings=compiled.expression_timings,
                ),
            ).to_arrow()
        finally:
            for name in registered:
                context.con.drop_table(name, force=True)

    def _execution_model(self, context: Any):
        from terrine.spec.validate import validate_design

        required_sources = set(validate_design(self.design, self.model).required_sources)
        return self.model._for_execution(
            context, source_names=required_sources, unit_name=self.design.unit
        )

    @staticmethod
    def _with_snapshot(key: Any, snapshot_id: str):
        from dataclasses import replace

        return replace(key, snapshot_id=snapshot_id)

    def publish(self, plan: Any, outputs: Any, lease: Any, run_id: str):
        from terrine.runtime.backends.base import StagedGeneration

        stage = StagedGeneration(
            plan.dataset,
            f"generation-{run_id}",
            outputs,
            run_id=run_id,
            snapshot_id=plan.snapshot_id,
        )
        return self._backend.publish_generation(
            stage, expected_manifest=plan.expected_base_manifest, lease=lease
        )


def open_published_frame(receipt: RunReceipt, *, con: Any, namespace: str):
    """Read metadata-preserving Arrow output from the exact published run generation."""
    import pyarrow as pa

    if not isinstance(receipt, RunReceipt) or receipt.state != "succeeded":
        raise ValueError("a succeeded run receipt is required")
    if receipt.published_generation is None:
        raise ValueError("run receipt has no published generation")
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    run = con.execute(
        f"SELECT dataset,state,published_generation FROM {schema}.runtime_runs WHERE run_id=?",
        [receipt.run_id],
    ).fetchone()
    if run is None or run[1] != "succeeded" or run[2] != receipt.published_generation:
        raise ValueError("run receipt does not match authoritative runtime state")
    publication = con.execute(
        f"SELECT dataset,generation,version FROM {schema}.runtime_run_publications WHERE run_id=?",
        [receipt.run_id],
    ).fetchone()
    if publication is None or (publication[0], publication[1]) != (run[0], run[2]):
        raise ValueError("run has no matching durable publication receipt")
    partitions = con.execute(
        f"SELECT p.partition FROM {schema}.runtime_run_results p "
        f"JOIN {schema}.runtime_partitions r ON r.partition=p.partition "
        "WHERE p.run_id=? AND r.dataset=? AND r.generation=? ORDER BY p.partition",
        [receipt.run_id, run[0], run[2]],
    ).fetchall()
    if not partitions:
        raise ValueError("published generation has no retained partitions")
    from terrine.data import MaterializedFrame
    from terrine.runtime.backends.base import PublicationReceipt
    from terrine.runtime.backends.duckdb import DuckDBBackend

    publication_receipt = PublicationReceipt(
        run[0], run[2], publication[2], tuple(item[0] for item in partitions)
    )
    backend = DuckDBBackend(con, namespace=namespace)
    frames = [
        MaterializedFrame.from_arrow(backend.read_partition(publication_receipt, partition[0]))
        for partition in partitions
    ]
    first = frames[0]
    if any(
        frame.roles != first.roles
        or frame.columns != first.columns
        or frame.provenance != first.provenance
        for frame in frames[1:]
    ):
        raise ValueError("published Arrow partitions do not share one Frame metadata contract")
    table = pa.concat_tables([frame.table for frame in frames])
    return MaterializedFrame(table, first.roles, first.columns, first.provenance)
