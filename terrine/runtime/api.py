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
    resolve_plan,
)
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.runtime.status import cancel_run, get_run

__all__ = [
    "Executor",
    "RunReceipt",
    "cancel_run",
    "find_run_by_request_key",
    "get_run",
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
    executor: Executor | None = None,
    resolver: ModelResolver | None = None,
    snapshot_authority: SnapshotAuthority | None = None,
) -> RunReceipt:
    """Verify and execute a plan, reopening owner-retained inputs for saved recipes."""
    document = json.loads(serialized)
    if isinstance(document, dict) and "plan_json" in document:
        if resolver is None or snapshot_authority is None:
            raise ValueError("saved recipes require an owner resolver and snapshot authority")
        if executor is None or not callable(getattr(executor, "run_snapshot_task", None)):
            raise ValueError("saved recipes require an executor with run_snapshot_task")
        recipe = SavedPlanRecipe.from_json(serialized)
        plan, _model, _design, recipe_destination = resolve_plan(
            recipe, resolver=resolver, namespace=namespace
        )
        if destination is not None and destination != recipe_destination:
            raise ValueError("destination differs from saved recipe")
        if request_key is not None:
            destination = recipe_destination
        elif destination is not None:
            raise ValueError("destination is only used with a request_key")
        receipt = snapshot_authority.find_retained(plan.snapshot_id, namespace=namespace)
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
