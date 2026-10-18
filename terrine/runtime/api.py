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
from terrine.runtime.recipe import DestinationRef
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
            if existing.state != "failed":
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
    return execute_plan(
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


def run_serialized_plan(
    serialized: bytes | str,
    *,
    con: Any,
    namespace: str,
    request_key: str | None = None,
    destination: DestinationRef | None = None,
    executor: Executor | None = None,
) -> RunReceipt:
    """Verify and execute a versioned plan using the same request-key replay path."""
    plan = RefreshPlan.deserialize(serialized)
    return run_plan(
        plan,
        con=con,
        namespace=namespace,
        request_key=request_key,
        destination=destination,
        executor=executor,
    )


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
        f"SELECT partition FROM {schema}.runtime_partitions "
        "WHERE dataset=? AND generation=? ORDER BY partition",
        [publication[0], publication[1]],
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
