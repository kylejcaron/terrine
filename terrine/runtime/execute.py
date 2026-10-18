from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

import pyarrow as pa

from terrine.compile.cost import _OUTPUT_BYTES_EXCEEDED, _ROWS_EXCEEDED
from terrine.runtime.leases import Lease, LeaseStore
from terrine.runtime.planner import RefreshPlan, TaskSpec
from terrine.runtime.state import RuntimeStore

__all__ = ["RunReceipt", "execute_plan"]


@dataclass(frozen=True, slots=True)
class RunReceipt:
    run_id: str
    state: str
    published_generation: str | None


def execute_plan(
    plan: RefreshPlan,
    *,
    con: Any,
    namespace: str,
    run_id: str,
    run_task: Callable[[TaskSpec, Mapping[str, pa.Table]], pa.Table],
    publish: Callable[[RefreshPlan, Mapping[str, pa.Table], Lease], Any],
    cancelled: Callable[[], bool] | None = None,
    max_tasks: int = 4096,
    lease_ttl: timedelta = timedelta(minutes=5),
) -> RunReceipt:
    """Execute one bounded task DAG, durably caching completed Arrow partitions."""
    if plan.namespace != namespace:
        raise ValueError("plan namespace does not match runtime namespace")
    if not run_id:
        raise ValueError("run_id must not be empty")
    if isinstance(max_tasks, bool) or max_tasks < 1 or len(plan.tasks) > max_tasks:
        raise ValueError("plan task count exceeds configured max_tasks")
    if lease_ttl <= timedelta(0):
        raise ValueError("lease_ttl must be positive")
    runtime_store = RuntimeStore(con, namespace=namespace)
    runtime_store.initialize()
    leases = LeaseStore(con, namespace=namespace)
    schema = leases._schema
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_runs ("
        "run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, fingerprint VARCHAR NOT NULL, "
        "state VARCHAR NOT NULL, published_generation VARCHAR, attempt_fence BIGINT)"
    )
    con.execute(f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS attempt_fence BIGINT")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_tasks ("
        "run_id VARCHAR NOT NULL, task_id VARCHAR NOT NULL, status VARCHAR NOT NULL, "
        "output_blob BLOB, task_fence BIGINT, PRIMARY KEY(run_id, task_id))"
    )
    con.execute(f"ALTER TABLE {schema}.runtime_tasks ADD COLUMN IF NOT EXISTS task_fence BIGINT")
    run = con.execute(
        f"SELECT dataset, fingerprint, state, published_generation "
        f"FROM {schema}.runtime_runs WHERE run_id=?",
        [run_id],
    ).fetchone()
    if run is not None:
        if run[0] != plan.dataset or run[1] != plan.recipe.fingerprint:
            raise ValueError("run_id is already bound to a different plan")
        if run[2] in {"succeeded", "cancelled"}:
            _release_run_snapshot_refs(runtime_store, plan.snapshot_id, run_id)
            return RunReceipt(run_id, run[2], run[3])
    else:
        con.execute(
            f"INSERT INTO {schema}.runtime_runs "
            "(run_id, dataset, fingerprint, state, published_generation) "
            "VALUES (?, ?, ?, 'running', NULL)",
            [run_id, plan.dataset, plan.recipe.fingerprint],
        )
    leases.initialize()
    lease = leases.acquire(f"dataset:{plan.dataset}", run_id, ttl=lease_ttl, now=datetime.now(UTC))
    transition = con.execute(
        f"UPDATE {schema}.runtime_runs SET state='running', attempt_fence=? "
        "WHERE run_id=? AND state IN ('running', 'failed') "
        "AND (attempt_fence IS NULL OR attempt_fence<?) AND EXISTS ("
        f"SELECT 1 FROM {schema}.runtime_leases WHERE resource=? AND owner=? "
        "AND fence=? AND expires_at>?) RETURNING run_id",
        [
            lease.fence,
            run_id,
            lease.fence,
            lease.resource,
            lease.owner,
            lease.fence,
            datetime.now(UTC).replace(tzinfo=None),
        ],
    ).fetchone()
    if transition is None:
        leases.release(lease)
        current = con.execute(
            f"SELECT state, published_generation FROM {schema}.runtime_runs WHERE run_id=?",
            [run_id],
        ).fetchone()
        if current is None:
            raise RuntimeError("run disappeared while claiming its lease")
        return RunReceipt(run_id, current[0], current[1])
    active_reference = json.dumps((run_id, lease.fence), separators=(",", ":"))
    try:
        runtime_store.pin_snapshot(plan.snapshot_id, kind="active_run", reference=active_reference)
    except Exception:
        con.execute(
            f"UPDATE {schema}.runtime_runs SET state='failed' "
            "WHERE run_id=? AND state='running' AND attempt_fence=?",
            [run_id, lease.fence],
        )
        leases.release(lease)
        raise
    heartbeat_con = con.cursor()
    heartbeat_store = LeaseStore(heartbeat_con, namespace=namespace)
    outputs: dict[str, pa.Table] = {}
    stop_heartbeat = threading.Event()
    heartbeat_error: list[Exception] = []

    def heartbeat() -> None:
        interval = min(lease_ttl.total_seconds() / 3, 1.0)
        current = lease
        while not stop_heartbeat.wait(interval):
            try:
                current = heartbeat_store.renew(current, ttl=lease_ttl, now=datetime.now(UTC))
            except Exception as error:
                heartbeat_error.append(error)
                stop_heartbeat.set()
                return

    heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
    heartbeat_thread.start()
    try:
        for task in plan.tasks:
            saved = con.execute(
                f"SELECT status, output_blob FROM {schema}.runtime_tasks "
                "WHERE run_id=? AND task_id=?",
                [run_id, task.task_id],
            ).fetchone()
            if saved is not None and saved[0] == "complete":
                output = _decode(saved[1])
            else:
                claimed_task = con.execute(
                    f"INSERT INTO {schema}.runtime_tasks "
                    "(run_id, task_id, status, output_blob, task_fence) "
                    f"SELECT ?, ?, 'running', NULL, ? WHERE EXISTS ("
                    f"SELECT 1 FROM {schema}.runtime_leases WHERE resource=? AND owner=? "
                    "AND fence=? AND expires_at>?) "
                    "ON CONFLICT(run_id, task_id) DO UPDATE SET status='running', "
                    "output_blob=NULL, task_fence=excluded.task_fence "
                    "WHERE runtime_tasks.task_fence IS NULL "
                    "OR runtime_tasks.task_fence<excluded.task_fence RETURNING task_id",
                    [
                        run_id,
                        task.task_id,
                        lease.fence,
                        lease.resource,
                        lease.owner,
                        lease.fence,
                        datetime.now(UTC).replace(tzinfo=None),
                    ],
                ).fetchone()
                if claimed_task is None:
                    raise ValueError("lease attempt was fenced before task start")
                dependencies = MappingProxyType({key: outputs[key] for key in task.dependencies})
                output = run_task(task, dependencies)
                if not isinstance(output, pa.Table):
                    raise TypeError("task runner must return a PyArrow Table")
                if task.layer == "frame":
                    _check_budget(task, output, plan, outputs)
                leases.assert_current(lease)
                completed = con.execute(
                    f"UPDATE {schema}.runtime_tasks SET status='complete', output_blob=? "
                    "WHERE run_id=? AND task_id=? AND status='running' AND task_fence=? "
                    "AND EXISTS ("
                    f"SELECT 1 FROM {schema}.runtime_leases WHERE resource=? AND owner=? "
                    "AND fence=? AND expires_at>?) RETURNING task_id",
                    [
                        _encode(output),
                        run_id,
                        task.task_id,
                        lease.fence,
                        lease.resource,
                        lease.owner,
                        lease.fence,
                        datetime.now(UTC).replace(tzinfo=None),
                    ],
                ).fetchone()
                if completed is None:
                    raise ValueError("lease attempt was fenced before task completion")
            outputs[task.task_id] = output
        if heartbeat_error:
            raise heartbeat_error[0]
        if cancelled is not None and cancelled():
            terminal = con.execute(
                f"UPDATE {schema}.runtime_runs SET state='cancelled' "
                "WHERE run_id=? AND state='running' AND attempt_fence=? RETURNING run_id",
                [run_id, lease.fence],
            ).fetchone()
            if terminal is None:
                raise ValueError("lease attempt was fenced before cancellation")
            return RunReceipt(run_id, "cancelled", None)
        leases.assert_current(lease)
        published = publish(plan, MappingProxyType(outputs), lease)
        generation = getattr(published, "generation", published)
        generation_text = str(generation) if generation is not None else None
        if generation_text is not None:
            runtime_store.replace_published_snapshot(
                plan.dataset, generation_text, plan.snapshot_id, lease=lease
            )
        terminal = con.execute(
            f"UPDATE {schema}.runtime_runs SET state='succeeded', published_generation=? "
            "WHERE run_id=? AND state='running' AND attempt_fence=? RETURNING run_id",
            [generation_text, run_id, lease.fence],
        ).fetchone()
        if terminal is None:
            raise ValueError("lease attempt was fenced before recording success")
        return RunReceipt(run_id, "succeeded", generation_text)
    except Exception:
        con.execute(
            f"UPDATE {schema}.runtime_runs SET state='failed' "
            "WHERE run_id=? AND state='running' AND published_generation IS NULL "
            "AND attempt_fence=?",
            [run_id, lease.fence],
        )
        raise
    finally:
        stop_heartbeat.set()
        heartbeat_thread.join()
        heartbeat_con.close()
        state = con.execute(
            f"SELECT state FROM {schema}.runtime_runs WHERE run_id=?", [run_id]
        ).fetchone()
        try:
            if state is not None and state[0] in {"succeeded", "cancelled"}:
                _release_run_snapshot_refs(runtime_store, plan.snapshot_id, run_id)
        finally:
            leases.release(lease)


def _encode(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _decode(payload: bytes) -> pa.Table:
    return pa.ipc.open_stream(payload).read_all()


def _release_run_snapshot_refs(store: RuntimeStore, snapshot_id: str, run_id: str) -> None:
    for attempt in range(5):
        try:
            for kind, reference in store.snapshot_references(snapshot_id):
                if kind != "active_run":
                    continue
                try:
                    owner, _fence = json.loads(reference)
                except (TypeError, ValueError):
                    if reference == run_id:
                        store.unpin_snapshot(snapshot_id, kind=kind, reference=reference)
                    continue
                if owner == run_id:
                    store.unpin_snapshot(snapshot_id, kind=kind, reference=reference)
            return
        except Exception as exc:
            if type(exc).__name__ != "TransactionException":
                raise
            if attempt == 4:
                return
            time.sleep(0.01 * (2**attempt))


# A terminal replay retries cleanup; transient conflicts must not erase its receipt.
def _check_budget(
    task: TaskSpec,
    table: pa.Table,
    plan: RefreshPlan,
    outputs: Mapping[str, pa.Table],
) -> None:
    budget = plan.budget
    if budget is None:
        return
    rows = table.num_rows
    output_bytes = table.nbytes
    final_tasks = {item.task_id for item in plan.tasks if item.layer == "frame"}
    for task_id, output in outputs.items():
        if task_id in final_tasks:
            rows += output.num_rows
            output_bytes += output.nbytes
    if budget.max_rows is not None and rows > budget.max_rows:
        raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
    if budget.max_output_bytes is not None and output_bytes > budget.max_output_bytes:
        raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
