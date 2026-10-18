from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

import pyarrow as pa

from terrine.runtime.leases import Lease, LeaseStore
from terrine.runtime.planner import RefreshPlan, TaskSpec

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
    leases = LeaseStore(con, namespace=namespace)
    schema = leases._schema
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_runs ("
        "run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, fingerprint VARCHAR NOT NULL, "
        "state VARCHAR NOT NULL, published_generation VARCHAR)"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_tasks ("
        "run_id VARCHAR NOT NULL, task_id VARCHAR NOT NULL, status VARCHAR NOT NULL, "
        "output_blob BLOB, PRIMARY KEY(run_id, task_id))"
    )
    run = con.execute(
        f"SELECT dataset, fingerprint, state, published_generation "
        f"FROM {schema}.runtime_runs WHERE run_id=?",
        [run_id],
    ).fetchone()
    if run is not None:
        if run[0] != plan.dataset or run[1] != plan.recipe.fingerprint:
            raise ValueError("run_id is already bound to a different plan")
        if run[2] in {"succeeded", "cancelled"}:
            return RunReceipt(run_id, run[2], run[3])
    else:
        con.execute(
            f"INSERT INTO {schema}.runtime_runs VALUES (?, ?, ?, 'running', NULL)",
            [run_id, plan.dataset, plan.recipe.fingerprint],
        )
    leases.initialize()
    now = datetime.now(UTC)
    lease = leases.acquire(f"dataset:{plan.dataset}", run_id, ttl=lease_ttl, now=now)
    outputs: dict[str, pa.Table] = {}
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
                con.execute(
                    f"INSERT INTO {schema}.runtime_tasks VALUES (?, ?, 'running', NULL) "
                    "ON CONFLICT(run_id, task_id) DO UPDATE SET status='running', output_blob=NULL",
                    [run_id, task.task_id],
                )
                dependencies = MappingProxyType({key: outputs[key] for key in task.dependencies})
                output = run_task(task, dependencies)
                if not isinstance(output, pa.Table):
                    raise TypeError("task runner must return a PyArrow Table")
                _check_budget(output, plan)
                con.execute(
                    f"UPDATE {schema}.runtime_tasks SET status='complete', output_blob=? "
                    "WHERE run_id=? AND task_id=?",
                    [_encode(output), run_id, task.task_id],
                )
            outputs[task.task_id] = output
            leases.renew(lease, ttl=lease_ttl, now=datetime.now(UTC))
        if cancelled is not None and cancelled():
            con.execute(
                f"UPDATE {schema}.runtime_runs SET state='cancelled' WHERE run_id=?", [run_id]
            )
            return RunReceipt(run_id, "cancelled", None)
        leases.assert_current(lease)
        published = publish(plan, MappingProxyType(outputs), lease)
        generation = getattr(published, "generation", published)
        generation_text = str(generation) if generation is not None else None
        con.execute(
            f"UPDATE {schema}.runtime_runs SET state='succeeded', published_generation=? "
            "WHERE run_id=?",
            [generation_text, run_id],
        )
        return RunReceipt(run_id, "succeeded", generation_text)
    except Exception:
        con.execute(f"UPDATE {schema}.runtime_runs SET state='failed' WHERE run_id=?", [run_id])
        raise
    finally:
        leases.release(lease)


def _encode(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _decode(payload: bytes) -> pa.Table:
    return pa.ipc.open_stream(payload).read_all()


def _check_budget(table: pa.Table, plan: RefreshPlan) -> None:
    budget = plan.budget
    if budget is None:
        return
    if budget.max_rows is not None and table.num_rows > budget.max_rows:
        raise ValueError("materialized partition exceeds max_rows")
    if budget.max_output_bytes is not None and table.nbytes > budget.max_output_bytes:
        raise ValueError("materialized partition exceeds max_output_bytes")
