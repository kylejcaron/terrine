from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

import pyarrow as pa

from terrine.compile.cost import _OUTPUT_BYTES_EXCEEDED, _ROWS_EXCEEDED
from terrine.errors import RefusalSpec, TerrineError
from terrine.runtime.arrow import _PREFIX, ArrowOutput
from terrine.runtime.events import event_transaction, record_run_event
from terrine.runtime.leases import Lease, LeaseStore
from terrine.runtime.planner import RefreshPlan, TaskSpec
from terrine.runtime.recipe import DestinationRef
from terrine.runtime.state import RuntimeStore
from terrine.runtime.status import _recover_interrupted_runs
from terrine.runtime.synchronization import run_publication_lock

__all__ = ["RunReceipt", "execute_plan"]
_REQUEST_KEY_CONFLICT = RefusalSpec(
    "terrine.runtime.request_key_conflict",
    "Request key is already bound to a different plan or destination.",
)
_EXECUTION_FAILED = RefusalSpec(
    "terrine.runtime.execution_failed",
    "Runtime execution failed outside a coded Terrine refusal.",
)
_CONCURRENCY = RefusalSpec(
    "terrine.runtime.concurrency_limit", "Owner concurrent run limit has been reached."
)


def _error_code(error: Exception) -> str:
    return error.code if isinstance(error, TerrineError) else _EXECUTION_FAILED.code


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
    run_task: Callable[[TaskSpec, Mapping[str, pa.Table | ArrowOutput]], pa.Table | ArrowOutput],
    publish: Callable[[RefreshPlan, Mapping[str, pa.Table | ArrowOutput], Lease], Any],
    request_key: str | None = None,
    destination: DestinationRef | None = None,
    cancelled: Callable[[], bool] | None = None,
    max_tasks: int = 4096,
    max_concurrency: int = 1,
    max_active_runs: int | None = None,
    lease_ttl: timedelta = timedelta(minutes=5),
) -> RunReceipt:
    """Execute a bounded DAG; callbacks must isolate connections when concurrency exceeds one."""
    if plan.namespace != namespace:
        raise ValueError("plan namespace does not match runtime namespace")
    if not run_id:
        raise ValueError("run_id must not be empty")
    if isinstance(max_tasks, bool) or max_tasks < 1 or len(plan.tasks) > max_tasks:
        raise ValueError("plan task count exceeds configured max_tasks")
    if isinstance(max_concurrency, bool) or max_concurrency < 1:
        raise ValueError("max_concurrency must be positive")
    if max_active_runs is not None and (
        isinstance(max_active_runs, bool)
        or not isinstance(max_active_runs, int)
        or max_active_runs < 1
    ):
        raise ValueError("max_active_runs must be positive")
    if request_key is not None and (
        not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 256
    ):
        raise ValueError("request_key must be a non-empty string of at most 256 characters")
    if (request_key is None) != (destination is None):
        raise ValueError("request_key and destination must be supplied together")
    if destination is not None and (
        not isinstance(destination, DestinationRef) or destination.dataset != plan.dataset
    ):
        raise ValueError("destination must identify the planned dataset")
    destination_json = (
        json.dumps(destination.to_dict(), sort_keys=True, separators=(",", ":"))
        if destination is not None
        else None
    )
    runtime_store = RuntimeStore(con, namespace=namespace)
    runtime_store.initialize()
    leases = LeaseStore(con, namespace=namespace)
    schema = leases._schema
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_runs ("
        "run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, fingerprint VARCHAR NOT NULL, "
        "state VARCHAR NOT NULL, published_generation VARCHAR, attempt_fence BIGINT, "
        "updated_at TIMESTAMP, heartbeat_at TIMESTAMP)"
    )
    con.execute(f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS attempt_fence BIGINT")
    con.execute(f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP")
    con.execute(
        f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP"
    )
    for column in ("snapshot_id VARCHAR", "task_count BIGINT", "error_code VARCHAR"):
        con.execute(f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS {column}")
    con.execute(f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS request_key VARCHAR")
    con.execute(
        f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS destination_json VARCHAR"
    )
    con.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS runtime_runs_request_destination "
        f"ON {schema}.runtime_runs (request_key, destination_json)"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_tasks ("
        "run_id VARCHAR NOT NULL, task_id VARCHAR NOT NULL, status VARCHAR NOT NULL, "
        "output_blob BLOB, task_fence BIGINT, PRIMARY KEY(run_id, task_id))"
    )
    con.execute(f"ALTER TABLE {schema}.runtime_tasks ADD COLUMN IF NOT EXISTS task_fence BIGINT")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_run_results "
        "(run_id VARCHAR NOT NULL, partition VARCHAR NOT NULL, PRIMARY KEY(run_id,partition))"
    )
    dependencies = {dependency for task in plan.tasks for dependency in task.dependencies}
    result_partitions = sorted(
        task.task_id for task in plan.tasks if task.task_id not in dependencies
    )
    leases.initialize()
    _recover_interrupted_runs(con, namespace=namespace)
    lease = None
    existing_request = None
    with event_transaction(con, namespace=namespace):
        if request_key is not None:
            existing_request = con.execute(
                f"SELECT run_id, dataset, fingerprint, state, published_generation "
                f"FROM {schema}.runtime_runs WHERE request_key=? AND destination_json=?",
                [request_key, destination_json],
            ).fetchone()
        if existing_request is not None:
            if (
                existing_request[1] != plan.dataset
                or existing_request[2] != plan.recipe.fingerprint
            ):
                raise _REQUEST_KEY_CONFLICT.error()
            if existing_request[0] != run_id:
                receipt = RunReceipt(existing_request[0], existing_request[3], existing_request[4])
                if receipt.state in {"succeeded", "cancelled"}:
                    _release_run_snapshot_refs(runtime_store, plan.snapshot_id, receipt.run_id)
                return receipt
        run = con.execute(
            f"SELECT dataset, fingerprint, state, published_generation, request_key, "
            f"destination_json FROM {schema}.runtime_runs WHERE run_id=?",
            [run_id],
        ).fetchone()
        if max_active_runs is not None and (
            run is None or run[2] not in {"succeeded", "cancelled"}
        ):
            active = con.execute(
                f"SELECT COUNT(*) FROM {schema}.runtime_runs r "
                f"JOIN {schema}.runtime_leases l ON l.resource='dataset:' || r.dataset "
                "AND l.owner=r.run_id AND l.fence=r.attempt_fence "
                "WHERE r.state IN ('queued','running','cancel_requested') "
                "AND l.expires_at>(current_timestamp AT TIME ZONE 'UTC') AND r.run_id<>?",
                [run_id],
            ).fetchone()[0]
            if active >= max_active_runs:
                raise _CONCURRENCY.error(limit=max_active_runs)
        if run is not None:
            if run[0] != plan.dataset or run[1] != plan.recipe.fingerprint:
                raise ValueError("run_id is already bound to a different plan")
            if request_key is not None and (run[4] != request_key or run[5] != destination_json):
                raise _REQUEST_KEY_CONFLICT.error()
        else:
            con.execute(
                f"INSERT INTO {schema}.runtime_runs "
                "(run_id, dataset, fingerprint, state, published_generation, request_key, "
                "destination_json, updated_at, snapshot_id, task_count) "
                "VALUES (?, ?, ?, 'queued', NULL, ?, ?, ?, ?, ?)",
                [
                    run_id,
                    plan.dataset,
                    plan.recipe.fingerprint,
                    request_key,
                    destination_json,
                    datetime.now(UTC).replace(tzinfo=None),
                    plan.snapshot_id,
                    len(plan.tasks),
                ],
            )
            record_run_event(
                run_id,
                "lifecycle",
                {"state": "queued"},
                con=con,
                namespace=namespace,
                _in_transaction=True,
            )
        if run is None or run[2] not in {"succeeded", "cancelled"}:
            con.execute(
                f"UPDATE {schema}.runtime_runs SET state="
                "CASE WHEN state='failed' THEN 'queued' ELSE state END WHERE run_id=?",
                [run_id],
            )
            lease = leases.acquire(
                f"dataset:{plan.dataset}",
                run_id,
                ttl=lease_ttl,
                now=datetime.now(UTC),
                _in_transaction=True,
            )
            con.execute(
                f"UPDATE {schema}.runtime_runs SET attempt_fence=? WHERE run_id=?",
                [lease.fence, run_id],
            )
        if result_partitions:
            for partition in result_partitions:
                con.execute(
                    f"INSERT INTO {schema}.runtime_run_results VALUES (?, ?) "
                    "ON CONFLICT DO NOTHING",
                    [run_id, partition],
                )
    if run is not None and run[2] in {"succeeded", "cancelled"}:
        _release_run_snapshot_refs(runtime_store, plan.snapshot_id, run_id)
        return RunReceipt(run_id, run[2], run[3])
    assert lease is not None
    current = con.execute(
        f"SELECT state, published_generation FROM {schema}.runtime_runs WHERE run_id=?",
        [run_id],
    ).fetchone()
    if current is not None and current[0] in {"succeeded", "cancelled"}:
        leases.release(lease)
        _release_run_snapshot_refs(runtime_store, plan.snapshot_id, run_id)
        return RunReceipt(run_id, current[0], current[1])
    if run is not None and run[2] == "cancel_requested":
        current = con.execute(
            f"SELECT state, published_generation FROM {schema}.runtime_runs WHERE run_id=?",
            [run_id],
        ).fetchone()
        if current is not None and current[0] == "cancel_requested" and current[1] is None:
            with event_transaction(con, namespace=namespace):
                con.execute(
                    f"UPDATE {schema}.runtime_runs SET state='cancelled', attempt_fence=?, "
                    "updated_at=? WHERE run_id=? AND state='cancel_requested' "
                    "AND published_generation IS NULL",
                    [lease.fence, datetime.now(UTC).replace(tzinfo=None), run_id],
                )
                record_run_event(
                    run_id,
                    "lifecycle",
                    {"state": "cancelled"},
                    con=con,
                    namespace=namespace,
                    _in_transaction=True,
                )
            leases.release(lease)
            _release_run_snapshot_refs(runtime_store, plan.snapshot_id, run_id)
            return RunReceipt(run_id, "cancelled", current[1])
        leases.release(lease)
        if current is None:
            raise RuntimeError("run disappeared while recovering cancellation")
        return RunReceipt(run_id, current[0], current[1])
    try:
        _check_expected_base_manifest(plan, con=con, namespace=namespace)
    except Exception as error:
        try:
            reconciled = _reconcile_committed_publication(
                plan,
                run_id=run_id,
                lease=lease,
                runtime_store=runtime_store,
                con=con,
                schema=schema,
                namespace=namespace,
            )
            if reconciled is not None:
                _release_run_snapshot_refs(runtime_store, plan.snapshot_id, run_id)
                return reconciled
            with event_transaction(con, namespace=namespace):
                rejected = con.execute(
                    f"UPDATE {schema}.runtime_runs SET state='failed', attempt_fence=?, "
                    "updated_at=?, error_code=? WHERE run_id=? AND state IN ('queued','running') "
                    "AND (attempt_fence IS NULL OR attempt_fence<=?) RETURNING run_id",
                    [
                        lease.fence,
                        datetime.now(UTC).replace(tzinfo=None),
                        _error_code(error),
                        run_id,
                        lease.fence,
                    ],
                ).fetchone()
                if rejected is not None:
                    record_run_event(
                        run_id,
                        "lifecycle",
                        {"state": "failed"},
                        con=con,
                        namespace=namespace,
                        _in_transaction=True,
                    )
        finally:
            leases.release(lease)
        raise
    try:
        with event_transaction(con, namespace=namespace):
            transition = con.execute(
                f"UPDATE {schema}.runtime_runs SET state='running', attempt_fence=?, "
                "updated_at=?, error_code=NULL WHERE run_id=? "
                "AND state IN ('queued','running','failed') "
                "AND (attempt_fence IS NULL OR attempt_fence<=?) AND EXISTS ("
                f"SELECT 1 FROM {schema}.runtime_leases WHERE resource=? AND owner=? "
                "AND fence=? AND expires_at>?) RETURNING run_id",
                [
                    lease.fence,
                    datetime.now(UTC).replace(tzinfo=None),
                    run_id,
                    lease.fence,
                    lease.resource,
                    lease.owner,
                    lease.fence,
                    datetime.now(UTC).replace(tzinfo=None),
                ],
            ).fetchone()
            if transition is not None:
                record_run_event(
                    run_id,
                    "lifecycle",
                    {"state": "running"},
                    con=con,
                    namespace=namespace,
                    _in_transaction=True,
                )
    except Exception:
        leases.release(lease)
        raise
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
    except Exception as error:
        with event_transaction(con, namespace=namespace):
            failed = con.execute(
                f"UPDATE {schema}.runtime_runs SET state=CASE "
                "WHEN state='cancel_requested' THEN 'cancelled' ELSE 'failed' END, "
                "updated_at=?, error_code=? WHERE run_id=? "
                "AND state IN ('running','cancel_requested') "
                "AND attempt_fence=? RETURNING state",
                [datetime.now(UTC).replace(tzinfo=None), _error_code(error), run_id, lease.fence],
            ).fetchone()
            if failed is not None:
                record_run_event(
                    run_id,
                    "lifecycle",
                    {"state": failed[0]},
                    con=con,
                    namespace=namespace,
                    _in_transaction=True,
                )
        leases.release(lease)
        raise
    heartbeat_con = con.cursor()
    heartbeat_store = LeaseStore(heartbeat_con, namespace=namespace)
    outputs: dict[str, pa.Table | ArrowOutput] = {}
    stop_heartbeat = threading.Event()
    heartbeat_error: list[Exception] = []

    def heartbeat() -> None:
        interval = min(lease_ttl.total_seconds() / 3, 1.0)
        current = lease
        while not stop_heartbeat.wait(interval):
            try:
                now = datetime.now(UTC)
                with event_transaction(heartbeat_con, namespace=namespace):
                    current = heartbeat_store.renew(current, ttl=lease_ttl, now=now)
                    timestamp = now.replace(tzinfo=None)
                    recorded = heartbeat_con.execute(
                        f"UPDATE {schema}.runtime_runs SET heartbeat_at=?, updated_at=? "
                        "WHERE run_id=? AND state IN ('running','cancel_requested') "
                        "AND attempt_fence=? RETURNING run_id",
                        [timestamp, timestamp, run_id, lease.fence],
                    ).fetchone()
                    if recorded is None:
                        raise ValueError("run attempt was fenced before heartbeat persistence")
            except Exception as error:
                heartbeat_error.append(error)
                stop_heartbeat.set()
                return

    heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
    heartbeat_thread.start()
    try:
        pending = {task.task_id: task for task in plan.tasks}
        task_order = tuple(task.task_id for task in plan.tasks)
        inflight: dict[Future[pa.Table | ArrowOutput], TaskSpec] = {}
        active_partitions: set[str] = set()
        task_failure: Exception | None = None
        stop_requested = False
        with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            while (pending and task_failure is None) or inflight:
                if heartbeat_error:
                    raise heartbeat_error[0]
                if _cancellation_requested(con, schema, run_id, cancelled):
                    stop_requested = True
                    pending.clear()
                made_progress = False
                for task_id in task_order:
                    if task_failure is not None:
                        break
                    task = pending.get(task_id)
                    if task is None or len(inflight) >= max_concurrency:
                        continue
                    if not set(task.dependencies).issubset(outputs):
                        continue
                    if task.partition in active_partitions:
                        continue
                    if stop_requested or _cancellation_requested(con, schema, run_id, cancelled):
                        stop_requested = True
                        pending.clear()
                        break
                    del pending[task_id]
                    saved = con.execute(
                        f"SELECT status, output_blob FROM {schema}.runtime_tasks "
                        "WHERE run_id=? AND task_id=?",
                        [run_id, task.task_id],
                    ).fetchone()
                    if saved is not None and saved[0] == "complete":
                        outputs[task.task_id] = _decode(saved[1])
                        made_progress = True
                        continue
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
                    dependencies = MappingProxyType(
                        {key: outputs[key] for key in task.dependencies}
                    )
                    inflight[executor.submit(run_task, task, dependencies)] = task
                    active_partitions.add(task.partition)
                    made_progress = True
                if not inflight:
                    if pending:
                        raise ValueError("task dependencies contain a cycle or unknown task")
                    continue
                if len(inflight) < max_concurrency and made_progress and pending:
                    continue
                completed_futures, _ = wait(inflight, return_when=FIRST_COMPLETED)
                for future in completed_futures:
                    task = inflight.pop(future)
                    active_partitions.remove(task.partition)
                    try:
                        output = future.result()
                    except Exception as error:
                        if task_failure is None:
                            task_failure = error
                        continue
                    if not isinstance(output, (pa.Table, ArrowOutput)):
                        raise TypeError("task runner must return Arrow data or a retained output")
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
        if task_failure is not None:
            raise task_failure
        with run_publication_lock(namespace, plan.dataset):
            return _finish_run(
                plan=plan,
                outputs=outputs,
                run_id=run_id,
                lease=lease,
                publish=publish,
                schema=schema,
                cancelled=(lambda: True) if stop_requested else cancelled,
                con=con,
                runtime_store=runtime_store,
                leases=leases,
            )
    except Exception as error:
        publication_table = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE lower(table_schema)=lower(?) AND table_name='runtime_run_publications'",
            [namespace],
        ).fetchone()
        publication = (
            con.execute(
                f"SELECT generation FROM {schema}.runtime_run_publications WHERE run_id=?",
                [run_id],
            ).fetchone()
            if publication_table is not None
            else None
        )
        if publication is not None:
            generation = publication[0]
            if plan.tasks:
                runtime_store.replace_published_snapshot(
                    plan.dataset, generation, plan.snapshot_id, lease=lease
                )
            with event_transaction(con, namespace=namespace):
                dependencies = {
                    dependency for task in plan.tasks for dependency in task.dependencies
                }
                has_results = any(task.task_id not in dependencies for task in plan.tasks)
                if not has_results:
                    _copy_result_partitions(
                        con,
                        schema=schema,
                        run_id=run_id,
                        dataset=plan.dataset,
                        generation=generation,
                    )
                reconciled = con.execute(
                    f"UPDATE {schema}.runtime_runs SET state='succeeded', "
                    "published_generation=?, updated_at=? WHERE run_id=? "
                    "AND state IN ('running','cancel_requested') AND attempt_fence=? "
                    "RETURNING run_id",
                    [generation, datetime.now(UTC).replace(tzinfo=None), run_id, lease.fence],
                ).fetchone()
                if reconciled is not None:
                    record_run_event(
                        run_id,
                        "lifecycle",
                        {"state": "succeeded"},
                        con=con,
                        namespace=namespace,
                        _in_transaction=True,
                    )
                    return RunReceipt(run_id, "succeeded", generation)
        with event_transaction(con, namespace=namespace):
            terminal = con.execute(
                f"UPDATE {schema}.runtime_runs SET state=CASE "
                "WHEN state='cancel_requested' THEN 'cancelled' ELSE 'failed' END, "
                "updated_at=?, error_code=? WHERE run_id=? "
                "AND state IN ('running','cancel_requested') "
                "AND published_generation IS NULL AND attempt_fence=? RETURNING state",
                [datetime.now(UTC).replace(tzinfo=None), _error_code(error), run_id, lease.fence],
            ).fetchone()
            if terminal is not None:
                con.execute(
                    f"UPDATE {schema}.runtime_tasks SET status=? "
                    "WHERE run_id=? AND task_fence=? AND status='running'",
                    [terminal[0], run_id, lease.fence],
                )
                record_run_event(
                    run_id,
                    "lifecycle",
                    {"state": terminal[0]},
                    con=con,
                    namespace=namespace,
                    _in_transaction=True,
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


def _reconcile_committed_publication(
    plan: RefreshPlan,
    *,
    run_id: str,
    lease: Lease,
    runtime_store: RuntimeStore,
    con: Any,
    schema: str,
    namespace: str,
) -> RunReceipt | None:
    table = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE lower(table_schema)=lower(?) AND table_name='runtime_run_publications'",
        [namespace],
    ).fetchone()
    if table is None:
        return None
    publication = con.execute(
        f"SELECT generation FROM {schema}.runtime_run_publications WHERE run_id=?", [run_id]
    ).fetchone()
    if publication is None:
        return None
    manifest = con.execute(
        f"SELECT generation FROM {schema}.runtime_manifests WHERE dataset=?", [plan.dataset]
    ).fetchone()
    generation = publication[0]
    if plan.tasks and manifest is not None and manifest[0] == generation:
        runtime_store.replace_published_snapshot(
            plan.dataset, generation, plan.snapshot_id, lease=lease
        )
    with event_transaction(con, namespace=namespace):
        dependencies = {dependency for task in plan.tasks for dependency in task.dependencies}
        if not any(task.task_id not in dependencies for task in plan.tasks):
            _copy_result_partitions(
                con,
                schema=schema,
                run_id=run_id,
                dataset=plan.dataset,
                generation=generation,
            )
        reconciled = con.execute(
            f"UPDATE {schema}.runtime_runs SET state='succeeded', published_generation=?, "
            "attempt_fence=?, updated_at=? WHERE run_id=? AND state IN "
            "('queued','running','cancel_requested') AND "
            "(attempt_fence IS NULL OR attempt_fence<=?) RETURNING run_id",
            [generation, lease.fence, datetime.now(UTC).replace(tzinfo=None), run_id, lease.fence],
        ).fetchone()
        if reconciled is None:
            return None
        record_run_event(
            run_id,
            "lifecycle",
            {"state": "succeeded"},
            con=con,
            namespace=namespace,
            _in_transaction=True,
        )
    return RunReceipt(run_id, "succeeded", generation)


def _check_expected_base_manifest(plan: RefreshPlan, *, con: Any, namespace: str) -> None:
    if not plan.expected_base_manifest_bound:
        return
    exists = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE lower(table_schema)=lower(?) AND table_name='runtime_manifests'",
        [namespace],
    ).fetchone()
    current = (
        con.execute(
            f'SELECT generation FROM "{namespace}".runtime_manifests WHERE dataset=?',
            [plan.dataset],
        ).fetchone()
        if exists is not None
        else None
    )
    actual = current[0] if current is not None else None
    if actual != plan.expected_base_manifest:
        raise ValueError("expected base manifest does not match current publication")


def _copy_result_partitions(
    con: Any, *, schema: str, run_id: str, dataset: str, generation: str
) -> None:
    previous = con.execute(
        f"SELECT DISTINCT result.partition FROM {schema}.runtime_run_results result "
        f"JOIN {schema}.runtime_run_publications prior ON prior.run_id=result.run_id "
        "WHERE prior.dataset=? AND prior.generation=? AND prior.run_id<>? "
        "ORDER BY result.partition",
        [dataset, generation, run_id],
    ).fetchall()
    if not previous:
        version = con.execute(
            f"SELECT version FROM {schema}.runtime_run_publications WHERE run_id=?",
            [run_id],
        ).fetchone()
        if version is not None:
            prior_generation = con.execute(
                f"SELECT generation FROM {schema}.runtime_publications "
                "WHERE dataset=? AND version<? ORDER BY version DESC LIMIT 1",
                [dataset, version[0]],
            ).fetchone()
            if prior_generation is not None:
                previous = con.execute(
                    f"SELECT DISTINCT result.partition FROM {schema}.runtime_run_results result "
                    f"JOIN {schema}.runtime_run_publications prior ON prior.run_id=result.run_id "
                    "WHERE prior.dataset=? AND prior.generation=? AND prior.run_id<>? "
                    "ORDER BY result.partition",
                    [dataset, prior_generation[0], run_id],
                ).fetchall()
    for (partition,) in previous:
        con.execute(
            f"INSERT INTO {schema}.runtime_run_results VALUES (?, ?) ON CONFLICT DO NOTHING",
            [run_id, partition],
        )


def _encode(table: pa.Table | ArrowOutput) -> bytes:
    if isinstance(table, ArrowOutput):
        return table.to_bytes()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _decode(payload: bytes) -> pa.Table | ArrowOutput:
    if payload.startswith(_PREFIX):
        return ArrowOutput.from_bytes(payload)
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
    table: pa.Table | ArrowOutput,
    plan: RefreshPlan,
    outputs: Mapping[str, pa.Table | ArrowOutput],
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


def _cancellation_requested(
    con: Any, schema: str, run_id: str, cancelled: Callable[[], bool] | None
) -> bool:
    current = con.execute(
        f"SELECT state FROM {schema}.runtime_runs WHERE run_id=?", [run_id]
    ).fetchone()
    return (current is not None and current[0] == "cancel_requested") or (
        cancelled is not None and cancelled()
    )


def _finish_run(
    *,
    plan: RefreshPlan,
    outputs: Mapping[str, pa.Table | ArrowOutput],
    run_id: str,
    lease: Lease,
    cancelled: Callable[[], bool] | None,
    publish: Callable[[RefreshPlan, Mapping[str, pa.Table | ArrowOutput], Lease], Any],
    con: Any,
    schema: str,
    runtime_store: RuntimeStore,
    leases: LeaseStore,
) -> RunReceipt:
    if _cancellation_requested(con, schema, run_id, cancelled):
        with event_transaction(con, namespace=runtime_store.namespace):
            terminal = con.execute(
                f"UPDATE {schema}.runtime_runs SET state='cancelled', updated_at=? "
                "WHERE run_id=? AND state IN ('running','cancel_requested') "
                "AND attempt_fence=? RETURNING run_id",
                [datetime.now(UTC).replace(tzinfo=None), run_id, lease.fence],
            ).fetchone()
            if terminal is None:
                raise ValueError("lease attempt was fenced before cancellation")
            record_run_event(
                run_id,
                "lifecycle",
                {"state": "cancelled"},
                con=con,
                namespace=runtime_store.namespace,
                _in_transaction=True,
            )
        return RunReceipt(run_id, "cancelled", None)
    leases.assert_current(lease)
    published = publish(plan, MappingProxyType(dict(outputs)), lease)
    generation = getattr(published, "generation", published)
    generation_text = str(generation) if generation is not None else None
    if generation_text is not None and plan.tasks:
        runtime_store.replace_published_snapshot(
            plan.dataset, generation_text, plan.snapshot_id, lease=lease
        )
    with event_transaction(con, namespace=runtime_store.namespace):
        dependencies = {dependency for task in plan.tasks for dependency in task.dependencies}
        has_results = any(task.task_id not in dependencies for task in plan.tasks)
        if not has_results:
            publication_table = con.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE lower(table_schema)=lower(?) AND table_name='runtime_run_publications'",
                [runtime_store.namespace],
            ).fetchone()
            publication = (
                con.execute(
                    f"SELECT dataset,generation FROM {schema}.runtime_run_publications "
                    "WHERE run_id=?",
                    [run_id],
                ).fetchone()
                if publication_table is not None
                else None
            )
            if publication is not None:
                _copy_result_partitions(
                    con,
                    schema=schema,
                    run_id=run_id,
                    dataset=publication[0],
                    generation=publication[1],
                )
        terminal = con.execute(
            f"UPDATE {schema}.runtime_runs SET state='succeeded', published_generation=?, "
            "updated_at=? WHERE run_id=? AND state='running' AND attempt_fence=? "
            "RETURNING run_id",
            [generation_text, datetime.now(UTC).replace(tzinfo=None), run_id, lease.fence],
        ).fetchone()
        if terminal is None:
            raise ValueError("lease attempt was fenced before recording success")
        record_run_event(
            run_id,
            "lifecycle",
            {"state": "succeeded"},
            con=con,
            namespace=runtime_store.namespace,
            _in_transaction=True,
        )
    return RunReceipt(run_id, "succeeded", generation_text)
