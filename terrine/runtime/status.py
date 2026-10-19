from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from terrine.errors import RefusalSpec
from terrine.runtime.events import (
    _latest_event_cursor,
    event_read_transaction,
    event_transaction,
    record_run_event,
)
from terrine.runtime.leases import LeaseStore
from terrine.runtime.state import RuntimeStore
from terrine.runtime.synchronization import run_publication_lock

__all__ = ["RunStatus", "CancelReceipt", "get_run", "cancel_run"]

_UNAVAILABLE = RefusalSpec(
    "terrine.runtime.run_unavailable",
    "Run status is unavailable for the requested run.",
)


@dataclass(frozen=True, slots=True)
class RunStatus:
    run_id: str
    durable_state: str
    state_version: int | None = None
    updated_at: datetime | None = None
    heartbeat_at: datetime | None = None
    lease_expires_at: datetime | None = None
    freshness: Any | None = None
    phase: str | None = None
    progress: Any | None = None
    cost: Any | None = None
    readiness: Any | None = None
    error_code: str | None = None
    pinned_result_ref: str | None = None
    latest_event_cursor: str | None = None


@dataclass(frozen=True, slots=True)
class CancelReceipt:
    run_id: str
    disposition: str
    status: RunStatus


def get_run(run_id: str, *, con: Any, namespace: str) -> RunStatus:
    """Read persisted lifecycle state without inspecting customer source relations."""
    if not run_id:
        raise ValueError("run_id must not be empty")
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_runs ("
        "run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, fingerprint VARCHAR NOT NULL, "
        "state VARCHAR NOT NULL, published_generation VARCHAR, attempt_fence BIGINT, "
        "updated_at TIMESTAMP, heartbeat_at TIMESTAMP)"
    )
    leases = LeaseStore(con, namespace=namespace)
    leases.initialize()
    with event_read_transaction(con, namespace=namespace):
        columns = {
            item[0]
            for item in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE lower(table_schema)=lower(?) AND table_name='runtime_runs'",
                [namespace],
            ).fetchall()
        }
        updated_at = "updated_at" if "updated_at" in columns else "NULL"
        heartbeat_at = "heartbeat_at" if "heartbeat_at" in columns else "NULL"
        snapshot_id = "snapshot_id" if "snapshot_id" in columns else "NULL"
        task_count = "task_count" if "task_count" in columns else "NULL"
        error_code = "error_code" if "error_code" in columns else "NULL"
        row = con.execute(
            f"SELECT state, attempt_fence, {updated_at}, {heartbeat_at}, published_generation, "
            f"{snapshot_id}, {task_count}, {error_code} "
            f"FROM {schema}.runtime_runs WHERE run_id=?",
            [run_id],
        ).fetchone()
        if row is None:
            raise _UNAVAILABLE.error()
        lease = con.execute(
            f"SELECT l.expires_at FROM {schema}.runtime_leases l "
            f"JOIN {schema}.runtime_runs r ON l.resource='dataset:' || r.dataset "
            "AND l.owner=r.run_id AND l.fence=r.attempt_fence WHERE r.run_id=?",
            [run_id],
        ).fetchone()
        lease_expires_at = _utc(lease[0]) if lease else None
        progress = None
        if row[6] is not None:
            counts = con.execute(
                f"SELECT count(*) FILTER (WHERE status='complete'), "
                "count(*) FILTER (WHERE status='running') "
                f"FROM {schema}.runtime_tasks WHERE run_id=?",
                [run_id],
            ).fetchone()
            progress = {
                "completed_tasks": counts[0],
                "running_tasks": counts[1] if row[0] in {"running", "cancel_requested"} else 0,
                "total_tasks": row[6],
            }
        capture = con.execute(
            f"SELECT cost_json,readiness_json FROM {schema}.runtime_snapshot_captures "
            "WHERE snapshot_id=?",
            [row[5]],
        ).fetchone()
        phase = row[0]
        if row[0] == "running" and progress is not None:
            phase = (
                "publishing"
                if progress["completed_tasks"] == progress["total_tasks"]
                else "executing"
            )
        freshness = None
        if "heartbeat_at" in columns:
            freshness = (
                "terminal"
                if row[0] in {"succeeded", "failed", "cancelled"}
                else "current"
                if lease_expires_at is not None and lease_expires_at > datetime.now(UTC)
                else "stale"
            )
        return RunStatus(
            run_id,
            row[0],
            row[1],
            _utc(row[2]),
            _utc(row[3]),
            lease_expires_at,
            freshness=freshness,
            phase=phase,
            progress=progress,
            cost=json.loads(capture[0]) if capture is not None else None,
            readiness=json.loads(capture[1]) if capture is not None else None,
            error_code=row[7],
            pinned_result_ref=row[4],
            latest_event_cursor=_latest_event_cursor(run_id, con=con, namespace=namespace),
        )


def _utc(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


def cancel_run(run_id: str, *, con: Any, namespace: str) -> CancelReceipt:
    store = RuntimeStore(con, namespace=namespace)
    status = get_run(run_id, con=con, namespace=namespace)
    if status.durable_state in {"succeeded", "failed", "cancelled"}:
        return CancelReceipt(run_id, "already_terminal", status)
    row = con.execute(
        f"SELECT dataset FROM {store._schema}.runtime_runs WHERE run_id=?", [run_id]
    ).fetchone()
    if row is None:
        raise _UNAVAILABLE.error()
    columns = {
        column[0].casefold()
        for column in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE lower(table_schema)=lower(?) AND lower(table_name)='runtime_runs'",
            [namespace],
        ).fetchall()
    }
    updated_at = (
        ", updated_at=CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" if "updated_at" in columns else ""
    )
    with run_publication_lock(namespace, row[0]):
        with event_transaction(con, namespace=namespace):
            current = con.execute(
                f"SELECT state FROM {store._schema}.runtime_runs WHERE run_id=?",
                [run_id],
            ).fetchone()
            if current is None:
                raise _UNAVAILABLE.error()
            current_state = current[0]
            if current_state in {"succeeded", "failed", "cancelled"}:
                disposition = "already_terminal"
            elif current_state == "cancel_requested":
                disposition = "already_requested"
            elif current_state in {"queued", "running"}:
                next_state = "cancelled" if current_state == "queued" else "cancel_requested"
                transitioned = con.execute(
                    f"UPDATE {store._schema}.runtime_runs SET state=?{updated_at} "
                    "WHERE run_id=? AND state=? RETURNING run_id",
                    [next_state, run_id, current_state],
                ).fetchone()
                if transitioned is not None:
                    record_run_event(
                        run_id,
                        "lifecycle",
                        {"state": next_state},
                        con=con,
                        namespace=namespace,
                        _in_transaction=True,
                    )
                    disposition = "requested"
                else:
                    disposition = "already_terminal"
            else:
                disposition = "already_terminal"
        updated = get_run(run_id, con=con, namespace=namespace)
        return CancelReceipt(run_id, disposition, updated)


_INTERRUPTED = RefusalSpec(
    "terrine.runtime.owner_interrupted",
    "The runtime owner stopped before completing or publishing this run.",
)


def _recover_interrupted_runs(con: Any, *, namespace: str) -> None:
    """Reconcile attempts whose fenced execution lease is no longer live."""
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    exists = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE lower(table_schema)=lower(?) AND table_name='runtime_runs'",
        [namespace],
    ).fetchone()
    if exists is None:
        return
    con.execute(f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP")
    con.execute(f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS attempt_fence BIGINT")
    con.execute(f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS error_code VARCHAR")
    lease_table = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE lower(table_schema)=lower(?) AND table_name='runtime_leases'",
        [namespace],
    ).fetchone()
    if lease_table is None:
        return
    publications = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE lower(table_schema)=lower(?) AND table_name='runtime_run_publications'",
        [namespace],
    ).fetchone()
    with event_transaction(con, namespace=namespace):
        stale = con.execute(
            f"SELECT r.run_id,r.state,r.attempt_fence FROM {schema}.runtime_runs r "
            f"LEFT JOIN {schema}.runtime_leases l ON l.resource='dataset:' || r.dataset "
            "AND l.owner=r.run_id AND l.fence=r.attempt_fence "
            "WHERE r.state IN ('queued','running','cancel_requested') "
            "AND (r.state<>'queued' OR "
            "(r.attempt_fence IS NOT NULL AND r.updated_at IS NOT NULL)) "
            "AND (l.resource IS NULL OR l.expires_at<=current_timestamp) "
            "ORDER BY r.run_id"
        ).fetchall()
        for run_id, old_state, fence in stale:
            publication = (
                con.execute(
                    f"SELECT dataset,generation FROM {schema}.runtime_run_publications "
                    "WHERE run_id=?",
                    [run_id],
                ).fetchone()
                if publications is not None
                else None
            )
            state = (
                "succeeded"
                if publication is not None
                else "cancelled"
                if old_state == "cancel_requested"
                else "failed"
            )
            generation = publication[1] if publication is not None else None
            if publication is not None:
                con.execute(
                    f"CREATE TABLE IF NOT EXISTS {schema}.runtime_run_results "
                    "(run_id VARCHAR NOT NULL, partition VARCHAR NOT NULL, "
                    "PRIMARY KEY(run_id,partition))"
                )
                previous = con.execute(
                    f"SELECT DISTINCT result.partition FROM {schema}.runtime_run_results result "
                    f"JOIN {schema}.runtime_run_publications prior ON prior.run_id=result.run_id "
                    "WHERE prior.dataset=? AND prior.generation=? AND result.run_id<>? "
                    "ORDER BY result.partition",
                    [publication[0], publication[1], run_id],
                ).fetchall()
                for (partition,) in previous:
                    con.execute(
                        f"INSERT INTO {schema}.runtime_run_results VALUES (?, ?) "
                        "ON CONFLICT DO NOTHING",
                        [run_id, partition],
                    )
            transitioned = con.execute(
                f"UPDATE {schema}.runtime_runs SET state=?, "
                "published_generation=COALESCE(?,published_generation), updated_at=?, error_code=? "
                "WHERE run_id=? AND state=? AND attempt_fence IS NOT DISTINCT FROM ? "
                "AND NOT EXISTS ("
                f"SELECT 1 FROM {schema}.runtime_leases l "
                "WHERE l.resource='dataset:' || runtime_runs.dataset "
                "AND l.owner=runtime_runs.run_id "
                "AND l.fence=runtime_runs.attempt_fence AND l.expires_at>current_timestamp) "
                "RETURNING state",
                [
                    state,
                    generation,
                    datetime.now(UTC).replace(tzinfo=None),
                    _INTERRUPTED.code if state == "failed" else None,
                    run_id,
                    old_state,
                    fence,
                ],
            ).fetchone()
            if transitioned is not None:
                record_run_event(
                    run_id,
                    "lifecycle",
                    {"state": transitioned[0]},
                    con=con,
                    namespace=namespace,
                    _in_transaction=True,
                )
