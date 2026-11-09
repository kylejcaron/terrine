from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from terrine.errors import RefusalSpec, TerrineError
from terrine.runtime.events import (
    _latest_event_cursor,
    event_read_transaction,
    event_transaction,
    record_run_event,
)
from terrine.runtime.schema import check_runtime_schema
from terrine.runtime.state import RuntimeStore
from terrine.runtime.synchronization import run_publication_lock

__all__ = ["RunStatus", "CancelReceipt", "get_run", "cancel_run"]

if TYPE_CHECKING:
    from terrine.runtime.planner import RefreshPlan

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
    check_runtime_schema(con, namespace)
    schema = store._schema
    with event_read_transaction(con, namespace=namespace):
        row = con.execute(
            "SELECT state, attempt_fence, updated_at, heartbeat_at, published_generation, "
            f"snapshot_id, task_count, error_code FROM {schema}.runtime_runs WHERE run_id=?",
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
        freshness = (
            "terminal"
            if row[0] in {"succeeded", "failed", "cancelled"}
            else "current"
            if lease_expires_at is not None and lease_expires_at > datetime.now(UTC)
            else "stale"
        )
        cost = json.loads(capture[0]) if capture is not None else {}
        cost.update(_observed_costs(run_id, con=con, schema=schema))
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
            cost=cost,
            readiness=json.loads(capture[1]) if capture is not None else None,
            error_code=row[7],
            pinned_result_ref=row[4],
            latest_event_cursor=_latest_event_cursor(run_id, con=con, namespace=namespace),
        )


def _observed_costs(run_id: str, *, con: Any, schema: str) -> dict[str, Any]:
    row = con.execute(
        "SELECT COALESCE(SUM(attempts),0), "
        "COUNT(*) FILTER (WHERE status='complete' AND "
        "(layer IS NULL OR output_rows IS NULL OR output_bytes IS NULL)), "
        "COALESCE(SUM(output_rows) FILTER (WHERE status='complete' AND layer='frame'),0), "
        "COALESCE(SUM(output_bytes) FILTER (WHERE status='complete' AND layer='frame'),0), "
        "COALESCE(SUM(output_bytes) FILTER (WHERE status='complete' AND layer!='frame'),0) "
        f"FROM {schema}.runtime_tasks WHERE run_id=?",
        [run_id],
    ).fetchone()
    return {
        "status": "observed",
        "scope": "completed_task_artifacts",
        "attempts": row[0],
        "output_rows": None if row[1] else row[2],
        "output_bytes": None if row[1] else row[3],
        "intermediate_output_bytes": None if row[1] else row[4],
        "scan_bytes": None,
        "scan_reason": "executor_scan_observation_unavailable",
    }


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
                    f"UPDATE {store._schema}.runtime_runs "
                    "SET state=?, updated_at=CURRENT_TIMESTAMP AT TIME ZONE 'UTC' "
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


def _recover_interrupted_runs(con: Any, *, namespace: str, plan: RefreshPlan | None = None) -> None:
    """Reconcile attempts whose fenced execution lease is no longer live."""
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    with event_transaction(con, namespace=namespace):
        stale = con.execute(
            f"SELECT r.run_id,r.state,r.attempt_fence,r.fingerprint,p.dataset,p.generation "
            f"FROM {schema}.runtime_runs r "
            f"LEFT JOIN {schema}.runtime_leases l ON l.resource='dataset:' || r.dataset "
            "AND l.owner=r.run_id AND l.fence=r.attempt_fence "
            f"LEFT JOIN {schema}.runtime_run_publications p ON p.run_id=r.run_id "
            "WHERE r.state IN ('queued','running','cancel_requested') "
            "AND (r.state<>'queued' OR "
            "(r.attempt_fence IS NOT NULL AND r.updated_at IS NOT NULL)) "
            "AND (l.resource IS NULL OR l.expires_at<=(current_timestamp AT TIME ZONE 'UTC')) "
            "ORDER BY r.run_id"
        ).fetchall()
        if not stale:
            return
        from terrine.runtime.execute import _verify_generation
        from terrine.runtime.planner import admit_execution
        from terrine.runtime.saved_plan import _stored_plan_identities

        stale_ids = {run_id for run_id, *_rest in stale}
        fingerprints = {
            fingerprint
            for _run_id, _state, _fence, fingerprint, dataset, _generation in stale
            if dataset is not None
        }
        fingerprints.update(
            fingerprint
            for run_id, (state, fingerprint, _references) in store._snapshot_pinned_runs().items()
            if state == "failed" or (run_id in stale_ids and state != "cancel_requested")
        )
        identities = _stored_plan_identities(
            con, schema=schema, namespace=namespace, fingerprints=fingerprints
        )
        for run_id, old_state, fence, fingerprint, dataset, published_generation in stale:
            publication = (dataset, published_generation) if dataset is not None else None
            publication_error = None
            if publication is not None:
                identity = (
                    (plan, None)
                    if plan is not None and plan.definition.fingerprint == fingerprint
                    else identities.get(fingerprint)
                )
                try:
                    if identity is None:
                        raise ValueError("publication has no verified plan identity")
                    recovering_plan, saved_version = identity
                    admit_execution(
                        recovering_plan, namespace=namespace, saved_plan_version=saved_version
                    )
                    _verify_generation(
                        recovering_plan, publication[1], con=con, namespace=namespace
                    )
                except (ValueError, TerrineError):
                    publication = None
                    publication_error = "terrine.runtime.saved_plan_mismatch"
            state = (
                "succeeded"
                if publication is not None
                else "cancelled"
                if old_state == "cancel_requested"
                else "failed"
            )
            generation = publication[1] if publication is not None else None
            if publication is not None:
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
                "AND l.fence=runtime_runs.attempt_fence "
                "AND l.expires_at>(current_timestamp AT TIME ZONE 'UTC')) "
                "RETURNING state",
                [
                    state,
                    generation,
                    datetime.now(UTC).replace(tzinfo=None),
                    (publication_error or _INTERRUPTED.code) if state == "failed" else None,
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
        store.release_terminal_run_snapshot_references(
            _in_transaction=True, _plan_identities=identities
        )
