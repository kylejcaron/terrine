from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from terrine.errors import RefusalSpec
from terrine.runtime.leases import LeaseStore
from terrine.runtime.state import RuntimeStore

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
        "state VARCHAR NOT NULL, published_generation VARCHAR, attempt_fence BIGINT)"
    )
    leases = LeaseStore(con, namespace=namespace)
    leases.initialize()
    schema = store._schema
    con.execute(f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP")
    con.execute(
        f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP"
    )
    row = con.execute(
        f"SELECT state, attempt_fence, updated_at, heartbeat_at, published_generation "
        f"FROM {schema}.runtime_runs WHERE run_id=?",
        [run_id],
    ).fetchone()
    if row is None:
        raise _UNAVAILABLE.error()
    lease = con.execute(
        f"SELECT expires_at FROM {schema}.runtime_leases l JOIN {schema}.runtime_runs r "
        "ON l.resource='dataset:' || r.dataset WHERE r.run_id=?",
        [run_id],
    ).fetchone()
    return RunStatus(
        run_id,
        row[0],
        row[1],
        row[2],
        row[3],
        lease[0] if lease else None,
        pinned_result_ref=row[4],
    )


def cancel_run(run_id: str, *, con: Any, namespace: str) -> CancelReceipt:
    status = get_run(run_id, con=con, namespace=namespace)
    if status.durable_state in {"succeeded", "failed", "cancelled"}:
        return CancelReceipt(run_id, "already_terminal", status)
    if status.durable_state == "cancel_requested":
        return CancelReceipt(run_id, "already_requested", status)
    schema = RuntimeStore(con, namespace=namespace)._schema
    con.execute(
        f"UPDATE {schema}.runtime_runs SET state='cancel_requested', "
        "updated_at=current_timestamp WHERE run_id=? AND state IN ('queued','running')",
        [run_id],
    )
    updated = get_run(run_id, con=con, namespace=namespace)
    disposition = "requested" if updated.durable_state == "cancel_requested" else "already_terminal"
    return CancelReceipt(run_id, disposition, updated)
