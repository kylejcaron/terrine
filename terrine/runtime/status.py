from __future__ import annotations

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
        row = con.execute(
            f"SELECT state, attempt_fence, {updated_at}, {heartbeat_at}, published_generation "
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
        return RunStatus(
            run_id,
            row[0],
            row[1],
            _utc(row[2]),
            _utc(row[3]),
            _utc(lease[0]) if lease else None,
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
                    f"UPDATE {store._schema}.runtime_runs SET state=? "
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
