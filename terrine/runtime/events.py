from __future__ import annotations

import base64
import json
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from terrine.runtime.state import RuntimeStore

__all__ = ["RunEvent", "EventPage", "read_run_events", "record_run_event"]
_MAX_LIMIT = 1000
_RETENTION = 10000
_EVENT_WRITE_LOCK = threading.RLock()


@dataclass(frozen=True, slots=True)
class RunEvent:
    run_id: str
    sequence: int
    at: datetime
    kind: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class EventPage:
    events: tuple[RunEvent, ...]
    next_cursor: str | None
    has_more: bool
    cursor_gap: bool


def _ensure_events(con: Any, schema: str) -> None:
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_run_events ("
        "run_id VARCHAR NOT NULL, sequence BIGINT NOT NULL, event_at TIMESTAMP NOT NULL, "
        "kind VARCHAR NOT NULL, payload VARCHAR NOT NULL, PRIMARY KEY(run_id, sequence))"
    )


@contextmanager
def event_transaction(con: Any, *, namespace: str):
    """Serialize owner event writes with the state changes they describe."""
    store = RuntimeStore(con, namespace=namespace)
    with _EVENT_WRITE_LOCK:
        store.initialize()
        _ensure_events(con, store._schema)
        con.execute("BEGIN TRANSACTION")
        try:
            yield
        except BaseException:
            con.execute("ROLLBACK")
            raise
        else:
            con.execute("COMMIT")


@contextmanager
def event_read_transaction(con: Any, *, namespace: str):
    """Read run state and event cursors from one owner-consistent snapshot."""
    store = RuntimeStore(con, namespace=namespace)
    with _EVENT_WRITE_LOCK:
        store.initialize()
        _ensure_events(con, store._schema)
        con.execute("BEGIN TRANSACTION")
        try:
            yield
        except BaseException:
            con.execute("ROLLBACK")
            raise
        else:
            con.execute("COMMIT")


def record_run_event(
    run_id: str,
    kind: str,
    payload: dict[str, Any],
    *,
    con: Any,
    namespace: str,
    at: datetime | None = None,
    _in_transaction: bool = False,
) -> None:
    valid = (
        kind == "phase"
        and set(payload) == {"phase"}
        and isinstance(payload["phase"], str)
        and len(payload["phase"]) <= 128
    ) or (
        kind == "lifecycle"
        and set(payload) == {"state"}
        and payload["state"]
        in {"queued", "running", "cancel_requested", "succeeded", "failed", "cancelled"}
    )
    if not run_id or not valid:
        raise ValueError("event fields are invalid or contain forbidden data")
    event_at = at or datetime.now(UTC)
    event_at = event_at.replace(tzinfo=UTC) if event_at.tzinfo is None else event_at.astimezone(UTC)
    schema = RuntimeStore(con, namespace=namespace)._schema
    if not _in_transaction:
        with event_transaction(con, namespace=namespace):
            record_run_event(
                run_id,
                kind,
                payload,
                con=con,
                namespace=namespace,
                at=event_at,
                _in_transaction=True,
            )
        return
    if kind == "phase":
        previous = con.execute(
            f"SELECT event_at FROM {schema}.runtime_run_events "
            "WHERE run_id=? AND kind='phase' ORDER BY sequence DESC LIMIT 1",
            [run_id],
        ).fetchone()
        if (
            previous is not None
            and (event_at - previous[0].replace(tzinfo=UTC)).total_seconds() < 1
        ):
            return
    sequence = con.execute(
        f"SELECT coalesce(max(sequence), 0)+1 FROM {schema}.runtime_run_events WHERE run_id=?",
        [run_id],
    ).fetchone()[0]
    con.execute(
        f"INSERT INTO {schema}.runtime_run_events VALUES (?, ?, ?, ?, ?)",
        [run_id, sequence, event_at.replace(tzinfo=None), kind, json.dumps(payload)],
    )
    con.execute(
        f"DELETE FROM {schema}.runtime_run_events WHERE run_id=? AND sequence <= ?",
        [run_id, sequence - _RETENTION],
    )


def read_run_events(
    run_id: str, *, con: Any, namespace: str, after: str | None = None, limit: int = 100
) -> EventPage:
    if not run_id or isinstance(limit, bool) or limit < 1:
        raise ValueError("run_id and positive event limit are required")
    schema = RuntimeStore(con, namespace=namespace)._schema

    after_sequence = 0
    cursor_gap = False
    if after is not None:
        try:
            decoded = base64.urlsafe_b64decode(after + "=" * (-len(after) % 4))
            cursor_run, raw_sequence = json.loads(decoded)
            if cursor_run != run_id:
                raise ValueError
            after_sequence = int(raw_sequence)
        except (ValueError, TypeError, json.JSONDecodeError):
            cursor_gap = True
    cap = min(limit, _MAX_LIMIT)
    with event_read_transaction(con, namespace=namespace):
        oldest = con.execute(
            f"SELECT min(sequence) FROM {schema}.runtime_run_events WHERE run_id=?", [run_id]
        ).fetchone()[0]
        if after is not None and oldest is not None and after_sequence < oldest - 1:
            cursor_gap = True
        rows = con.execute(
            f"SELECT sequence, event_at, kind, payload FROM {schema}.runtime_run_events "
            "WHERE run_id=? AND sequence>? ORDER BY sequence LIMIT ?",
            [run_id, after_sequence, cap + 1],
        ).fetchall()
    selected = rows[:cap]
    events = tuple(
        RunEvent(run_id, sequence, at.replace(tzinfo=UTC), kind, json.loads(payload))
        for sequence, at, kind, payload in selected
    )
    next_cursor = _cursor(run_id, events[-1].sequence) if events else after
    return EventPage(events, next_cursor, len(rows) > len(selected), cursor_gap)


def _latest_event_cursor(run_id: str, *, con: Any, namespace: str) -> str | None:
    store = RuntimeStore(con, namespace=namespace)
    with _EVENT_WRITE_LOCK:
        store.initialize()
        _ensure_events(con, store._schema)
        sequence = con.execute(
            f"SELECT max(sequence) FROM {store._schema}.runtime_run_events WHERE run_id=?",
            [run_id],
        ).fetchone()[0]
    return _cursor(run_id, sequence) if sequence is not None else None


def _cursor(run_id: str, sequence: int) -> str:
    raw = json.dumps([run_id, sequence], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")
