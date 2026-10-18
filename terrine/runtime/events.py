from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from terrine.runtime.state import RuntimeStore

__all__ = ["RunEvent", "EventPage", "read_run_events", "record_run_event"]

_MAX_LIMIT = 1000
_RETENTION = 10000


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


def record_run_event(
    run_id: str, kind: str, payload: dict[str, Any], *, con: Any, namespace: str
) -> None:
    if (
        kind != "phase"
        or set(payload) != {"phase"}
        or not isinstance(payload["phase"], str)
        or len(payload["phase"]) > 128
    ):
        raise ValueError("event fields are invalid or contain forbidden data")
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    _ensure_events(con, schema)
    sequence = con.execute(
        f"SELECT coalesce(max(sequence), 0)+1 FROM {schema}.runtime_run_events WHERE run_id=?",
        [run_id],
    ).fetchone()[0]
    con.execute(
        f"INSERT INTO {schema}.runtime_run_events VALUES (?, ?, ?, ?, ?)",
        [run_id, sequence, datetime.now(UTC).replace(tzinfo=None), kind, json.dumps(payload)],
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
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    _ensure_events(con, schema)
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
    oldest = con.execute(
        f"SELECT min(sequence) FROM {schema}.runtime_run_events WHERE run_id=?", [run_id]
    ).fetchone()[0]
    if after is not None and oldest is not None and after_sequence < oldest - 1:
        cursor_gap = True
    cap = min(limit, _MAX_LIMIT)
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


def _cursor(run_id: str, sequence: int) -> str:
    raw = json.dumps([run_id, sequence], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")
