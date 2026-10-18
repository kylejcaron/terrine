from __future__ import annotations

import json
import os
import socket
import socketserver
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from terrine.runtime.events import read_run_events
from terrine.runtime.state import RuntimeStore
from terrine.runtime.status import cancel_run, get_run

__all__ = ["ControlEndpoint", "ControlServer", "ControlClient"]


@dataclass(frozen=True, slots=True)
class ControlEndpoint:
    path: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", Path(self.path).expanduser().resolve())


class ControlServer:
    """Owner-local Unix socket that serves control requests on the owner's connection."""

    def __init__(self, store: RuntimeStore, *, namespace: str, endpoint: ControlEndpoint) -> None:
        if store.namespace != namespace:
            raise ValueError("control namespace does not match runtime store")
        self.store = store
        self.namespace = namespace
        self.endpoint = endpoint
        self._server: socketserver.UnixStreamServer | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        if self._server is not None:
            return
        self.endpoint.path.parent.mkdir(parents=True, exist_ok=True)
        server = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                line = self.rfile.readline(65537)
                if not line or len(line) > 65536:
                    return
                try:
                    request = json.loads(line)
                    with server._lock:
                        response = server._dispatch(request)
                    self.wfile.write(
                        json.dumps({"ok": True, "value": response}, default=str).encode() + b"\n"
                    )
                except Exception:
                    self.wfile.write(b'{"ok":false,"code":"terrine.runtime.control_unavailable"}\n')

        self.endpoint.path.unlink(missing_ok=True)
        self._server = socketserver.ThreadingUnixStreamServer(str(self.endpoint.path), Handler)
        os.chmod(self.endpoint.path, 0o600)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        if self._server is None:
            return
        server, self._server = self._server, None
        server.shutdown()
        server.server_close()
        if self._thread is not None:
            self._thread.join()
            self._thread = None
        self.endpoint.path.unlink(missing_ok=True)

    def _dispatch(self, request: dict[str, Any]) -> Any:
        if request.get("namespace") != self.namespace:
            raise ValueError("control namespace mismatch")
        operation = request.get("operation")
        run_id = request.get("run_id")
        if operation == "status":
            if not isinstance(run_id, str) or not run_id:
                raise ValueError("run ID is required")
            return asdict(get_run(run_id, con=self.store.con, namespace=self.namespace))
        if operation == "events":
            if not isinstance(run_id, str) or not run_id:
                raise ValueError("run ID is required")
            page = read_run_events(
                run_id,
                con=self.store.con,
                namespace=self.namespace,
                after=request.get("after"),
                limit=request.get("limit", 100),
            )
            return {
                "events": [asdict(event) for event in page.events],
                "next_cursor": page.next_cursor,
                "has_more": page.has_more,
                "cursor_gap": page.cursor_gap,
            }
        if operation == "cancel":
            if not isinstance(run_id, str) or not run_id:
                raise ValueError("run ID is required")
            result = cancel_run(run_id, con=self.store.con, namespace=self.namespace)
            return {
                "run_id": result.run_id,
                "disposition": result.disposition,
                "status": asdict(result.status),
            }
        if operation == "request_key":
            from terrine.runtime.recipe import DestinationRef

            destination = request.get("destination")
            if not isinstance(destination, dict):
                raise ValueError("destination reference is required")
            target = DestinationRef(**destination)
            schema = self.store._schema
            self.store.con.execute(
                f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS request_key VARCHAR"
            )
            self.store.con.execute(
                f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS "
                "destination_json VARCHAR"
            )
            row = self.store.con.execute(
                f"SELECT run_id,state,published_generation FROM {schema}.runtime_runs "
                "WHERE request_key=? AND destination_json=?",
                [
                    request["request_key"],
                    json.dumps(asdict(target), sort_keys=True, separators=(",", ":")),
                ],
            ).fetchone()
            return (
                {"run_id": row[0], "state": row[1], "published_generation": row[2]} if row else None
            )
        raise ValueError("unsupported control operation")


class ControlClient:
    def __init__(self, endpoint: ControlEndpoint, *, namespace: str) -> None:
        self.endpoint = endpoint
        self.namespace = namespace

    def _request(self, operation: str, **payload: Any) -> Any:
        request = json.dumps({"namespace": self.namespace, "operation": operation, **payload})
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(2)
            client.connect(str(self.endpoint.path))
            client.sendall(request.encode() + b"\n")
            response = json.loads(client.makefile("rb").readline(65537))
        if not response.get("ok"):
            raise ConnectionError(response.get("detail", "owner control endpoint is unavailable"))
        return response["value"]

    def get_run(self, run_id: str):
        return self._request("status", run_id=run_id)

    def read_run_events(self, run_id: str, *, after: str | None = None, limit: int = 100):
        return self._request("events", run_id=run_id, after=after, limit=limit)

    def cancel_run(self, run_id: str):
        return self._request("cancel", run_id=run_id)

    def find_run_by_request_key(self, request_key: str, *, destination: Any):
        return self._request(
            "request_key", request_key=request_key, destination=asdict(destination)
        )
