from __future__ import annotations

import errno
import json
import os
import socket
import socketserver
import stat
import threading
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, overload

if TYPE_CHECKING:
    from terrine.runtime.planning import PlanInvocation, PlanRequest

from terrine.errors import RefusalSpec, TerrineError
from terrine.runtime.events import (
    EventPage,
    RunEvent,
    read_run_events,
)
from terrine.runtime.execute import RunReceipt
from terrine.runtime.state import RuntimeStore
from terrine.runtime.status import (
    CancelReceipt,
    RunStatus,
    _recover_interrupted_runs,
    cancel_run,
    get_run,
)

__all__ = ["ControlEndpoint", "ControlServer", "ControlClient"]

_MAX_CONTROL_MESSAGE_BYTES = 16_777_216
_MESSAGE_TOO_LARGE = RefusalSpec(
    "terrine.runtime.control_message_too_large",
    "Owner control message exceeds the configured limit.",
)
_CONTROL_UNAVAILABLE = RefusalSpec(
    "terrine.runtime.control_unavailable", "Owner control request was refused."
)


@dataclass(frozen=True, slots=True)
class ControlEndpoint:
    path: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", Path(os.path.abspath(Path(self.path).expanduser())))


class ControlServer:
    """Owner-local Unix socket that serves control requests on the owner's connection."""

    def __init__(
        self,
        store: RuntimeStore,
        *,
        namespace: str,
        endpoint: ControlEndpoint,
        executor: Any | None = None,
        plan_planner: Any | None = None,
        resolver: Any | None = None,
        snapshot_authority: Any | None = None,
    ) -> None:
        if store.namespace != namespace:
            raise ValueError("control namespace does not match runtime store")
        self.store = store
        self.namespace = namespace
        self.endpoint = endpoint
        self.executor = executor
        self.plan_planner = plan_planner
        self.resolver = resolver
        self.snapshot_authority = snapshot_authority
        self._server: socketserver.UnixStreamServer | None = None
        self._thread: threading.Thread | None = None
        self._endpoint_identity: tuple[int, int] | None = None

    def start(self) -> None:
        if self._server is not None:
            return
        self.endpoint.path.parent.mkdir(parents=True, exist_ok=True)
        _recover_interrupted_runs(self.store.con, namespace=self.namespace)
        server = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                self.connection.settimeout(2)
                line = self.rfile.readline(_MAX_CONTROL_MESSAGE_BYTES + 2)
                if not line:
                    return
                if len(line) > _MAX_CONTROL_MESSAGE_BYTES + 1:
                    response = json.dumps({"ok": False, "code": _MESSAGE_TOO_LARGE.code}).encode()
                    self.wfile.write(response + b"\n")
                    return
                con = server.store.con.cursor()
                try:
                    request = json.loads(line)
                    self.connection.settimeout(
                        None if request.get("operation") in {"cancel", "run", "plan"} else 2
                    )
                    response = server._dispatch(request, con=con)
                    response_bytes = json.dumps(
                        {"ok": True, "value": response}, default=str
                    ).encode()
                    if len(response_bytes) > _MAX_CONTROL_MESSAGE_BYTES:
                        response_bytes = json.dumps(
                            {"ok": False, "code": _MESSAGE_TOO_LARGE.code}
                        ).encode()
                    self.wfile.write(response_bytes + b"\n")
                except Exception as error:
                    code = getattr(error, "code", _CONTROL_UNAVAILABLE.code)
                    self.wfile.write(json.dumps({"ok": False, "code": code}).encode() + b"\n")
                finally:
                    con.close()

        class OwnerUnixStreamServer(socketserver.ThreadingUnixStreamServer):
            daemon_threads = True

        try:
            mode = self.endpoint.path.lstat().st_mode
        except FileNotFoundError:
            mode = None
        if mode is not None:
            if not stat.S_ISSOCK(mode):
                raise FileExistsError("control endpoint exists and is not a socket")
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                probe.connect(str(self.endpoint.path))
            except OSError as error:
                if error.errno != errno.ECONNREFUSED:
                    raise
                self.endpoint.path.unlink()
            else:
                raise FileExistsError("control endpoint is already active")
            finally:
                probe.close()
        self._server = OwnerUnixStreamServer(str(self.endpoint.path), Handler)
        os.chmod(self.endpoint.path, 0o600)
        socket_stat = self.endpoint.path.stat()
        self._endpoint_identity = (socket_stat.st_dev, socket_stat.st_ino)
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
        try:
            current = self.endpoint.path.lstat()
        except FileNotFoundError:
            current = None
        if (
            current is not None
            and self._endpoint_identity == (current.st_dev, current.st_ino)
            and stat.S_ISSOCK(current.st_mode)
        ):
            self.endpoint.path.unlink()
        self._endpoint_identity = None

    def _dispatch(self, request: dict[str, Any], *, con: Any) -> Any:
        if request.get("namespace") != self.namespace:
            raise ValueError("control namespace mismatch")
        _recover_interrupted_runs(con, namespace=self.namespace)
        operation = request.get("operation")
        run_id = request.get("run_id")
        if operation == "plan":
            if "plan_request" in request:
                if self.plan_planner is None:
                    raise ValueError("owner fresh-planning service is unavailable")
                from terrine.runtime.planning import PlanRequest

                planning_request = PlanRequest.from_dict(request["plan_request"])
                invocation = self.plan_planner.plan(
                    planning_request, con=con, namespace=self.namespace
                )
                return invocation.to_dict()
            from terrine.runtime.planner import RefreshPlan

            plan = RefreshPlan.deserialize(request["serialized"])
            if plan.namespace != self.namespace:
                raise ValueError("plan namespace does not match owner")
            return plan.serialize().decode("utf-8")
        if operation == "run":
            from terrine.runtime.api import run_serialized_plan
            from terrine.runtime.recipe import DestinationRef

            destination_data = request.get("destination")
            destination = (
                DestinationRef(**destination_data) if destination_data is not None else None
            )
            receipt = run_serialized_plan(
                request["serialized"],
                con=con,
                namespace=self.namespace,
                request_key=request.get("request_key"),
                destination=destination,
                executor=self.executor,
                resolver=self.resolver,
                snapshot_authority=self.snapshot_authority,
            )
            return asdict(receipt)

        if operation == "status":
            if not isinstance(run_id, str) or not run_id:
                raise ValueError("run ID is required")
            return asdict(get_run(run_id, con=con, namespace=self.namespace))
        if operation == "events":
            if not isinstance(run_id, str) or not run_id:
                raise ValueError("run ID is required")
            page = read_run_events(
                run_id,
                con=con,
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
            result = cancel_run(run_id, con=con, namespace=self.namespace)
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
            from terrine.runtime.api import find_run_by_request_key

            receipt = find_run_by_request_key(
                request["request_key"],
                destination=target,
                con=con,
                namespace=self.namespace,
            )
            return asdict(receipt) if receipt is not None else None
        raise ValueError("unsupported control operation")


class ControlClient:
    def __init__(self, endpoint: ControlEndpoint, *, namespace: str) -> None:
        self.endpoint = endpoint
        self.namespace = namespace

    def _request(self, operation: str, **payload: Any) -> Any:
        request = json.dumps({"namespace": self.namespace, "operation": operation, **payload})
        encoded = request.encode()
        if len(encoded) > _MAX_CONTROL_MESSAGE_BYTES:
            raise _MESSAGE_TOO_LARGE.error()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(None if operation in {"cancel", "run", "plan"} else 2)
            client.connect(str(self.endpoint.path))
            client.sendall(encoded + b"\n")
            with client.makefile("rb") as response_stream:
                line = response_stream.readline(_MAX_CONTROL_MESSAGE_BYTES + 2)
            if not line.endswith(b"\n") or len(line) > _MAX_CONTROL_MESSAGE_BYTES + 1:
                raise ConnectionError("owner control response exceeded its limit")
            response = json.loads(line)
        if not response.get("ok"):
            raise TerrineError(
                _CONTROL_UNAVAILABLE.template,
                code=response.get("code", _CONTROL_UNAVAILABLE.code),
                context={},
            )
        return response["value"]

    @overload
    def plan(self, serialized: bytes | str) -> str: ...

    @overload
    def plan(self, serialized: PlanRequest) -> PlanInvocation: ...

    def plan(self, serialized: bytes | str | PlanRequest) -> str | PlanInvocation:
        from terrine.runtime.planning import PlanInvocation, PlanRequest

        if isinstance(serialized, PlanRequest):
            value = self._request("plan", plan_request=serialized.to_dict())
            return PlanInvocation.from_dict(value)
        payload = serialized.decode("utf-8") if isinstance(serialized, bytes) else serialized
        return self._request("plan", serialized=payload)

    def run(
        self,
        serialized: bytes | str,
        *,
        request_key: str | None = None,
        destination: Any | None = None,
    ) -> RunReceipt:
        payload = serialized.decode("utf-8") if isinstance(serialized, bytes) else serialized
        value = self._request(
            "run",
            serialized=payload,
            request_key=request_key,
            destination=asdict(destination) if destination is not None else None,
        )
        return RunReceipt(value["run_id"], value["state"], value["published_generation"])

    def get_run(self, run_id: str) -> RunStatus:
        return _status(self._request("status", run_id=run_id))

    def read_run_events(
        self, run_id: str, *, after: str | None = None, limit: int = 100
    ) -> EventPage:
        value = self._request("events", run_id=run_id, after=after, limit=limit)
        events = tuple(
            RunEvent(
                event["run_id"],
                event["sequence"],
                datetime.fromisoformat(event["at"]),
                event["kind"],
                event["payload"],
            )
            for event in value["events"]
        )
        return EventPage(events, value["next_cursor"], value["has_more"], value["cursor_gap"])

    def cancel_run(self, run_id: str) -> CancelReceipt:
        value = self._request("cancel", run_id=run_id)
        return CancelReceipt(run_id, value["disposition"], _status(value["status"]))

    def find_run_by_request_key(self, request_key: str, *, destination: Any) -> RunReceipt | None:
        value = self._request(
            "request_key", request_key=request_key, destination=asdict(destination)
        )
        if value is None:
            return None
        return RunReceipt(value["run_id"], value["state"], value["published_generation"])


def _status(value: dict[str, Any]) -> RunStatus:
    for field in ("updated_at", "heartbeat_at", "lease_expires_at"):
        if value[field] is not None and isinstance(value[field], str):
            value[field] = datetime.fromisoformat(value[field])
    return RunStatus(**value)
