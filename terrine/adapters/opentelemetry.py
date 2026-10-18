from __future__ import annotations

import queue
import threading
from typing import Any

__all__ = ["OpenTelemetryAdapter"]

_STATES = frozenset({"queued", "running", "cancel_requested", "succeeded", "failed", "cancelled"})


class OpenTelemetryAdapter:
    """Project safe lifecycle states to application-configured OpenTelemetry instruments."""

    def __init__(self, tracer: Any, meter: Any, *, queue_size: int = 128) -> None:
        if isinstance(queue_size, bool) or queue_size < 1:
            raise ValueError("queue_size must be positive")
        self._tracer = tracer
        self._events: queue.Queue[str] = queue.Queue(maxsize=queue_size)
        try:
            self._counter = meter.create_counter("terrine.runtime.lifecycle")
        except Exception:
            self._counter = None
        self._stopping = threading.Event()
        self._worker = threading.Thread(target=self._export, name="terrine-otel", daemon=True)
        self._worker.start()

    def observe_event(self, event: Any) -> None:
        """Queue an allowlisted lifecycle state without blocking the caller."""
        if event.kind != "lifecycle" or not isinstance(event.payload, dict):
            return
        state = event.payload.get("state")
        if not isinstance(state, str) or state not in _STATES:
            return
        try:
            self._events.put_nowait(state)
        except queue.Full:
            pass

    def close(self) -> None:
        """Request worker shutdown without waiting indefinitely on an exporter."""
        self._stopping.set()
        self._worker.join(timeout=0.05)

    def _export(self) -> None:
        while not self._stopping.is_set() or not self._events.empty():
            try:
                state = self._events.get(timeout=0.05)
            except queue.Empty:
                continue
            attributes = {"terrine.run.state": state}
            try:
                if self._counter is not None:
                    self._counter.add(1, attributes)
            except Exception:
                pass
            try:
                span = self._tracer.start_span("terrine.runtime.lifecycle", attributes=attributes)
                span.end()
            except Exception:
                pass
            finally:
                self._events.task_done()
