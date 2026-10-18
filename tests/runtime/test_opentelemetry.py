from datetime import UTC, datetime
from threading import Event

from terrine.runtime.events import RunEvent


def test_otel_projects_only_allowlisted_lifecycle_state():
    from terrine.adapters.opentelemetry import OpenTelemetryAdapter

    spans = []
    measurements = []
    exported = Event()

    class Tracer:
        def start_span(self, name, *, attributes):
            spans.append((name, attributes))

            class Span:
                def end(self):
                    exported.set()

            return Span()

    class Counter:
        def add(self, value, attributes):
            measurements.append((value, attributes))

    class Meter:
        def create_counter(self, name):
            assert name == "terrine.runtime.lifecycle"
            return Counter()

    adapter = OpenTelemetryAdapter(Tracer(), Meter())
    try:
        adapter.observe_event(
            RunEvent(
                "sensitive-run-id",
                1,
                datetime(2026, 10, 18, tzinfo=UTC),
                "lifecycle",
                {
                    "state": "succeeded",
                    "request_key": "secret-key",
                    "raw_sql": "SELECT secret",
                    "dataset": "customer-name",
                },
            )
        )
        adapter.observe_event(
            RunEvent("another-run", 2, datetime(2026, 10, 18, tzinfo=UTC), "progress", {})
        )
        assert exported.wait(timeout=2)
    finally:
        adapter.close()

    assert spans == [("terrine.runtime.lifecycle", {"terrine.run.state": "succeeded"})]
    assert measurements == [(1, {"terrine.run.state": "succeeded"})]


def test_otel_exporter_failures_do_not_escape_runtime_callback():
    from terrine.adapters.opentelemetry import OpenTelemetryAdapter

    attempted = Event()

    class Tracer:
        def start_span(self, name, *, attributes):
            attempted.set()
            raise RuntimeError("export failed")

    class Counter:
        def add(self, value, attributes):
            raise RuntimeError("export failed")

    class Meter:
        def create_counter(self, name):
            return Counter()

    adapter = OpenTelemetryAdapter(Tracer(), Meter())
    try:
        adapter.observe_event(
            RunEvent("run", 1, datetime(2026, 10, 18, tzinfo=UTC), "lifecycle", {"state": "failed"})
        )
        assert attempted.wait(timeout=2)
    finally:
        adapter.close()
