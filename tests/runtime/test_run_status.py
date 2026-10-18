import duckdb
import pytest

from terrine.errors import TerrineError
from terrine.runtime.status import get_run


def test_missing_run_has_typed_unavailable_status():
    con = duckdb.connect(":memory:")
    with pytest.raises(TerrineError) as error:
        get_run("missing", con=con, namespace="owner")
    assert error.value.code == "terrine.runtime.run_unavailable"


def test_status_reads_durable_run_and_preserves_unknown_metrics():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR, dataset VARCHAR, fingerprint VARCHAR, "
        "state VARCHAR, published_generation VARCHAR, attempt_fence BIGINT)"
    )
    con.execute("INSERT INTO owner.runtime_runs VALUES ('r1','d','f','running',NULL,2)")
    status = get_run("r1", con=con, namespace="owner")
    assert status.run_id == "r1"
    assert status.durable_state == "running"
    assert status.cost is None
    assert status.freshness is None


def test_cancel_request_persists_lifecycle_event():
    from terrine.runtime.events import read_run_events
    from terrine.runtime.status import cancel_run

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR, dataset VARCHAR, fingerprint VARCHAR, "
        "state VARCHAR, published_generation VARCHAR, attempt_fence BIGINT)"
    )
    con.execute("INSERT INTO owner.runtime_runs VALUES ('r1','d','f','running',NULL,1)")

    receipt = cancel_run("r1", con=con, namespace="owner")

    events = read_run_events("r1", con=con, namespace="owner").events
    assert receipt.disposition == "requested"
    assert [(event.kind, event.payload) for event in events] == [
        ("lifecycle", {"state": "cancel_requested"})
    ]


def test_cancel_updates_timestamp_when_column_is_available():
    from datetime import UTC, datetime

    from terrine.runtime.status import cancel_run

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")

    con.execute("SET TimeZone = 'America/New_York'")
    before_row = con.execute("SELECT CURRENT_TIMESTAMP AT TIME ZONE 'UTC'").fetchone()
    assert before_row is not None
    before = before_row[0]
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR, dataset VARCHAR, fingerprint VARCHAR, "
        "state VARCHAR, published_generation VARCHAR, attempt_fence BIGINT, updated_at TIMESTAMP)"
    )
    con.execute(
        "INSERT INTO owner.runtime_runs VALUES "
        "('r1','d','f','running',NULL,1,TIMESTAMP '2000-01-01')"
    )

    receipt = cancel_run("r1", con=con, namespace="owner")
    after_row = con.execute("SELECT CURRENT_TIMESTAMP AT TIME ZONE 'UTC'").fetchone()
    assert after_row is not None
    after = after_row[0]

    assert receipt.status.updated_at is not None
    assert datetime(2000, 1, 1, tzinfo=UTC) < receipt.status.updated_at
    assert before.replace(tzinfo=UTC) <= receipt.status.updated_at <= after.replace(tzinfo=UTC)


def test_repeated_cancel_returns_already_requested():
    from terrine.runtime.status import cancel_run

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR, dataset VARCHAR, fingerprint VARCHAR, "
        "state VARCHAR, published_generation VARCHAR, attempt_fence BIGINT)"
    )
    con.execute("INSERT INTO owner.runtime_runs VALUES ('r1','d','f','running',NULL,1)")

    first = cancel_run("r1", con=con, namespace="owner")
    second = cancel_run("r1", con=con, namespace="owner")

    assert first.disposition == "requested"
    assert second.disposition == "already_requested"


def test_status_reports_latest_event_cursor_for_resume():
    from terrine.runtime.events import read_run_events, record_run_event

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR, dataset VARCHAR, fingerprint VARCHAR, "
        "state VARCHAR, published_generation VARCHAR, attempt_fence BIGINT)"
    )
    con.execute("INSERT INTO owner.runtime_runs VALUES ('r1','d','f','running',NULL,1)")
    record_run_event("r1", "lifecycle", {"state": "running"}, con=con, namespace="owner")

    status = get_run("r1", con=con, namespace="owner")
    page = read_run_events("r1", con=con, namespace="owner", after=status.latest_event_cursor)

    assert (
        status.latest_event_cursor == read_run_events("r1", con=con, namespace="owner").next_cursor
    )
    assert page.events == ()


def test_status_cursor_and_state_share_a_consistent_snapshot(monkeypatch):
    import threading

    import terrine.runtime.status as status_module
    from terrine.runtime.events import event_transaction, read_run_events, record_run_event

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute(
        "CREATE TABLE owner.runtime_runs (run_id VARCHAR, dataset VARCHAR, fingerprint VARCHAR, "
        "state VARCHAR, published_generation VARCHAR, attempt_fence BIGINT)"
    )
    con.execute("INSERT INTO owner.runtime_runs VALUES ('r1','d','f','running',NULL,1)")
    record_run_event("r1", "lifecycle", {"state": "running"}, con=con, namespace="owner")
    writer_started = threading.Event()
    writer_done = threading.Event()

    def publish_terminal_event():
        connection = con.cursor()
        try:
            writer_started.set()
            with event_transaction(connection, namespace="owner"):
                connection.execute(
                    "UPDATE owner.runtime_runs SET state='succeeded' WHERE run_id='r1'"
                )
                record_run_event(
                    "r1",
                    "lifecycle",
                    {"state": "succeeded"},
                    con=connection,
                    namespace="owner",
                    _in_transaction=True,
                )
        finally:
            connection.close()
            writer_done.set()

    original_cursor = status_module._latest_event_cursor

    def interleaved_cursor(run_id, *, con, namespace):
        writer = threading.Thread(target=publish_terminal_event)
        writer.start()
        assert writer_started.wait(timeout=5)
        return original_cursor(run_id, con=con, namespace=namespace)

    monkeypatch.setattr(status_module, "_latest_event_cursor", interleaved_cursor)
    status = get_run("r1", con=con, namespace="owner")
    assert writer_done.wait(timeout=5)
    resumed = read_run_events("r1", con=con, namespace="owner", after=status.latest_event_cursor)

    assert status.durable_state == "running"
    assert [event.payload["state"] for event in resumed.events] == ["succeeded"]
