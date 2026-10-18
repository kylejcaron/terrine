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
