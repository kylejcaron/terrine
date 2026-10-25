import duckdb
import pytest

from terrine.runtime.events import read_run_events, record_run_event
from tests._timing import HANG_GUARD_SECONDS


def test_event_cursor_is_exclusive_bounded_and_run_bound():

    from datetime import UTC, datetime, timedelta

    con = duckdb.connect(":memory:")
    started = datetime(2026, 10, 18, tzinfo=UTC)
    for sequence in range(4):
        record_run_event(
            "r1",
            "phase",
            {"phase": str(sequence)},
            con=con,
            namespace="owner",
            at=started + timedelta(seconds=sequence),
        )
    first = read_run_events("r1", con=con, namespace="owner", limit=2)
    assert len(first.events) == 2
    second = read_run_events("r1", con=con, namespace="owner", after=first.next_cursor, limit=2)
    assert len(second.events) == 2
    assert second.events[0].sequence == first.events[-1].sequence + 1
    assert read_run_events("r2", con=con, namespace="owner", after=first.next_cursor).cursor_gap


def test_event_payload_is_strictly_allowlisted():
    con = duckdb.connect(":memory:")
    with pytest.raises(ValueError):
        record_run_event(
            "r1",
            "phase",
            {"phase": "active", "details": {"api_key": "secret"}},
            con=con,
            namespace="owner",
        )


def test_event_record_rejects_empty_run_id():
    with pytest.raises(ValueError):
        record_run_event(
            "", "phase", {"phase": "active"}, con=duckdb.connect(":memory:"), namespace="owner"
        )


def test_progress_events_coalesce_within_one_second_without_coalescing_lifecycle():
    from datetime import UTC, datetime, timedelta

    con = duckdb.connect(":memory:")
    started = datetime(2026, 10, 18, tzinfo=UTC)
    record_run_event("r1", "phase", {"phase": "planning"}, con=con, namespace="owner", at=started)
    record_run_event(
        "r1",
        "phase",
        {"phase": "executing"},
        con=con,
        namespace="owner",
        at=started + timedelta(milliseconds=500),
    )
    record_run_event(
        "r1",
        "lifecycle",
        {"state": "succeeded"},
        con=con,
        namespace="owner",
        at=started + timedelta(milliseconds=600),
    )

    events = read_run_events("r1", con=con, namespace="owner").events
    assert [(event.kind, event.payload) for event in events] == [
        ("phase", {"phase": "planning"}),
        ("lifecycle", {"state": "succeeded"}),
    ]


def test_event_retention_reports_cursor_gap(monkeypatch):
    from datetime import UTC, datetime, timedelta

    import terrine.runtime.events as events_module

    monkeypatch.setattr(events_module, "_RETENTION", 3)
    con = duckdb.connect(":memory:")
    started = datetime(2026, 10, 18, tzinfo=UTC)
    cursors = []
    for sequence in range(5):
        record_run_event(
            "r1",
            "lifecycle",
            {"state": "running"},
            con=con,
            namespace="owner",
            at=started + timedelta(seconds=sequence),
        )
        cursors.append(read_run_events("r1", con=con, namespace="owner").next_cursor)

    page = read_run_events("r1", con=con, namespace="owner", after=cursors[0])
    assert page.cursor_gap
    assert [event.sequence for event in page.events] == [3, 4, 5]


def test_event_page_keeps_retention_boundary_and_rows_in_one_snapshot(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    import terrine.runtime.events as events_module

    monkeypatch.setattr(events_module, "_RETENTION", 2)
    con = duckdb.connect(":memory:")
    record_run_event("r1", "lifecycle", {"state": "queued"}, con=con, namespace="owner")
    cursor = read_run_events("r1", con=con, namespace="owner").next_cursor
    for state in ("running", "cancel_requested"):
        record_run_event("r1", "lifecycle", {"state": state}, con=con, namespace="owner")
    boundary_read = Event()
    appended = Event()

    class PausedPageConnection:
        def execute(self, query, parameters=None):
            if query.startswith("SELECT sequence, event_at, kind, payload"):
                boundary_read.set()
                assert appended.wait(timeout=HANG_GUARD_SECONDS)
            return con.execute(query, parameters)

    def append_and_trim():
        writer = con.cursor()
        try:
            assert boundary_read.wait(timeout=HANG_GUARD_SECONDS)
            writer.execute("BEGIN TRANSACTION")
            try:
                record_run_event(
                    "r1",
                    "lifecycle",
                    {"state": "cancelled"},
                    con=writer,
                    namespace="owner",
                    _in_transaction=True,
                )
            except BaseException:
                writer.execute("ROLLBACK")
                raise
            else:
                writer.execute("COMMIT")
        finally:
            writer.close()
            appended.set()

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            writer = executor.submit(append_and_trim)
            page = read_run_events(
                "r1", con=PausedPageConnection(), namespace="owner", after=cursor
            )
            writer.result(timeout=HANG_GUARD_SECONDS)
        assert not page.cursor_gap
        assert [event.sequence for event in page.events] == [2, 3]
        resumed = read_run_events("r1", con=con, namespace="owner", after=page.next_cursor)
        assert not resumed.cursor_gap
        assert [event.sequence for event in resumed.events] == [4]
    finally:
        con.close()


def test_concurrent_lifecycle_emission_allocates_unique_sequences():
    from concurrent.futures import ThreadPoolExecutor
    from datetime import UTC, datetime, timedelta
    from threading import Barrier

    con = duckdb.connect(":memory:")
    start = Barrier(8)

    def emit(sequence):
        connection = con.cursor()
        try:
            start.wait(timeout=HANG_GUARD_SECONDS)
            record_run_event(
                "r1",
                "lifecycle",
                {"state": "running"},
                con=connection,
                namespace="owner",
                at=datetime(2026, 10, 18, tzinfo=UTC) + timedelta(seconds=sequence),
            )
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=8) as executor:
        tuple(executor.map(emit, range(8)))

    events = read_run_events("r1", con=con, namespace="owner").events
    assert [event.sequence for event in events] == list(range(1, 9))


def test_consumer_deduplicates_redelivered_pages_and_resumes_from_acknowledged_cursor():
    con = duckdb.connect(":memory:")
    for state in ("queued", "running"):
        record_run_event("r1", "lifecycle", {"state": state}, con=con, namespace="owner")
    first = read_run_events("r1", con=con, namespace="owner", limit=2)
    redelivered = read_run_events("r1", con=con, namespace="owner", limit=2)
    assert redelivered == first
    record_run_event("r1", "lifecycle", {"state": "succeeded"}, con=con, namespace="owner")
    resumed = read_run_events("r1", con=con, namespace="owner", after=first.next_cursor, limit=2)
    delivered = (*first.events, *redelivered.events, *resumed.events, *resumed.events)
    acknowledged = {(event.run_id, event.sequence): event for event in delivered}
    assert list(acknowledged) == [("r1", 1), ("r1", 2), ("r1", 3)]
    assert [event.payload["state"] for event in acknowledged.values()] == [
        "queued",
        "running",
        "succeeded",
    ]
    assert read_run_events("r1", con=con, namespace="owner", after=resumed.next_cursor).events == ()
