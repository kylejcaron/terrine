import duckdb
import pytest

from terrine.runtime.events import read_run_events, record_run_event


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
    with pytest.raises(ValueError, match="forbidden"):
        record_run_event(
            "r1",
            "phase",
            {"phase": "active", "details": {"api_key": "secret"}},
            con=con,
            namespace="owner",
        )


def test_event_record_rejects_empty_run_id():
    with pytest.raises(ValueError, match="invalid"):
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
