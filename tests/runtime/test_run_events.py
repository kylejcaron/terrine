import duckdb
import pytest

from terrine.runtime.events import read_run_events, record_run_event


def test_event_cursor_is_exclusive_bounded_and_run_bound():
    con = duckdb.connect(":memory:")
    for sequence in range(4):
        record_run_event("r1", "phase", {"phase": str(sequence)}, con=con, namespace="owner")
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
