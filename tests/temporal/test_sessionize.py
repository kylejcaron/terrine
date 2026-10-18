from datetime import UTC, datetime

import ibis

from terrine.temporal.sessionize import sessionize


def test_gap_equality_stays_in_session_and_tied_events_do_not_split():
    con = ibis.duckdb.connect()
    events = con.create_table(
        "events",
        ibis.memtable(
            {
                "unit": ["u", "u", "u", "u"],
                "anchor": [1, 1, 1, 1],
                "time": [
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, 2, 0, 1, tzinfo=UTC),
                ],
                "id": ["a", "c", "b", "d"],
            }
        ).to_pyarrow(),
    )
    result = (
        sessionize(
            events,
            unit_column="unit",
            anchor_id_column="anchor",
            timestamp_column="time",
            event_id_column="id",
            gap_seconds=3600,
            max_events=4,
        )
        .to_pyarrow()
        .to_pylist()
    )
    assert [(row["id"], row["session_index"]) for row in result] == [
        ("a", 0),
        ("b", 0),
        ("c", 0),
        ("d", 1),
    ]
    assert result[0]["previous_event_id"] is None
    assert result[0]["transition_reason"] == "missing_predecessor"


def test_overlapping_each_anchor_sessions_are_independent():
    con = ibis.duckdb.connect()
    events = con.create_table(
        "events",
        ibis.memtable(
            {
                "unit": ["u", "u"],
                "anchor": [1, 2],
                "time": [datetime(2025, 1, 1, tzinfo=UTC)] * 2,
                "id": ["e", "e"],
            }
        ).to_pyarrow(),
    )
    result = (
        sessionize(
            events,
            unit_column="unit",
            anchor_id_column="anchor",
            timestamp_column="time",
            event_id_column="id",
            gap_seconds=0,
            max_events=2,
        )
        .to_pyarrow()
        .to_pylist()
    )
    assert {(row["anchor"], row["session_index"]) for row in result} == {(1, 0), (2, 0)}


def test_session_start_temporary_name_preserves_same_named_input_property():
    con = ibis.duckdb.connect()
    events = con.create_table(
        "events",
        ibis.memtable(
            {
                "unit": ["u"],
                "anchor": [1],
                "time": [datetime(2025, 1, 1, tzinfo=UTC)],
                "id": ["a"],
                "_new_session": ["user-value"],
            }
        ).to_pyarrow(),
    )
    result = (
        sessionize(
            events,
            unit_column="unit",
            anchor_id_column="anchor",
            timestamp_column="time",
            event_id_column="id",
            gap_seconds=0,
            max_events=1,
        )
        .to_pyarrow()
        .to_pylist()
    )
    assert result[0]["_new_session"] == "user-value"

def test_sessions_expose_summary_and_transition_schemas():
    con = ibis.duckdb.connect()
    events = con.create_table(
        "events",
        ibis.memtable(
            {
                "unit": ["u", "u", "u"],
                "anchor": [1, 1, 1],
                "time": [
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, 0, 0, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, 1, tzinfo=UTC),
                ],
                "id": ["a", "b", "c"],
            }
        ).to_pyarrow(),
    )
    result = sessionize(
        events,
        unit_column="unit",
        anchor_id_column="anchor",
        timestamp_column="time",
        event_id_column="id",
        gap_seconds=1,
        max_events=3,
    ).to_pyarrow().to_pylist()
    assert [(row["id"], row["session_event_count"]) for row in result] == [
        ("a", 2),
        ("b", 2),
        ("c", 1),
    ]
    assert result[0]["transition"] is None
    assert result[1]["transition"] == {"from_event_id": "a", "to_event_id": "b"}
    assert result[2]["transition"] == {"from_event_id": "b", "to_event_id": "c"}
