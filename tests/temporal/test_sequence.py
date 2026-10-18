from datetime import UTC, datetime

import ibis
import pyarrow as pa
import pytest

from terrine.errors import TerrineError
from terrine.temporal.sequence import event_sequence


def _tables(rows):
    events = ibis.memtable(
        pa.table(
            {
                "unit": [row[0] for row in rows],
                "ts": [row[1] for row in rows],
                "event_id": [row[2] for row in rows],
                "value": [row[3] for row in rows],
            }
        )
    )
    anchors = ibis.memtable(
        pa.table(
            {
                "unit": ["u"],
                "anchor_id": ["a"],
                "t0": [datetime(2025, 1, 1, 12, tzinfo=UTC)],
            }
        )
    )
    return events, anchors


def _run(rows, representation="positions", max_events=5):
    events, anchors = _tables(rows)
    return event_sequence(
        events,
        anchors,
        window_start=0,
        window_end=3600,
        max_events=max_events,
        representation=representation,
        properties=("value",),
        internal_properties=(),
        label_property=None,
        unit_column="unit",
        anchor_id_column="anchor_id",
        timestamp_column="ts",
        event_id_column="event_id",
        observed_from_column=None,
        observed_until_column=None,
        complete_from=datetime(2025, 1, 1, tzinfo=UTC),
        complete_until=datetime(2025, 1, 2, tzinfo=UTC),
    )


def test_sequence_window_is_exact_half_open_at_intraday_anchor():
    base = datetime(2025, 1, 1, 12, tzinfo=UTC)
    result = _run(
        [
            ("u", base, "at-start", 1),
            ("u", datetime(2025, 1, 1, 13, tzinfo=UTC), "at-end", 2),
        ]
    )
    result.validate(execute=lambda query: query.to_pyarrow())
    rows = result.table.to_pyarrow().to_pylist()
    events = [row for row in rows if row["is_event"]]
    assert [row["event_id"] for row in events] == ["at-start"]


def test_sequence_tie_order_is_stable_under_input_shuffle():
    base = datetime(2025, 1, 1, 12, tzinfo=UTC)
    first = _run([("u", base, "z", 1), ("u", base, "a", 2)])
    second = _run([("u", base, "a", 2), ("u", base, "z", 1)])

    def event_ids(result):
        return [row["event_id"] for row in result.table.to_pyarrow().to_pylist() if row["is_event"]]

    assert event_ids(first) == event_ids(second) == ["a", "z"]
    ordered_events = [row for row in first.table.to_pyarrow().to_pylist() if row["is_event"]]
    assert [row["sequence_position"] for row in ordered_events] == [0, 1]


def test_sequence_max_events_refuses_before_array_or_position_output():
    base = datetime(2025, 1, 1, 12, tzinfo=UTC)
    events, anchors = _tables([("u", base, str(i), i) for i in range(3)])
    result = event_sequence(
        events,
        anchors,
        window_start=0,
        window_end=3600,
        max_events=2,
        representation="arrays",
        properties=("value",),
        internal_properties=(),
        label_property=None,
        unit_column="unit",
        anchor_id_column="anchor_id",
        timestamp_column="ts",
        event_id_column="event_id",
        observed_from_column=None,
        observed_until_column=None,
        complete_from=datetime(2025, 1, 1, tzinfo=UTC),
        complete_until=datetime(2025, 1, 2, tzinfo=UTC),
    )
    with pytest.raises(TerrineError) as error:
        result.validate(execute=lambda query: query.to_pyarrow())
    assert error.value.code == "terrine.sequence.max_events_exceeded"


def test_complete_empty_anchor_has_header_and_empty_array():
    outside_window = _run([("u", datetime(2025, 1, 1, 14, tzinfo=UTC), "outside", 1)])
    positions = outside_window
    position_rows = positions.table.to_pyarrow().to_pylist()
    assert len(position_rows) == 1
    header = position_rows[0]
    assert (header["is_event"], header["event_count"], header["sequence_complete"]) == (
        False,
        0,
        True,
    )

    arrays = _run(
        [("u", datetime(2025, 1, 1, 14, tzinfo=UTC), "outside", 1)],
        representation="arrays",
    )
    array_header = arrays.table.to_pyarrow().to_pylist()[0]
    assert array_header["event_values"] == []


def test_array_and_position_representations_have_same_ordered_events():
    rows = [
        ("u", datetime(2025, 1, 1, 12, tzinfo=UTC), "b", 2),
        ("u", datetime(2025, 1, 1, 12, tzinfo=UTC), "a", 1),
    ]
    position = _run(rows).table.to_pyarrow().to_pylist()
    children = [row for row in position if row["is_event"]]
    array = _run(rows, representation="arrays").table.to_pyarrow().to_pylist()[0]
    assert [row["event_id"] for row in children] == ["a", "b"]
    assert [row["value"] for row in children] == [1, 2]
    assert array["event_values"] == [
        {
            "event_id": "a",
            "event_time": datetime(2025, 1, 1, 12, tzinfo=UTC),
            "value": 1,
            "value_reason": None,
        },
        {
            "event_id": "b",
            "event_time": datetime(2025, 1, 1, 12, tzinfo=UTC),
            "value": 2,
            "value_reason": None,
        },
    ]


@pytest.mark.parametrize("representation", ["positions", "arrays"])
def test_unavailable_anchor_has_only_header_in_both_representations(representation):
    events, anchors = _tables([("u", datetime(2025, 1, 1, 14, tzinfo=UTC), "outside", 1)])
    anchors = anchors.mutate(observed_until=ibis.literal(datetime(2025, 1, 1, 12, 30, tzinfo=UTC)))
    result = event_sequence(
        events,
        anchors,
        window_start=0,
        window_end=3600,
        max_events=5,
        representation=representation,
        properties=("value",),
        internal_properties=(),
        label_property=None,
        unit_column="unit",
        anchor_id_column="anchor_id",
        timestamp_column="ts",
        event_id_column="event_id",
        observed_from_column=None,
        observed_until_column="observed_until",
        complete_from=datetime(2025, 1, 1, tzinfo=UTC),
        complete_until=datetime(2025, 1, 2, tzinfo=UTC),
    )
    rows = result.table.to_pyarrow().to_pylist()
    assert len(rows) == 1
    assert rows[0]["is_event"] is False
    assert rows[0]["event_count"] is None
    assert rows[0]["sequence_complete"] is False
    assert rows[0]["sequence_reason"] == "unit_observation"
    if representation == "arrays":
        assert rows[0]["event_values"] is None


def test_known_null_event_property_has_stable_reason():
    rows = [
        ("u", datetime(2025, 1, 1, 12, tzinfo=UTC), "value", 1),
        ("u", datetime(2025, 1, 1, 12, tzinfo=UTC), "null", None),
    ]
    position = _run(rows).table.to_pyarrow().to_pylist()
    child = next(row for row in position if row["is_event"])
    assert child["value"] is None
    assert child["value_reason"] == "known_null"

    array = _run(rows, representation="arrays").table.to_pyarrow().to_pylist()[0]
    assert array["event_values"][0] == {
        "event_id": "null",
        "event_time": datetime(2025, 1, 1, 12, tzinfo=UTC),
        "value": None,
        "value_reason": "known_null",
    }
