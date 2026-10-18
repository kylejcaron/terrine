import pyarrow as pa
import pytest

from terrine.data import ExecutionSnapshot, MaterializedFrame, Provenance, Roles
from terrine.estimate.survival import ElapsedTime, kaplan_meier


def frame(durations, events, entries=None):
    def elapsed(values):
        return pa.array(
            [
                None if value is None else {"seconds": value[0], "nanoseconds": value[1]}
                for value in values
            ],
            type=pa.struct(
                [
                    pa.field("seconds", pa.int64()),
                    pa.field("nanoseconds", pa.int32()),
                ]
            ),
        )

    columns = {"duration": elapsed(durations), "event": pa.array(events, type=pa.bool_())}
    if entries is not None:
        columns["entry"] = elapsed(entries)
    table = pa.table({"unit": pa.array([f"u{i}" for i in range(len(events))]), **columns})
    return MaterializedFrame(
        table,
        Roles(
            "unit",
            "unit",
            "t0",
            None,
            (),
            ("duration", "event", *(("entry",) if entries is not None else ())),
        ),
        {},
        Provenance(
            {},
            {},
            "design",
            "model",
            {},
            ExecutionSnapshot(None, "arrow", {}, None, "verified"),
            "test",
            None,
        ),
    )


def test_km_ties_and_time_zero_events_match_hand_calculation():
    result = kaplan_meier(
        frame([(0, 0), (1, 0), (1, 0), (2, 0)], [True, True, False, False]),
        duration="duration",
        event="event",
    )
    assert result.at_risk == (4, 3, 1)
    assert result.events == (1, 1, 0)
    assert result.censored == (0, 1, 1)
    assert result.survival == pytest.approx((0.75, 0.5, 0.5))
    assert result.variance == pytest.approx((0.046875, 0.0625, 0.0625))
    assert result.time == (ElapsedTime(0), ElapsedTime(1), ElapsedTime(2))


def test_km_subsecond_event_censor_and_entry_order_exactly():
    result = kaplan_meier(
        frame(
            [(0, 3), (0, 2), (0, 3)],
            [True, True, True],
            [(0, 1), (0, 1), (0, 3)],
        ),
        duration="duration",
        event="event",
        entry="entry",
    )
    assert result.time == (ElapsedTime(0, 2), ElapsedTime(0, 3))
    assert result.at_risk == (2, 2)
    assert result.events == (1, 2)
    assert result.censored == (0, 0)
    assert result.survival == pytest.approx((0.5, 0.0))
    assert result.variance[-1] is None


def test_km_rejects_duplicate_independent_unit_rows():
    source = frame([(1, 0), (2, 0)], [True, False])
    table = source.table.set_column(0, "unit", pa.array(["same", "same"]))
    duplicate = MaterializedFrame(table, source.roles, source.columns, source.provenance)
    with pytest.raises(Exception, match="unique"):
        kaplan_meier(duplicate, duration="duration", event="event")


def test_km_timestamp_inputs_preserve_nanoseconds_relative_to_each_t0():
    origin = 1_735_689_600_000_000_000
    table = pa.table(
        {
            "unit": ["u1", "u2"],
            "t0": pa.array([origin + 100, origin], type=pa.timestamp("ns", tz="UTC")),
            "duration": pa.array([origin + 103, origin + 2], type=pa.timestamp("ns", tz="UTC")),
            "entry": pa.array([origin + 101, origin + 1], type=pa.timestamp("ns", tz="UTC")),
            "event": [True, False],
        }
    )
    source = frame([(3, 0), (2, 0)], [True, False], [(1, 0), (1, 0)])
    result = kaplan_meier(
        MaterializedFrame(table, source.roles, source.columns, source.provenance),
        duration="duration",
        event="event",
        entry="entry",
    )
    assert result.time == (ElapsedTime(0, 2), ElapsedTime(0, 3))


def test_greenwood_variance_stays_unavailable_after_survival_reaches_zero():
    source = frame(
        [(1, 0), (3, 0)],
        [True, False],
        [(0, 0), (2, 0)],
    )
    result = kaplan_meier(source, duration="duration", event="event", entry="entry")
    assert result.survival == (0.0, 0.0)
    assert result.variance == (None, None)
