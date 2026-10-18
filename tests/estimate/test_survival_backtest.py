from datetime import UTC, datetime

import pyarrow as pa
import pytest

from terrine.data import (
    ColumnMeta,
    ExecutionSnapshot,
    FeatureCutoff,
    MaterializedFrame,
    Provenance,
    Roles,
)
from terrine.errors import TerrineError
from terrine.estimate.survival import (
    ElapsedTime,
    backtest_survival,
    to_surrogate_arrow,
)


def _frame(*, later_event=datetime(2025, 1, 5, tzinfo=UTC)):
    table = pa.table(
        {
            "unit": pa.array(["u1", "u2"]),
            "t0": pa.array(
                [
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 2, tzinfo=UTC),
                ],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "entry": pa.array(
                [datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "duration": pa.array(
                [datetime(2025, 1, 3, tzinfo=UTC), later_event],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "event": pa.array([True, True]),
            "feature": pa.array([1.0, None]),
            "feature_complete": pa.array([True, False]),
            "feature_reason": pa.array([None, "incomplete_coverage"]),
            "outcome_complete": pa.array([True, True]),
            "outcome_reason": pa.array([None, None]),
        }
    )
    return MaterializedFrame(
        table,
        Roles("unit", "unit", "t0", None, ("feature",), ("duration", "event", "entry")),
        {
            "feature": ColumnMeta(
                None, "feature", None, None, "feature_complete", "feature_reason"
            ),
            "duration": ColumnMeta(
                None, "outcome", None, None, "outcome_complete", "outcome_reason"
            ),
        },
        Provenance(
            {"feature": FeatureCutoff(-86400, True, "recorded_at")},
            {},
            "design-hash",
            "model-hash",
            {"events": "generation-1"},
            ExecutionSnapshot("snapshot-1", "arrow", {}, None, "verified"),
            "time_to_event",
            None,
        ),
    )


def test_backtest_recensors_survivors_and_is_post_cutoff_invariant():
    cutoffs = (datetime(2025, 1, 3, tzinfo=UTC), datetime(2025, 1, 4, tzinfo=UTC))
    captured = []

    def fitter(data):
        captured.append(data)
        return data

    returned = None
    for run, late_event in enumerate(
        (datetime(2025, 1, 8, tzinfo=UTC), datetime(2025, 1, 9, tzinfo=UTC))
    ):
        results = backtest_survival(
            _frame(later_event=late_event),
            duration="duration",
            event="event",
            entry="entry",
            prediction_cutoffs=cutoffs,
            fitter=fitter,
        )
        if run == 0:
            returned = results
    assert returned is not None and len(returned) == 2
    assert returned[0] is captured[0]
    assert [frame.table.to_pylist() for frame in captured[:2]] == [
        frame.table.to_pylist() for frame in captured[2:]
    ]
    assert [
        (row["unit"], row["event"], row["duration"]) for row in captured[0].table.to_pylist()
    ] == [
        ("u1", False, datetime(2025, 1, 3, tzinfo=UTC)),
        ("u2", False, datetime(2025, 1, 3, tzinfo=UTC)),
    ]
    assert [
        (row["unit"], row["event"], row["duration"]) for row in captured[1].table.to_pylist()
    ] == [
        ("u1", True, datetime(2025, 1, 3, tzinfo=UTC)),
        ("u2", False, datetime(2025, 1, 4, tzinfo=UTC)),
    ]
    assert all(
        frame.provenance.measurement_knowledge == "retrospective_effective_time"
        for frame in captured
    )
    assert all(
        row["feature"] is not None or row["feature_reason"] for row in captured[1].table.to_pylist()
    )


def test_backtest_recensors_at_row_specific_t0_and_excludes_late_entrants():
    cutoff = datetime(2025, 1, 1, 12, tzinfo=UTC)
    seen = []
    backtest_survival(
        _frame(),
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[cutoff],
        fitter=lambda data: seen.extend(data.table.to_pylist()),
    )
    assert [row["unit"] for row in seen] == ["u1"]
    assert seen[0]["duration"] == cutoff
    assert seen[0]["event"] is False


def test_backtest_compares_elapsed_time_across_arrow_timestamp_units():
    source = _frame()
    source_table = source.table.set_column(
        source.table.schema.get_field_index("entry"),
        "entry",
        source.table["entry"].cast(pa.timestamp("us", tz="UTC")),
    )
    source_table = source_table.set_column(
        source_table.schema.get_field_index("duration"),
        "duration",
        source.table["duration"].cast(pa.timestamp("us", tz="UTC")),
    )
    source = MaterializedFrame(source_table, source.roles, source.columns, source.provenance)
    cutoff = datetime(2025, 1, 4, tzinfo=UTC)
    seen = []
    backtest_survival(
        source,
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[cutoff],
        fitter=lambda data: seen.extend(data.table.to_pylist()),
    )
    assert [(row["event"], row["duration"]) for row in seen] == [
        (True, datetime(2025, 1, 3, tzinfo=UTC)),
        (False, cutoff),
    ]


def test_backtest_rejects_naive_or_unsorted_cutoffs():
    for cutoffs in (
        [datetime(2025, 1, 3)],
        [datetime(2025, 1, 4, tzinfo=UTC), datetime(2025, 1, 3, tzinfo=UTC)],
    ):
        with pytest.raises(TerrineError):
            backtest_survival(
                _frame(),
                duration="duration",
                event="event",
                entry="entry",
                prediction_cutoffs=cutoffs,
                fitter=lambda data: data,
            )


def test_recorded_at_request_refuses_without_recorded_at_source():
    with pytest.raises(TerrineError):
        backtest_survival(
            _frame(),
            duration="duration",
            event="event",
            entry="entry",
            prediction_cutoffs=[datetime(2025, 1, 4, tzinfo=UTC)],
            fitter=lambda data: data,
            knowledge="recorded_at",
        )


def test_surrogate_arrow_round_trip_preserves_projection_metadata_and_null_reasons():
    surrogate = to_surrogate_arrow(_frame(), features=("feature",), outcomes=("duration", "event"))
    restored = type(surrogate).from_arrow(surrogate.to_arrow())
    assert restored.data.equals(surrogate.data)
    assert restored.metadata == surrogate.metadata
    assert restored.metadata["schema_version"] == 1
    assert restored.metadata["design_hash"] == "design-hash"
    assert "unit" in restored.data.column_names
    assert "t0" in restored.data.column_names
    assert "feature_reason" in restored.data.column_names
    assert restored.data["feature"].null_count == 1
    assert restored.data["feature_reason"].to_pylist() == [None, "incomplete_coverage"]
    assert ElapsedTime(2, 3).nanoseconds == 3
