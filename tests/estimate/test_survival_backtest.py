from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

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
            "entry_time": pa.array(
                [datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "duration": pa.array(
                [datetime(2025, 1, 3, tzinfo=UTC), later_event],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "event_time": pa.array(
                [datetime(2025, 1, 3, tzinfo=UTC), later_event],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "censor_time": pa.array(
                [datetime(2025, 1, 12, tzinfo=UTC)] * 2,
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
        Roles("unit", "unit", "t0", None, ("feature",), ("duration", "event")),
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


def test_backtest_masks_future_event_time_and_recensors_censor_time():
    captured = []
    backtest_survival(
        _frame(later_event=datetime(2025, 1, 8, tzinfo=UTC)),
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[datetime(2025, 1, 4, tzinfo=UTC)],
        fitter=lambda data: captured.append(data.table.to_pylist()),
    )
    rows = captured[0]
    assert rows[0]["event_time"] == datetime(2025, 1, 3, tzinfo=UTC)
    assert rows[1]["event_time"] is None
    assert [row["censor_time"] for row in rows] == [
        datetime(2025, 1, 4, tzinfo=UTC),
        datetime(2025, 1, 4, tzinfo=UTC),
    ]


@pytest.mark.parametrize(
    "cutoff",
    [
        datetime(2025, 1, 1, 0, 0, 0, 500000, tzinfo=UTC),
        datetime(2025, 1, 1, 0, 0, 0, 500001, tzinfo=UTC),
    ],
)
def test_backtest_preserves_fractional_cutoff_for_seconds_censor_input(cutoff):
    source = _frame()
    table = source.table
    for name in ("t0", "censor_time"):
        table = table.set_column(
            table.schema.get_field_index(name),
            name,
            table[name].cast(pa.timestamp("s", tz="UTC")),
        )
    source = MaterializedFrame(table, source.roles, source.columns, source.provenance)
    captured = []
    backtest_survival(
        source,
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[cutoff],
        fitter=lambda data: captured.append(data.table),
    )
    assert captured[0]["censor_time"][0].as_py() == cutoff
    assert captured[0].schema.field("censor_time").type == pa.timestamp("us", tz="UTC")
def test_backtest_preserves_subsecond_cutoff_with_seconds_origin():
    source = _frame()
    source_table = source.table.set_column(
        source.table.schema.get_field_index("t0"),
        "t0",
        source.table["t0"].cast(pa.timestamp("s", tz="UTC")),
    )
    source = MaterializedFrame(source_table, source.roles, source.columns, source.provenance)
    cutoff = datetime(2025, 1, 1, 0, 0, 0, 500000, tzinfo=UTC)
    captured = []
    backtest_survival(
        source,
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[cutoff],
        fitter=lambda data: captured.append(data.table),
    )
    assert captured[0].num_rows == 1
    assert captured[0].schema.field("duration").type == pa.timestamp("ns", tz="UTC")
    assert captured[0]["duration"][0].as_py() == cutoff


def test_backtest_preserves_wide_microsecond_timestamps():
    source = _frame()
    table = source.table
    for name in ("t0", "entry", "duration", "event_time", "censor_time"):
        table = table.set_column(
            table.schema.get_field_index(name),
            name,
            table[name].cast(pa.timestamp("us", tz="UTC")),
        )
    origin = datetime(1600, 1, 1, tzinfo=UTC)
    cutoff = datetime(1600, 1, 1, 0, 0, 0, 500000, tzinfo=UTC)
    shift = datetime(2025, 1, 1, tzinfo=UTC)
    delta = origin - shift
    for name in ("t0", "entry", "duration", "event_time", "censor_time"):
        table = table.set_column(
            table.schema.get_field_index(name),
            name,
            pa.array(
                [value.as_py() + delta for value in table[name]],
                type=pa.timestamp("us", tz="UTC"),
            ),
        )
    source = MaterializedFrame(table, source.roles, source.columns, source.provenance)
    seen = []
    backtest_survival(
        source,
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[cutoff],
        fitter=lambda data: seen.append(data.table),
    )
    assert seen[0]["duration"][0].as_py() == cutoff
    assert seen[0].schema.field("duration").type == pa.timestamp("us", tz="UTC")


def test_backtest_masks_submicrosecond_event_without_python_datetime_conversion():
    source = _frame()
    table = source.table.set_column(
        source.table.schema.get_field_index("event_time"),
        "event_time",
        pa.array(
            [1735862400000000001, 1735862400000000001],
            type=pa.timestamp("ns", tz="UTC"),
        ),
    )
    source = MaterializedFrame(table, source.roles, source.columns, source.provenance)
    seen = []
    backtest_survival(
        source,
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[datetime(2025, 1, 3, tzinfo=UTC)],
        fitter=lambda data: seen.append(data.table),
    )
    assert seen[0]["event_time"].to_pylist() == [None, None]


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
    assert "entry_time" in restored.data.column_names
    assert "feature_reason" in restored.data.column_names
    assert restored.data["feature"].null_count == 1
    assert restored.data["feature_reason"].to_pylist() == [None, "incomplete_coverage"]
    columns = cast(dict[str, object], restored.metadata["columns"])
    feature = cast(dict[str, object], columns["feature"])
    roles = cast(dict[str, object], restored.metadata["roles"])
    assert feature["reason_column"] == "feature_reason"
    assert roles["outcomes"] == ["duration", "event", "entry_time"]
    assert ElapsedTime(2, 3).nanoseconds == 3


def test_surrogate_arrow_retains_ltv_age_axis():
    source = _frame()
    table = source.table.append_column("age", pa.array([1.0, 2.0]))
    source = MaterializedFrame(
        table, source.roles, source.columns, replace(source.provenance, shape="ltv")
    )
    surrogate = to_surrogate_arrow(source, features=("feature",), outcomes=("duration",))
    restored = type(surrogate).from_arrow(surrogate.to_arrow())
    assert "age" in restored.data.column_names
    outcome_columns = cast(list[str], restored.metadata["outcome_columns"])
    assert "age" in outcome_columns


def test_surrogate_retains_support_for_automatically_retained_outcomes():
    source = _frame()
    table = source.table.append_column("period_value", pa.array([1.0, 2.0]))
    table = table.append_column("cumulative_value", pa.array([1.0, 3.0]))
    table = table.append_column("cumulative_value_complete", pa.array([True, False]))
    table = table.append_column("age", pa.array([1.0, 2.0]))
    table = table.append_column("cumulative_value_reason", pa.array([None, "incomplete_period"]))
    table = table.append_column("cumulative_value_error", pa.array([0.0, None]))
    columns = dict(source.columns)
    columns["cumulative_value"] = ColumnMeta(
        None,
        "outcome",
        None,
        None,
        "cumulative_value_complete",
        "cumulative_value_reason",
        numeric_policy="checked_float64_v1",
        absolute_error_column="cumulative_value_error",
    )
    roles = replace(source.roles, outcomes=("period_value", "cumulative_value"))
    source = MaterializedFrame(table, roles, columns, replace(source.provenance, shape="ltv"))
    surrogate = to_surrogate_arrow(source, features=("feature",), outcomes=("period_value",))
    assert {
        "cumulative_value_complete",
        "cumulative_value_reason",
        "cumulative_value_error",
    } <= set(surrogate.data.column_names)
    assert {
        "cumulative_value_complete",
        "cumulative_value_reason",
        "cumulative_value_error",
    } <= set(cast(list[str], surrogate.metadata["support_columns"]))


def test_backtest_accepts_wide_cutoff_with_nanosecond_inputs():
    seen = []
    backtest_survival(
        _frame(),
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[datetime(2500, 1, 1, tzinfo=UTC)],
        fitter=lambda data: seen.append(data.table),
    )
    assert seen[0].num_rows == 2
    assert seen[0]["duration"].to_pylist() == [
        datetime(2025, 1, 3, tzinfo=UTC),
        datetime(2025, 1, 5, tzinfo=UTC),
    ]


def test_backtest_coded_refusal_for_malformed_cutoff_and_optional_timestamp():
    with pytest.raises(TerrineError):
        backtest_survival(
            _frame(),
            duration="duration",
            event="event",
            entry="entry",
            prediction_cutoffs=cast(list[datetime], [None]),
            fitter=lambda data: data,
        )
    source = _frame()
    table = source.table.set_column(
        source.table.schema.get_field_index("event_time"),
        "event_time",
        pa.array([1, 2], type=pa.int64()),
    )
    with pytest.raises(TerrineError):
        backtest_survival(
            MaterializedFrame(table, source.roles, source.columns, source.provenance),
            duration="duration",
            event="event",
            entry="entry",
            prediction_cutoffs=[datetime(2025, 1, 4, tzinfo=UTC)],
            fitter=lambda data: data,
        )


def test_backtest_fractional_cutoff_expands_seconds_and_milliseconds_output():
    source = _frame()
    table = source.table
    for name in ("t0", "entry", "duration", "event_time", "censor_time"):
        table = table.set_column(
            table.schema.get_field_index(name),
            name,
            table[name].cast(pa.timestamp("ms", tz="UTC")),
        )
    source = MaterializedFrame(table, source.roles, source.columns, source.provenance)
    cutoff = datetime(2025, 1, 1, 0, 0, 0, 500001, tzinfo=UTC)
    seen = []
    backtest_survival(
        source,
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[cutoff],
        fitter=lambda data: seen.append(data.table),
    )
    assert seen[0].schema.field("duration").type == pa.timestamp("us", tz="UTC")
    assert seen[0]["duration"][0].as_py() == cutoff


def test_backtest_wide_cutoff_emits_microsecond_censor_time():
    source = _frame()
    table = source.table.set_column(
        source.table.schema.get_field_index("censor_time"),
        "censor_time",
        pa.nulls(source.table.num_rows, type=pa.timestamp("ns", tz="UTC")),
    )
    source = MaterializedFrame(table, source.roles, source.columns, source.provenance)
    cutoff = datetime(2500, 1, 1, tzinfo=UTC)
    seen = []
    backtest_survival(
        source,
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[cutoff],
        fitter=lambda data: seen.append(data.table),
    )
    assert seen[0].schema.field("duration").type == pa.timestamp("ns", tz="UTC")
    assert seen[0].schema.field("censor_time").type == pa.timestamp("us", tz="UTC")
    assert seen[0]["censor_time"].to_pylist() == [cutoff, cutoff]


def test_backtest_mixed_resolution_cutoff_preserves_millisecond_censor_values():
    source = _frame()
    table = source.table.set_column(
        source.table.schema.get_field_index("censor_time"),
        "censor_time",
        source.table["censor_time"].cast(pa.timestamp("ms", tz="UTC")),
    )
    source = MaterializedFrame(table, source.roles, source.columns, source.provenance)
    cutoff = datetime(2025, 1, 4, 0, 0, 0, 500001, tzinfo=UTC)
    seen = []
    backtest_survival(
        source,
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[cutoff],
        fitter=lambda data: seen.append(data.table),
    )
    assert seen[0].schema.field("duration").type == pa.timestamp("ns", tz="UTC")
    assert seen[0].schema.field("censor_time").type == pa.timestamp("us", tz="UTC")
    assert seen[0]["censor_time"][0].as_py() == cutoff


def test_backtest_earlier_cutoff_is_independent_of_later_wide_cutoff():
    source = _frame()
    table = source.table.set_column(
        source.table.schema.get_field_index("censor_time"),
        "censor_time",
        pa.array([None, 1735862400000000001], type=pa.timestamp("ns", tz="UTC")),
    )
    source = MaterializedFrame(table, source.roles, source.columns, source.provenance)
    early = datetime(2025, 1, 4, tzinfo=UTC)
    seen = []
    backtest_survival(
        source,
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[early, datetime(2500, 1, 1, tzinfo=UTC)],
        fitter=lambda data: seen.append(data.table),
    )
    first_alone = []
    backtest_survival(
        source,
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[early],
        fitter=lambda data: first_alone.append(data.table),
    )
    assert seen[0]["censor_time"].to_pylist() == first_alone[0]["censor_time"].to_pylist()


def test_backtest_cutoff_beyond_nanosecond_maximum_within_year_2262():
    source = _frame()
    table = source.table.set_column(
        source.table.schema.get_field_index("censor_time"),
        "censor_time",
        pa.nulls(source.table.num_rows, type=pa.timestamp("ns", tz="UTC")),
    )
    source = MaterializedFrame(table, source.roles, source.columns, source.provenance)
    cutoff = datetime(2262, 12, 31, tzinfo=UTC)
    seen = []
    backtest_survival(
        source,
        duration="duration",
        event="event",
        entry="entry",
        prediction_cutoffs=[cutoff],
        fitter=lambda data: seen.append(data.table),
    )
    assert seen[0]["censor_time"].to_pylist() == [cutoff, cutoff]
