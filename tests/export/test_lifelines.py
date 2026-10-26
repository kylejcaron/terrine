from dataclasses import replace

import pyarrow as pa
import pytest

import terrine.export as exports
from terrine.data import ColumnMeta, Coverage, Roles
from terrine.errors import TerrineError


@pytest.fixture
def survival(frame):
    origin = 1_735_689_600_000_000_000
    table = frame.table.append_column(
        "duration",
        pa.array([1, 2_000_000_001], type=pa.duration("ns")),
    )
    table = table.set_column(
        2, "t0", pa.array([origin, origin + 86_400_000_000_000], type=pa.timestamp("ns", tz="UTC"))
    )
    table = table.append_column(
        "entry_time",
        pa.array(
            [origin, origin + 86_400_000_000_000 + 1_000_000_000], type=pa.timestamp("ns", tz="UTC")
        ),
    )
    table = table.append_column("event", pa.array([True, False]))
    table = table.append_column("w", pa.array([2, 3]))
    return replace(
        frame,
        table=table,
        roles=replace(frame.roles, outcomes=("duration", "event"), weight="w", cluster_unit="id"),
        columns={
            **frame.columns,
            "duration": ColumnMeta(None, "outcome", None, None, "complete", "reason"),
            "event": ColumnMeta(None, "outcome", None, None, "complete", "reason"),
        },
        provenance=replace(frame.provenance, shape="time_to_event"),
    )


def test_km_uses_elapsed_seconds_and_preserves_entry_and_weights(survival):
    result = exports.to_lifelines_km(survival)
    assert result.durations.tolist() == pytest.approx([1e-9, 2.000000001], abs=1e-16)
    assert result.entry is not None
    assert result.weights is not None
    assert result.entry.tolist() == [0.0, 1.0]
    assert result.event_observed.tolist() == [True, False]
    assert result.weights.tolist() == [2, 3]
    assert result.excluded_counts == {}


def test_cox_projects_only_features_and_assigned_fields(survival):
    result = exports.to_lifelines_cox(survival)
    assert set(result.df.columns) == {"x", "duration", "event", "entry_time", "w", "id"}
    assert result.duration_col == "duration"
    assert result.event_col == "event"
    assert result.entry_col == "entry_time"
    assert result.weights_col == "w"
    assert result.cluster_col == "id"
    assert result.df["duration"].tolist() == pytest.approx([1e-9, 2.000000001], abs=1e-16)


@pytest.mark.parametrize("helper", ["to_lifelines_km", "to_lifelines_cox"])
def test_survival_drop_counts_shared_outcome_evidence_once(survival, helper):
    table = survival.table.set_column(6, "complete", pa.array([False, True])).set_column(
        7, "reason", pa.array(["incomplete_initial_history", None])
    )
    result = getattr(exports, helper)(replace(survival, table=table), drop_incomplete=True)
    assert result.excluded_counts == {"incomplete_initial_history": 1}
    if helper == "to_lifelines_km":
        assert result.durations.tolist() == [2.000000001]
    else:
        assert result.df["id"].tolist() == [2]


@pytest.mark.parametrize("helper", ["to_lifelines_km", "to_lifelines_cox"])
def test_survival_runs_shared_preflight_before_shape_check(frame, helper):
    candidate = replace(
        frame, provenance=replace(frame.provenance, treatment_design="grace_initiation")
    )
    with pytest.raises(TerrineError) as error:
        getattr(exports, helper)(candidate)
    assert error.value.code == "terrine.export.grace_baseline_treatment"


@pytest.mark.parametrize(
    "column,values", [("w", [0, 1]), ("w", [float("inf"), 1]), ("event", [None, True])]
)
def test_survival_refuses_invalid_fit_inputs(survival, column, values):
    table = survival.table.set_column(
        survival.table.schema.get_field_index(column), column, pa.array(values)
    )
    with pytest.raises(TerrineError) as error:
        exports.to_lifelines_km(replace(survival, table=table))
    assert error.value.code == "terrine.lifelines.invalid_input"


def test_real_multibin_life_table_converts_without_individual_cluster(native_inputs):
    from datetime import UTC, datetime

    from terrine.model import Model
    from terrine.spec import Design, EventAnchor, LifeTable, TimeToEvent

    catalog, con = native_inputs
    con.raw_sql("UPDATE labels SET ts = TIMESTAMPTZ '2025-01-05 00:00:00+00'")
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={
            "events": Coverage("2025-01-01", "2025-01-08"),
            "labels": Coverage("2025-01-01", "2025-01-08"),
        },
    )
    data = (
        Design(
            "user",
            EventAnchor("session", which="first"),
            shape=LifeTable(TimeToEvent("churn"), "1d"),
            administrative_cutoff=datetime(2025, 1, 7, tzinfo=UTC),
        )
        .build(model)
        .materialize()
    )
    result = exports.to_lifelines_km(data)
    assert data.table.num_rows == 3
    assert result.durations.tolist() == [2.0]
    assert result.event_observed.tolist() == [True]
    assert result.weights is not None
    assert result.weights.tolist() == [2]
    assert result.entry is None


def _risk_table(frame, *, delayed=0, groups=(0, 0)):
    table = pa.table(
        {
            "unit": groups,
            "time_bin": [0, 1],
            "events": [1, 0],
            "censored": [0, 2],
            "delayed_entries": [delayed, 0],
            "events__complete": [True, True],
            "events__reason": pa.array([None, None], type=pa.string()),
            "censored__complete": [True, True],
            "censored__reason": pa.array([None, None], type=pa.string()),
        }
    )
    return replace(
        frame,
        table=table,
        roles=Roles("unit", "time_bin", "time_bin", None, ("time_bin",), ("events", "censored")),
        columns={
            name: ColumnMeta(None, "outcome", None, None, f"{name}__complete", f"{name}__reason")
            for name in ("events", "censored")
        },
        provenance=replace(frame.provenance, shape="life_table"),
    )


def test_risk_counts_preserve_event_and_censor_weights(frame):
    result = exports.to_lifelines_km(_risk_table(frame))
    assert result.durations.tolist() == [0.0, 1.0]
    assert result.event_observed.tolist() == [True, False]
    assert result.weights is not None
    assert result.weights.tolist() == [1, 2]


@pytest.mark.parametrize(
    "delayed,groups,code",
    [
        (1, (0, 0), "terrine.lifelines.delayed_entry"),
        (0, (0, 1), "terrine.lifelines.unsupported_shape"),
    ],
)
def test_risk_table_refuses_unrecoverable_entry_or_pooled_groups(frame, delayed, groups, code):
    with pytest.raises(TerrineError) as error:
        exports.to_lifelines_km(_risk_table(frame, delayed=delayed, groups=groups))
    assert error.value.code == code
    assert error.value.context["route"] == "per-unit time-to-event records"


def test_incomplete_risk_bins_cannot_be_dropped_into_a_curve(frame):
    risk = _risk_table(frame)
    table = risk.table.set_column(5, "events__complete", pa.array([False, True])).set_column(
        6, "events__reason", pa.array(["incomplete_initial_history", None])
    )
    with pytest.raises(TerrineError) as error:
        exports.to_lifelines_km(replace(risk, table=table), drop_incomplete=True)
    assert error.value.code == "terrine.export.incomplete_outcome"


def test_risk_aggregation_retains_incomplete_reasons_for_preflight(frame):
    from datetime import UTC, datetime, timedelta

    from terrine.compile.shapes.life_table import aggregate_life_table

    origin = datetime(2025, 1, 1, tzinfo=UTC)
    records = pa.table(
        {
            "t0": [origin, origin],
            "duration": pa.array([timedelta(days=1)] * 2, type=pa.duration("us")),
            "entry_time": [origin, origin],
            "event": [False, False],
            "duration__complete": [False, False],
            "duration__reason": ["incomplete_initial_history", "missing_delayed_entry"],
            "censor_reason": ["incomplete_initial_history", "missing_delayed_entry"],
        }
    )
    table = aggregate_life_table(records, interval_seconds=86400, group_by=[])
    assert table["events__complete"].to_pylist() == [False, False]
    assert table["events__reason"].to_pylist() == [
        ["incomplete_initial_history", "missing_delayed_entry"],
        ["incomplete_initial_history", "missing_delayed_entry"],
    ]
    risk = replace(_risk_table(frame), table=table)
    with pytest.raises(TerrineError) as error:
        exports.to_lifelines_km(risk)
    assert error.value.code == "terrine.export.incomplete_outcome"
    assert error.value.context["excluded_counts"] == {
        "incomplete_initial_history": 2,
        "missing_delayed_entry": 2,
    }
