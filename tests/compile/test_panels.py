from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.spec import Design, EventAnchor, EventPanel, Initiation, Schedule, Sequential, agg
from tests.reference.panels import event_period_rows


def _event_model(native_inputs, coverage=None):
    catalog, con = native_inputs
    if coverage is None:
        coverage = Coverage("2025-01-01", "2025-01-31")
    return Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": coverage},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )


def test_sequential_schedule_emits_one_row_per_unit_trial(native_inputs):
    model = _event_model(native_inputs)
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-05")),
        treatment=Initiation("events", "session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    result = design.build(model)._compiled.table.to_pyarrow().to_pylist()

    assert {(row["unit"], row["t0"]) for row in result} == {
        (unit, datetime(2025, 1, day, tzinfo=UTC)) for unit in ("a", "b") for day in (3, 4)
    }


def test_initiation_is_classified_from_information_available_at_exact_t0(native_inputs):
    model = _event_model(native_inputs)
    model.con.raw_sql(
        "UPDATE events SET ts = TIMESTAMPTZ '2025-01-04 00:00:00+00' WHERE user_id = 'b'"
    )
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-05")),
        treatment=Initiation("events", "session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    rows = design.build(model)._compiled.table.to_pyarrow().to_pylist()

    assert {(row["unit"], row["t0"].day, row["treatment"]) for row in rows} == {
        ("a", 3, True),
        ("a", 4, True),
        ("b", 3, False),
        ("b", 4, True),
    }


def test_later_control_initiation_refuses_sustained_no_initiation_strategy(native_inputs):
    from terrine.errors import TerrineError

    model = _event_model(native_inputs)
    model.con.raw_sql(
        "UPDATE events SET ts = TIMESTAMPTZ '2025-01-04 00:00:00+00' WHERE user_id = 'b'"
    )
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-05")),
        treatment=Initiation("events", "session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    with pytest.raises(TerrineError) as error:
        design.build(model).materialize()

    assert error.value.code == "terrine.compile.later_control_initiation"


def test_event_panel_emits_only_declared_relative_periods(native_inputs):
    model = _event_model(native_inputs)
    design = EventPanel(
        Design("user", EventAnchor("session", "first")),
        periods=(-1, 0, 2),
        period_seconds=86_400,
    )

    result = design.build(model)._compiled.table.to_pyarrow().to_pylist()
    expected = event_period_rows(
        pd.DataFrame(
            {
                "unit": ["a", "b"],
                "event_id": ["a1", "b1"],
                "t0": [datetime(2025, 1, 3, tzinfo=UTC)] * 2,
            }
        ),
        (-1, 0, 2),
        86_400,
    )

    assert len(result) == 6
    assert {row["event_period"] for row in result} == {-1, 0, 2}
    assert {row["parent_t0"] for row in result} == {datetime(2025, 1, 3, tzinfo=UTC)}
    assert {(row["unit"], row["anchor_id"]["period"]) for row in result} == {
        (unit, period) for unit in ("a", "b") for period in (-1, 0, 2)
    }
    assert {(row["unit"], row["event_period"], row["t0"]) for row in result} == set(
        expected[["unit", "event_period", "t0"]].itertuples(index=False, name=None)
    )
    assert {row["t0"] for row in result} == {
        datetime(2025, 1, 3, tzinfo=UTC) + timedelta(days=period) for period in (-1, 0, 2)
    }


def test_event_period_translates_nontrivial_outcome_window(native_inputs):
    model = _event_model(native_inputs)
    design = EventPanel(
        Design(
            "user",
            EventAnchor("session", "first"),
            outcomes={"future_sessions": agg("sessions", window=("1d", "3d"))},
        ),
        periods=(-1, 0),
        period_seconds=86_400,
    )

    compiled = design.build(model)._compiled
    rows = compiled.table.to_pyarrow().to_pylist()

    assert {row["t0"] for row in rows} == {
        datetime(2025, 1, 2, tzinfo=UTC),
        datetime(2025, 1, 3, tzinfo=UTC),
    }
    window = compiled.columns["future_sessions"].window
    assert window is not None
    assert window.start == 86_400
    assert window.end == 259_200
    assert {(row["unit"], row["event_period"]) for row in rows} == {
        (unit, period) for unit in ("a", "b") for period in (-1, 0)
    }


def test_sequential_requires_complete_source_history_from_declared_start(native_inputs):
    from terrine.errors import TerrineError

    model = _event_model(native_inputs)
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-05")),
        treatment=Initiation("events", "session", history_start=datetime(2024, 1, 1, tzinfo=UTC)),
    )

    with pytest.raises(TerrineError) as error:
        design.validate(model)

    assert error.value.code == "terrine.design.incomplete_treatment_history"


def test_sequential_requires_source_history_through_last_trial_t0(native_inputs):
    from terrine.errors import TerrineError

    model = _event_model(native_inputs, Coverage("2025-01-01", "2025-01-03"))
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-05")),
        treatment=Initiation("events", "session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    with pytest.raises(TerrineError) as error:
        design.validate(model)

    assert error.value.code == "terrine.design.incomplete_treatment_history"


def test_sequential_exposes_trial_identity_and_normalized_aggregation_weights(native_inputs):
    from terrine.spec import TrialWeight

    model = _event_model(native_inputs)
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-05")),
        treatment=Initiation("events", "session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
        trial_weight=TrialWeight("equal_trial"),
    )

    rows = design.build(model)._compiled.table.to_pyarrow().to_pylist()

    assert {(row["trial_id"], row["trial_weight"]) for row in rows} == {
        ("schedule:0", 0.5),
        ("schedule:1", 0.5),
    }


def test_sequential_explicit_trial_weights_are_normalized_on_output(native_inputs):
    from terrine.spec import TrialWeight

    model = _event_model(native_inputs)
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-05")),
        treatment=Initiation("events", "session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
        trial_weight=TrialWeight("explicit", {"schedule:0": 2, "schedule:1": 6}),
    )

    rows = design.build(model)._compiled.table.to_pyarrow().to_pylist()

    assert {(row["trial_id"], row["trial_weight"]) for row in rows} == {
        ("schedule:0", 0.25),
        ("schedule:1", 0.75),
    }


def test_persistent_adoption_refuses_contradictory_history(native_inputs):
    from terrine.errors import TerrineError
    from terrine.semantic.models import Catalog
    from terrine.spec import PersistentAdoption

    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE events ADD COLUMN status VARCHAR")
    con.raw_sql("UPDATE events SET status = CASE WHEN user_id = 'a' THEN 'adopted' ELSE 'off' END")
    con.raw_sql(
        "INSERT INTO events (user_id, ts, event_id, alternate_event_id, status) "
        "VALUES ('a', TIMESTAMPTZ '2025-01-04 00:00:00+00', 'a3', 'a4', 'off')"
    )
    payload = catalog.model_dump(mode="python")
    payload["sources"]["events"]["properties"] = ("status",)
    payload["properties"]["status"] = {
        "source": "events",
        "column": "status",
        "dtype": "string",
        "as_of": "event_time",
    }
    catalog = Catalog.model_validate(payload)
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-05")),
        treatment=PersistentAdoption(
            "events",
            "status",
            "adopted",
            datetime(2025, 1, 1, tzinfo=UTC),
            persistence="absorbing",
        ),
    )

    with pytest.raises(TerrineError) as error:
        design.build(model).materialize()

    assert error.value.code == "terrine.compile.contradictory_persistent_adoption_history"


def test_sequential_names_explicit_target_weight_column(native_inputs):
    model = _event_model(native_inputs)
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-04"),
            features={"target_weight": agg("sessions", window=("-1d", "0d"))},
        ),
        treatment=Initiation("events", "session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    frame = design.build(model)

    assert frame.roles.target_weight == "target_weight"
    assert "target_weight" in frame._compiled.table.columns
