from datetime import UTC, datetime, timedelta

import pandas as pd

from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.spec import Design, EventAnchor, EventPanel, Initiation, Schedule, Sequential, agg
from tests.reference.panels import event_period_rows


def _event_model(native_inputs):
    catalog, con = native_inputs
    return Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )


def test_sequential_schedule_emits_one_row_per_unit_trial(native_inputs):
    model = _event_model(native_inputs)
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-05")),
        treatment=Initiation("events", "session", history_start=datetime(2024, 1, 1, tzinfo=UTC)),
    )

    result = design.build(model)._compiled.table.to_pyarrow().to_pylist()

    assert {(row["unit"], row["t0"]) for row in result} == {
        (unit, datetime(2025, 1, day, tzinfo=UTC)) for unit in ("a", "b") for day in (3, 4)
    }


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
