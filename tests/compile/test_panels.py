from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from terrine.compile.dispatch import compile_design
from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.spec import (
    CalendarPanel,
    Design,
    EventAnchor,
    EventPanel,
    Initiation,
    Schedule,
    Sequential,
    agg,
)
from tests.reference.panels import event_period_rows, sequential_rows


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


def _validated(design, model):
    design.validate(model)
    compiled = compile_design(design, model)
    compiled.validate(execute=model.con.to_pyarrow)
    return compiled


def test_sequential_schedule_emits_one_row_per_unit_trial(native_inputs):
    model = _event_model(native_inputs)
    base = Design("user", Schedule("1d", "2025-01-03", "2025-01-04"))
    sequential = Sequential(
        base,
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )
    calendar = CalendarPanel(sequential)
    results = [
        design.build(model)._compiled.table.to_pyarrow().to_pylist()
        for design in (sequential, calendar)
    ]
    expected = sequential_rows(
        units=("a", "b"),
        trial_times=(
            datetime(2025, 1, 3, tzinfo=UTC),
            datetime(2025, 1, 4, tzinfo=UTC),
        ),
        treatment_times={
            "a": datetime(2025, 1, 3, tzinfo=UTC),
            "b": datetime(2025, 1, 3, tzinfo=UTC),
        },
    )
    expected_rows = set(
        expected[["unit", "trial_id", "t0", "treatment"]].itertuples(index=False, name=None)
    )
    for result in results:
        actual_rows = {
            (row["unit"], row["trial_id"], row["t0"], row["treatment"]) for row in result
        }
        assert actual_rows == expected_rows
    assert results[0] == results[1]


def test_initiation_is_classified_from_information_available_at_exact_t0(native_inputs):
    model = _event_model(native_inputs)
    model.con.raw_sql(
        "UPDATE events SET ts = TIMESTAMPTZ '2025-01-04 00:00:00+00' WHERE user_id = 'b'"
    )
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-04")),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    rows = design.build(model)._compiled.table.to_pyarrow().to_pylist()

    assert {(row["unit"], row["t0"].day, row["treatment"]) for row in rows} == {
        ("a", 3, True),
        ("a", 4, True),
        ("b", 3, False),
        ("b", 4, True),
    }


def test_sequential_later_initiation_uses_reanchored_outcome_support(native_inputs):
    from terrine.errors import TerrineError
    from terrine.spec import event_time

    model = _event_model(native_inputs)
    model.con.raw_sql(
        "UPDATE events SET ts = TIMESTAMPTZ '2025-01-05 12:00:00+00' WHERE user_id = 'b'"
    )
    selector = event_time("session", window=("2d", "3d"))
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-03"),
            outcomes={
                "sessions_after_selector": agg(
                    "sessions", window=("0d", "1d"), relative_to=selector
                )
            },
        ),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    with pytest.raises(TerrineError) as error:
        _validated(design, model)

    assert error.value.code == "terrine.compile.later_control_initiation"


@pytest.mark.parametrize("wrapped", [False, True], ids=["plain", "predicate"])
@pytest.mark.parametrize("nested", [False, True], ids=["direct", "nested"])
def test_sequential_followup_includes_outcome_selector_dependencies(native_inputs, wrapped, nested):
    import ibis

    from terrine.compile.shapes.sequential import _followup_at
    from terrine.errors import TerrineError
    from terrine.spec import event_time, gt

    model = _event_model(native_inputs)
    model.con.raw_sql(
        "UPDATE events SET ts = TIMESTAMPTZ '2025-01-09 18:00:00+00' WHERE user_id = 'b'"
    )
    selector = event_time("session", window=("2d", "7d"))
    window = ("-2d", "-1d")
    if nested:
        selector = event_time("session", window=("-1d", "-12h"), relative_to=selector)
        window = ("-1d", "-12h")
    value = agg("sessions", window=window, relative_to=selector)
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-03"),
            outcomes={"label": gt(value, 0) if wrapped else value},
        ),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )
    assert model.con.execute(
        _followup_at(design, model, ibis.literal(datetime(2025, 1, 3, tzinfo=UTC)))
    ).to_pydatetime() == datetime(2025, 1, 10, tzinfo=UTC)

    with pytest.raises(TerrineError) as error:
        _validated(design, model)

    assert error.value.code == "terrine.compile.later_control_initiation"


def test_sequential_followup_covers_predicate_outcome_horizon(native_inputs):
    import ibis

    from terrine.compile.shapes.sequential import _followup_at
    from terrine.spec import all_of, eq, gt

    model = _event_model(native_inputs)
    aggregate = agg("sessions", window=("0s", "30d"))
    values = (
        aggregate,
        all_of(eq(agg("sessions", window=("2d", "3d")), 0), gt(aggregate, 0)),
    )
    ends = []
    for value in values:
        design = Sequential(
            Design(
                "user",
                Schedule("1d", "2025-01-03", "2025-01-03"),
                outcomes={"label": value},
            ),
            treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
        )
        ends.append(
            model.con.execute(
                _followup_at(design, model, ibis.literal(datetime(2025, 1, 3, tzinfo=UTC)))
            ).to_pydatetime()
        )
    assert ends == [datetime(2025, 2, 2, tzinfo=UTC)] * 2


def test_later_control_initiation_refuses_sustained_no_initiation_strategy(native_inputs):

    from terrine.errors import TerrineError

    model = _event_model(native_inputs)
    model.con.raw_sql(
        "UPDATE events SET ts = TIMESTAMPTZ '2025-01-04 00:00:00+00' WHERE user_id = 'b'"
    )
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-04")),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    with pytest.raises(TerrineError) as error:
        _validated(design, model)

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
            features={"prior_sessions": agg("sessions", window=("-2d", "-1d"))},
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
    expected = event_period_rows(
        pd.DataFrame(
            {
                "unit": ["a", "b"],
                "event_id": ["a1", "b1"],
                "t0": [datetime(2025, 1, 3, tzinfo=UTC)] * 2,
            }
        ),
        (-1, 0),
        86_400,
        windows={"future_sessions": (86_400, 259_200)},
        cutoffs={"prior_sessions": -86_400},
    )
    expected_rows = expected.to_dict("records")
    assert {row["windows"]["future_sessions"] for row in expected_rows} == {
        (
            datetime(2025, 1, 3, tzinfo=UTC),
            datetime(2025, 1, 5, tzinfo=UTC),
        ),
        (
            datetime(2025, 1, 4, tzinfo=UTC),
            datetime(2025, 1, 6, tzinfo=UTC),
        ),
    }
    assert {row["cutoffs"]["prior_sessions"] for row in expected_rows} == {
        datetime(2025, 1, 1, tzinfo=UTC),
        datetime(2025, 1, 2, tzinfo=UTC),
    }
    assert compiled.feature_cutoffs["prior_sessions"].latest_seconds == -86_400


def test_sequential_requires_complete_source_history_from_declared_start(native_inputs):
    from terrine.errors import TerrineError

    model = _event_model(native_inputs)
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-04")),
        treatment=Initiation("session", history_start=datetime(2024, 1, 1, tzinfo=UTC)),
    )

    with pytest.raises(TerrineError) as error:
        design.validate(model)

    assert error.value.code == "terrine.design.incomplete_treatment_history"


def test_sequential_requires_source_history_through_last_trial_t0(native_inputs):
    from terrine.errors import TerrineError

    model = _event_model(native_inputs, Coverage("2025-01-01", "2025-01-03"))
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-04")),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    with pytest.raises(TerrineError) as error:
        design.validate(model)

    assert error.value.code == "terrine.design.incomplete_treatment_history"


def test_sequential_exposes_trial_identity_and_normalized_aggregation_weights(native_inputs):
    from terrine.spec import TrialWeight

    model = _event_model(native_inputs)
    design = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-04")),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
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
        Design("user", Schedule("1d", "2025-01-03", "2025-01-04")),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
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
        Design("user", Schedule("1d", "2025-01-03", "2025-01-04")),
        treatment=PersistentAdoption(
            "status", adopted_value="adopted", history_start=datetime(2025, 1, 1, tzinfo=UTC)
        ),
    )

    with pytest.raises(TerrineError) as error:
        _validated(design, model)

    assert error.value.code == "terrine.compile.contradictory_persistent_adoption_history"


def test_sequential_names_explicit_target_weight_column(native_inputs):
    model = _event_model(native_inputs)
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-03"),
            features={"target_weight": agg("sessions", window=("-1d", "0d"))},
        ),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    frame = design.build(model)

    assert frame.roles.target_weight == "target_weight"
    assert "target_weight" in frame._compiled.table.columns


def test_later_initiation_at_trial_specific_exclusive_horizon_is_not_refused(native_inputs):
    model = _event_model(native_inputs)
    model.con.raw_sql(
        "UPDATE events SET ts = TIMESTAMPTZ '2025-01-10 12:00:00+00' WHERE user_id = 'b'"
    )
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-03"),
            outcomes={"sessions_after_t0": agg("sessions", window=("0d", "7d"))},
        ),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    _validated(design, model)


@pytest.mark.parametrize(
    ("observed_until", "raises"),
    [("2025-01-10 00:00:00+00", False), ("2025-01-09 23:59:59+00", True)],
)
def test_sequential_roster_history_uses_exclusive_trial_horizon(
    native_inputs, observed_until, raises
):
    from terrine.errors import TerrineError

    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE users ADD observed_until TIMESTAMPTZ")
    con.raw_sql(f"UPDATE users SET observed_until = TIMESTAMPTZ '{observed_until}'")
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id", observed_until="observed_until")},
        event_ids={"events": "event_id"},
    )
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-03"),
            outcomes={"sessions_after_t0": agg("sessions", window=("0d", "7d"))},
        ),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    if raises:
        with pytest.raises(TerrineError) as error:
            _validated(design, model)
        assert error.value.code == "terrine.design.incomplete_unit_treatment_history"
    else:
        _validated(design, model)


def test_sequential_roster_history_ignores_ineligible_anchor_rows(native_inputs):
    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE users ADD observed_from TIMESTAMPTZ")
    con.raw_sql(
        "UPDATE users SET observed_from = TIMESTAMPTZ '2025-01-02 00:00:00+00' WHERE user_id = 'b'"
    )
    con.raw_sql(
        "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-02 00:00:00+00', 'a0', 'a00')"
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id", observed_from="observed_from")},
        event_ids={"events": "event_id"},
    )
    from terrine.spec import gt

    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-03"),
            eligibility={"prior_session": gt(agg("sessions", window=("-1d", "0d")), 0)},
        ),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    rows = model.con.to_pyarrow(_validated(design, model).table).to_pylist()

    assert [row["unit"] for row in rows] == ["a"]


def test_sequential_anticipation_requires_source_coverage_through_buffer(native_inputs):
    from terrine.errors import TerrineError

    model = _event_model(native_inputs, Coverage("2025-01-01", "2025-01-10"))
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-03"),
            outcomes={"sessions_after_t0": agg("sessions", window=("0d", "7d"))},
        ),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
        anticipation="2d",
    )

    with pytest.raises(TerrineError) as error:
        design.build(model)
    assert error.value.code == "terrine.design.incomplete_treatment_history"


def test_sequential_anticipation_preserves_schedule_fallback_without_outcomes(native_inputs):
    from terrine.errors import TerrineError

    model = _event_model(native_inputs)
    model.con.raw_sql(
        "UPDATE events SET ts = TIMESTAMPTZ '2025-01-06 00:00:00+00' WHERE user_id = 'b'"
    )
    design = Sequential(
        Design("user", Schedule("7d", "2025-01-03", "2025-01-09")),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
        anticipation="2d",
    )

    with pytest.raises(TerrineError) as error:
        _validated(design, model)
    assert error.value.code == "terrine.compile.later_control_initiation"
