from datetime import UTC, datetime

import pandas as pd
import pytest

from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.spec import Design, Initiation, MatchedT0Sample, Schedule, Sequential
from tests.reference.panels import matched_t0_probabilities


def _model(native_inputs):
    catalog, con = native_inputs
    con.raw_sql("DELETE FROM events WHERE user_id = 'b'")
    con.raw_sql("INSERT INTO users VALUES ('c', 'cc'), ('d', 'dd')")
    return Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )


def test_matched_t0_samples_controls_without_replacement_and_records_probability(native_inputs):
    model = _model(native_inputs)
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-04"),
            sample=MatchedT0Sample((), controls_per_treated=2, seed=27),
        ),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    frame = design.build(model)
    roles = frame.roles
    rows = frame._compiled.table.to_pyarrow().to_pylist()

    assert len(rows) == 3
    treated = [row for row in rows if row["treatment"]]
    controls = [row for row in rows if not row["treatment"]]
    assert [row["unit"] for row in treated] == ["a"]
    assert treated[0]["assignment_probability"] == 1
    assert all(row["assignment_probability"] == pytest.approx(2 / 3) for row in controls)
    risk_set = pd.DataFrame(
        [
            {"unit": unit, "trial_id": "schedule:0", "treatment": unit == "a"}
            for unit in ("a", "b", "c", "d")
        ]
    )
    retained = pd.DataFrame(rows)[["unit", "trial_id", "treatment"]]
    expected = matched_t0_probabilities(
        risk_set, retained, treatment="treatment", strata=("trial_id",)
    )
    assert dict(zip(expected["unit"], expected["assignment_probability"], strict=True)) == {
        row["unit"]: row["assignment_probability"] for row in rows
    }
    assert roles.trial == "trial_id"
    assert roles.assignment_probability == "assignment_probability"
    assert roles.target_weight is None
    materialized = design.build(model).materialize()
    restored = type(materialized).from_arrow(materialized.to_arrow())
    assert restored.roles == roles
    assert restored.provenance.sampling is not None
    assert restored.provenance.sampling.strata == ()
    model.con.raw_sql("CREATE OR REPLACE TABLE users AS SELECT * FROM users ORDER BY user_id DESC")
    reordered_model = Model.from_catalog(
        model.catalog,
        con=model.con,
        readiness=model.readiness,
        units=model.units,
        event_ids=model.event_ids,
    )
    reordered = design.build(reordered_model)._compiled.table.to_pyarrow().to_pylist()
    assert {(row["unit"], row["assignment_probability"]) for row in reordered} == {
        (row["unit"], row["assignment_probability"]) for row in rows
    }


def test_matched_t0_refuses_insufficient_controls_during_materialization(native_inputs):
    from terrine.errors import TerrineError

    model = _model(native_inputs)
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-04"),
            sample=MatchedT0Sample((), controls_per_treated=4, seed=27),
        ),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    with pytest.raises(TerrineError) as error:
        design.build(model).materialize()

    assert error.value.code == "terrine.compile.insufficient_matched_t0_controls"


def test_matched_t0_strata_must_name_compiled_features(native_inputs):
    from terrine.errors import TerrineError

    model = _model(native_inputs)
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-04"),
            sample=MatchedT0Sample(("missing_stratum",), controls_per_treated=1, seed=0),
        ),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    with pytest.raises(TerrineError) as error:
        design.validate(model)

    assert error.value.code == "terrine.design.missing_reference"


def test_matched_t0_convenience_lowers_policy_onto_design(native_inputs):
    from terrine.spec import matched_t0

    model = _model(native_inputs)
    base = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-04")),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    frame = matched_t0(base, model, sample=MatchedT0Sample((), 1, 13))

    assert frame.roles.assignment_probability == "assignment_probability"
    materialized = frame.materialize()
    assert materialized.provenance.sampling is not None
    assert materialized.provenance.sampling.seed == 13


def test_matched_t0_keeps_no_controls_from_zero_treated_strata(native_inputs):
    model = _model(native_inputs)
    model.con.raw_sql("DELETE FROM events WHERE user_id = 'a'")
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-04"),
            sample=MatchedT0Sample((), controls_per_treated=1, seed=0),
        ),
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    )

    rows = design.build(model)._compiled.table.to_pyarrow().to_pylist()

    assert rows == []
