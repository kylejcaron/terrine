import pytest

from terrine.data import Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.spec import Design, EventAnchor, agg


def _frame(native_inputs, *, label_coverage_end="2025-01-31", missing_label=False):
    catalog, con = native_inputs
    if missing_label:
        labels = con.table("labels").filter(con.table("labels").user_id != "b").execute()
        con.drop_table("labels")
        con.create_table("labels", labels)
    readiness = {name: Coverage("2025-01-01", "2025-01-31") for name in catalog.sources}
    readiness["labels"] = Coverage("2025-01-01", label_coverage_end)
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness=readiness,
        units={"user": UnitSource("users", "user_id")},
    )
    return Design(
        "user",
        EventAnchor("session"),
        features={
            "past_sessions": agg("sessions", window=("-1d", "0d")),
            "earlier_sessions": agg("sessions", window=("-2d", "-1d")),
        },
        outcomes={"churn": agg("churned", window=("0d", "1d"))},
    ).build(model)


def test_training_set_selects_named_features_and_derives_label_support(native_inputs):
    frame = _frame(native_inputs)
    training = frame.training_set(label="churn", features=("past_sessions",))
    assert training is not frame
    assert training.roles.features == ("past_sessions",)
    assert training.roles.outcomes == ("churn",)
    assert "past_sessions" in training.expr.columns
    assert "earlier_sessions" not in training.expr.columns
    assert training.columns["churn"].complete_column
    assert training.columns["churn"].reason_column


def test_training_set_rejects_unknown_or_duplicate_selections(native_inputs):
    frame = _frame(native_inputs)
    with pytest.raises(TerrineError):
        frame.training_set(label="missing")
    with pytest.raises(ValueError):
        frame.training_set(label=("churn", "churn"))


def test_training_set_incomplete_label_policy_is_explicit(native_inputs):
    frame = _frame(native_inputs)
    kept = frame.training_set(label="churn", incomplete_labels="keep")
    dropped = frame.training_set(label="churn", incomplete_labels="drop")
    assert kept._training_view.incomplete_labels == "keep"
    assert dropped._training_view.incomplete_labels == "drop"
    with pytest.raises(ValueError):
        frame.training_set(label="churn", incomplete_labels="impute")


def test_incomplete_labels_drop_rows_and_keep_null_reasons(native_inputs):
    frame = _frame(native_inputs, label_coverage_end="2025-01-01", missing_label=True)
    dropped = frame.training_set(label="churn", incomplete_labels="drop").materialize()
    kept = frame.training_set(label="churn", incomplete_labels="keep").materialize()
    assert dropped.table.num_rows == 0, (kept.columns["churn"], kept.table.to_pylist())
    assert kept.table.num_rows == 2
    assert kept.table["churn"].null_count == 2
    reason_column = kept.columns["churn"].reason_column
    assert reason_column is not None
    assert kept.table[reason_column].to_pylist() == ["incomplete_coverage", "incomplete_coverage"]
    assert dropped.provenance.dataset_view.counts_by_reason == {"incomplete_coverage": 2}


def test_training_projection_preserves_baseline_treatment_role(native_inputs):
    from terrine.semantic.models import Catalog
    from terrine.spec import FixedDate, Treatment

    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE events ADD COLUMN enabled BOOLEAN")
    con.raw_sql("UPDATE events SET enabled = (user_id = 'a')")
    definitions = catalog.model_dump(mode="json")
    definitions["sources"]["events"]["properties"] = ["enabled"]
    definitions["properties"]["enabled"] = {
        "source": "events",
        "column": "enabled",
        "dtype": "bool",
        "as_of": "event_time",
    }
    model = Model.from_catalog(
        Catalog.model_validate(definitions),
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )
    frame = Design(
        "user",
        FixedDate("2025-01-04"),
        outcomes={"future": agg("sessions", window=("0s", "1d"))},
        treatment=Treatment("enabled", known_at_t0=True),
    ).build(model)

    materialized = frame.training_set(label="future", features=()).materialize()
    assert materialized.roles.treatment == "treatment"
    assert "treatment" in materialized.table.column_names


def test_training_view_hashes_selected_feature_definition_and_parent_design(native_inputs):
    from terrine.spec import EventAnchor

    frame = _frame(native_inputs)
    first = frame.training_set(label="churn", features=("past_sessions",)).materialize()
    second = frame.training_set(label="churn", features=("earlier_sessions",)).materialize()
    assert (
        first.provenance.dataset_view.feature_definition_hash
        != second.provenance.dataset_view.feature_definition_hash
    )

    changed_design = Design(
        "user",
        EventAnchor("session"),
        features={"past_sessions": agg("sessions", window=("-3d", "0d"))},
        outcomes={"churn": agg("churned", window=("0d", "1d"))},
    ).build(frame._model)
    changed = changed_design.training_set(label="churn", features=("past_sessions",)).materialize()
    first_view = first.provenance.dataset_view
    changed_view = changed.provenance.dataset_view
    assert first_view is not None and changed_view is not None
    assert first_view.view_hash != changed_view.view_hash


def test_training_view_does_not_claim_unapplied_split(native_inputs):
    from terrine.dataset.splits import TimeSplit

    frame = _frame(native_inputs)
    with pytest.raises(TerrineError) as error:
        frame.training_set(label="churn", split=TimeSplit(train_until="2025-01-05"))
    assert error.value.code == "terrine.dataset.split_not_applied"


def test_downstream_vocabulary_is_fitted_only_from_training_rows():
    from terrine.dataset.splits import TimeSplit, assign_splits

    rows = [
        {
            "id": "train",
            "origin": "2025-01-01T00:00:00+00:00",
            "label_end": "2025-01-02T00:00:00+00:00",
            "token": "training-only",
        },
        {
            "id": "validation",
            "origin": "2025-01-04T00:00:00+00:00",
            "label_end": "2025-01-05T00:00:00+00:00",
            "token": "validation-only",
        },
    ]
    assignments = assign_splits(
        rows,
        TimeSplit(train_until="2025-01-03T00:00:00+00:00"),
    )
    vocabulary = {row["token"] for row in rows if assignments[row["id"]].split == "train"}
    assert vocabulary == {"training-only"}
