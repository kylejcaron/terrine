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
