from datetime import UTC, datetime

from terrine.data import Coverage, UnitSource
from terrine.export import ArrowSurrogate, to_surrogate_arrow
from terrine.model import Model
from terrine.spec import (
    Design,
    FixedDate,
    Initiation,
    MatchedT0Sample,
    Schedule,
    Sequential,
    TrialWeight,
    agg,
    event_time,
)


def test_surrogate_arrow_round_trip_preserves_selected_columns_and_provenance(frame):
    exported = to_surrogate_arrow(frame, features=("x",), outcomes=("y",))
    restored = ArrowSurrogate.from_arrow(exported.to_arrow())

    assert restored.data.equals(exported.data)
    assert restored.metadata == exported.metadata
    assert {"id", "anchor", "t0", "x", "y", "complete", "reason"} <= set(restored.data.column_names)
    assert restored.metadata["feature_columns"] == ["x"]
    assert restored.metadata["outcome_columns"] == ["y"]


def test_surrogate_round_trip_retains_selected_event_relative_timing(native_inputs):
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={name: Coverage("2025-01-01", "2025-01-31") for name in ("events", "labels")},
        units={"user": UnitSource("users", "user_id")},
    )
    design = Design(
        "user",
        FixedDate("2025-01-06"),
        features={
            "selected": agg(
                "sessions",
                window=("0d", "1d"),
                relative_to=event_time("session", window=("-4d", "-2d")),
            ),
            "excluded": event_time("churn", window=("-4d", "-2d")),
        },
    )
    materialized = design.build(model).materialize()
    timing = materialized.provenance.expression_timings["selected"]
    support = (timing.point_time_column, timing.start_bound_column, timing.end_bound_column)
    exported = to_surrogate_arrow(materialized, features=("selected",), outcomes=())
    restored = ArrowSurrogate.from_arrow(exported.to_arrow())
    support_columns = restored.metadata["support_columns"]
    provenance = restored.metadata["provenance"]
    assert isinstance(support_columns, list)
    assert isinstance(provenance, dict)

    for name in support:
        assert name is not None
        assert name in restored.data.column_names
        assert restored.data[name].equals(materialized.table[name])
        assert name in support_columns
    assert provenance["expression_timings"] == {
        "selected": materialized.to_metadata()["provenance"]["expression_timings"]["selected"]
    }
    assert "excluded" not in restored.data.column_names
    assert restored.metadata == exported.metadata


def test_surrogate_round_trip_retains_sampled_sequential_roles(native_inputs):
    catalog, con = native_inputs
    con.raw_sql("DELETE FROM events WHERE user_id = 'b'")
    con.raw_sql("INSERT INTO users VALUES ('c', 'cc'), ('d', 'dd')")
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
    )
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-04"),
            features={"target_weight": agg("sessions", window=("-1d", "0d"))},
            sample=MatchedT0Sample((), controls_per_treated=2, seed=27),
        ),
        treatment=Initiation("events", "session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
        trial_weight=TrialWeight("equal_trial"),
    )
    materialized = design.build(model).materialize()
    exported = to_surrogate_arrow(materialized, features=(), outcomes=("treatment",))
    restored = ArrowSurrogate.from_arrow(exported.to_arrow())
    roles = restored.metadata["roles"]
    provenance = restored.metadata["provenance"]
    assert isinstance(roles, dict)
    assert isinstance(provenance, dict)

    for role in ("trial", "assignment_probability", "target_weight", "trial_weight"):
        name = getattr(materialized.roles, role)
        assert name is not None
        assert roles[role] == name
        assert name in restored.data.column_names
        assert restored.data[name].equals(materialized.table[name])
    assert provenance["sampling"] == materialized.to_metadata()["provenance"]["sampling"]
    assert restored.metadata["feature_columns"] == []
    assert restored.metadata == exported.metadata
