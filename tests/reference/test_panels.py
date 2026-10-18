from datetime import UTC, datetime

import pandas as pd

from tests.reference.panels import (
    calendar_panel_rows,
    event_period_rows,
    matched_t0_probabilities,
    sequential_rows,
)


def test_calendar_panel_reference_is_canonical_sequential_reference():
    trials = (datetime(2025, 1, 3, tzinfo=UTC), datetime(2025, 1, 4, tzinfo=UTC))
    common = {
        "units": ("a", "b"),
        "trial_times": trials,
        "treatment_times": {"a": trials[0]},
        "trial_weights": (0.25, 0.75),
        "windows": {"y": (-86_400, 0)},
        "cutoffs": {"x": 3_600},
    }
    sequential = sequential_rows(**common)
    calendar = calendar_panel_rows(**common)

    pd.testing.assert_frame_equal(calendar, sequential)
    assert sequential[["unit", "trial_id", "treatment"]].to_dict("records") == [
        {"unit": unit, "trial_id": f"schedule:{trial_index}", "treatment": treatment}
        for trial_index in range(2)
        for unit, treatment in (("a", True), ("b", False))
    ]
    assert sequential.loc[0, "windows"]["y"] == (
        datetime(2025, 1, 2, tzinfo=UTC),
        datetime(2025, 1, 3, tzinfo=UTC),
    )
    assert sequential.loc[0, "cutoffs"]["x"] == datetime(2025, 1, 3, 1, tzinfo=UTC)


def test_event_period_reference_retains_parent_identity_and_translates_windows():
    events = pd.DataFrame(
        [{"unit": "u1", "event_id": "e1", "t0": datetime(2025, 1, 3, tzinfo=UTC)}]
    )
    rows = event_period_rows(
        events,
        (-1, 0, 2),
        86_400,
        windows={"outcome": (86_400, 259_200)},
        cutoffs={"feature": 3_600},
    )

    assert rows[["unit", "parent_event_id", "event_period", "anchor_id"]].to_dict("records") == [
        {"unit": "u1", "parent_event_id": "e1", "event_period": period, "anchor_id": ("e1", period)}
        for period in (-1, 0, 2)
    ]
    assert rows.iloc[0]["t0"] == datetime(2025, 1, 2, tzinfo=UTC)
    assert rows.iloc[0]["windows"]["outcome"] == (
        datetime(2025, 1, 3, tzinfo=UTC),
        datetime(2025, 1, 5, tzinfo=UTC),
    )
    assert rows.iloc[0]["cutoffs"]["feature"] == datetime(2025, 1, 2, 1, tzinfo=UTC)


def test_matched_t0_reference_computes_exact_control_inclusion_probabilities():
    risk_set = pd.DataFrame(
        [
            {"unit": "t", "t0": "2025-01-03", "region": "north", "treatment": True},
            *[
                {"unit": f"c{i}", "t0": "2025-01-03", "region": "north", "treatment": False}
                for i in range(4)
            ],
        ]
    )
    retained = risk_set.iloc[[0, 1, 2]]
    expected = matched_t0_probabilities(
        risk_set,
        retained,
        treatment="treatment",
        strata=("t0", "region"),
    )
    assert expected["assignment_probability"].tolist() == [1.0, 0.5, 0.5]
