from __future__ import annotations

import numpy as np
import pytest

from terrine.simulate import PanelWorldConfig, simulate_panel
from terrine.simulate.truth import panel_ate


def test_panel_world_generates_trial_varying_potential_outcomes_and_truth():
    config = PanelWorldConfig(
        n_units=40,
        effect_kind="heterogeneous",
        trial_masses="unequal",
        overlap="overlap",
        assignment_profile="profile_a",
        outcome_noise_sd=0,
    )
    world = simulate_panel(config, seed=13)
    observed, oracle = world.observations.to_pylist(), world.oracle.to_pylist()

    assert {row["lambda"] for row in observed} == {0.25, 0.75}
    assert panel_ate(world) == pytest.approx(3.75)
    for row, truth in zip(observed, oracle, strict=True):
        expected = truth["y1"] if row["a"] else truth["y0"]
        assert row["y"] == expected
        assert truth["y1"] - truth["y0"] == pytest.approx(truth["effect"], abs=1e-14)
    assert {row["e"] for row in oracle if row["trial"] == "early"} != {
        row["e"] for row in oracle if row["trial"] == "late"
    }


def test_zero_effect_world_truth_is_zero_for_every_trial():
    world = simulate_panel(
        PanelWorldConfig(n_units=24, effect_kind="zero", outcome_noise_sd=0), seed=4
    )
    assert panel_ate(world) == 0.0
    assert set(world.oracle["effect"].to_pylist()) == {0.0}


def test_panel_world_is_seed_reproducible_and_disjoint_trials_have_distinct_units():
    config = PanelWorldConfig(n_units=12, overlap="disjoint")
    left, right = simulate_panel(config, seed=91), simulate_panel(config, seed=91)
    assert left.observations.equals(right.observations)
    assert left.oracle.equals(right.oracle)
    rows = left.observations.to_pylist()
    early = {row["unit"] for row in rows if row["trial"] == "early"}
    late = {row["unit"] for row in rows if row["trial"] == "late"}
    assert early.isdisjoint(late)
    assert len(early | late) == 24


def test_panel_truth_has_an_independent_realized_cohort_target():
    world = simulate_panel(PanelWorldConfig(n_units=9, outcome_noise_sd=0), seed=17)
    expected = np.mean([row["effect"] for row in world.oracle.to_pylist()])
    assert panel_ate(world, target="cohort") == pytest.approx(expected)
