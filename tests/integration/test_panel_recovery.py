import pytest

from terrine.estimate.panel import panel_aipw
from tests.parameter_recovery.test_panel_calibration import _frame


@pytest.mark.parametrize("seed", (0, 7, 19))
def test_heterogeneous_cohort_effects_recover_across_seeds(seed):
    cell = ("heterogeneous", "unequal", "overlap", "profile_a")
    result = panel_aipw(
        _frame(seed, cell, n_units=120),
        outcome="y",
        treatment="a",
        trial="trial",
        folds=3,
        seed=seed,
    )
    assert result.value == pytest.approx(3.75, abs=1.0)
    assert result.n == 240
    assert result.n_clusters == 120
