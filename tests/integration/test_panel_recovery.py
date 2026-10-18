import pytest

from terrine.estimate.panel import panel_aipw
from tests.estimate.test_panel_aipw import panel_frame


@pytest.mark.parametrize("seed", (0, 7, 19))
def test_heterogeneous_cohort_effects_recover_across_seeds(seed):
    frame = panel_frame()
    measured = frame
    result = panel_aipw(measured, outcome="y", treatment="a", trial="trial", folds=3, seed=seed)
    # Unequal declared trial masses (1:3) weight the true trial effects 1.5, 4.5.
    assert result.value == pytest.approx(3.75, abs=0.75)
    assert result.n == 360
    assert result.n_clusters == 180
