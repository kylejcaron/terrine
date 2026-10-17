"""Separate-arm regression-adjusted baseline ATE."""

from typing import Literal

from terrine.data import MaterializedFrame
from terrine.estimate._estimate import estimate
from terrine.estimate.nuisance import NuisanceSpec
from terrine.results import DiagnosticPolicy, Estimate


def regression_adjustment(
    frame: MaterializedFrame,
    *,
    outcome: str | None = None,
    estimand: Literal["ATE"] = "ATE",
    folds: int = 5,
    seed: int = 0,
    alpha: float = 0.05,
    nuisance: NuisanceSpec | None = None,
    diagnostics: DiagnosticPolicy | None = None,
) -> Estimate:
    """Estimate an ATE; propensity fits supply diagnostic-only overlap evidence."""
    return estimate(
        frame,
        method="regression_adjustment",
        outcome=outcome,
        estimand=estimand,
        folds=folds,
        seed=seed,
        alpha=alpha,
        nuisance=nuisance,
        diagnostics=diagnostics,
    )
