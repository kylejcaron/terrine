"""Target-normalized Horvitz–Thompson baseline ATE."""

from typing import Literal

from terrine.data import MaterializedFrame
from terrine.estimate._estimate import estimate
from terrine.estimate.nuisance import NuisanceSpec
from terrine.results import DiagnosticPolicy, Estimate


def ipw(
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
    """Estimate an ATE with honest propensity fits and joint parametric uncertainty."""
    return estimate(
        frame,
        method="ipw",
        outcome=outcome,
        estimand=estimand,
        folds=folds,
        seed=seed,
        alpha=alpha,
        nuisance=nuisance,
        diagnostics=diagnostics,
    )
