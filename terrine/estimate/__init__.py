"""Native fixed-cohort baseline ATE estimation, independent of warehouse APIs."""

from terrine.estimate.aipw import aipw
from terrine.estimate.ipw import ipw
from terrine.estimate.nuisance import LinearTerms, NuisanceSpec
from terrine.estimate.regression import regression_adjustment
from terrine.results import DiagnosticPolicy

__all__ = [
    "ipw",
    "aipw",
    "regression_adjustment",
    "LinearTerms",
    "NuisanceSpec",
    "DiagnosticPolicy",
]
