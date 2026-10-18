"""Predeclared repeated-trial AIPW recovery grid; run only in calibration tier."""

from __future__ import annotations

import itertools
import json
import math
import os
import platform
import statistics
import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import Literal, cast

import numpy as np
import pytest
from scipy.stats import binomtest, chi2, norm

from terrine.data import ColumnMeta, TrialWeightMeta
from terrine.errors import TerrineError
from terrine.estimate.panel import panel_aipw
from terrine.estimate.stacked import stacked_event_study
from terrine.simulate import PanelWorldConfig, simulate_panel
from terrine.simulate.truth import panel_ate
from terrine.windows import Window
from tests.estimate.test_panel_aipw import panel_frame
from tests.estimate.test_stacked_event_study import _frame as _stacked_frame

SMOKE_SEEDS = (1, 2, 7)
SMOKE_ONLY = os.environ.get("TERRINE_CALIBRATION_SMOKE") == "1"
SMOKE_N_UNITS = int(os.environ.get("TERRINE_CALIBRATION_SMOKE_N_UNITS", "120"))
SEEDS = SMOKE_SEEDS if SMOKE_ONLY else tuple(range(400))
N_UNITS = SMOKE_N_UNITS if SMOKE_ONLY else 2000
FAMILY_ALPHA = 0.01
CELLS = tuple(
    itertools.product(
        ("zero", "heterogeneous"),
        ("equal", "unequal"),
        ("disjoint", "overlap"),
        ("profile_a", "profile_b"),
    )
)
PER_GATE_ALPHA = FAMILY_ALPHA / (3 * len(CELLS))
BIAS_CRITICAL = float(norm.isf(PER_GATE_ALPHA / 2))
STUDENTIZATION_LIMITS = (
    float(chi2.ppf(PER_GATE_ALPHA / 2, len(SEEDS))),
    float(chi2.ppf(1 - PER_GATE_ALPHA / 2, len(SEEDS))),
)


def _config(cell: tuple[str, str, str, str], n_units: int = N_UNITS) -> PanelWorldConfig:
    effect_kind, mass_kind, overlap_kind, profile = cell
    return PanelWorldConfig(
        n_units=n_units,
        effect_kind=cast(Literal["zero", "heterogeneous"], effect_kind),
        trial_masses=cast(Literal["equal", "unequal"], mass_kind),
        overlap=cast(Literal["disjoint", "overlap"], overlap_kind),
        assignment_profile=cast(Literal["profile_a", "profile_b"], profile),
    )


def _frame(seed: int, cell: tuple[str, str, str, str], n_units: int = N_UNITS):
    base = panel_frame()
    world = simulate_panel(_config(cell, n_units), seed=seed)
    weights = {"early": 1.0, "late": 1.0} if cell[1] == "equal" else {"early": 1.0, "late": 3.0}
    meta = replace(base.provenance.panel, trial_weight=TrialWeightMeta("explicit", weights))
    provenance = replace(base.provenance, panel=meta)
    return replace(base, table=world.observations, provenance=provenance)


def _paired_refusal(seed: int) -> dict[str, str]:
    frame = _stacked_frame()
    columns = dict(frame.columns)
    columns["y"] = ColumnMeta(Window("0d", "7d"), "outcome", None, None, None, None)
    contaminated = replace(frame, columns=columns)
    try:
        stacked_event_study(
            contaminated,
            outcome="y",
            treatment="treatment",
            cohort="cohort",
            period="period",
        )
    except TerrineError as error:
        return {"seed": str(seed), "code": error.code, "reason": str(error.context["reason"])}
    return {"seed": str(seed), "code": "unexpected_success", "reason": "contaminated_reference"}


def _truth(cell: tuple[str, str, str, str]) -> float:
    return panel_ate(simulate_panel(_config(cell, n_units=1), seed=0))


def test_zero_effect_calibration_truth_is_zero():
    assert _truth(("zero", "equal", "disjoint", "profile_a")) == 0.0


def _manifest(cell: tuple[str, str, str, str]) -> dict:
    return {
        "cell": {
            "effect": cell[0],
            "trial_masses": cell[1],
            "unit_overlap": cell[2],
            "profile": cell[3],
        },
        "seeds": list(SEEDS),
        "n_units_per_trial": N_UNITS,
        "run_mode": "smoke_only" if SMOKE_ONLY else "full_calibration",
        "alpha": 0.05,
        "family_alpha": FAMILY_ALPHA,
        "bias_log_two_sided_critical": BIAS_CRITICAL,
        "studentization_chi_square_limits": list(STUDENTIZATION_LIMITS),
        "paired_refusal_attempted": 0,
        "paired_refusal_completed": 0,
        "paired_refusal_codes": [],
        "attempted": 0,
        "completed": 0,
        "refused": 0,
        "refusals": [],
        "errors": [],
    }


def _evidence_path(cell: tuple[str, str, str, str]) -> Path:
    directory = Path(
        os.environ.get("TERRINE_CALIBRATION_DIR", "docs/validation/generated/panel_calibration")
    )
    directory.mkdir(parents=True, exist_ok=True)
    return directory / ("-".join(cell) + ".json")


@pytest.mark.slow
@pytest.mark.parameter_recovery
@pytest.mark.parametrize("cell", CELLS, ids=lambda cell: "-".join(cell))
def test_panel_calibration_cell(cell):
    evidence = _manifest(cell)
    estimates, standard_errors, widths, statuses, covered = [], [], [], [], []
    started = time.perf_counter()
    truth = _truth(cell)
    for seed in SEEDS:
        evidence["paired_refusal_attempted"] += 1
        refusal = _paired_refusal(seed)
        evidence["paired_refusal_codes"].append(refusal)
        if (
            refusal["code"] == "terrine.estimate.stacked_input"
            and refusal["reason"] == "outcome_window_contaminates_reference"
        ):
            evidence["paired_refusal_completed"] += 1
        evidence["attempted"] += 1
        try:
            result = panel_aipw(
                _frame(seed, cell),
                outcome="y",
                treatment="a",
                trial="trial",
                folds=5,
                seed=seed,
            )
            estimates.append(result.value)
            standard_errors.append(result.se)
            widths.append(result.ci[1] - result.ci[0])
            covered.append(result.ci[0] <= truth <= result.ci[1])
            statuses.extend(item.status for item in result.diagnostics)
            evidence["completed"] += 1
        except TerrineError as error:
            evidence["refused"] += 1
            evidence["refusals"].append({"seed": seed, "code": error.code})
        except Exception as error:
            evidence["refused"] += 1
            evidence["errors"].append(
                {"seed": seed, "type": type(error).__name__, "detail": str(error)}
            )
    if estimates:
        errors = np.asarray(estimates, dtype=np.float64) - truth
        bias = float(errors.mean())
        empirical_sd = float(errors.std(ddof=1))
        bias_z = abs(bias) / (empirical_sd / math.sqrt(len(errors)))
        bias_log_p = math.log(2.0) + float(norm.logsf(bias_z))
        studentization = float(np.sum((errors / np.asarray(standard_errors)) ** 2))
        studentization_log_p = math.log(2.0) + min(
            float(chi2.logcdf(studentization, len(errors))),
            float(chi2.logsf(studentization, len(errors))),
        )
        coverage_count = int(sum(covered))
        coverage_p = float(binomtest(coverage_count, len(errors), 0.95).pvalue)
        evidence.update(
            {
                "truth": truth,
                "bias": bias,
                "empirical_sd": empirical_sd,
                "mean_se": float(np.mean(standard_errors)),
                "rmse": float(np.sqrt(np.mean(errors * errors))),
                "coverage_count": coverage_count,
                "median_interval_width": float(statistics.median(widths)),
                "bias_log_p": bias_log_p,
                "coverage_exact_binomial_p": coverage_p,
                "studentization_chi_square": studentization,
                "studentization_log_p": studentization_log_p,
                "diagnostic_statuses": {
                    status: statuses.count(status) for status in sorted(set(statuses))
                },
            }
        )
    evidence.update(
        {
            "runtime_seconds": time.perf_counter() - started,
            "environment": {
                "python": sys.version,
                "platform": platform.platform(),
                "numpy": np.__version__,
                "scipy": __import__("scipy").__version__,
            },
        }
    )
    _evidence_path(cell).write_text(
        json.dumps(evidence, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    assert evidence["attempted"] == len(SEEDS)
    assert evidence["completed"] == len(SEEDS), {
        "refusals": evidence["refusals"],
        "errors": evidence["errors"],
    }
    assert evidence["paired_refusal_attempted"] == len(SEEDS)
    assert evidence["paired_refusal_completed"] == len(SEEDS), evidence["paired_refusal_codes"]
    if not SMOKE_ONLY:
        assert evidence["bias_log_p"] >= math.log(PER_GATE_ALPHA)
        assert evidence["coverage_exact_binomial_p"] >= PER_GATE_ALPHA
        assert (
            STUDENTIZATION_LIMITS[0]
            <= evidence["studentization_chi_square"]
            <= STUDENTIZATION_LIMITS[1]
        )
