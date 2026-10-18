"""Predeclared repeated-trial AIPW and stacked CATT recovery grids."""

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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast

import numpy as np
import pyarrow as pa
import pytest
from scipy.stats import binomtest, chi2, norm

from terrine.data import ColumnMeta, TrialWeightMeta
from terrine.errors import TerrineError
from terrine.estimate.inference import critical_value
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


def test_smoke_evidence_uses_a_separate_default_directory(monkeypatch, tmp_path):
    monkeypatch.setattr(sys.modules[__name__], "SMOKE_ONLY", True)
    monkeypatch.delenv("TERRINE_CALIBRATION_DIR", raising=False)
    monkeypatch.chdir(tmp_path)

    path = _evidence_path(("zero", "equal", "disjoint", "profile_a"))

    assert path.parent == Path("docs/validation/generated/panel_calibration_smoke")


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
    default_directory = (
        "docs/validation/generated/panel_calibration_smoke"
        if SMOKE_ONLY
        else "docs/validation/generated/panel_calibration"
    )
    directory = Path(os.environ.get("TERRINE_CALIBRATION_DIR", default_directory))
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


CATT_CELLS = tuple(
    itertools.product(
        ("equal", "unequal"),
        ("disjoint", "overlap"),
        ("profile_a", "profile_b"),
    )
)
CATT_PER_GATE_ALPHA = FAMILY_ALPHA / (3 * len(CATT_CELLS) * 3)
CATT_STUDENTIZATION_LIMITS = (
    float(chi2.ppf(CATT_PER_GATE_ALPHA / 2, len(SEEDS))),
    float(chi2.ppf(1 - CATT_PER_GATE_ALPHA / 2, len(SEEDS))),
)


def _stacked_calibration_frame(seed: int, cell: tuple[str, str, str], n_units: int):

    mass_kind, overlap_kind, profile = cell
    base = _stacked_frame()
    rng = np.random.default_rng(seed)
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    cohorts = (origin + timedelta(days=3), origin + timedelta(days=6))
    control_count = n_units // 2 if overlap_kind == "overlap" else n_units // 4
    treatment_count = n_units // 4
    unit_values: dict[str, tuple[float, float]] = {}
    rows = []
    error_scale, trend_scale = (1.0, 0.25) if profile == "profile_a" else (1.5, 0.75)

    for cohort_index, cohort_time in enumerate(cohorts):
        if overlap_kind == "overlap":
            controls = [f"control:{index}" for index in range(control_count)]
            treated = [f"treated:{cohort_index}:{index}" for index in range(treatment_count)]
        else:
            start = cohort_index * (n_units // 2)
            controls = [f"unit:{index}" for index in range(start, start + n_units // 4)]
            treated = [
                f"unit:{index}" for index in range(start + n_units // 4, start + n_units // 2)
            ]
        for unit, is_treated in [
            *((unit, True) for unit in treated),
            *((unit, False) for unit in controls),
        ]:
            baseline, trend = unit_values.setdefault(
                unit,
                (float(rng.normal(10.0, 2.0)), float(rng.normal(0.2, trend_scale))),
            )
            for event_period in (-1, 0):
                period_time = cohort_time + timedelta(days=event_period)
                effect = (1.0 + cohort_index) if is_treated and event_period == 0 else 0.0
                rows.append(
                    {
                        "unit": unit,
                        "anchor_id": f"{cohort_time.isoformat()}:{period_time.isoformat()}:{unit}",
                        "t0": period_time,
                        "treatment": is_treated,
                        "cohort": cohort_time,
                        "period": period_time,
                        "event_period": event_period,
                        "control_eligible": not is_treated,
                        "__terrine_treatment_initiation_time": cohort_time if is_treated else None,
                        "y": baseline
                        + trend * event_period
                        + effect
                        + rng.normal(0.0, error_scale),
                        "q": 1.0,
                    }
                )

    weights = (
        {cohort.isoformat(): weight for cohort, weight in zip(cohorts, (1.0, 1.0), strict=True)}
        if mass_kind == "equal"
        else {
            cohort.isoformat(): weight for cohort, weight in zip(cohorts, (1.0, 3.0), strict=True)
        }
    )
    return replace(base, table=pa.Table.from_pylist(rows)), weights


@pytest.mark.parametrize("cell", CATT_CELLS, ids=lambda cell: "-".join(cell))
def test_stacked_catt_heterogeneity_recovers_in_fast_smoke(cell):
    for seed in SMOKE_SEEDS:
        frame, weights = _stacked_calibration_frame(seed, cell, n_units=240)
        result = stacked_event_study(
            frame,
            outcome="y",
            treatment="treatment",
            cohort="cohort",
            period="period",
            cohort_weights=weights,
        )
        assert result.effects == pytest.approx((1.0, 2.0), abs=0.8)
        assert result.aggregate_effects[0] == pytest.approx(
            1.5 if cell[0] == "equal" else 1.75, abs=0.8
        )


def _catt_evidence_path(cell: tuple[str, str, str]) -> Path:
    directory = Path(
        os.environ.get(
            "TERRINE_CALIBRATION_DIR",
            "docs/validation/generated/panel_calibration_smoke"
            if SMOKE_ONLY
            else "docs/validation/generated/panel_calibration",
        )
    )
    directory.mkdir(parents=True, exist_ok=True)
    return directory / ("stacked-" + "-".join(cell) + ".json")


@pytest.mark.slow
@pytest.mark.parameter_recovery
@pytest.mark.parametrize("cell", CATT_CELLS, ids=lambda cell: "stacked-" + "-".join(cell))
def test_stacked_catt_calibration_cell(cell):
    aggregate_truth = 1.5 if cell[0] == "equal" else 1.75
    truths = (1.0, 2.0, aggregate_truth)
    effects = [[], [], []]
    standard_errors = [[], [], []]
    covered = [[], [], []]
    critical = critical_value(0.05)
    started = time.perf_counter()
    refusals, errors = [], []
    for seed in SEEDS:
        try:
            frame, weights = _stacked_calibration_frame(seed, cell, n_units=N_UNITS)
            result = stacked_event_study(
                frame,
                outcome="y",
                treatment="treatment",
                cohort="cohort",
                period="period",
                cohort_weights=weights,
            )
            estimates = (*result.effects, result.aggregate_effects[0])
            standard_errors_for_targets = (
                *result.standard_errors,
                result.aggregate_standard_errors[0],
            )
            for index, (truth, estimate, se) in enumerate(
                zip(truths, estimates, standard_errors_for_targets, strict=True)
            ):
                effects[index].append(estimate)
                standard_errors[index].append(se)
                covered[index].append(estimate - critical * se <= truth <= estimate + critical * se)
        except TerrineError as error:
            refusals.append({"seed": seed, "code": error.code})
        except Exception as error:
            errors.append({"seed": seed, "type": type(error).__name__, "detail": str(error)})

    contrast_evidence = []
    for index, truth in enumerate(truths):
        values = np.asarray(effects[index], dtype=np.float64)
        ses = np.asarray(standard_errors[index], dtype=np.float64)
        if len(values) < 2:
            contrast_evidence.append({"target": index, "truth": truth, "completed": len(values)})
            continue
        errors_from_truth = values - truth
        bias = float(errors_from_truth.mean())
        empirical_sd = float(errors_from_truth.std(ddof=1))
        bias_z = abs(bias) / (empirical_sd / math.sqrt(len(values)))
        bias_log_p = math.log(2.0) + float(norm.logsf(bias_z))
        studentization = float(np.sum((errors_from_truth / ses) ** 2))
        coverage_count = int(sum(covered[index]))
        coverage_p = float(binomtest(coverage_count, len(values), 0.95).pvalue)
        contrast_evidence.append(
            {
                "target": index,
                "truth": truth,
                "bias": bias,
                "empirical_sd": empirical_sd,
                "mean_se": float(np.mean(ses)),
                "rmse": float(np.sqrt(np.mean(errors_from_truth * errors_from_truth))),
                "coverage_count": coverage_count,
                "coverage_exact_binomial_p": coverage_p,
                "bias_log_p": bias_log_p,
                "studentization_chi_square": studentization,
            }
        )
    evidence = {
        "estimator": "stacked_cohort_time",
        "cell": {
            "trial_masses": cell[0],
            "unit_overlap": cell[1],
            "profile": cell[2],
            "cohort_effects": [1.0, 2.0],
        },
        "seeds": list(SEEDS),
        "n_units": N_UNITS,
        "run_mode": "smoke_only" if SMOKE_ONLY else "full_calibration",
        "family_alpha": FAMILY_ALPHA,
        "per_gate_alpha": CATT_PER_GATE_ALPHA,
        "attempted": len(SEEDS),
        "completed": len(SEEDS) - len(refusals) - len(errors),
        "refused": len(refusals),
        "refusals": refusals,
        "errors": errors,
        "targets": contrast_evidence,
        "studentization_chi_square_limits": list(CATT_STUDENTIZATION_LIMITS),
        "runtime_seconds": time.perf_counter() - started,
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "numpy": np.__version__,
            "scipy": __import__("scipy").__version__,
        },
    }
    _catt_evidence_path(cell).write_text(
        json.dumps(evidence, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    assert evidence["completed"] == len(SEEDS), evidence
    if not SMOKE_ONLY:
        for target in contrast_evidence:
            assert target["bias_log_p"] >= math.log(CATT_PER_GATE_ALPHA), evidence
            assert target["coverage_exact_binomial_p"] >= CATT_PER_GATE_ALPHA, evidence
            assert (
                CATT_STUDENTIZATION_LIMITS[0]
                <= target["studentization_chi_square"]
                <= CATT_STUDENTIZATION_LIMITS[1]
            ), evidence
