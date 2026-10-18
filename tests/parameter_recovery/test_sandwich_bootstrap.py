"""Full-refit bootstrap reference for sandwich uncertainty."""

from __future__ import annotations

import json
import os
import platform
import time
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from terrine.estimate import aipw, ipw, regression_adjustment
from terrine.estimate.folds import assign_folds
from terrine.simulate import WorldConfig, simulate
from tests.fixtures.baseline import materialized_baseline

METHODS = {"ipw": ipw, "aipw": aipw, "regression_adjustment": regression_adjustment}
BOOTSTRAPS = 400
DATASET_SEEDS = range(10)
N_UNITS = 8000
FOLDS = 5
FIT_SEED = 17


def _fold(unit: int, seed: int = FIT_SEED) -> int:
    encoded = b"i" + (8).to_bytes(8, "big") + unit.to_bytes(8, "big", signed=True)
    digest = sha256(b"terrine-fold-v1" + seed.to_bytes(8, "big") + encoded).digest()
    return int.from_bytes(digest[:8], "big") % FOLDS


def _resampled_frame(frame, indices, source_folds, identity_start):
    table = frame.table.take(pa.array(indices, type=pa.int64()))
    identities = []
    next_identity = identity_start
    for source_index in indices:
        target_fold = int(source_folds[source_index])
        while _fold(next_identity) != target_fold:
            next_identity += 1
        identities.append(next_identity)
        next_identity += 1
    unit = frame.roles.unit
    position = table.column_names.index(unit)
    table = table.set_column(position, unit, pa.array(identities, type=pa.int64()))
    return replace(frame, table=table)


@pytest.mark.slow
@pytest.mark.parameter_recovery
@pytest.mark.parametrize("method", tuple(METHODS))
def test_full_refit_bootstrap_variance_matches_sandwich(method, tmp_path):
    method_fn = METHODS[method]
    evidence = []
    for dataset_seed in DATASET_SEEDS:
        dataset_started = time.perf_counter()
        world = simulate(WorldConfig(n_units=N_UNITS), seed=dataset_seed)
        frame = materialized_baseline(world)
        point = method_fn(frame, outcome="net_revenue", folds=FOLDS, seed=FIT_SEED)
        ids = frame.table.column(frame.roles.unit).to_pylist()
        original_folds = assign_folds(ids, folds=FOLDS, seed=FIT_SEED)
        rng = np.random.Generator(np.random.PCG64(np.random.SeedSequence([dataset_seed, 9127])))
        bootstrap_values = []
        failures = []
        for draw in range(BOOTSTRAPS):
            indices = rng.integers(0, N_UNITS, size=N_UNITS, dtype=np.int64)
            sample = _resampled_frame(frame, indices, original_folds, 2**61 + draw * N_UNITS)
            try:
                result = method_fn(sample, outcome="net_revenue", folds=FOLDS, seed=FIT_SEED)
            except Exception as exc:
                failures.append({"draw": draw, "code": getattr(exc, "code", type(exc).__name__)})
                continue
            bootstrap_values.append(result.value)
        values = np.asarray(bootstrap_values, dtype=np.float64)
        centered = values - np.mean(values)
        variance = float(np.var(values, ddof=1))
        fourth = float(np.mean(centered**4))
        variance_mcse = float(np.sqrt(max(0.0, fourth - variance**2) / BOOTSTRAPS))
        record = {
            "method": method,
            "dataset_seed": dataset_seed,
            "configuration": {
                "n_units": N_UNITS,
                "propensity_quadratic": 0.0,
                "outcome_quadratic": 0.0,
                "target_weight_slope": 0.0,
                "outcome_noise_sd": 2.0,
                "measured_features": ["x", "plan_tier"],
                "propensity_terms": {
                    "main_effects": ["x", "plan_tier"],
                    "squares": [],
                    "interactions": [],
                },
                "outcome_terms": {
                    "main_effects": ["x", "plan_tier"],
                    "squares": [],
                    "interactions": [],
                },
                "folds": FOLDS,
                "estimator_seed": FIT_SEED,
                "resamples": BOOTSTRAPS,
                "rng": "PCG64(SeedSequence([dataset_seed, 9127]))",
                "family_alpha": 0.01,
                "variance_log_tolerance": 0.20,
            },
            "attempted": BOOTSTRAPS,
            "completed": len(values),
            "refused": failures,
            "bootstrap_variance": variance,
            "sandwich_variance": point.se**2,
            "bootstrap_variance_mcse": variance_mcse,
            "runtime_seconds": time.perf_counter() - dataset_started,
            "environment": {"python": platform.python_version(), "numpy": np.__version__},
        }
        evidence.append(record)
        output_dir = Path(
            os.environ.get(
                "TERRINE_CALIBRATION_DIR",
                "docs/validation/generated/baseline-bootstrap",
            )
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        output = output_dir / f"{method}-seed-{dataset_seed}.json"
        output.write_text(
            json.dumps(record, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        assert not failures, f"{method} seed {dataset_seed}: failed bootstrap draws {failures}"
        assert len(values) == BOOTSTRAPS
        assert np.isfinite(variance) and variance > 0
        assert np.isfinite(point.se) and point.se > 0
        allowance = 0.20 + 4 * variance_mcse / variance
        assert abs(np.log(variance / point.se**2)) <= allowance
    assert len(evidence) == 10
    summary_path = tmp_path / f"{method}-summary.json"
    summary_path.write_text(
        json.dumps(evidence, indent=2),
        encoding="utf-8",
    )
