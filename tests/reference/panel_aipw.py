"""Independent row-loop reference for declared trial-weighted AIPW scores."""

import math

import pandas as pd


def trial_weighted_score(rows: pd.DataFrame, trial_weights: dict[str, float]) -> float:
    """Evaluate the stated per-trial score without production estimator helpers."""
    weights = {
        key: value / math.fsum(trial_weights.values()) for key, value in trial_weights.items()
    }
    total = 0.0
    for trial, group in rows.groupby("trial", sort=True):
        q = group["q"]
        total += (
            weights[str(trial)]
            * math.fsum(float(v) for v in q * group["psi"])
            / math.fsum(float(v) for v in q)
        )
    return total
