"""Independent row-loop oracle for weighted stacked DiD contrasts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class CATTReference:
    effects: tuple[float, ...]
    covariance: tuple[tuple[float, ...], ...]


def clustered_catt_reference(
    rows, *, reference_period, period_seconds, comparison_periods, unit_weight=None
):
    cohorts = sorted({row["cohort"] for row in rows})
    vectors = []
    effects = []
    unit_scores = {}
    for cohort in cohorts:
        ref = cohort + timedelta(seconds=reference_period * period_seconds)
        for event_period in comparison_periods:
            comparison = cohort + timedelta(seconds=event_period * period_seconds)
            by_unit = {}
            for row in rows:
                if row["cohort"] == cohort and row["period"] in (ref, comparison):
                    entry = by_unit.setdefault(row["unit"], {})
                    entry[row["period"]] = row
            treated, controls = [], []
            for unit, periods in by_unit.items():
                if ref not in periods or comparison not in periods:
                    continue
                baseline, followup = periods[ref], periods[comparison]
                q = float(followup[unit_weight]) if unit_weight else 1.0
                pair = (followup["y"] - baseline["y"], q)
                (treated if followup["treatment"] else controls).append((unit, *pair))
            q_t = math.fsum(item[2] for item in treated)
            q_c = math.fsum(item[2] for item in controls)
            mu_t = math.fsum(item[1] * item[2] for item in treated) / q_t
            mu_c = math.fsum(item[1] * item[2] for item in controls) / q_c
            effects.append(mu_t - mu_c)
            score = {}
            for unit, delta, q in treated:
                score[unit] = q * (delta - mu_t) / q_t
            for unit, delta, q in controls:
                score[unit] = score.get(unit, 0.0) - q * (delta - mu_c) / q_c
            vectors.append(score)
    for vector_index, vector in enumerate(vectors):
        for unit, score in vector.items():
            unit_scores.setdefault(unit, [0.0] * len(vectors))[vector_index] += score
    width = len(vectors)
    covariance = [[0.0] * width for _ in range(width)]
    for score in unit_scores.values():
        for i in range(width):
            for j in range(width):
                covariance[i][j] += score[i] * score[j]
    return CATTReference(tuple(effects), tuple(tuple(row) for row in covariance))
