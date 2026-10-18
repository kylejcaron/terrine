from dataclasses import replace

import pandas as pd
import pyarrow as pa
import pytest

from terrine.data import MaterializedFrame, PanelMeta, TrialWeightMeta
from terrine.estimate.nuisance import LinearTerms, NuisanceSpec
from terrine.estimate.panel import panel_aipw
from tests.estimate.test_panel_aipw import panel_frame


def test_duplicate_trials_are_summed_within_unit_for_cluster_uncertainty():
    frame = panel_frame()
    rows = frame.table.to_pandas()
    rows["trial"] = rows["trial"].map({"early": "early-a", "late": "late-a"})
    duplicated = rows.copy()
    duplicated["trial"] = duplicated["trial"].map({"early-a": "early-b", "late-a": "late-b"})
    expanded = pd.concat([rows, duplicated], ignore_index=True)
    expanded["anchor_id"] = expanded["trial"]
    provenance = replace(
        frame.provenance,
        panel=PanelMeta(
            "sequential",
            treatment=frame.provenance.panel.treatment,
            trial_weight=TrialWeightMeta(
                "explicit",
                {"early-a": 1.0, "early-b": 1.0, "late-a": 3.0, "late-b": 3.0},
            ),
            trials=("early-a", "early-b", "late-a", "late-b"),
        ),
    )
    repeated = MaterializedFrame(
        pa.Table.from_pandas(expanded, preserve_index=False),
        frame.roles,
        frame.columns,
        provenance,
    )
    original_result = panel_aipw(frame, outcome="y", treatment="a", trial="trial", folds=3, seed=5)
    repeated_result = panel_aipw(
        repeated, outcome="y", treatment="a", trial="trial", folds=3, seed=5
    )
    assert repeated_result.value == pytest.approx(original_result.value)
    assert repeated_result.se == pytest.approx(original_result.se)
    assert repeated_result.n == 720
    assert repeated_result.n_clusters == 180


def test_cluster_standard_error_matches_independent_row_reference():
    import math

    import numpy as np

    frame = panel_frame()
    result = panel_aipw(frame, outcome="y", treatment="a", trial="trial", folds=3, seed=17)
    rows = frame.table.select(["unit", "trial", "q", "x"]).to_pandas()
    rows["effect"] = rows["trial"].map({"early": 1.5, "late": 4.5}) + 0.2 * rows["x"]
    trial_masses = {"early": 0.25, "late": 0.75}
    cluster_scores = dict.fromkeys(rows["unit"].unique(), 0.0)
    for trial, group in rows.groupby("trial", sort=True):
        target = group["q"].to_numpy()
        target = target * len(target) / math.fsum(target)
        mean = math.fsum(
            float(w * value) for w, value in zip(target, group["effect"], strict=True)
        ) / len(target)
        for unit, weight, effect in zip(group["unit"], target, group["effect"], strict=True):
            cluster_scores[unit] += trial_masses[trial] * weight * (effect - mean)
    influence = np.asarray(list(cluster_scores.values()))
    centered = influence - math.fsum(influence) / len(influence)
    reference_se = math.sqrt(
        math.fsum(float(value * value) for value in centered)
        / (len(influence) * (len(influence) - 1))
    )
    assert result.se == pytest.approx(reference_se, abs=1e-10)


def test_disjoint_trial_cluster_se_matches_global_unit_reference():
    import math

    import numpy as np

    frame = panel_frame()
    rows = frame.table.to_pandas()
    rows = rows.loc[
        ((rows["trial"] == "early") & (rows["unit"] < 90))
        | ((rows["trial"] == "late") & (rows["unit"] >= 90))
    ].copy()
    disjoint = MaterializedFrame(
        pa.Table.from_pandas(rows, preserve_index=False),
        frame.roles,
        frame.columns,
        frame.provenance,
    )
    result = panel_aipw(
        disjoint,
        outcome="y",
        treatment="a",
        trial="trial",
        folds=3,
        seed=31,
        nuisance=NuisanceSpec(propensity=LinearTerms(features=())),
    )
    masses = {"early": 0.25, "late": 0.75}
    n_units = rows["unit"].nunique()
    cluster_score = dict.fromkeys(rows["unit"].unique(), 0.0)
    for trial, group in rows.groupby("trial", sort=True):
        q = group["q"].to_numpy()
        q = q * len(q) / math.fsum(q)
        effect = (
            group["trial"].map({"early": 1.5, "late": 4.5}).to_numpy() + 0.2 * group["x"].to_numpy()
        )
        mean = math.fsum(float(w * value) for w, value in zip(q, effect, strict=True)) / len(q)
        scale = n_units / len(group)
        for unit, weight, value in zip(group["unit"], q, effect, strict=True):
            cluster_score[unit] += masses[trial] * scale * weight * (value - mean)
    scores = np.asarray(list(cluster_score.values()))
    centered = scores - math.fsum(scores) / n_units
    expected = math.sqrt(
        math.fsum(float(value * value) for value in centered) / (n_units * (n_units - 1))
    )
    assert result.se == pytest.approx(expected, abs=1e-10)
