from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from terrine.data import (
    ColumnMeta,
    ExecutionSnapshot,
    FeatureCutoff,
    MaterializedFrame,
    PanelMeta,
    Provenance,
    Roles,
    TreatmentHistoryMeta,
    TrialWeightMeta,
)
from terrine.estimate.nuisance import NuisanceSpec
from terrine.estimate.panel import panel_aipw
from terrine.windows import Window
from tests.reference.panel_aipw import trial_weighted_score


def panel_frame(*, sampled=False, explicit_weights=None, target_scales=None):
    n_units = 180
    unit = np.arange(n_units)
    x_unit = np.sin(unit * 1.31) + np.cos(unit * 0.47)
    frames = []
    for trial, intercept, propensity, effect in (
        ("early", 1.0, -0.8, 1.5),
        ("late", 7.0, 0.9, 4.5),
    ):
        x = x_unit
        probability = 1 / (1 + np.exp(-(propensity + 0.7 * x)))
        treatment = (
            (unit * 37 + (0 if trial == "early" else 11)) % 100 < probability * 100
        ).astype(int)
        y = intercept + 1.3 * x + treatment * (effect + 0.2 * x)
        frames.append(pd.DataFrame({"unit": unit, "trial": trial, "a": treatment, "x": x, "y": y}))
    rows = pd.concat(frames, ignore_index=True)
    rows["anchor_id"] = rows["trial"]
    rows["t0"] = pd.Timestamp("2025-01-01", tz="UTC")
    rows["complete"] = True
    rows["reason"] = None
    rows["q"] = 1 + 0.2 * np.abs(rows["x"])
    if target_scales is not None:
        rows["q"] *= rows["trial"].map(target_scales)
    weights = explicit_weights or {"early": 1.0, "late": 3.0}
    rows["lambda"] = rows["trial"].map(weights)
    table = pa.Table.from_pandas(rows, preserve_index=False)
    if sampled:
        from terrine.data import SamplingMeta

        sampling = SamplingMeta(("stratum",), 1, 4)
    else:
        sampling = None
    cutoffs = {
        "x": FeatureCutoff(0, True, "recorded_at"),
        "q": FeatureCutoff(0, True, "recorded_at"),
    }
    provenance = Provenance(
        cutoffs,
        {},
        "panel-design",
        "panel-model",
        {},
        ExecutionSnapshot(None, "arrow", {}, None, "unverified"),
        "sequential",
        None,
        treatment_knowledge="recorded_at",
        treatment_known_at_t0=True,
        panel=PanelMeta(
            "sequential",
            treatment=TreatmentHistoryMeta(
                "initiation",
                "assignments",
                "started",
                datetime(2020, 1, 1, tzinfo=UTC),
                "recorded_at",
            ),
            trial_weight=TrialWeightMeta("explicit", weights),
        ),
        sampling=sampling,
    )
    return MaterializedFrame(
        table,
        Roles(
            "unit",
            "anchor_id",
            "t0",
            "a",
            ("x",),
            ("y",),
            cluster_unit="unit",
            trial="trial",
            target_weight="q",
            trial_weight="lambda",
        ),
        {
            "a": ColumnMeta(None, "treatment", None, None, None, None),
            "x": ColumnMeta(None, "feature", None, None, None, None),
            "q": ColumnMeta(None, "feature", None, None, None, None),
            "y": ColumnMeta(Window(0, 86400), "outcome", None, None, "complete", "reason"),
        },
        provenance,
    )


def test_trial_weights_change_target_only_as_declared():
    frame = panel_frame()
    result = panel_aipw(frame, outcome="y", treatment="a", trial="trial", folds=3, seed=7)
    reverse = panel_frame(explicit_weights={"early": 3.0, "late": 1.0})
    changed = panel_aipw(reverse, outcome="y", treatment="a", trial="trial", folds=3, seed=7)
    assert result.value != pytest.approx(changed.value)
    assert result.estimand == "trial_weighted_ATE"
    assert result.n == 360
    assert result.n_clusters == 180
    assert result.method == "panel_aipw"
    assert result.inference == "unit_cluster_crossfit_sandwich"


def test_trial_weight_common_scale_is_invariant():
    first = panel_aipw(
        panel_frame(explicit_weights={"early": 1.0, "late": 3.0}),
        outcome="y",
        treatment="a",
        trial="trial",
        folds=3,
        seed=11,
    )
    scaled = panel_aipw(
        panel_frame(explicit_weights={"early": 10.0, "late": 30.0}),
        outcome="y",
        treatment="a",
        trial="trial",
        folds=3,
        seed=11,
    )
    assert scaled.value == pytest.approx(first.value)
    assert scaled.se == pytest.approx(first.se)


def test_trial_specific_nuisance_recovers_varying_assignment_and_baselines():
    frame = panel_frame()
    rows = frame.table.select(["trial", "q", "x"]).to_pandas()
    rows["psi"] = rows["trial"].map({"early": 1.5, "late": 4.5}) + 0.2 * rows["x"]
    expected = trial_weighted_score(rows, {"early": 1.0, "late": 3.0})
    result = panel_aipw(
        frame,
        outcome="y",
        treatment="a",
        trial="trial",
        folds=3,
        seed=13,
        nuisance=NuisanceSpec(),
    )
    assert result.value == pytest.approx(expected, abs=1e-10)
    assert result.se > 0


def test_target_weights_are_normalized_within_each_trial():
    base = panel_aipw(panel_frame(), outcome="y", treatment="a", trial="trial", folds=3, seed=17)
    scaled_targets = panel_aipw(
        panel_frame(target_scales={"early": 9.0, "late": 0.2}),
        outcome="y",
        treatment="a",
        trial="trial",
        folds=3,
        seed=17,
    )
    assert scaled_targets.value == pytest.approx(base.value)
    assert scaled_targets.se == pytest.approx(base.se)


def test_trial_order_and_row_order_preserve_result():
    frame = panel_frame()
    reversed_frame = MaterializedFrame(
        frame.table.take(pa.array(list(reversed(range(frame.table.num_rows))))),
        frame.roles,
        frame.columns,
        frame.provenance,
    )
    original = panel_aipw(frame, outcome="y", treatment="a", trial="trial", folds=3, seed=23)
    reversed_result = panel_aipw(
        reversed_frame, outcome="y", treatment="a", trial="trial", folds=3, seed=23
    )
    assert reversed_result.value == pytest.approx(original.value, abs=1e-12)
    assert reversed_result.se == pytest.approx(original.se, abs=1e-12)


def test_sampled_analysis_population_is_refused():
    from terrine.errors import TerrineError

    with pytest.raises(TerrineError) as exc:
        panel_aipw(
            panel_frame(sampled=True),
            outcome="y",
            treatment="a",
            trial="trial",
            folds=3,
        )
    assert exc.value.code == "terrine.estimate.panel_input"
