from dataclasses import replace

import numpy as np
import pyarrow as pa
import pytest
from scipy.optimize import root

from terrine.errors import TerrineError
from terrine.estimate import LinearTerms, NuisanceSpec, aipw, regression_adjustment
from terrine.estimate._data import prepare
from terrine.estimate.folds import assign_folds
from terrine.estimate.nuisance import Encoder, crossfit, fit_logistic, fit_outcome


def test_heldout_values_cannot_change_training_encoding_or_scaling(frame):
    data = prepare(frame)
    ids = assign_folds(data.units, folds=2, seed=0)
    train = ids != 0
    spec = NuisanceSpec(propensity=LinearTerms(squares=("x",)), outcome=LinearTerms(squares=("x",)))
    first = crossfit(frame, outcome="y", method="aipw", folds=2, seed=0, nuisance=spec).fits[0]
    changed = data.features["x"].copy()
    changed[~train] += 3
    changed_y = data.y.copy()
    changed_y[~train] += 7
    perturbed = replace(
        frame,
        table=frame.table.set_column(
            frame.table.column_names.index("x"), "x", pa.array(changed)
        ).set_column(frame.table.column_names.index("y"), "y", pa.array(changed_y)),
    )
    second = crossfit(perturbed, outcome="y", method="aipw", folds=2, seed=0, nuisance=spec).fits[0]
    np.testing.assert_array_equal(first.propensity_design[train], second.propensity_design[train])
    np.testing.assert_array_equal(first.beta, second.beta)
    np.testing.assert_array_equal(first.theta0, second.theta0)
    np.testing.assert_array_equal(first.theta1, second.theta1)
    assert not np.allclose(first.propensity_design[~train], second.propensity_design[~train])


def test_square_only_spans_declared_raw_predictor(frame):
    data = prepare(frame)
    enc = Encoder.fit(
        data.features,
        data.kinds,
        np.ones(data.n, dtype=bool),
        LinearTerms(features=(), squares=("x",)),
    )
    design = enc.transform(data.features)
    y = 2 + 3 * data.features["x"] ** 2
    fit = fit_outcome(design, y, data.q, fold=0, arm=1)
    np.testing.assert_allclose(design @ fit, y, atol=1e-12)
    assert design.shape[1] == 2


def test_linear_rank_separation_and_solver_contracts():
    x = np.column_stack([np.ones(12), np.tile([-1.0, 0.0, 1.0], 4)])
    a = np.tile([0.0, 1.0, 0.0, 1.0], 3)
    q = np.linspace(0.8, 1.2, 12)
    beta = fit_logistic(x, a, q, fold=0)
    independent = root(lambda b: x.T @ (q * (a - 1 / (1 + np.exp(-x @ b)))), np.zeros(2))
    assert independent.success
    np.testing.assert_allclose(beta, independent.x, atol=2e-7)
    for action, code in [
        (lambda: fit_logistic(x, (x[:, 1] > 0).astype(float), q, fold=0), "separation"),
        (
            lambda: fit_outcome(np.ones((8, 2)), np.arange(8.0), np.ones(8), fold=0, arm=0),
            "rank_deficient",
        ),
        (
            lambda: fit_outcome(np.eye(2), np.arange(2.0), np.ones(2), fold=0, arm=0),
            "insufficient_training",
        ),
    ]:
        with pytest.raises(TerrineError) as exc:
            action()
        assert exc.value.code == "terrine.estimate." + code


@pytest.mark.parametrize("gap", [1e-7, 1e-8])
def test_crossed_central_pair_has_finite_unpenalized_mle(gap):
    raw = np.array([-2.0, -1.0, -gap, gap, 1.0, 2.0])
    x = np.column_stack([np.ones(6), raw / np.std(raw)])
    a = np.array([0.0, 0.0, 1.0, 0.0, 1.0, 1.0])
    beta = fit_logistic(x, a, np.ones(6), fold=0)
    eta = x @ beta
    gradient = x.T @ (a - 1 / (1 + np.exp(-eta))) / len(a)
    assert np.all(np.isfinite(beta))
    assert np.max(np.abs(gradient)) <= 1e-8


@pytest.mark.parametrize(
    ("raw", "a"),
    [
        ([-2.0, -1.0, -0.5, 0.5, 1.0, 2.0], [0.0, 0.0, 0.0, 1.0, 1.0, 1.0]),
        ([-2.0, -1.0, 0.0, 0.0, 1.0, 2.0], [0.0, 0.0, 1.0, 0.0, 1.0, 1.0]),
    ],
)
def test_complete_and_quasi_separation_retain_their_refusal(raw, a):
    x = np.column_stack([np.ones(6), raw])
    with pytest.raises(TerrineError) as exc:
        fit_logistic(x, np.asarray(a), np.ones(6), fold=0)
    assert exc.value.code == "terrine.estimate.separation"


def test_unresolved_lp_feasibility_is_not_separation():
    raw = np.array([-2.0, -1.0, -1e-12, 1e-12, 1.0, 2.0])
    x = np.column_stack([np.ones(6), raw / np.std(raw)])
    a = np.array([0.0, 0.0, 1.0, 0.0, 1.0, 1.0])
    with pytest.raises(TerrineError) as exc:
        fit_logistic(x, a, np.ones(6), fold=0)
    assert exc.value.code == "terrine.estimate.nonconvergence"
    assert exc.value.context["criterion"] == "separation_feasibility"


def test_unseen_category_and_absent_arm_level_refuse(frame, permissive):
    data = prepare(frame)
    ids = assign_folds(data.units, folds=2, seed=0)
    levels = np.where(ids == 0, "heldout", "train")
    candidate = replace(
        frame,
        table=frame.table.append_column("cat", pa.array(levels)),
        roles=replace(frame.roles, features=("x", "cat")),
        provenance=replace(
            frame.provenance,
            feature_cutoffs={
                **frame.provenance.feature_cutoffs,
                "cat": frame.provenance.feature_cutoffs["x"],
            },
        ),
    )
    with pytest.raises(TerrineError) as exc:
        aipw(candidate, folds=2, diagnostics=permissive)
    assert exc.value.code == "terrine.estimate.unseen_category"
    levels = np.where(data.a == 1, "treated", "control")
    candidate = replace(
        candidate,
        table=candidate.table.set_column(
            candidate.table.column_names.index("cat"), "cat", pa.array(levels)
        ),
    )
    with pytest.raises(TerrineError) as exc:
        regression_adjustment(candidate, folds=2, diagnostics=permissive)
    assert exc.value.code == "terrine.estimate.rank_deficient"


def test_unknown_terms_refuse(frame):
    with pytest.raises(TerrineError) as exc:
        aipw(frame, nuisance=NuisanceSpec(propensity=LinearTerms(squares=("missing",))))
    assert exc.value.code == "terrine.estimate.invalid_terms"


def test_nonconvergence_and_condition_failures_have_no_fallback(monkeypatch):
    from terrine.estimate import nuisance

    x = np.column_stack([np.ones(12), np.tile([-1.0, 0.0, 1.0], 4)])
    a = np.tile([0.0, 1.0, 0.0, 1.0], 3)
    real_minimize = nuisance.optimize.minimize

    def unconverged(*args, **kwargs):
        result = real_minimize(*args, **kwargs)
        result.success = False
        return result

    monkeypatch.setattr(nuisance.optimize, "minimize", unconverged)
    with pytest.raises(TerrineError) as exc:
        fit_logistic(x, a, np.ones(12), fold=0)
    assert exc.value.code == "terrine.estimate.nonconvergence"
    ill = np.column_stack([np.ones(12), np.linspace(-1.0, 1.0, 12) * 1e-9])
    with pytest.raises(TerrineError) as exc:
        fit_outcome(ill, np.arange(12.0), np.ones(12), fold=0, arm=1)
    assert exc.value.code == "terrine.estimate.ill_conditioned_nuisance"


def test_dictionary_codes_are_categories_not_numeric_trends(frame, permissive):
    codes = np.arange(frame.table.num_rows) % 3
    a = frame.table["a"].to_numpy()
    table = frame.table.append_column("category", pa.array(codes).dictionary_encode())
    table = table.set_column(
        table.column_names.index("y"), "y", pa.array(np.array([3.0, 7.0, -2.0])[codes] + 2 * a)
    )
    candidate = replace(
        frame,
        table=table,
        roles=replace(frame.roles, features=("x", "category")),
        provenance=replace(
            frame.provenance,
            feature_cutoffs={
                **frame.provenance.feature_cutoffs,
                "category": frame.provenance.feature_cutoffs["x"],
            },
        ),
    )
    result = regression_adjustment(candidate, folds=2, diagnostics=permissive)
    assert result.value == pytest.approx(2.0, abs=1e-12)
    with pytest.raises(TerrineError) as exc:
        aipw(
            candidate,
            folds=2,
            diagnostics=permissive,
            nuisance=NuisanceSpec(propensity=LinearTerms(squares=("category",))),
        )
    assert exc.value.code == "terrine.estimate.invalid_terms"


def test_known_linear_predictor_recovers_independent_noiseless_world(permissive):
    from terrine.simulate import WorldConfig, simulate
    from terrine.simulate.truth import ate
    from tests.fixtures.baseline import materialized_baseline

    world = simulate(WorldConfig(n_units=128, outcome_noise_sd=0), seed=17)
    result = regression_adjustment(materialized_baseline(world), folds=2, diagnostics=permissive)
    assert result.value == pytest.approx(ate(world, target="cohort"), abs=5e-12)
