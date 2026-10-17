import math
from dataclasses import replace

import numpy as np
import pyarrow as pa
import pytest
from scipy import linalg
from scipy.special import log_ndtr

from terrine.data import FeatureCutoff
from terrine.errors import TerrineError
from terrine.estimate import LinearTerms, NuisanceSpec, aipw, ipw, regression_adjustment
from terrine.estimate._data import prepare
from terrine.estimate.inference import (
    _bread_condition,
    _mean,
    _mean_columns,
    _nuisance_blocks,
    _standard_error,
    _unit_influence,
    critical_value,
    pseudo_outcome,
    sandwich,
)
from terrine.estimate.nuisance import crossfit
from terrine.results import Estimate


@pytest.mark.parametrize("method", ["ipw", "aipw", "regression_adjustment"])
def test_sandwich_matches_independent_stacked_equation_reference(frame, method):
    frame = replace(
        frame,
        roles=replace(frame.roles, weight="q"),
        provenance=replace(
            frame.provenance,
            weight_kind="baseline_function",
            weight_cutoff=FeatureCutoff(-1, True, "recorded_at"),
            weight_depends_on_treatment=False,
        ),
    )
    data = prepare(frame)
    pred = crossfit(frame, outcome="y", method=method, folds=3, seed=0, nuisance=NuisanceSpec())
    psi = pseudo_outcome(method, data.a, data.y, pred.log_e, pred.log1m_e, pred.mu0, pred.mu1)
    tau = float(np.dot(data.q, psi) / np.sum(data.q))
    shared = pred.fits[0].train & pred.fits[1].train
    assert np.count_nonzero(shared) > 0
    # Independent dense assembly: fold propensity, control, treated; target last.
    columns = []
    blocks = []
    derivatives = []
    fold_scores = []
    for fit in pred.fits:
        train = fit.train
        test = ~train
        first_column = len(columns)
        x = fit.propensity_design
        z = fit.outcome_design
        e = 1 / (1 + np.exp(-x @ fit.beta))
        if method != "regression_adjustment":
            columns.append((train * data.q * (data.a - e))[:, None] * x)
            blocks.append(-(x.T * (train * data.q * e * (1 - e))) @ x / data.n)
            residual1 = data.y if method == "ipw" else data.y - pred.mu1
            residual0 = data.y if method == "ipw" else data.y - pred.mu0
            d = data.q * (
                -data.a * residual1 * np.exp(pred.log1m_e - pred.log_e)
                - (1 - data.a) * residual0 * np.exp(pred.log_e - pred.log1m_e)
            )
            derivatives.append(np.mean((test * d)[:, None] * x, axis=0))
        if method != "ipw":
            assert z is not None
            for arm, theta in [(0, fit.theta0), (1, fit.theta1)]:
                assert theta is not None
                w = train * data.q * (data.a == arm)
                columns.append((w * (data.y - z @ theta))[:, None] * z)
                blocks.append(-(z.T * w) @ z / data.n)
                if method == "regression_adjustment":
                    d = data.q * (1 if arm else -1)
                elif arm:
                    d = data.q * (1 - data.a * np.exp(-pred.log_e))
                else:
                    d = data.q * (-1 + (1 - data.a) * np.exp(-pred.log1m_e))
                derivatives.append(np.mean((test * d)[:, None] * z, axis=0))
        fold_scores.append(np.column_stack(columns[first_column:]))
    columns.append((data.q * (psi - tau))[:, None])
    ref_scores = np.column_stack(columns)
    assert np.linalg.norm(fold_scores[0].T @ fold_scores[1]) > 1e-3
    ref_bread = np.zeros((ref_scores.shape[1], ref_scores.shape[1]))
    offset = 0
    for block in blocks:
        p = len(block)
        ref_bread[offset : offset + p, offset : offset + p] = block
        offset += p
    ref_bread[-1, :-1] = np.concatenate(derivatives)
    ref_bread[-1, -1] = -np.mean(data.q)
    actual_blocks = list(_nuisance_blocks(data, pred, method))
    for (block, derivative, _, _), expected_block, expected_derivative in zip(
        actual_blocks, blocks, derivatives, strict=True
    ):
        np.testing.assert_allclose(block, expected_block, atol=2e-12)
        np.testing.assert_allclose(derivative, expected_derivative, atol=2e-12)
    influence = -np.linalg.solve(ref_bread.T, np.eye(len(ref_bread))[-1]) @ ref_scores.T
    np.testing.assert_allclose(
        _unit_influence(data, pred, method, tau), influence, atol=2e-11, rtol=2e-11
    )
    expected = np.std(influence, ddof=1) / np.sqrt(data.n)
    assert sandwich(data, pred, method, tau) == pytest.approx(expected, rel=1e-10)
    if method != "aipw":
        score_only = np.std(data.q * (psi - tau), ddof=1) / np.sqrt(data.n) / np.mean(data.q)
        assert abs(expected - score_only) > 1e-5
    # Differentiate the complete independently assembled joint system.
    parameters = []
    for fit in pred.fits:
        if method != "regression_adjustment":
            parameters.extend(fit.beta)
        if method != "ipw":
            assert fit.theta0 is not None and fit.theta1 is not None
            parameters.extend(fit.theta0)
            parameters.extend(fit.theta1)
    parameters.append(tau)
    parameters = np.asarray(parameters)

    def equations_at(parameters):
        equations = []
        offset = 0
        heldout_score = np.empty(data.n)
        for fit in pred.fits:
            x = fit.propensity_design
            z = fit.outcome_design
            beta = fit.beta
            if method != "regression_adjustment":
                beta = parameters[offset : offset + x.shape[1]]
                offset += x.shape[1]
            e = 1 / (1 + np.exp(-x @ beta))
            if method != "regression_adjustment":
                equations.append((fit.train * data.q * (data.a - e))[:, None] * x)
            m0 = m1 = np.zeros(data.n)
            if method != "ipw":
                assert z is not None
                theta0 = parameters[offset : offset + z.shape[1]]
                offset += z.shape[1]
                theta1 = parameters[offset : offset + z.shape[1]]
                offset += z.shape[1]
                m0 = z @ theta0
                m1 = z @ theta1
                for arm, mean in [(0, m0), (1, m1)]:
                    equations.append(
                        (fit.train * data.q * (data.a == arm) * (data.y - mean))[:, None] * z
                    )
            if method == "ipw":
                score = data.a * data.y / e - (1 - data.a) * data.y / (1 - e)
            elif method == "aipw":
                score = (
                    m1 - m0 + data.a * (data.y - m1) / e - (1 - data.a) * (data.y - m0) / (1 - e)
                )
            else:
                score = m1 - m0
            heldout_score[~fit.train] = score[~fit.train]
        equations.append((data.q * (heldout_score - parameters[-1]))[:, None])
        return np.mean(np.column_stack(equations), axis=0)

    numerical = np.column_stack(
        [
            (
                equations_at(parameters + np.eye(len(parameters))[j] * 1e-5)
                - equations_at(parameters - np.eye(len(parameters))[j] * 1e-5)
            )
            / 2e-5
            for j in range(len(parameters))
        ]
    )
    np.testing.assert_allclose(ref_bread, numerical, atol=2e-9, rtol=2e-8)


def test_weight_scale_and_row_order_invariance(frame, permissive):
    weighted = replace(
        frame,
        roles=replace(frame.roles, weight="q"),
        provenance=replace(
            frame.provenance,
            weight_kind="baseline_function",
            weight_cutoff=FeatureCutoff(-1, True, "recorded_at"),
            weight_depends_on_treatment=False,
        ),
    )
    index = frame.table.column_names.index("q")
    scaled = replace(
        weighted,
        table=weighted.table.set_column(
            index, "q", pa.array(weighted.table["q"].to_numpy() * 1e280)
        ),
    )
    scaled = replace(scaled, table=scaled.table.take(pa.array(np.arange(119, -1, -1))))
    for method in (ipw, aipw, regression_adjustment):
        first = method(weighted, folds=2, diagnostics=permissive)
        second = method(scaled, folds=2, diagnostics=permissive)
        np.testing.assert_allclose(
            [first.value, first.se, *first.ci],
            [second.value, second.se, *second.ci],
            rtol=1e-10,
            atol=1e-11,
        )
        assert first.provenance["folds"]["digest"] == second.provenance["folds"]["digest"]
        for d1, d2 in zip(first.diagnostics, second.diagnostics, strict=True):
            assert d1.status == d2.status
            for key, value in d1.metrics.items():
                if isinstance(value, float):
                    assert d2.metrics[key] == pytest.approx(value, abs=1e-10)
        restored = Estimate.from_json(first.to_json())
        assert restored == first
        assert "predictions" not in restored.provenance


def test_extreme_alpha_uses_log_tail_roundtrip():
    for alpha in (0.05, 1e-100, np.nextafter(0.0, 1.0)):
        z = critical_value(float(alpha))
        assert log_ndtr(-z) == pytest.approx(np.log(alpha) - np.log(2), abs=2e-12)
    for alpha in (0, 1, np.nan):
        with pytest.raises(TerrineError) as exc:
            critical_value(alpha)
        assert exc.value.code == "terrine.estimate.invalid_alpha"


def test_singular_bread_refuses_after_successful_nuisance_fits(frame):
    data = prepare(frame)
    pred = crossfit(frame, outcome="y", method="ipw", folds=2, seed=0, nuisance=NuisanceSpec())
    scaled = replace(data, y=data.y * 1e12)
    psi = pseudo_outcome("ipw", scaled.a, scaled.y, pred.log_e, pred.log1m_e, None, None)
    with pytest.raises(TerrineError) as exc:
        sandwich(scaled, pred, "ipw", float(np.mean(psi)))
    assert exc.value.code == "terrine.estimate.singular_inference"


def test_aipw_one_sided_misspecification_keeps_nuisance_terms(frame):
    x = frame.table["x"].to_numpy()
    a = frame.table["a"].to_numpy()
    y = 3 + x * x + a * (2 + x * x) + np.cos(np.arange(len(a)) * 2.3)
    candidate = replace(
        frame, table=frame.table.set_column(frame.table.column_names.index("y"), "y", pa.array(y))
    )
    terms = NuisanceSpec(propensity=LinearTerms(features=()), outcome=LinearTerms(features=()))
    data = prepare(candidate)
    pred = crossfit(candidate, outcome="y", method="aipw", folds=2, seed=0, nuisance=terms)
    psi = pseudo_outcome("aipw", data.a, data.y, pred.log_e, pred.log1m_e, pred.mu0, pred.mu1)
    tau = float(np.mean(psi))
    assert max(np.max(np.abs(c)) for _, c, _, _ in _nuisance_blocks(data, pred, "aipw")) > 1e-3
    score_only = np.std(psi, ddof=1) / np.sqrt(data.n)
    assert abs(sandwich(data, pred, "aipw", tau) - score_only) > 1e-5


def test_large_finite_means_and_centering_do_not_overflow():
    values = np.full((12, 2), -2e307)
    np.testing.assert_array_equal(_mean_columns(values), [-2e307, -2e307])
    assert _mean(np.array([1.6e308, -1.6e308]), np.array([3.0, 1.0])) == pytest.approx(8e307)
    influence = np.array([-1.6e308, 1.6e308, 1.6e308, 1.6e308])
    # In units of 1.6e308, centered values are [-1.5, .5, .5, .5].
    assert _standard_error(influence) == pytest.approx(8e307)


def test_centering_preserves_small_deviations_on_large_finite_offset():
    step = math.ulp(1e308)
    influence = np.array([1e308 + j * step for j in range(4)])
    assert _standard_error(influence) == pytest.approx(step * math.sqrt(5 / 12), rel=1e-14)


def test_public_large_finite_ipw_has_coded_bread_refusal(frame, permissive):
    candidate = replace(
        frame,
        roles=replace(frame.roles, features=()),
        table=frame.table.set_column(
            frame.table.column_names.index("y"), "y", pa.array(np.full(120, 2e307))
        ),
    )
    with pytest.raises(TerrineError) as exc:
        ipw(
            candidate,
            folds=2,
            nuisance=NuisanceSpec(propensity=LinearTerms(features=())),
            diagnostics=permissive,
        )
    assert exc.value.code == "terrine.estimate.singular_inference"


@pytest.mark.parametrize(
    ("blocks", "derivatives", "qbar"),
    [
        (
            [np.diag([-2.0, -2.0]), np.diag([-0.5, -3.0])],
            [np.array([0.0, 1.2]), np.array([0.7, 0.0])],
            1.3,
        ),
        ([np.diag([-1.0, -4.0])], [np.zeros(2)], 2.0),
        ([np.array([[-1.0]])], [np.array([1e3])], 1.0),
    ],
)
def test_arrowhead_condition_matches_full_spectral_condition(blocks, derivatives, qbar):
    diagonal = linalg.block_diag(*blocks)
    dense = np.zeros((len(diagonal) + 1, len(diagonal) + 1))
    dense[:-1, :-1] = diagonal
    dense[-1, :-1] = np.concatenate(derivatives)
    dense[-1, -1] = -qbar
    eigenvalues = []
    couplings = []
    for block, derivative in zip(blocks, derivatives, strict=True):
        eigen, basis = np.linalg.eigh(block)
        eigenvalues.extend(eigen)
        couplings.extend(basis.T @ derivative)
    got = _bread_condition(np.asarray(eigenvalues), np.asarray(couplings), qbar)
    assert got == pytest.approx(np.linalg.cond(dense), rel=2e-10)


def test_joint_spectral_gate_rejects_large_coupling_with_well_conditioned_blocks():
    with pytest.raises(TerrineError) as exc:
        _bread_condition(np.array([-1.0, -1.0]), np.array([1e4, 0.0]), 1.0)
    assert exc.value.code == "terrine.estimate.singular_inference"
