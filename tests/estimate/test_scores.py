from fractions import Fraction

import numpy as np
import pytest

from terrine.estimate.inference import pseudo_outcome, target_derivatives


def test_ate_scores_match_fraction_reference():
    a = np.array([1, 0, 1, 0])
    y = np.array([3.0, -2.0, 5.0, 1.0])
    e = [Fraction(1, 4), Fraction(2, 3), Fraction(3, 4), Fraction(1, 3)]
    m0, m1 = np.array([1.0, 2.0, 3.0, 4.0]), np.array([4.0, 6.0, 8.0, 10.0])
    q = [Fraction(1), Fraction(2), Fraction(3), Fraction(4)]
    for method in ("ipw", "aipw", "regression_adjustment"):
        scores = []
        for i in range(4):
            ip = a[i] * Fraction(int(y[i])) / e[i] - (1 - a[i]) * Fraction(int(y[i])) / (1 - e[i])
            ra = Fraction(int(m1[i] - m0[i]))
            aug = (
                ra
                + a[i] * Fraction(int(y[i] - m1[i])) / e[i]
                - (1 - a[i]) * Fraction(int(y[i] - m0[i])) / (1 - e[i])
            )
            scores.append({"ipw": ip, "aipw": aug, "regression_adjustment": ra}[method])
        expected = sum(w * s for w, s in zip(q, scores, strict=True)) / sum(q)
        got = pseudo_outcome(
            method, a, y, np.log([float(v) for v in e]), np.log([float(1 - v) for v in e]), m0, m1
        )
        assert np.dot(np.array(q, dtype=float), got) / float(sum(q)) == pytest.approx(
            float(expected)
        )


@pytest.mark.parametrize("method", ["ipw", "aipw", "regression_adjustment"])
def test_joint_jacobian_matches_independent_numerical_derivative(method):
    a = np.array([1.0, 0.0, 1.0, 0.0])
    y = np.array([2.0, -1.0, 4.0, 3.0])
    x = np.array([[1.0, -1.0], [1.0, 2.0], [1.0, 0.2], [1.0, -0.5]])
    q = np.array([1.0, 2.0, 0.7, 1.3])
    par = np.array([0.2, -0.3, 1.0, 0.1, 2.0, -0.4])

    def evaluate(p):
        eta = x @ p[:2]
        return q * pseudo_outcome(
            method, a, y, -np.logaddexp(0, -eta), -np.logaddexp(0, eta), x @ p[2:4], x @ p[4:]
        )

    eta = x @ par[:2]
    got = target_derivatives(
        method,
        a,
        y,
        q,
        -np.logaddexp(0, -eta),
        -np.logaddexp(0, eta),
        x @ par[2:4],
        x @ par[4:],
        x,
        x,
    )
    ref = np.column_stack(
        [
            (evaluate(par + np.eye(6)[j] * 1e-5) - evaluate(par - np.eye(6)[j] * 1e-5)) / 2e-5
            for j in range(6)
        ]
    )
    np.testing.assert_allclose(np.column_stack(got), ref, atol=2e-9, rtol=2e-9)


def test_augmentation_derivatives_retain_small_complements():
    eta = np.array([-50.0, 50.0])
    a = np.array([0.0, 1.0])
    x = np.ones((2, 1))
    _, control, treated = target_derivatives(
        "aipw",
        a,
        np.zeros(2),
        np.ones(2),
        -np.logaddexp(0, -eta),
        -np.logaddexp(0, eta),
        np.zeros(2),
        np.zeros(2),
        x,
        x,
    )
    assert control[0, 0] == pytest.approx(float(np.exp(-50.0)), rel=1e-15, abs=0)
    assert treated[1, 0] == pytest.approx(float(-np.exp(-50.0)), rel=1e-15, abs=0)
