"""Fixed-dimensional, training-only linear predictors and native nuisance fits."""

from __future__ import annotations

import math
from dataclasses import dataclass
from fractions import Fraction
from typing import Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, field_validator
from scipy import linalg, optimize, special

from terrine.data import MaterializedFrame
from terrine.diagnostics._data import category_key
from terrine.errors import RefusalSpec
from terrine.estimate._data import NONFINITE_DATA, PreparedData, prepare
from terrine.estimate.folds import ONE_ARM, assign_folds, fold_digest

INSUFFICIENT_TRAINING = RefusalSpec(
    "terrine.estimate.insufficient_training",
    "Nuisance fit needs more training rows than declared columns.",
)
INVALID_TERMS = RefusalSpec(
    "terrine.estimate.invalid_terms",
    "Declare known main effects and numeric square/interaction terms.",
)
UNSEEN_CATEGORY = RefusalSpec(
    "terrine.estimate.unseen_category",
    "Column {column} has a held-out level absent from training; "
    "specify a supported predictor before fitting.",
)
RANK_DEFICIENT = RefusalSpec(
    "terrine.estimate.rank_deficient",
    "The declared training design lacks full numerical rank; "
    "revise terms or obtain more arm support.",
)
SEPARATION = RefusalSpec(
    "terrine.estimate.separation",
    "Complete or quasi separation prevents a finite unpenalized propensity MLE.",
)
NONCONVERGENCE = RefusalSpec(
    "terrine.estimate.nonconvergence",
    "The unpenalized propensity fit did not meet the convergence policy; no fallback is used.",
)
ILL_CONDITIONED_NUISANCE = RefusalSpec(
    "terrine.estimate.ill_conditioned_nuisance",
    "The scaled nuisance design or Hessian violates the numerical condition policy.",
)
CONDITION_LIMIT = np.finfo(np.float64).eps ** -0.5


class LinearTerms(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    features: tuple[str, ...] | None = None
    squares: tuple[str, ...] = ()
    interactions: tuple[tuple[str, str], ...] = ()

    @field_validator("features", "squares")
    @classmethod
    def canonical_names(cls, value):
        if value is None:
            return value
        if any(not name for name in value):
            raise ValueError("feature names must be nonempty")
        return tuple(sorted(set(value)))

    @field_validator("interactions")
    @classmethod
    def canonical_pairs(cls, value):
        if any(not left or not right for left, right in value):
            raise ValueError("feature names must be nonempty")
        return tuple(sorted({tuple(sorted(pair)) for pair in value}))


class NuisanceSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    propensity: LinearTerms = LinearTerms()
    outcome: LinearTerms = LinearTerms()


def _raw(features: dict[str, np.ndarray], term: tuple[str, ...]) -> np.ndarray:
    if len(term) == 1:
        return features[term[0]]
    with np.errstate(over="ignore", invalid="ignore"):
        raw = features[term[0]] * features[term[1]]
    if not np.all(np.isfinite(raw)):
        raise NONFINITE_DATA.error(column=":".join(term), reason="nonfinite_model_term")
    return raw


@dataclass(frozen=True)
class Encoder:
    terms: tuple[tuple[str, ...], ...]
    levels: dict[str, tuple[object, ...]]
    transforms: tuple[tuple[float, float, float, float], ...]

    @classmethod
    def fit(
        cls,
        features: dict[str, np.ndarray],
        kinds: dict[str, str],
        train: np.ndarray,
        spec: LinearTerms,
    ) -> Encoder:
        mains = tuple(features) if spec.features is None else spec.features
        names = (
            set(mains) | set(spec.squares) | {name for pair in spec.interactions for name in pair}
        )
        if names - features.keys():
            raise INVALID_TERMS.error(
                reason="unknown_feature", columns=sorted(names - features.keys())
            )
        numeric = {name for name in features if kinds[name] == "numeric"}
        if set(spec.squares) - numeric or any(
            left not in numeric or right not in numeric for left, right in spec.interactions
        ):
            raise INVALID_TERMS.error(reason="numeric_polynomials_required")
        terms: list[tuple[str, ...]] = [(name,) for name in mains if kinds[name] == "numeric"]
        terms.extend(sorted({(name, name) for name in spec.squares} | set(spec.interactions)))
        levels = {
            name: tuple(sorted(set(features[name][train]), key=category_key))
            for name in mains
            if kinds[name] == "categorical"
        }
        transforms = []
        for term in terms:
            values = _raw(features, term)[train]
            if not len(values):
                raise INSUFFICIENT_TRAINING.error(reason="empty_training_complement")
            # Transform each declared raw term, not centered features before products.
            # Intercept + this affine transform spans exactly intercept + raw term.
            reference = float(np.min(values)) / 2 + float(np.max(values)) / 2
            delta = values - reference
            magnitude = float(np.max(np.abs(delta)))
            if magnitude == 0:
                magnitude = 1.0
            scaled = delta / magnitude
            center = math.fsum(scaled) / len(values)
            scale = math.sqrt(math.fsum(float(v - center) ** 2 for v in scaled) / len(values))
            transforms.append((reference, magnitude, center, scale if scale else 1.0))
        return cls(tuple(terms), levels, tuple(transforms))

    def transform(self, features: dict[str, np.ndarray]) -> np.ndarray:
        n = len(next(iter(features.values()))) if features else 0
        return self.transform_n(features, n)

    def transform_n(self, features: dict[str, np.ndarray], n: int) -> np.ndarray:
        columns = [np.ones(n)]
        for term, (reference, magnitude, center, scale) in zip(
            self.terms, self.transforms, strict=True
        ):
            with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
                values = ((_raw(features, term) - reference) / magnitude - center) / scale
            if not np.all(np.isfinite(values)):
                raise NONFINITE_DATA.error(column=":".join(term), reason="nonfinite_scaled_design")
            columns.append(values)
        for name, levels in self.levels.items():
            if any(value not in levels for value in features[name]):
                raise UNSEEN_CATEGORY.error(column=name)
            columns.extend((features[name] == level).astype(float) for level in levels[1:])
        return np.column_stack(columns)


def _design(x: np.ndarray, q: np.ndarray, *, fold: int, arm: int | None) -> np.ndarray:
    n, p = x.shape
    context = {"fold": fold, "arm": arm, "n_train": n, "n_parameters": p}
    if n <= p:
        raise INSUFFICIENT_TRAINING.error(**context)
    if not np.all(np.isfinite(x)) or not np.all(np.isfinite(q)) or np.any(q <= 0):
        raise ILL_CONDITIONED_NUISANCE.error(**context, reason="nonfinite_design_or_weights")
    weighted = x * np.sqrt(q / math.fsum(q))[:, None]
    singular = linalg.svdvals(weighted, check_finite=False)
    tolerance = np.finfo(float).eps * max(weighted.shape) * singular[0]
    if singular[-1] <= tolerance:
        raise RANK_DEFICIENT.error(**context)
    condition = float(singular[0] / singular[-1])
    if condition > CONDITION_LIMIT:
        raise ILL_CONDITIONED_NUISANCE.error(
            **context, criterion="scaled_design", condition=condition, limit=CONDITION_LIMIT
        )
    return weighted


def _separation_certificate(signed: np.ndarray, direction: np.ndarray | None) -> bool:
    """Validate the cone certificate, not the LP's approximate feasibility flag."""
    if direction is None or not np.all(np.isfinite(direction)):
        return False
    # Products and their compensated sum have a conservative gamma_(2p+1)
    # forward-error bound. Ambiguous signs are resolved on the exact dyadic
    # inputs, including genuine zero margins under quasi separation.
    eps = np.finfo(float).eps
    count = 2 * signed.shape[1] + 1
    if count * eps >= 1:
        return False
    gamma = count * eps / (1 - count * eps)
    normalized = False
    for row in signed:
        with np.errstate(over="ignore", invalid="ignore"):
            products = row * direction
        if not np.all(np.isfinite(products)):
            return False
        magnitude = float(np.max(np.abs(products)))
        margin = 0.0 if magnitude == 0 else math.fsum(products / magnitude)
        error = 0.0
        if magnitude:
            error = (
                gamma * math.fsum(np.abs(products / magnitude))
                + count * np.finfo(float).smallest_subnormal / magnitude
            )
        exact = None
        if margin < -error:
            return False
        if margin <= error:
            exact = sum(
                (
                    Fraction(float(x)) * Fraction(float(b))
                    for x, b in zip(row, direction, strict=True)
                ),
                Fraction(0),
            )
            if exact < 0:
                return False
            normalized |= exact >= 1
        elif margin - error >= 1 / magnitude:
            normalized = True
    if normalized:
        return True
    # Only an unresolved normalization needs exact accumulation of every
    # margin. The usual mean-margin-two certificate already has a row > 1.
    total = sum(
        (
            Fraction(float(x)) * Fraction(float(b))
            for row in signed
            for x, b in zip(row, direction, strict=True)
        ),
        Fraction(0),
    )
    return total >= 1


def _check_separation(x: np.ndarray, a: np.ndarray, *, fold: int) -> None:
    signed = (2 * a - 1)[:, None] * x
    # HiGHS supports feasibility tolerances down to 1e-10. The first pass
    # uses its documented 1e-7 scale; only a checked certificate proves
    # separation. A failed certificate gets a strict, row-scaled solve.
    for tolerance, scaled in ((1e-7, False), (1e-10, True)):
        constraints = signed
        if scaled:
            constraints = signed / np.max(np.abs(signed), axis=1)[:, None]
        normalization = np.array([math.fsum(constraints[:, j] / len(x)) for j in range(x.shape[1])])
        result = optimize.linprog(
            np.zeros(x.shape[1]),
            A_ub=np.vstack([-constraints, -normalization]),
            # Mean margin >= 2 leaves slack for the certificate's sum >= 1.
            b_ub=np.r_[np.zeros(len(x)), -2.0],
            bounds=[(None, None)] * x.shape[1],
            method="highs",
            options={
                "primal_feasibility_tolerance": tolerance,
                "dual_feasibility_tolerance": tolerance,
            },
        )
        if result.status == 2:
            return
        if result.success and _separation_certificate(signed, result.x):
            raise SEPARATION.error(fold=fold)
    raise NONCONVERGENCE.error(
        fold=fold,
        criterion="separation_feasibility",
        solver_status=int(result.status),
        feasibility_tolerance=1e-10,
    )


def fit_logistic(x: np.ndarray, a: np.ndarray, q: np.ndarray, *, fold: int) -> np.ndarray:
    _design(x, q, fold=fold, arm=None)
    _check_separation(x, a, fold=fold)
    w = q / math.fsum(q)

    def loss(beta):
        eta = x @ beta
        return float(np.dot(w, np.logaddexp(0, np.where(a == 1, -eta, eta))))

    def gradient(beta):
        eta = x @ beta
        residual = np.where(a == 1, -special.expit(-eta), special.expit(eta))
        return x.T @ (w * residual)

    def hessian(beta):
        eta = x @ beta
        variance = np.exp(-np.logaddexp(0, -eta) - np.logaddexp(0, eta))
        return (x.T * (w * variance)) @ x

    result = optimize.minimize(
        loss,
        np.zeros(x.shape[1]),
        method="trust-exact",
        jac=gradient,
        hess=hessian,
        options={"maxiter": 200, "gtol": 1e-8},
    )
    norm = float(np.max(np.abs(gradient(result.x))))
    if (
        not result.success
        or not math.isfinite(norm)
        or norm > 1e-8
        or not np.all(np.isfinite(result.x))
        or not np.all(np.isfinite(x @ result.x))
    ):
        raise NONCONVERGENCE.error(
            fold=fold,
            iterations=int(result.nit),
            gradient_norm=norm if math.isfinite(norm) else None,
        )
    condition = float(np.linalg.cond(hessian(result.x)))
    if not math.isfinite(condition) or condition > CONDITION_LIMIT:
        raise ILL_CONDITIONED_NUISANCE.error(
            fold=fold,
            criterion="propensity_hessian",
            condition=condition if math.isfinite(condition) else None,
            limit=CONDITION_LIMIT,
        )
    return result.x


def fit_outcome(x: np.ndarray, y: np.ndarray, q: np.ndarray, *, fold: int, arm: int) -> np.ndarray:
    weighted = _design(x, q, fold=fold, arm=arm)
    reference = float(np.min(y)) / 2 + float(np.max(y)) / 2
    response = (y - reference) * np.sqrt(q / math.fsum(q))
    if not np.all(np.isfinite(response)):
        raise ILL_CONDITIONED_NUISANCE.error(
            fold=fold, arm=arm, reason="nonfinite_weighted_response"
        )
    theta, _, rank, _ = linalg.lstsq(weighted, response, lapack_driver="gelsd", check_finite=False)
    if rank != x.shape[1]:
        raise RANK_DEFICIENT.error(fold=fold, arm=arm)
    theta[0] += reference
    if not np.all(np.isfinite(theta)) or not np.all(np.isfinite(x @ theta)):
        raise ILL_CONDITIONED_NUISANCE.error(
            fold=fold, arm=arm, reason="nonfinite_coefficients_or_prediction"
        )
    return theta


@dataclass(frozen=True)
class FoldFit:
    fold: int
    train: np.ndarray
    propensity_design: np.ndarray
    outcome_design: np.ndarray | None
    beta: np.ndarray
    theta0: np.ndarray | None
    theta1: np.ndarray | None


@dataclass(frozen=True)
class CrossFitPredictions:
    log_e: np.ndarray
    log1m_e: np.ndarray
    mu0: np.ndarray | None
    mu1: np.ndarray | None
    fold_ids: np.ndarray
    fits: tuple[FoldFit, ...]
    digest: str


def _crossfit_prepared(
    data: PreparedData, *, method: str, folds: int, seed: int, nuisance: NuisanceSpec
) -> CrossFitPredictions:
    fold_ids = assign_folds(data.units, folds=folds, seed=seed)
    log_e = np.empty(data.n)
    log1m_e = np.empty(data.n)
    mu0 = None if method == "ipw" else np.empty(data.n)
    mu1 = None if method == "ipw" else np.empty(data.n)
    fits = []
    for fold in range(folds):
        train = fold_ids != fold
        heldout = ~train
        if len(np.unique(data.a[train])) != 2:
            raise ONE_ARM.error(fold=fold, reason="training_complement")
        encoder = Encoder.fit(data.features, data.kinds, train, nuisance.propensity)
        x = encoder.transform_n(data.features, data.n)
        z = None
        theta0 = theta1 = None
        if method != "ipw":
            z = (
                x
                if nuisance.outcome == nuisance.propensity
                else Encoder.fit(data.features, data.kinds, train, nuisance.outcome).transform_n(
                    data.features, data.n
                )
            )
            subset0 = train & (data.a == 0)
            subset1 = train & (data.a == 1)
            theta0 = fit_outcome(z[subset0], data.y[subset0], data.q[subset0], fold=fold, arm=0)
            theta1 = fit_outcome(z[subset1], data.y[subset1], data.q[subset1], fold=fold, arm=1)
        beta = fit_logistic(x[train], data.a[train], data.q[train], fold=fold)
        eta = x[heldout] @ beta
        if not np.all(np.isfinite(eta)):
            raise ILL_CONDITIONED_NUISANCE.error(fold=fold, reason="nonfinite_heldout_predictor")
        log_e[heldout] = -np.logaddexp(0, -eta)
        log1m_e[heldout] = -np.logaddexp(0, eta)
        if method != "ipw":
            assert z is not None and mu0 is not None and mu1 is not None
            assert theta0 is not None and theta1 is not None
            mu0[heldout] = z[heldout] @ theta0
            mu1[heldout] = z[heldout] @ theta1
        fits.append(FoldFit(fold, train, x, z, beta, theta0, theta1))
    return CrossFitPredictions(
        log_e, log1m_e, mu0, mu1, fold_ids, tuple(fits), fold_digest(data.units, fold_ids)
    )


def crossfit(
    frame: MaterializedFrame,
    *,
    outcome: str,
    method: Literal["ipw", "aipw", "regression_adjustment"],
    folds: int,
    seed: int,
    nuisance: NuisanceSpec,
) -> CrossFitPredictions:
    return _crossfit_prepared(
        prepare(frame, outcome=outcome), method=method, folds=folds, seed=seed, nuisance=nuisance
    )
