# Native baseline ATE estimation

`terrine.estimate` consumes a `terrine.data.MaterializedFrame` containing observed
Arrow data. It does not require a warehouse, `Model`, `Frame`, a semantic catalog,
or increment. The three entry points have the same signature:

```python
from terrine.estimate import (
    DiagnosticPolicy,
    LinearTerms,
    NuisanceSpec,
    aipw,
    ipw,
    regression_adjustment,
)

result = aipw(
    materialized,
    outcome="net_revenue",
    estimand="ATE",
    folds=5,
    seed=0,
    alpha=0.05,
    nuisance=NuisanceSpec(),
    diagnostics=DiagnosticPolicy(),
)
print(result.value, result.se, result.ci)
saved = result.to_json()
```

Omit `outcome` only when exactly one outcome is declared. A saved result can be
loaded with `terrine.results.Estimate.from_json(saved)`. Results and diagnostic
contexts are deeply immutable; serialization preserves nulls and their reasons.
Results retain specifications, numerical policies, fold identity, source/snapshot
metadata, and assumptions, but not observations or fitted row-level predictions.

## Target and identifying assumptions

For independent units with baseline treatment `A`, pre-treatment features `X`,
complete-horizon outcome `Y`, and known positive target weight `q(X)`, the target is

\[
\tau_q = \frac{E[q(X)\{Y(1)-Y(0)\}]}{E[q(X)]}.
\]

Without a weight role, `q=1`. With `Q=sum(q)`, held-out propensity `e`, and held-out
separate-arm outcome predictions `m0,m1`, the estimates are:

- **IPW:** `sum(q * (A*Y/e - (1-A)*Y/(1-e))) / Q`. This is target-normalized
  Horvitz–Thompson, not arm-normalized Hájek weighting.
- **Regression adjustment:** `sum(q * (m1-m0)) / Q`.
- **AIPW:** `sum(q * (m1-m0 + A*(Y-m1)/e - (1-A)*(Y-m0)/(1-e))) / Q`.

These require consistency and a well-defined baseline intervention, no
interference, baseline conditional exchangeability, positivity on the target
support, correct temporal construction, and complete follow-up. Features must
not be treatment descendants. Metadata and diagnostics cannot establish these
assumptions. IPW additionally needs correct propensity specification; regression
adjustment needs correct arm outcome specification; AIPW needs at least one of
those nuisance families to be correct. None promises finite-sample unbiasedness.

## Admissible materialized inputs

The causal consumer validates metadata before fitting, and never drops or imputes
rows:

- One row per independent unit, stable nonnull signed-int64 or exact UTF-8 unit
  IDs, both observed treatment arms, and one common timezone-aware time zero.
  An explicit cluster field must identify the same independent units; changing
  a cluster label does not license repeated-row inference.
  Common time-zero identity is checked in Arrow's exact timestamp domain;
  nanosecond values do not require optional pandas or conversion to Python dates.
- `shape="cross_section"`, `treatment_design="baseline_binary"`, an explicitly
  declared treatment known at time zero, and declared treatment knowledge.
- Typed `Provenance.feature_cutoffs` for every baseline feature, plus safe proof
  for every declared feature and eligibility cutoff: strictly before zero, or
  zero with strict exclusion. Retrospective effective-time knowledge remains
  labeled as retrospective, not historical recorded-time availability.
- Nonempty eligibility predicates require a resolved `EligibilitySummary`:
  nonnegative reconciling counts, no unknown candidates, and retained count equal
  to the Arrow row count. A known-false exclusion is allowed. Without predicates,
  an optional summary must describe an all-retained input. The summary audits
  declared selection, not undeclared omissions from an upstream source.
- The selected outcome has a fixed postbaseline horizon and a Boolean complete
  flag for every unit. Incomplete observation or source coverage refuses even
  when finite pre-exit events exist. Membership exit is not measurement exit.
  Null labels, completeness flags, and exact reasons survive Arrow/JSON loading;
  estimation does not turn partial outcomes into zero or remove their units.
- Required measured columns have no missing, NaN or infinite values. Boolean,
  text, and primitive dictionary features are categorical. Missing categorical
  values are not levels; numeric dictionary labels are not numeric trends.

The same fixed selected outcome horizon applies to the whole cohort. An
initiation-during-grace treatment is not baseline treatment: moving the outcome
start beyond grace does not remove future survival/adherence selection. Such a
frame can be described but these estimators refuse it. ATT, panels, repeated
trials, censoring models, and sampled-control inference need different estimands
and equations, not a relabeled ATE.

### Fixed target weights, not sampling weights

A manually weighted frame declares `Roles.weight`,
`weight_kind="baseline_function"`, a safe `weight_cutoff`, and
`weight_depends_on_treatment=False`. Weights must be finite and strictly positive.
All nuisance fits and the final target use the same `q`. Normalization first
divides by the largest weight and then normalizes the mean, avoiding overflow
from a common positive rescaling. A dynamic range that loses a positive weight
in binary64 refuses rather than silently assigning zero weight.

These are known fixed functions of baseline covariates, not fitted weights or
sampling/design weights. Treatment-dependent and sampled-control weights are
not admitted. The current compiled study design has no weight setter; supported
weighted inputs are explicitly constructed `MaterializedFrame` instances.

### Precision and certificates

Estimation uses binary64, separately from the compiler's integer/decimal
arithmetic. Integers outside `[-2**53,2**53]` refuse. Decimal conversion compares
the original decimal to the binary64 cast against
`absolute_error <= 1e-12 + 1e-12*abs(original)`, not a float-to-float round trip.
Conversion bounds and original floating-format rounding bounds are retained in
result provenance.

Compiler `numeric_policy` and `absolute_error_column` are preserved separately.
Every nonnull certified value needs a finite nonnegative bound; null values
retain null bounds and an exact reason. Missing/invalid certificates refuse.
The estimate never regenerates a certificate from rounded data, relabels a
floating aggregate exact, or interprets a numerical bound as a confidence
interval or historical-availability proof. Original manually measured floats
need no fabricated compiler certificate. Unknown warehouse generations and
snapshot identifiers remain null.

## Honest folds and declared native models

`LinearTerms(features=None, squares=(), interactions=())` includes an intercept
and, by default, every declared baseline feature main effect. For example:

```python
specification = NuisanceSpec(
    propensity=LinearTerms(squares=("engagement",)),
    outcome=LinearTerms(interactions=(("engagement", "account_age"),)),
)
```

Explicit `features=()` means intercept-only unless polynomial terms are supplied.
Squares and interactions require numeric features, not category codes. Terms
are deduplicated and canonicalized. Each declared **raw term** is centered/scaled
as an affine reparameterization with the intercept: requesting only `X**2` never
silently adds a missing `X` main effect.

Vocabulary and numerical transforms are learned on each training complement
only. Boolean/text/dictionary levels use deterministic typed ordering and
reference-level one-hot coding. A held-out unseen level refuses rather than
becoming a baseline level. A level missing from one outcome training arm can
make that arm's design rank deficient; columns are not dropped or pooled.

Unit folds use SHA-256 with prefix `b"terrine-fold-v1"`, unsigned 64-bit big-endian
seed, then type tag (`i` or `s`), unsigned 64-bit byte length, and payload. Integers
use eight-byte big-endian two's complement; strings preserve exact UTF-8 without
Unicode normalization. The first eight digest bytes, interpreted big-endian,
are reduced modulo the fold count. The assignment and its digest do not depend
on row order. Require `2 <= folds <= n`, `0 <= seed < 2**64`, no empty held-out
fold, and both treatment arms in every training complement. Seeds are not retried
and folds are not stratified after inspecting outcomes.

Models are unpenalized target-weighted logistic regression and separate-arm
weighted least squares. Outcome fitting uses rank-revealing SVD. Logistic fitting
checks finite-MLE existence by separation feasibility: a separating direction
has all signed margins nonnegative and their sum at least one. Approximate LP
success alone is not a certificate: signed margins and normalization are checked,
ambiguous signs are resolved exactly, and unresolved feasibility refuses as
numerical nonconvergence rather than declaring separation. The solver uses
`trust-exact` with analytical gradient/Hessian, at most 200 iterations, successful
termination, and scaled mean-loss gradient infinity norm at most `1e-8`.
Every actual training design needs more rows than columns and full numerical
rank; scaled-design and logistic-Hessian condition policies use
`eps_float64**(-1/2)`. These are numerical acceptance policies, not model adequacy
theorems. There is no regularization, propensity clipping, fit-on-all-data,
replacement model, or other fallback.

## Uncertainty is a joint sandwich

Every estimator uses `inference="crossfit_parametric_sandwich"`. For each fold,
include its training-only logistic score when propensity enters the point
estimate and its separate-arm training outcome scores when outcome regression
enters it. Add the final target equation `q*(pseudo_outcome-tau)`. Each independent
unit contributes to every fold whose training complement contains it; overlapping
training complements must not be treated as independent samples.

The analytical Jacobian includes the held-out target's nuisance derivatives,
including for AIPW under one-sided misspecification. With mean Jacobian `B` and
unit scores `g_i`, solve for the required row of `B**(-1)` without constructing an
inverse. For `h_i = -e_tau' B**(-1) g_i`, compute

`se**2 = sum((h_i-mean(h))**2) / (n*(n-1))`.

Production inference projects nuisance blocks directly into one unit-influence
vector instead of allocating a full joint score matrix or mostly zero bread.
The full spectral condition gate uses block eigenbases and bracketed outer
eigenvalues; unresolved numerical bounds refuse rather than weakening the gate.
Means and centered variance calculations scale before compensated accumulation.

Diagnostic-only propensity parameters are not added to regression adjustment's
point-estimate equations. Singular/ill-conditioned bread, nonfinite influence,
or invalid numerical results refuse. A score-only variance is not substituted.
The normal critical value is `-ndtri_exp(log(alpha)-log(2))`, with finite
`0 < alpha < 1`; no quantile is constructed from `1-alpha` or an underflowing
`alpha/2`.

This is asymptotic parametric inference with fixed-dimensional regular nuisance
models, fixed finite fold count, finite weighted-score second moments, and a
nonsingular population Jacobian. It is not generic black-box-ML or finite-sample
inference. Compiler input-error bounds are not included in the sampling SE.

## Diagnostics and refusals

Standalone `terrine.diagnostics.overlap(frame, *, log_propensity,
log_complement, policy=None)` and `balance(frame, *,
log_treatment_weights=None, policy=None)` have no estimation dependency.
Unsupported integer conversion precision yields unavailable balance evidence
rather than rounding distinct covariate values into a false balance pass.

Default policy:

| Policy | Default |
| --- | ---: |
| `overlap_warn` | 0.05 |
| `overlap_fail` | 0.01 |
| `max_fail_share` | 0.01 |
| `min_arm_ess` | 20 |
| `smd_warn` | 0.10 |
| `smd_fail` | 0.25 |

Overlap reports target-weighted within-arm histograms, ranges, target tail shares,
and arm ESS under complete weights `q/e` or `q/(1-e)`. Probability comparisons use
both supplied log probabilities, never a subtracted complement. Failure means a
target fail-tail share strictly above `max_fail_share`, arm ESS strictly below
`min_arm_ess`, or numerical non-support. Any mass outside the warning interval
warns if there is no failure. Histogram edges and normalization semantics are
included in diagnostic context; numerical non-support preserves null metrics
with exact reasons rather than nonfinite numbers.

Balance reports every numeric feature and every categorical level contrast. Its
before means use target weights; adjusted log weights already include `q` and are
never multiplied by `q` again. Both before and after SMD use the same pooled
preweight scale. Each arm variance uses reliability denominator
`sum(q)-sum(q*q)/sum(q)`. Equal constant means have SMD zero. Zero scale with
unequal means has null SMD and reason `zero_scale_unequal_means`, not infinity or
an invented epsilon. Only post-adjustment SMD gates: strictly above `smd_fail`
fails, strictly above `smd_warn` warns, otherwise passes. Before imbalance is
only descriptive.

IPW/AIPW provide their complete held-out inverse-probability weights. AIPW balance
is a configurable conservative screen, not a theorem that its propensity model
must be correct when the outcome model is correct. Regression adjustment does
not rebalance observed covariates: after metrics are null with
`no_balance_adjustment`, and balance is `not_applicable`. It still requires overlap
from its diagnostic-only propensity reference weights. No features gives
`not_applicable/no_features`.

All applicable evidence is computed before deciding whether to return an effect.
Any failed or unavailable required diagnostic raises
`terrine.estimate.diagnostic_failure` with immutable evidence **and no effect**.
Warnings accompany a returned estimate; they do not certify identification.
Policies are explicit user screening choices, not automatic trimming rules.

Other stable refusal suffixes in `terrine.estimate` identify the remedy:

- `treatment_required`, `unsupported_estimand`, `unsupported_design`,
  `postbaseline_covariate`: declare the supported baseline target and supply
  valid causal/time/eligibility metadata; use a separately justified method for
  other designs.
- `invalid_outcome`, `missing_data`, `incomplete_followup`, `nonfinite_data`:
  repair measurement or follow-up at its source, not by silently dropping rows.
- `invalid_weight`, `unsupported_weight_kind`: provide known positive fixed
  baseline target weights or a sampling-aware method for the different design.
- `duplicate_unit`, `invalid_folds`, `one_arm`, `insufficient_training`: obtain
  independent comparison units and adequate actual training support. There is
  no automatic seed search or repeated-row variance correction.
- `unsupported_feature_dtype`, `input_precision`, `invalid_terms`,
  `unseen_category`: supply admitted measurements and a defensible declared
  predictor; certificate and vocabulary problems are not silently repaired.
- `rank_deficient`, `separation`, `nonconvergence`,
  `ill_conditioned_nuisance`, `singular_inference`, `nonfinite_result`: inspect
  model/support/scale and obtain data or a separately justified procedure.
- `invalid_alpha`: choose a finite tail level strictly between zero and one.

Refusals contain structured counts, column/fold/arm and numerical criteria, not
customer rows or an unapproved effect. Metadata truth and unmeasured confounding
remain the analyst's responsibility even when every screen passes.
