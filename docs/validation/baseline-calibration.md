# Baseline estimator calibration record

## Predeclared method and manifest

This file records the Task 4.4 simulation experiment. The ten inference cells are fixed before results are read:

| Cell family | Methods | DGP parameters | Target slope | Replications |
| --- | --- | --- | --- | --- |
| Both nuisance families correctly specified | IPW, AIPW, regression adjustment | `gamma=0`, `b=0` | `0`, `0.25` | seeds `0..399`, `n=2000` per cell |
| Propensity correct, nonlinear outcome omitted | IPW, AIPW | `gamma=0`, `b=2` | `0` | seeds `0..399`, `n=2000` per cell |
| Outcome correct, nonlinear propensity omitted | AIPW, regression adjustment | `gamma=1`, `b=0` | `0` | seeds `0..399`, `n=2000` per cell |

The first family has six method/weight cells and the latter two have two each. Every cell uses the default linear nuisance terms; the quadratic DGP term is deliberately omitted in the one-sided misspecification cells. The separate nonlinear-term smoke supplies `x²` to both nuisance models. Population truth is `2.15 + 0.25*target_weight_slope`; the paired oracle's realized cohort truth is reported separately and is never substituted into population coverage.

The experiment uses PCG64 generator version 1 and fixed seed inputs. Estimator folds are five, with the replication seed supplied to the canonical fold assignment; intervals use `alpha=0.05`. The family error level is `0.01`; each bias, coverage and studentization gate uses `0.01/(3*10)`. Bias is tested against its empirical Monte Carlo SE using a two-sided normal log-tail critical value. Coverage uses a two-sided exact binomial test under `p=0.95`. Studentized squared errors are summed and compared to the central chi-square acceptance region using lower-tail and survival-function quantiles. A failed estimator fit, diagnostic refusal or other exception remains in the refused count with seed and code; it is not removed from the attempted denominator.

Evidence JSON is written at the deterministic paths `docs/validation/generated/baseline-calibration/<cell>.json`, `docs/validation/generated/baseline-size/<scenario>.json`, and `docs/validation/generated/baseline-bootstrap/<method>-seed-<seed>.json`. Set `TERRINE_CALIBRATION_DIR` to redirect generated evidence. Calibration can be run with:

```sh
make test-calibration TESTS='tests/parameter_recovery -m parameter_recovery'
```

The calibration suite comprises 4,000 native estimator fits for the ten-cell grid, 1,500 fits for the size checks, and 12,000 full-refit bootstrap fits. **Measured runtime:** 1 h 29 min wall time with 12 xdist workers on a 12-core Apple Silicon machine; the longest single test took about 83 minutes. Cell, seed and bootstrap identities are independent and stable under process parallelism; evidence filenames are disjoint across cases.

The size experiment uses five unweighted method/scenario combinations (three correctly specified methods and the two one-sided AIPW cases), sizes `500`, `2000`, and `8000`, and seeds `0..99`. For each size it records bias, RMSE, mean estimated SE, MSE and `sd(error²)/sqrt(100)`. At family level `0.01` allocated across five comparisons, the one-sided upper MSE bound at `8000` must be below the one-sided lower bound at `500`; step-by-step monotonicity is not required.

The bootstrap comparison uses ten fixed `n=8000` datasets for each native method, 400 multinomial unit resamples per dataset, five original fold assignments, and complete nuisance refitting in every draw. Replicate IDs are distinct but assigned to the source unit's original fold so copies cannot leak across held-out and training partitions. All failed draws are recorded and fail the comparison. The bootstrap variance MCSE is computed from the fourth centered bootstrap moment; with positive finite variances require `abs(log(v_boot/v_sandwich)) <= 0.20 + 4*MCSE(v_boot)/v_boot`.

## Executed results

Command: `uv run pytest -n 12 tests/parameter_recovery -m parameter_recovery`
(equivalent to `make test-calibration`). Outcome: **19 passed** in 5,356 s.
There were no reruns, no seed changes and no gate changes. The environment was
Python 3.13.5, NumPy 2.5.4, SciPy 1.18.1 and macOS 15.7.4 arm64. The JSON
evidence for every cell, scenario and bootstrap dataset is committed under
`docs/validation/generated/`.

### Ten-cell inference grid (n = 2000, seeds 0..399)

Every cell completed 400 of 400 attempted replications with no refusals.
Overlap passed in every replication. Balance passed for IPW and AIPW and is
not applicable to regression adjustment. Bias, exact-binomial coverage and
chi-square studentization gates all passed at family level 0.01/(3·10).

| Cell | attempted/completed/refused | Population truth | Mean error | Empirical SD | Mean SE | 95% CI coverage | Σ studentized² | Median CI width | Cell runtime (s) |
|---|---|---|---|---|---|---|---|---|---|
| `correct-ipw-weight-0` | 400/400/0 | 2.1500 | +0.0171 | 0.1016 | 0.1076 | 388/400 | 362.5 | 0.420 | 4077 |
| `correct-aipw-weight-0` | 400/400/0 | 2.1500 | +0.0073 | 0.0918 | 0.0948 | 382/400 | 375.0 | 0.371 | 4027 |
| `correct-regression_adjustment-weight-0` | 400/400/0 | 2.1500 | +0.0069 | 0.0917 | 0.0934 | 380/400 | 386.4 | 0.366 | 4058 |
| `correct-ipw-weight-0.25` | 400/400/0 | 2.2125 | +0.0119 | 0.1041 | 0.1087 | 385/400 | 366.3 | 0.423 | 4009 |
| `correct-aipw-weight-0.25` | 400/400/0 | 2.2125 | +0.0064 | 0.0940 | 0.0954 | 382/400 | 388.1 | 0.373 | 4035 |
| `correct-regression_adjustment-weight-0.25` | 400/400/0 | 2.2125 | +0.0061 | 0.0939 | 0.0940 | 380/400 | 399.0 | 0.368 | 4034 |
| `omit-outcome-ipw` | 400/400/0 | 2.1500 | +0.0166 | 0.1115 | 0.1173 | 386/400 | 364.4 | 0.457 | 4061 |
| `omit-outcome-aipw` | 400/400/0 | 2.1500 | +0.0070 | 0.0963 | 0.0995 | 384/400 | 374.6 | 0.390 | 4058 |
| `omit-propensity-aipw` | 400/400/0 | 2.1500 | +0.0014 | 0.0965 | 0.0948 | 377/400 | 415.1 | 0.371 | 4105 |
| `omit-propensity-ra` | 400/400/0 | 2.1500 | +0.0009 | 0.0958 | 0.0933 | 376/400 | 423.2 | 0.365 | 4058 |

These are observations within the passing predeclared gates, not new claims:

- The unweighted IPW cells have the largest positive mean error, about
  3.4 Monte Carlo SEs from zero. That is below the Bonferroni-allocated
  two-sided critical value.
- IPW standard errors are mildly conservative, with mean SE above empirical SD.
- In the omitted-propensity cells, mean SE falls slightly below empirical SD
  and the studentized sums exceed 400. Both remain inside the chi-square
  acceptance region.

### Sample-size recovery (seeds 0..99; MSE ± its Monte Carlo SE)

All 1,500 fits completed without refusals. For every combination, the
one-sided upper MSE bound at 8000 is below the lower bound at 500 at family
level 0.01/5.

| Scenario | n = 500 | n = 2000 | n = 8000 |
|---|---|---|---|
| `correct-ipw` | MSE 0.06006 ± 0.00812, bias +0.0457 | MSE 0.01116 ± 0.00139, bias +0.0169 | MSE 0.00219 ± 0.00031, bias +0.0090 |
| `correct-aipw` | MSE 0.03948 ± 0.00523, bias −0.0018 | MSE 0.00928 ± 0.00120, bias +0.0085 | MSE 0.00202 ± 0.00029, bias +0.0046 |
| `correct-regression_adjustment` | MSE 0.03743 ± 0.00493, bias −0.0049 | MSE 0.00936 ± 0.00122, bias +0.0074 | MSE 0.00215 ± 0.00031, bias +0.0048 |
| `aipw-propensity-correct-outcome-omitted` | MSE 0.04499 ± 0.00577, bias +0.0003 | MSE 0.01074 ± 0.00146, bias +0.0054 | MSE 0.00212 ± 0.00030, bias +0.0029 |
| `aipw-outcome-correct-propensity-omitted` | MSE 0.03265 ± 0.00348, bias −0.0234 | MSE 0.00974 ± 0.00128, bias +0.0019 | MSE 0.00180 ± 0.00027, bias −0.0020 |

### Full-refit bootstrap versus sandwich variance (10 datasets × 400 draws, n = 8000)

| Method | Completed/attempted draws | Refused | Max \|log(v_boot/v_sandwich)\| | Min allowance | Max (statistic − allowance) |
|---|---|---|---|---|---|
| `ipw` | 4000/4000 | 0 | 0.104 | 0.463 | −0.363 |
| `aipw` | 4000/4000 | 0 | 0.100 | 0.464 | −0.366 |
| `regression_adjustment` | 4000/4000 | 0 | 0.119 | 0.466 | −0.347 |

Each allowance is `0.20 + 4·MCSE(v_boot)/v_boot`; its Monte Carlo term was about
0.26 here. The largest observed log ratio was 0.119, well inside every allowance.

## Interpretation

A planted effect or a single successful estimate is not an interval calibration. Population truth is the DGP expectation, while each finite generated cohort has a different paired-outcome average. The interval experiment evaluates coverage of the population target over independent worlds. The calibration grid is evidence for this finite set of generating laws, sample sizes, nuisance specifications and inference procedure, not a proof of universal normality, causal identification, double robustness under arbitrary misspecification, or transportability to censored, repeated-trial or sampled-control data.
