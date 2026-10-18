# Sequential-panel calibration record

## Predeclared design

The panel calibration is a 16-cell factorial design (2 × 2 × 2 × 2), with 400 fixed seeds (`0..399`) and `n_units=2000` in every cell. Factors are: zero versus heterogeneous cohort effects; equal versus unequal trial aggregation masses; disjoint versus overlapping repeated-trial units; and two trial-varying assignment-rate/baseline profiles. Each replication also runs a fixed contaminated-reference case whose expected result is the stable `terrine.estimate.stacked_input` refusal with reason `outcome_window_contaminates_reference`, not an estimate.

The calibration harness also predeclares eight stochastic stacked cohort-time
(CATT) recovery cells: heterogeneous cohort effects `(1.0, 2.0)` crossed with
equal/unequal cohort aggregation weights, disjoint/overlapping unit populations,
and two outcome trend/noise profiles. Each CATT cell uses the same 400 seeds
(`0..399`) and `n_units=2000`; every seed must recover both cohort-specific
effects and the normalized aggregate target. CATT bias, 95% exact-binomial
coverage, and studentization gates are Bonferroni allocated across both
contrasts in all eight cells. Full-run evidence is one `stacked-<cell>.json`
record per CATT cell.
The CATT per-gate alpha is `0.01/(3*8*2)`.

Every clean cell requires all 400 replications to complete successfully; any unexpected refusal fails the cell and stays in the denominator. Evidence records attempted, completed/refused counts, refusal codes/seeds, bias, empirical standard deviation, mean standard error, RMSE, exact interval-coverage count, median interval width, diagnostic statuses, runtime/environment and exact seed list. At family alpha `0.01`, bias, exact-binomial 0.95 coverage and chi-square studentization tests each receive `0.01/(3*16)`. The two-sided bias test uses a log-tail normal critical value; coverage uses the exact binomial test; studentization compares the sum of squared error/SE ratios to its chi-square range. All cutoffs are frozen in the harness before results are observed.

## Gates and execution

Full-grid command:

```sh
uv run pytest tests/parameter_recovery/test_panel_calibration.py -m 'slow and parameter_recovery' -q
```

The ordinary suite retains a deterministic three-seed, `n_units=120` repeated-trial AIPW smoke and a three-seed CATT smoke over all eight cells at `n_units=240`. The CATT smoke command exercised here is:

```sh
uv run pytest tests/parameter_recovery/test_panel_calibration.py::test_stacked_catt_heterogeneity_recovers_in_fast_smoke -q
```

The per-cell JSON writer was also exercised for one marked CATT cell without
running the full grid:

```sh
TERRINE_CALIBRATION_SMOKE=1 TERRINE_CALIBRATION_DIR=/tmp/terrine-catt-smoke \
uv run pytest tests/parameter_recovery/test_panel_calibration.py \
  -m 'slow and parameter_recovery' -k 'stacked-equal-overlap-profile_a' -q
```

This smoke completed 3/3 seeds and wrote
`/tmp/terrine-catt-smoke/stacked-equal-overlap-profile_a.json`; it is smoke-only
evidence and is not committed as a full-calibration result.

The marked full calibration emits separate CATT JSON records alongside the 16 AIPW records. Based on the 24-replication, eight-cell CATT smoke and the previously executed eight-worker AIPW grid, the exact full-grid command is estimated to take roughly 30–45 minutes serially; this is an estimate, not an execution result. The full grid was not run for this change.

The existing executed-results table below covers only the original 16-cell repeated-trial AIPW grid. It does not constitute CATT calibration evidence. No CATT full-cell results or JSON evidence are claimed until that grid is actually run.

The full grid is intentionally not run as part of ordinary pytest. Per-cell JSON from full calibration runs is written beneath `docs/validation/generated/panel_calibration/`.

## Executed results

Command: `uv run pytest -n 8 tests/parameter_recovery/test_panel_calibration.py -m 'slow and parameter_recovery'`
(the full-grid command above, parallelized over 8 xdist workers). Outcome:
**16 passed** in 103 s wall time. There were no reruns, seed changes or gate
changes. The environment was Python 3.13.5, NumPy 2.5.4, SciPy 1.18.1 and
macOS 15.7.4 arm64. Committed per-cell JSON evidence is in
`docs/validation/generated/panel_calibration/`.

Every cell completed 400 of 400 estimates. Every paired contaminated-reference
case raised the expected `terrine.estimate.stacked_input` refusal with reason
`outcome_window_contaminates_reference`. Bias, exact-binomial coverage and
chi-square studentization gates all passed at the predeclared family level of
0.01.

| Cell | Attempted / completed / paired refusals | Mean error | Empirical SD | Mean SE | 95% CI coverage (exact binomial p) | Median CI width |
|---|---|---|---|---|---|---|
| `heterogeneous-equal-disjoint-profile_a` | 400/400/400 | +0.0027 | 0.0692 | 0.0715 | 385/400 (p=0.301) | 0.280 |
| `heterogeneous-equal-disjoint-profile_b` | 400/400/400 | −0.0030 | 0.0637 | 0.0682 | 385/400 (p=0.301) | 0.267 |
| `heterogeneous-equal-overlap-profile_a` | 400/400/400 | +0.0043 | 0.0706 | 0.0717 | 383/400 (p=0.567) | 0.281 |
| `heterogeneous-equal-overlap-profile_b` | 400/400/400 | −0.0018 | 0.0688 | 0.0684 | 383/400 (p=0.567) | 0.268 |
| `heterogeneous-unequal-disjoint-profile_a` | 400/400/400 | +0.0030 | 0.0781 | 0.0807 | 386/400 (p=0.205) | 0.315 |
| `heterogeneous-unequal-disjoint-profile_b` | 400/400/400 | −0.0026 | 0.0728 | 0.0768 | 388/400 (p=0.066) | 0.301 |
| `heterogeneous-unequal-overlap-profile_a` | 400/400/400 | +0.0034 | 0.0794 | 0.0808 | 384/400 (p=0.422) | 0.316 |
| `heterogeneous-unequal-overlap-profile_b` | 400/400/400 | −0.0010 | 0.0766 | 0.0769 | 387/400 (p=0.134) | 0.301 |
| `zero-equal-disjoint-profile_a` | 400/400/400 | +0.0026 | 0.0691 | 0.0713 | 385/400 (p=0.301) | 0.279 |
| `zero-equal-disjoint-profile_b` | 400/400/400 | −0.0031 | 0.0635 | 0.0680 | 385/400 (p=0.301) | 0.266 |
| `zero-equal-overlap-profile_a` | 400/400/400 | +0.0037 | 0.0703 | 0.0713 | 384/400 (p=0.422) | 0.279 |
| `zero-equal-overlap-profile_b` | 400/400/400 | −0.0025 | 0.0681 | 0.0680 | 380/400 (p=1.000) | 0.266 |
| `zero-unequal-disjoint-profile_a` | 400/400/400 | +0.0029 | 0.0780 | 0.0805 | 388/400 (p=0.066) | 0.315 |
| `zero-unequal-disjoint-profile_b` | 400/400/400 | −0.0027 | 0.0723 | 0.0766 | 390/400 (p=0.021) | 0.300 |
| `zero-unequal-overlap-profile_a` | 400/400/400 | +0.0028 | 0.0790 | 0.0804 | 384/400 (p=0.422) | 0.315 |
| `zero-unequal-overlap-profile_b` | 400/400/400 | −0.0016 | 0.0761 | 0.0765 | 386/400 (p=0.205) | 0.299 |

Matching heterogeneous and zero-effect cells use the same seeds and noise, so their estimation errors are nearly identical. The smallest coverage p-value, 0.021 (390/400), reflects conservative intervals. It remains above the cell's Bonferroni-allocated threshold.

## Interpretation limits

Recovery and interval coverage are evidence only for this finite family of simulated laws, sample sizes, nuisance specifications and inference implementation. They do not establish causal identification, parallel trends, universal normality, or power in an applied study. In particular, failure to reject a joint pretrend test is not evidence that untreated trends are parallel; placebo and negative-control diagnostics have their own sample-size and power limits.
