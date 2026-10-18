# Sequential-panel calibration record

## Predeclared design

The panel calibration is a 16-cell factorial design (2 × 2 × 2 × 2), with 400 fixed seeds (`0..399`) and `n_units=2000` in every cell. Factors are: zero versus heterogeneous cohort effects; equal versus unequal trial aggregation masses; disjoint versus overlapping repeated-trial units; and two trial-varying assignment-rate/baseline profiles. Each replication also runs a fixed contaminated-reference case whose expected result is the stable `terrine.estimate.stacked_input` refusal with reason `outcome_window_contaminates_reference`, not an estimate.

Every clean cell requires all 400 replications to complete successfully; any unexpected refusal fails the cell and stays in the denominator. Evidence records attempted, completed/refused counts, refusal codes/seeds, bias, empirical standard deviation, mean standard error, RMSE, exact interval-coverage count, median interval width, diagnostic statuses, runtime/environment and exact seed list. At family alpha `0.01`, bias, exact-binomial 0.95 coverage and chi-square studentization tests each receive `0.01/(3*16)`. The two-sided bias test uses a log-tail normal critical value; coverage uses the exact binomial test; studentization compares the sum of squared error/SE ratios to its chi-square range. All cutoffs are frozen in the harness before results are observed.

## Gates and execution

Full-grid command:

```sh
uv run pytest tests/parameter_recovery/test_panel_calibration.py -m 'slow and parameter_recovery' -q
```

The ordinary suite contains only a deterministic three-seed, `n_units=120` recovery smoke. For an explicit calibration-harness wiring check that writes a clearly marked smoke-only record to a temporary directory, run:

```sh
TERRINE_CALIBRATION_SMOKE=1 \
TERRINE_CALIBRATION_DIR=/tmp/terrine-panel-calibration-smoke \
uv run pytest tests/parameter_recovery/test_panel_calibration.py \
  -m 'slow and parameter_recovery' \
  -k 'heterogeneous-unequal-overlap-profile_a' -q
```

Smoke mode uses fixed seeds `(1, 2, 7)`, `n_units=120`, skips only the statistical calibration gates, still requires every requested estimate and paired refusal to complete, and writes `run_mode: smoke_only`. If no output directory is supplied, its default `docs/validation/generated/panel_calibration_smoke/` is separate from the full-calibration evidence directory. Do not treat smoke JSON as a full-cell calibration record.

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
