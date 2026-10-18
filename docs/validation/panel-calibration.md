# Sequential-panel calibration record

## Predeclared design

The panel calibration is a 16-cell factorial design (2 × 2 × 2 × 2), with 400 fixed seeds (`0..399`) and `n_units=2000` in every cell. Factors are: zero versus heterogeneous cohort effects; equal versus unequal trial aggregation masses; disjoint versus overlapping repeated-trial units; and two trial-varying assignment-rate/baseline profiles. Each replication also runs a fixed contaminated-reference case whose expected result is the stable `terrine.estimate.stacked_input` refusal with reason `outcome_window_contaminates_reference`, not an estimate.

The calibration harness also predeclares eight stochastic stacked cohort-time
(CATT) recovery cells: heterogeneous cohort effects `(1.0, 2.0)` crossed with
equal/unequal cohort aggregation weights, disjoint/overlapping unit populations,
and two outcome trend/noise profiles. Each CATT cell uses the same 400 seeds
(`0..399`) and `n_units=2000`; every seed must recover both cohort-specific
effects and normalized aggregate target. CATT bias, 95% exact-binomial coverage,
and studentization gates are Bonferroni allocated across all three targets in
all eight cells. Full-run evidence is one `stacked-<cell>.json` record per CATT
cell. The CATT per-gate alpha is `0.01/(3*8*3)`.

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

The marked full calibration emits separate CATT JSON records alongside the 16 AIPW records.

The full grid is intentionally not run as part of ordinary pytest. Per-cell JSON from full calibration runs is written beneath `docs/validation/generated/panel_calibration/`.

## Executed results

Command: `uv run pytest -n 8 tests/parameter_recovery/test_panel_calibration.py -m 'slow and parameter_recovery'`
(the full-grid command above, parallelized over 8 xdist workers), run after the
final stacked-estimator fixes. Outcome: **24 passed** (16 repeated-trial AIPW
cells and 8 stacked CATT cells) in 820 s wall time. There were no reruns, seed
changes or gate changes. The environment was Python 3.13.5, NumPy 2.5.4,
SciPy 1.18.1 and macOS 15.7.4 arm64. Committed per-cell JSON evidence is in
`docs/validation/generated/panel_calibration/`.

### Repeated-trial AIPW cells

Every cell completed 400 of 400 estimates. Every paired contaminated-reference
case raised the expected `terrine.estimate.stacked_input` refusal with reason
`outcome_window_contaminates_reference`. Bias, exact-binomial coverage and
chi-square studentization gates all passed at the predeclared family level of
0.01. An earlier run of the same 16 cells, before the stacked fixes, produced
identical values.

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

### Stacked CATT cells (targets: cohort 1, cohort 2, normalized aggregate)

Every cell completed 400 of 400 replications with no refusals. All bias,
coverage and studentization gates passed at per-gate alpha
`0.01/(3·8·3) ≈ 1.39e-4`; the chi-square acceptance range is
`[301.1, 516.9]`.

| Cell | Target | Truth | Attempted / completed / refused | Mean error | Empirical SD | Mean SE | 95% CI coverage (exact binomial p) | Σ studentized² |
|---|---|---|---|---|---|---|---|---|
| `stacked-equal-disjoint-profile_a` | cohort 1 | 1.0 | 400/400/0 | +0.0042 | 0.0925 | 0.0907 | 376/400 (p=0.357) | 415.9 |
| `stacked-equal-disjoint-profile_a` | cohort 2 | 2.0 | 400/400/0 | −0.0034 | 0.0915 | 0.0906 | 381/400 (p=0.909) | 408.4 |
| `stacked-equal-disjoint-profile_a` | aggregate | 1.5 | 400/400/0 | +0.0004 | 0.0635 | 0.0641 | 379/400 (p=0.818) | 391.1 |
| `stacked-equal-disjoint-profile_b` | cohort 1 | 1.0 | 400/400/0 | +0.0086 | 0.1453 | 0.1420 | 376/400 (p=0.357) | 418.6 |
| `stacked-equal-disjoint-profile_b` | cohort 2 | 2.0 | 400/400/0 | −0.0061 | 0.1450 | 0.1420 | 382/400 (p=0.731) | 417.9 |
| `stacked-equal-disjoint-profile_b` | aggregate | 1.5 | 400/400/0 | +0.0013 | 0.1009 | 0.1004 | 377/400 (p=0.490) | 402.0 |
| `stacked-equal-overlap-profile_a` | cohort 1 | 1.0 | 400/400/0 | +0.0056 | 0.0791 | 0.0785 | 377/400 (p=0.490) | 407.6 |
| `stacked-equal-overlap-profile_a` | cohort 2 | 2.0 | 400/400/0 | −0.0000 | 0.0821 | 0.0786 | 375/400 (p=0.250) | 435.1 |
| `stacked-equal-overlap-profile_a` | aggregate | 1.5 | 400/400/0 | +0.0028 | 0.0598 | 0.0558 | 368/400 (p=0.011) | 457.9 |
| `stacked-equal-overlap-profile_b` | cohort 1 | 1.0 | 400/400/0 | +0.0111 | 0.1239 | 0.1228 | 379/400 (p=0.818) | 409.3 |
| `stacked-equal-overlap-profile_b` | cohort 2 | 2.0 | 400/400/0 | +0.0007 | 0.1292 | 0.1231 | 373/400 (p=0.108) | 439.0 |
| `stacked-equal-overlap-profile_b` | aggregate | 1.5 | 400/400/0 | +0.0059 | 0.0948 | 0.0886 | 373/400 (p=0.108) | 456.8 |
| `stacked-unequal-disjoint-profile_a` | cohort 1 | 1.0 | 400/400/0 | +0.0042 | 0.0925 | 0.0907 | 376/400 (p=0.357) | 415.9 |
| `stacked-unequal-disjoint-profile_a` | cohort 2 | 2.0 | 400/400/0 | −0.0034 | 0.0915 | 0.0906 | 381/400 (p=0.909) | 408.4 |
| `stacked-unequal-disjoint-profile_a` | aggregate | 1.75 | 400/400/0 | −0.0015 | 0.0714 | 0.0717 | 383/400 (p=0.567) | 397.2 |
| `stacked-unequal-disjoint-profile_b` | cohort 1 | 1.0 | 400/400/0 | +0.0086 | 0.1453 | 0.1420 | 376/400 (p=0.357) | 418.6 |
| `stacked-unequal-disjoint-profile_b` | cohort 2 | 2.0 | 400/400/0 | −0.0061 | 0.1450 | 0.1420 | 382/400 (p=0.731) | 417.9 |
| `stacked-unequal-disjoint-profile_b` | aggregate | 1.75 | 400/400/0 | −0.0024 | 0.1135 | 0.1123 | 377/400 (p=0.490) | 408.8 |
| `stacked-unequal-overlap-profile_a` | cohort 1 | 1.0 | 400/400/0 | +0.0056 | 0.0791 | 0.0785 | 377/400 (p=0.490) | 407.6 |
| `stacked-unequal-overlap-profile_a` | cohort 2 | 2.0 | 400/400/0 | −0.0000 | 0.0821 | 0.0786 | 375/400 (p=0.250) | 435.1 |
| `stacked-unequal-overlap-profile_a` | aggregate | 1.75 | 400/400/0 | +0.0014 | 0.0666 | 0.0623 | 373/400 (p=0.108) | 454.0 |
| `stacked-unequal-overlap-profile_b` | cohort 1 | 1.0 | 400/400/0 | +0.0111 | 0.1239 | 0.1228 | 379/400 (p=0.818) | 409.3 |
| `stacked-unequal-overlap-profile_b` | cohort 2 | 2.0 | 400/400/0 | +0.0007 | 0.1292 | 0.1231 | 373/400 (p=0.108) | 439.0 |
| `stacked-unequal-overlap-profile_b` | aggregate | 1.75 | 400/400/0 | +0.0033 | 0.1052 | 0.0984 | 371/400 (p=0.050) | 455.6 |

Equal- and unequal-mass cells share seeds, so their cohort-specific rows are
identical and only the aggregate target changes. In the overlapping-population
cells, aggregate standard errors run about 6–7% below the empirical SD, with
studentized sums near 455 and the lowest coverage at 368/400. These cells pass
the predeclared gates, but intervals for overlapping-unit aggregates are
modestly anti-conservative in this finite design.

## Interpretation limits

Recovery and interval coverage are evidence only for this finite family of simulated laws, sample sizes, nuisance specifications and inference implementation. They do not establish causal identification, parallel trends, universal normality, or power in an applied study. In particular, failure to reject a joint pretrend test is not evidence that untreated trends are parallel; placebo and negative-control diagnostics have their own sample-size and power limits.
