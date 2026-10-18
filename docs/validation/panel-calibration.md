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

Smoke mode uses fixed seeds `(1, 2, 7)`, `n_units=120`, skips only the statistical calibration gates, still requires every requested estimate and paired refusal to complete, and writes `run_mode: smoke_only`. Do not treat its JSON as a full-cell calibration record. An implementation-owner timing probe ran the same three seeds at `n_units=2000` in 0.38 seconds of test-call time; scaling to 16 cells × 400 replications gives an estimated full-grid runtime of about 14 minutes on that environment. This is an estimate, not a full-grid run.

The full grid is intentionally not run as part of ordinary pytest. Per-cell JSON from full calibration runs is written beneath `docs/validation/generated/panel_calibration/`; a missing JSON record means that full cell has not been run. The committed records and outcomes are added after the full command executes; no calibration results are claimed in this document before then.

## Interpretation limits

Recovery and interval coverage are evidence only for this finite family of simulated laws, sample sizes, nuisance specifications and inference implementation. They do not establish causal identification, parallel trends, universal normality, or power in an applied study. In particular, failure to reject a joint pretrend test is not evidence that untreated trends are parallel; placebo and negative-control diagnostics have their own sample-size and power limits.

Not yet run. The full grid must be executed by the implementation owner before publishing calibration outputs; the command above is intentionally separate from fast smoke verification.
