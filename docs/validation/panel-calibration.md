# Sequential-panel calibration record

## Predeclared design

The panel calibration is a 16-cell factorial design (2 × 2 × 2 × 2), with 400 fixed seeds (`0..399`) and `n_units=2000` in every cell. Factors are: zero versus heterogeneous cohort effects; equal versus unequal trial aggregation masses; disjoint versus overlapping repeated-trial units; and two trial-varying assignment-rate/baseline profiles. Each cell has a paired contamination/anticipation refusal case. Refusal cases are expected to produce their predeclared stable Terrine refusal code; they are not dropped into the estimation denominator.

Every clean cell requires all 400 replications to complete successfully. Evidence records attempted, completed and refused counts, refusal codes/seeds, bias, empirical standard deviation, mean standard error, RMSE, coverage count, median interval width, diagnostic statuses, runtime/environment and exact seed list. A failure remains in the cell accounting. No calibration output is inferred from a planted-effect smoke test.

## Gates and execution

Full-grid command:

```sh
uv run pytest tests/parameter_recovery/test_panel_calibration.py -m 'slow and parameter_recovery' -q
```

The ordinary suite contains only a deterministic three-seed, `n_units=120` recovery smoke. The full grid is intentionally not run as part of ordinary pytest. Per-cell JSON is written only after its cell has actually run beneath `docs/validation/generated/panel_calibration/`; a missing JSON record means that cell has not been run. The committed records and outcomes are added after the full command executes; no calibration results are claimed in this document before then.

## Interpretation limits

Recovery and interval coverage are evidence only for this finite family of simulated laws, sample sizes, nuisance specifications and inference implementation. They do not establish causal identification, parallel trends, universal normality, or power in an applied study. In particular, failure to reject a joint pretrend test is not evidence that untreated trends are parallel; placebo and negative-control diagnostics have their own sample-size and power limits.

Not yet run. The full grid must be executed by the implementation owner before publishing calibration outputs; the command above is intentionally separate from fast smoke verification.
