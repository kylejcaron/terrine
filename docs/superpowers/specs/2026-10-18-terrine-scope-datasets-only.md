# terrine scope: datasets, not estimates

Owner decision, 2026-10-18. Supersedes the handoff's estimation content (§2 "causal estimates are gated by diagnostics", §12, §16 causal mode, §18 estimator phases) and every plan section that implements it. Where a plan conflicts with this document, this document wins.

## Decision

terrine helps people and agents quickly and safely build well-constructed, point-in-time-correct datasets from source data. It is a standalone package: it must be fully useful with no relationship to increment. It makes no causal claims and ships no effect estimators. Estimation belongs to downstream libraries; terrine hands them correct data with its column roles and provenance.

Reasons: the owner's stated intent; increment already ships observational IPTW/DML/AIPW with clustered variance; mature downstream libraries exist for every estimator terrine was reimplementing.

## In scope (unchanged unless noted)

- Anchors, windows, aggregates, as-of attributes, expansion, all frame shapes (cross-section, sequential/scheduled, calendar and event panels, cohorts, LTV, time-to-event, state panels, sequences), static temporal validation, null-not-zero censoring, coverage/readiness, materialization runtime, ML staging, orchestrators, dataset agent tools.
- Dataset semantics stay: `treatment`, anchor, trial and cluster roles; `Schedule` (described as repeated time zeros, not as target-trial emulation); grace windows (descriptive).
- **Export preflight (new, shared by every hand-off).** Before any library hand-off: incomplete outcome windows are refused or dropped with excluded counts per exact reason (never zero-filled or imputed); unknown eligibility is refused; repeated rows per independent unit require a cluster column; grace-defined treatment is refused as a baseline treatment. Move these checks out of `terrine/estimate/_data.py` and `panel.py` rather than rewriting them. One refusal code per hazard, shared across entry points.
- **Descriptive reports.** Eligibility/attrition, completeness and null reasons, censoring share, group sizes, means and variances, raw standardized mean differences, common-support summaries of observed covariates. Computed from centered, mergeable moments in the warehouse. No p-values, intervals, pass/fail causal gates or propensity models.
- **Risk-set (life) table shape.** Per group and time bin: number at risk, events, censored by reason, delayed entries. Counts and plain rates only; no survival-curve product (it assumes independent censoring) and no intervals. Curves come from lifelines.
- **Simulator.** Keep raw-source worlds, packaged `baseline.yaml` and reference builders. The oracle is the expected dataset rows (anchors, window values, null reasons, censoring), not a treatment effect.

Rule of thumb: terrine outputs counts, plain rates and exact dataset values. Anything that needs a statistical model, assumption or inference happens downstream.

## Integrations (light first)

Each integration maps frame roles to the library's arguments on top of the export preflight. Start as documentation recipes; write a helper only where the mapping has a real gotcha.

| Library | Form | Notes |
| --- | --- | --- |
| DoubleML | Recipe | `DoubleMLData(..., cluster_cols=...)` for repeated rows |
| DoWhy | Recipe | `CausalModel` has no cluster or weight arguments: one-row-per-unit frames only |
| duckreg | Recipe | Runs on the same Ibis connection against the materialized frame table; no row export |
| lifelines | Helper | Converts censoring, delayed entry and the risk-set table into `duration_col` / `event_col` / `entry_col` / weights |
| diff-diff | Helper | Derives `first_treat` (including never-treated encoding per diff-diff's convention). Document and test only Callaway–Sant'Anna, Sun–Abraham and event studies; never SynthDiD (owner guardrail) |
| sklearn / Arrow | Existing ML staging | Labeled datasets, not causal hand-offs |
| increment | Optional recipe | One integration among others; never a requirement |

Tests: preflight refusals as unit tests; per library, one tiny integration-tier example that actually calls the library. No estimate parity, calibration or coverage tests.

## Out of scope (remove)

- `terrine/estimate/`: IPW, AIPW, regression adjustment, nuisance models, folds, inference, panel AIPW, stacked DiD, native Kaplan–Meier, streaming estimation, nuisance sampling.
- `terrine/diagnostics/` inferential parts: pretrends, placebo, negative-control tests, propensity-based overlap. Keep only what becomes a descriptive report.
- Potential-outcome/ATE truth in `terrine/simulate/`; estimator parameter-recovery and calibration suites.
- Agent causal mode (Task 12.1 keeps dataset tools only); Task 10.1 sensitivity analysis; the estimation companion plan; Task 8.1 step 3 (streaming estimation, nuisance sampling) and estimator reductions in step 4.

## Cutover

1. Preserve before removing: tag the current heads of the branches that carry estimation code (`archive/native-estimation-2026-10-18`) and commit in-progress estimation work to a `parked/` branch. No history rewrites; removal is a forward commit.
2. Move noncausal helpers out of `estimate/` first: `to_surrogate_arrow` / `ArrowSurrogate` go to the dataset export module; keep `backtest_survival` only if it is a pure cutoff-split dataset utility.
3. Update every surface in the same change (AGENTS.md "Complete changes"): plans (delete the estimation companion; amend build, study-api, dataset-workflows, warehouse-scale), refusal registries (`terrine.estimate.*` codes replaced by preflight codes), `__all__` facades, examples, guides, CHANGELOG, fixtures, tests.
4. Rescope kata: 10.1 becomes library hand-offs; 12.1 drops causal mode; 8.1 drops step 3 and estimator reductions.
