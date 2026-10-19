# terrine scope: datasets, not estimates

Owner decision, 2026-10-18. Supersedes the handoff's estimation content (§2 "causal estimates are gated by diagnostics", §12, §16 causal mode, §18 estimator phases) and every plan section that implements it. Where a plan conflicts with this document, this document wins.

## Decision

terrine helps people and agents quickly and safely build well-constructed, point-in-time-correct datasets from source data. It is a standalone package: it must be fully useful with no relationship to increment. It makes no causal claims and ships no effect estimators. Estimation belongs to downstream libraries; terrine hands them correct data with its column roles and provenance.

Reasons: the owner's stated intent; increment already ships observational IPTW/DML/AIPW with clustered variance; mature downstream libraries exist for every estimator terrine was reimplementing.

## Scope ledger

The authoritative scope for terrine's plans. Owner-confirmed 2026-10-18; rows without an owner quote were confirmed by the owner's answer to a direct question that date. A capability not listed here is out of scope until the owner confirms it.

| Capability | Source | Confirmed |
| --- | --- | --- |
| Datasets from source data and semantic definitions: anchors, windows, aggregates, as-of attributes, pre-treatment covariates, post-treatment outcomes, static temporal validation, null-not-zero censoring | owner: "is it easy for users to make a dataset from their semantic layer? sometimes even including pre-treatment covariates and post treatment outcomes?"; "its meant to help users and agents quickly make datasets more safely from source data" | yes |
| Standalone package, usable with no relationship to increment | owner: "we should still be supporting datasets irrelevant of their relationship to increment. this is its own package at the end of the day" | yes |
| Optional increment compatibility adapter | owner: "if needed, make our own semantic layer and longer term increment can become a dependency of this" | yes |
| Scale and ML staging | owner: "will this scale to massive datasets if needed? ml style workflows where they need to stage data?" | yes |
| Training and scoring views: label role with censoring policy, time/unit splits, batch-scoring snapshots | owner-pasted proposal ("Keep training-set and snapshot export"), confirmed 2026-10-18 | yes |
| Incremental materialization runtime | owner: "also consider incremental materialization, etc" | yes |
| Common-stack integration (warehouses via Ibis, dbt, local files) | owner: "and works well with common existing stacks" | yes |
| Orchestrator adapters: Airflow, Dagster, dbt after-build recipe | handoff §14; confirmed 2026-10-18 | yes |
| Agent dataset tools (no causal mode) | owner: "even a tool for AI agents to easily and safely create datasets" | yes |
| Live run telemetry: status, event paging, cancellation, optional OpenTelemetry export | owner asked "are there any opportunities for live telemetry for agents as they use this and this runs?"; confirmed 2026-10-18 | yes |
| State-panel dataset shape | owner: "I dont think we need to add the MOST estimator here right now, but support the dataset builder shape seems useful" | yes |
| Panel dataset shapes: sequential (repeated time zeros), calendar comparison, event panels | handoff §5, §7; confirmed 2026-10-18 | yes |
| Cohort, LTV trajectory and time-to-event dataset shapes | handoff §7; confirmed 2026-10-18 | yes |
| Risk-set (life) table | owner: "for kaplan meier what do you think? we should definitely atleast have the table" | yes |
| Event sequences, funnels, sessions, re-anchoring | handoff §5.2; confirmed 2026-10-18 | yes |
| Nonadditive metrics (exact distinct, min/max, quantiles) and optional approximate sketches | handoff §11; confirmed 2026-10-18 | yes |
| Descriptive dataset reports (attrition, completeness, group summaries, raw SMD) | agent-proposed; owner: "your recs sound good too!" | yes |
| Library hand-offs: export preflight, recipes, lifelines and diff-diff helpers | owner: "potentially integration / contract support for popular packages like dowhy"; "maybe dowhy and diff-diff as well"; "lets start with lighter idea as a starting point" | yes |
| Native causal estimators, inferential diagnostics, estimator calibration, agent causal mode | handoff §2, §12, §16 | no: removed ("we shouldnt be claiming causality, just well constructed datasets") |

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
| DoWhy | Recipe | One-row-per-unit, unweighted frames only. `CausalModel` has no cluster or weight arguments (extra keywords are stored, not applied), so the recipe refuses repeated units and declared weights |
| duckreg | Recipe | Runs on the same Ibis connection against the materialized frame table; only compressed aggregates leave the warehouse, never rows |
| lifelines | Helper | Two contracts. Kaplan–Meier: per-unit time-to-event records map to `KaplanMeierFitter.fit(durations, event_observed, entry=..., weights=...)`. Cox: a dataframe with `duration_col`, `event_col`, `entry_col`, `weights_col`, `cluster_col`. The risk-set table converts to lifelines input only without delayed entry (lifelines' table conversion reads event and censor counts only); with delayed entry, use per-unit records |
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

1. Preserve before removing, at the moment of cutover: tag the head of every branch that contains `terrine/estimate/` as `archive/native-estimation-2026-10-18/<branch>` (one tag per branch; a tag can point to only one commit) and record each SHA in a comment on kata `v0j6`. Commit in-progress estimation work (e.g. `terrine/estimate/sampling.py` in `.worktrees/warehouse-scale`) to a separate `parked/` branch. No history rewrites; removal is a forward commit on the integration branch `feat/dataset-engine` (`.worktrees/build`, per kata `k909`), then propagated to the branches stacked on it.
2. Move noncausal helpers out of `estimate/` first: `to_surrogate_arrow` / `ArrowSurrogate` go to the dataset export module; keep `backtest_survival` only if it is a pure cutoff-split dataset utility.
3. Update every surface in the same change (AGENTS.md "Complete changes"): plans (delete the estimation companion; amend build, study-api, dataset-workflows, warehouse-scale), refusal registries (`terrine.estimate.*` codes replaced by preflight codes), `__all__` facades, examples, guides, CHANGELOG, fixtures, tests.
4. Rescope kata: 10.1 becomes library hand-offs; 12.1 drops causal mode; 8.1 drops step 3 and estimator reductions.
