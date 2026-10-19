# Library Hand-offs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Execute inline in the existing handoffs worktree; no additional worktrees or agents.

**Goal:** Deliver safe, optional downstream library hand-offs for roadmap Task 10.1.

**Architecture:** Keep DoubleML, DoWhy and duckreg as executable documentation recipes; keep only lifelines and diff-diff mapping helpers in `terrine/export/`. Every route consumes shared export preflight, preserving exact exclusion reasons. Extend preflight to aggregate warehouse evidence without collecting individual rows for duckreg.

**Tech Stack:** Python 3.13+, Arrow, Ibis/DuckDB, optional DoubleML, DoWhy, duckreg, lifelines and diff-diff.

**Spec:** `docs/superpowers/specs/2026-10-18-terrine-scope-datasets-only.md` (authoritative main version); Task 10.1 in `2026-10-16-terrine-build.md`; `AGENTS.md`.

## Global Constraints

- No effect estimators or causal claims in terrine; no estimate parity, calibration or coverage tests.
- Reuse `terrine.export.{incomplete_outcome,unknown_eligibility,cluster_required,grace_baseline_treatment}` across every entry point. Unknown eligibility and grace treatment cannot be dropped.
- DoWhy accepts only one row per independent unit and no declared weights; extra kwargs do not apply weights or clustering.
- duckreg uses the same Ibis connection and materialized table; only aggregate evidence and compressed statistics leave the warehouse.
- lifelines risk-set conversion must refuse delayed entry and direct users to per-unit records. KM and Cox are distinct contracts.
- diff-diff supports only Callaway–Sant'Anna, Sun–Abraham and their event studies in recipes/tests; never SynthDiD.
- Use one extra per downstream library (`doubleml`, `dowhy`, `duckreg`, `lifelines`, `diff-diff`): consumers avoid unrelated dependencies and DoWhy's Python <3.14 restriction does not limit the other libraries. CI installs all five on Python 3.13. Increment remains its existing optional extra, not a new required recipe.
- Before each implementation commit: format/check `terrine tests examples integration`, `ty check terrine tests`, `tach check`, focused tests. Final: full tests with eight workers, integration examples with extras, every `examples/*.py`.

## Review Focus

1. Timestamp `duration` is an endpoint, not elapsed time: subtract t0 before float conversion; preserve delayed entry on the same clock (Task 2).
2. Binned risk sets cannot recover exact event times or entry times: expose bin-index durations explicitly and refuse delayed entries, multiple pooled groups and incomplete histories (Task 2).
3. Zero/negative periods collide with never-treated encoding: shift integer periods positive without compressing gaps; timestamp periods require declared cadence. Derive timing and validate adoption before outcome exclusions (Task 3).
4. DoWhy must refuse repeated units even with a valid cluster role and refuse every weight role rather than forwarding ignored kwargs (Task 1).
5. Warehouse preflight must not export rows, and dropping must retain exact per-reason counts while filtering the same relation duckreg reads (Task 1).

---

### Task 1: Shared preflight and executable recipes

**Files:** Modify `terrine/export/preflight.py`, `terrine/export/__init__.py`, `pyproject.toml`, `uv.lock`, `.github/workflows/ci.yml`, `tach.toml`; create `integration/handoffs/` fixtures and `test_recipes.py`; extend `tests/export/test_preflight.py`; create `docs/guides/library-handoffs.md`; update README and CHANGELOG.

**Interfaces:** Retain `preflight_export(data: MaterializedFrame, *, drop_incomplete: bool = False, consumer: Literal["dowhy"] | None = None) -> ExportPreflight`. The DoWhy eligibility option uses shared coded refusals, not a constructor wrapper. Add context manager `preflight_warehouse(table: ibis.expr.types.Table, *, roles: Roles, columns: Mapping[str, ColumnMeta], provenance: Provenance, drop_incomplete: bool = False) -> Iterator[WarehousePreflight]`: open a DuckDB snapshot transaction spanning evidence and downstream fit, yield a filtered relation and immutable exclusions, then roll back temporary views on exit. Refuse nesting/unsupported backends with routes forward. Count each excluded row once per exact reason, including shared outcome evidence.

- Step 1: Add failing unit cases for DoWhy repeated units (cluster present), all declared weight roles, preflight precedence, warehouse refuse/drop parity, reason counts and a second connection mutating the source after evidence (the fit population remains pinned). Fixtures use local DuckDB, never a remote warehouse.
- Step 2: Run `uv run pytest -q tests/export/test_preflight.py`; verify new behaviors fail.
- Step 3: Implement shared decisions and warehouse aggregate evidence. Use only aggregate count/reason queries; never `.to_pyarrow()` on individual rows. Keep unsupported-library and shape refusals coded with routes forward.
- Step 4: Add per-library optional extras and one CI job. Write one small integration example each calling `DoubleMLData(..., cluster_cols=...)`, `CausalModel(...).identify_effect()`, and same-connection `DuckRegression(...).fit()`. Only skip when the corresponding package is absent. Docs reproduce these recipes, map role names, disclose explicit local exports and downstream responsibility. DoWhy 0.14 is Python 3.13 only within terrine's supported interpreter range; state that restriction and keep the other extras usable on 3.14.
- Step 5: Run focused tests/integrations and all precommit gates; commit `feat: add preflight-backed library recipes`.

### Task 2: lifelines contracts

**Files:** Create `terrine/export/lifelines.py`, `tests/export/test_lifelines.py`, `integration/handoffs/test_lifelines.py`; modify export facade, risk-set completeness metadata as necessary, guide and CHANGELOG.

**Interfaces:** `to_lifelines_km(data: MaterializedFrame, *, drop_incomplete: bool = False)` returns durations, event_observed, entry and weights plus exclusion counts. `to_lifelines_cox(data: MaterializedFrame, *, drop_incomplete: bool = False)` returns dataframe and duration_col/event_col/entry_col/weights_col/cluster_col plus exclusions. Results are named frozen records. Cox projects declared features plus assigned survival/weight/cluster fields only: no outcome, identity, support columns or automatic treatment predictors. KM risk-set input is one group, no delayed entry; use count weights at bin indices, not expanded rows. Shared preflight recognizes aggregate group/bin grain (not independent-unit repetition). LifeTable aggregation retains per-bin completeness and exact reason lists; count-column metadata points to these. Never drop individual bins to repair an incomplete risk trajectory: use per-unit records. Cox requires per-unit records.

- Step 1: Write failing tests: timestamp subtraction before float conversion with different t0 and delayed entry, weight/cluster mappings, feature-only Cox predictors with outcome/support columns present, incomplete drop counts, preflight-before-shape refusal, real materialized multibin LifeTable conversion with compressed event/censor weights, delayed-entry refusal, incomplete histories and multi-group refusal. Keep censoring distinct from incomplete outcome windows.
- Step 2: Run `uv run pytest -q tests/export/test_lifelines.py`; verify failures.
- Step 3: Implement elapsed seconds `(endpoint_ns - t0_ns) / 1e9`, entry on the same origin, requiring `0 <= entry <= duration`, finite positive declared weights, and exact event flags. This is data conversion, no statistical error model; floating conversion rounds elapsed durations only. Risk-set conversion is explicitly coarsened to `time_bin` (bin units), no claims of exact event-time recovery.
- Step 4: One tiny integration test actually fits KM from records and risk counts, and Cox with role arguments. Update guide with assumptions, optional imports and routes forward.
- Step 5: Run focused tests/integration and all precommit gates; commit `feat: add lifelines dataset handoffs`.

### Task 3: diff-diff first-treatment mapping

**Files:** Create `terrine/export/diff_diff.py`, `tests/export/test_diff_diff.py`, `integration/handoffs/test_diff_diff.py`; modify export facade, guide and CHANGELOG.

**Interfaces:** `to_diff_diff(data: MaterializedFrame, *, period_col: str, period_seconds: int | None = None, drop_incomplete: bool = False)` returns a dataframe, unit/time/first_treat argument names, immutable period mapping and excluded counts. Treatment uses the declared role; derive first persistent adoption and validate reversal from original supplied history, before dropping outcomes. Zero means untreated throughout supplied history, not lifetime never-treated. Shift integer periods by `1 - min(period)` preserving gaps. Timestamp/date periods use the explicit cadence or existing panel period_seconds; require exact alignment and preserve missing ticks. Build encoding before exclusions. No cadence means a coded refusal directing users to declare one.

- Step 1: Write failing tests for unsorted rows, never-treated zero, dates/nonpositive integer periods, original/exclusion-induced gaps retaining distances, excluded first-adoption/all treated rows retaining cohort, reversal hidden by exclusion, duplicate unit-period rows, binary treatment/reversal, output collisions and shared preflight/drop counts. Pin literal first_treat values.
- Step 2: Run `uv run pytest -q tests/export/test_diff_diff.py`; verify failures.
- Step 3: Implement the mapping without changing row order, silently repairing treatment histories or overwriting source columns. Validate supported persistent adoption input and non-null ordered periods; refuse unsupported variants with routes forward.
- Step 4: One tiny integration example calls CallawaySantAnna and SunAbraham, accessing event-study outputs; assert accepted units/periods, not estimates. Document only these methods and event studies.
- Step 5: Run focused tests/integration and all precommit gates; commit `feat: add diff-diff panel handoff`.

### Task 4: Review and final evidence

- Step 1: List branch reviews with `roborev list --repo /Users/kylejcaron/terrine --branch feat/library-handoffs --open --json --limit 20`; wait/show each, inspect findings against actual contracts, fix valid findings with focused regression tests and precommit gates (at most three review cycles). Comment and close handled reviews.
- Step 2: Run `uv run pytest -q -n 8 tests`, `uv run pytest -q integration/handoffs`, and every `examples/*.py` with all relevant extras installed. Record commands, outputs, review dispositions and remaining limitations in final handoff; do not push.

## Review dispositions and scope

- Plan review 8974: findings 1–5 accepted; aggregate risk-set grain/completeness, Cox projection, cadence/history-before-exclusion and snapshot lifetime are specified above.
- Finding 7 declined: the owner's assignment explicitly enumerates every adopted library and contract and calls the scope binding. Increment compatibility and all other roadmap capabilities are background, not new work here.
- Findings 6/8 concern pre-existing withdrawn/historical master tasks. This plan executes only rescoped Task 10.1 against existing implementations, creates no roadmap children and has no estimator prerequisite. It does not authorize reimplementing completed foundation/runtime work or editing unrelated roadmap subsystems.
- Dependency choice revised from a combined extra to per-library extras after inspecting PyPI requirements: DoWhy 0.14 excludes Python 3.14; all-five integration CI is Python 3.13. This confines the restriction to consumers choosing DoWhy.

