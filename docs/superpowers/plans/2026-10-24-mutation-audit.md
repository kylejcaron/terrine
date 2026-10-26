# Mutation-Guided Test Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Find, with mutation evidence, which tests on terrine's most expensive modules catch no fault another test does not catch, delete those that have no distinct oracle, and record the faults no test catches.

**Architecture:** A reproducible `make mutate MODULE=… MUTATION_TESTS=…` target runs mutmut on one module against its own test selection, then the test-audit kill-matrix script records which tests kill each mutant and computes a greedy set cover. Candidates outside the cover are reviewed by hand and deleted only when they have no regression link and no distinct oracle; mutation score must be unchanged after deletion.

**Tech Stack:** mutmut 3, pytest, the test-audit skill's `kill_matrix.py` and `test_inventory.py`, `agent-slots` for core budget.

**Spec:** owner request 2026-10-24 ("lets do mutmut and the rest") approving step 4 of the test-audit proposal; global AGENTS testing rules (mutation score is the quality measure; unique-kill gate); the test-audit skill (Phase 2 rules).

## Scope ledger

| Capability | Source | Confirmed | Task |
| --- | --- | --- | --- |
| Mutation runner target and configuration | Owner: "lets do mutmut" | Yes | 1 |
| Kill matrices and evidence-based deletions on the heaviest modules | Same (proposal step 4) | Yes | 2, 3 |
| Per-capability test cost table for a capability-cut decision | Owner: "lets do mutmut and the rest", replying to the proposal whose step 5 read "Capability cuts: your decision, with the per-capability numbers in hand" and the offer "I can list test count and time per capability" | Yes, as a report only | 4 |
| Capability (public API) cuts | Proposal step 5: product decision | No; owner decides from Task 4's table | — |
| New tests for surviving mutants | test-audit: survivors are a to-do list, not part of the audit | No; recorded only | — |

## Divergence — owner-approved scope revision, 2026-10-25

- Finish `terrine/backends/numeric.py` with a full campaign, kill attribution,
  candidate review, and the fresh post-deletion gate if any tests are removed.
- For `terrine/temporal/aggregate.py`, run a seeded random approximately 25%
  sample. Record the seed and every sampled mutant name. Apply the same
  completeness and deletion safeguards within the sample; its score is an
  estimate, not an exact whole-module score. Prefer keeping uncertain candidates.
- Do not start `terrine/compile/columns.py` or the compile shapes. If the first
  two selections show substantial redundancy (more than 15% of selected tests
  outside the cover and lacking a distinct oracle), report that finding and stop
  rather than expand the campaigns.
- Retain the per-capability cost table and final unit/non-live integration
  verification. Target the remaining audit at approximately two to three hours.

## Global Constraints

- Delete only with evidence: a test outside the greedy cover, with no regression/issue link and no distinct oracle (tolerance, error code, parity, cross-path equivalence). Keep the better-specified of interchangeable tests.
- Exclude only reviewed mutations to non-contract message prose (exception/log text). Keep behavioral string mutations (refusal codes, null reasons, output field names, aggregation selectors, parsed literals). Record every exclusion, identically across campaigns.
- After deletions, discard all cached mutant results, refresh mutmut's copied test tree and test discovery, and freshly execute the baseline mutant set: every previously killed, non-excluded mutant must still be killed; restore any test whose removal breaks that.
- Mutation and kill-matrix pytest runs use `-n 0` (mutmut needs in-process collection to associate tests with functions; the repository default is `-n auto`), keep the agent-slots plugin, and bound mutmut's own parallelism to the allocated cores.
- Evidence completeness gates every deletion: a green baseline on the selection, the expected test identities collected, every mutant run completed, and a reproduced failing test for each killed mutant counted in the cover. Crashes, timeouts, missing reports or unattributed mutants are unresolved and block deletions that depend on them.
- Mutation runs are local and on demand, never in the PR gate; wrap with `agent-slots -j <cores> --`.
- Module selection, in order: `terrine/backends/numeric.py`, `terrine/temporal/aggregate.py`, `terrine/compile/columns.py`, then the compile shapes with the most test time (`time_to_event.py`, `state_panel.py`). Each module's test selection is its unit file(s) plus the integration modules that exercise it; record the selection.
- Baseline (main 5fb0218): unit 1,214 items (27 s local), non-live integration 257 items; CI 5 min 40 s.

## Review Focus

1. A mutant killed only by a live-warehouse probe is not counted as killed by the local selection; such probes are never deletion candidates. Task 2.
2. Equivalent mutants (no behavioral change) are marked, not chased. Task 2.
3. A deletion that drops the module's score is reverted. Task 3.
4. Flaky kills (timeouts) are not treated as unique kills. Task 2.
5. The runner leaves no `mutants/` artifacts in the repository. Task 1.

---

### Task 1: Mutation runner

**Files:** `pyproject.toml` (dev dependency `mutmut`, `[tool.mutmut]`), `uv.lock`, `Makefile` (`mutate` target), `.gitignore` (`mutants/`), `AGENTS.md` (command).

- [ ] `make mutate MODULE=terrine/backends/numeric.py MUTATION_TESTS="tests/backends/test_numeric.py integration/backends/test_numeric.py"` runs mutmut on that module only, selects those tests with `-n 0`, and prints `mutmut results --all`. Verify on `numeric.py`: non-empty test-to-function associations, and one known mutant reproduced as killed by a named test in the matrix pass.
- [ ] Commit `test: add an on-demand mutation runner`.

### Task 2: Kill matrices

- [ ] For each module in order: run Task 1's target, then `kill_matrix.py --tests … --module … --pytest-args "-n 0 -p no:randomly"`, keeping the campaign workspace until attribution is done. Check completeness (Global Constraints). Record score, survivors (to-do list), equivalent mutants, exclusions, cover, candidates, tests that killed nothing, unresolved rows, runtime.

### Task 3: Evidence-based deletions

- [ ] Review candidates per module; delete those meeting the Global Constraint; re-run mutation; confirm unchanged score. Commit per module `test: remove tests that catch no distinct fault in <module>`.

### Task 4: Per-capability cost table

- [ ] For each public capability (top-level exports grouped: sketches/approximate metrics, funnels/sessions/event sequences, binned state, life tables, retention, cohorts/LTV, state panels, time-to-event, calendar comparison, sequential panels, core windows/as-of): unit and integration test count and time, source lines, and docs pages. Report only.

### Report

Per module: score before/after, survivors, deletions with reasons; item counts and times before/after; the capability table.
