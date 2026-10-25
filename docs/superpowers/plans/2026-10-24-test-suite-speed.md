# Unit Test Suite Speed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut the unit tier's CI wall time (the `checks` job, 9½ min; 528 s of tests on a 2-CPU runner) without losing any test's ability to catch its bug.

**Architecture:** Three independent levers: fit the harness to the runner and shard the unit tier as integration already is; make slow tests cheap where their cost is incidental (one query per case, whole pipelines for one rule, rebuilt fixtures); remove structural waste the repository rules already forbid. Deleting tests for redundancy needs a mutation run and is out of scope here.

**Tech Stack:** pytest 8, pytest-xdist, pytest-split, DuckDB 1.5.6, Ibis 12, GitHub Actions `ubuntu-latest`.

**Spec:** owner request 2026-10-24 ("lets do it") approving steps 1–3 of the test-audit proposal; `AGENTS.md` § Tests and validation (unit tests under about 2 s alone; composition and unit-boundary tests that cannot meet the budget move to `integration/`, which runs on every push). This repository has no `slow` marker; do not add one.

## Scope ledger

| Capability | Source | Confirmed | Task |
| --- | --- | --- | --- |
| Harness: `-n auto`, unit-tier sharding in CI, `xfail_strict`, `--strict-config` | Owner approval of proposal step 1 | Yes | 1 |
| Rewrite slow tests that are slow by construction; reclassify the rest | Owner approval of step 2 | Yes | 2 |
| Rewrite wording-pinned `match=` tests, read no-assert tests, collapse grids > 12 | Owner approval of step 3 | Yes | 3 |
| Mutation runs and redundancy deletions | Proposal step 4 | No; later | — |
| Capability (public API) cuts | Proposal step 5, owner's product decision | No | — |

## Global Constraints

- Baseline (local M3 Pro, `-n 8`, 2026-10-24, main 61a505a): 1,292 items; 306 s summed test time; 52 s wall; collection 3 s; 89 items over 1 s; half the time in 58 items; `compile` 135 s, `temporal` 71 s, `backends` 47 s. CI `checks` job: 9 min 25 s, of which pytest 528 s.
- No test loses its oracle: a rewritten test asserts the same contract (values, codes, tolerances) and is red-checked with a deliberate one-line break of the code it guards (never `git stash`); record the break and failure per rewritten test.
- No deletions for redundancy. Item count may drop only by grid collapse (one representative per equivalence class kept) or by deleting a test verified to assert no consumer-visible behavior (Task 3, reason recorded). Report unit-tier and combined unit + integration counts separately, since moves change the former but not the latter.
- Tests assert contracts, never message wording (`AGENTS.md`).
- Repository-wide: `make check` must still run the full unit tier locally; CI shards are a CI concern.

## Review Focus

1. A rewritten test that batches cases into one query still fails when a single case regresses (red-check one case, not all). Task 2.
2. Module-scoped fixtures do not leak state between tests under xdist and `pytest-randomly`. Task 2.
3. Sharded unit jobs together run exactly the full collected set (no test lost to sharding). Task 1.
4. `xfail_strict` / `--strict-config` do not silently disable a test. Task 1.
5. A test moved to the integration tier still runs in CI. Task 2.

---

### Task 1: Harness

**Files:** `pyproject.toml` (`[tool.pytest.ini_options]`), `.github/workflows/ci.yml` (`checks` job), `tests/.test_durations` (new, pytest-split), `Makefile` if needed, `AGENTS.md` (commands).

- [ ] Replace `-n 8` with `-n auto`; add `xfail_strict = true` and `--strict-config`; fix anything they surface.
- [ ] Split the CI unit run: keep lint/typecheck in `checks`; add a `unit` matrix of 4 groups using `pytest-split` (`--splits 4 --group N --splitting-algorithm least_duration --durations-path tests/.test_durations`), stored durations from a full local run. Verify the union of the four groups' collected node IDs equals the full collection.
- [ ] Measure: CI wall time of the slowest unit shard and of the whole workflow. Commit `ci: shard the unit tier and size workers to the runner`.

### Task 2: Slow tests

**Files:** the 89 tests over 1 s, starting with `tests/backends/test_numeric.py` (47 s), `tests/compile/test_time_to_event.py` (42 s), `tests/temporal/test_window_aggregates.py` (32 s), `tests/temporal/test_retention.py` (28 s), `tests/compile/test_columns.py` (19 s), `tests/compile/test_state_panel.py` (18 s).

- [ ] For each, classify the cost: (a) one compile/query per case → evaluate all cases in one table/query; (b) a full Design pipeline for one narrow rule → test the rule at its owning function; (c) setup rebuilt per test → module-scoped fixture; (d) genuinely composition-level, or a unit-boundary test that cannot meet the budget → move to the matching `integration/` module. For each move, bring its fixture chain along (e.g. `native_inputs` lives in `tests/conftest.py`, which `integration/` does not inherit), keep or update existing helper imports (e.g. `integration/state_panels/test_state_panel_workflows.py` imports from `tests`), and run the destination module on its own.
- [ ] Rewrite, keeping each test's assertions and oracle; red-check each rewritten test. Commit per file group (`test: …`).
- [ ] Re-measure with `--durations=0 --durations-min=0` at the same worker count as the baseline (and `-n 0` alone for any remaining overrun, to rule out contention). Target: no unit item over 2 s; record any remaining 1–2 s items with their reason. Refresh `integration/.test_durations` for moved tests.

### Task 3: Structural cleanup

**Files:** the 44 `match=` sites, 14 no-assert tests and 2 grids > 12 listed by the inventory (`/tmp/inv.json` regenerated on the branch).

- [ ] Replace `match=` that pins message wording with the stable code/context assertion. Where `match=` actually identifies *which* failure occurred (e.g. an injected I/O error in `tests/dataset/test_recipe.py`), keep that identity by asserting the injected exception object, its type and structured attributes, or `__cause__`, plus the observable postcondition (e.g. temporary file removed); red-check each replacement with a deliberate break.
- [ ] Read each no-assert test: import-isolation and "asserted in a helper" tests stay (note why); give the rest an assertion, or delete one that verifiably asserts nothing consumer-visible and record the reason.
- [ ] Collapse the two large grids to boundary cases (plus a Hypothesis property where values vary along one path).
- [ ] Commit `test: assert codes instead of wording and collapse value grids`.

### Report

Before/after for every baseline number; list of rewrites with their red-check evidence; tests moved between tiers; CI timings.
