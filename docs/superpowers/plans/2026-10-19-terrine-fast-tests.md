# Fast Test Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut the ordinary unit suite (`uv run pytest -q -n 8 tests`) from 384–632 s to about 90 s without losing any consumer-visible behavior it protects.

**Architecture:** First remove the parallel-run slowdown, so measurements are trustworthy. Then re-tier every slow test by what it actually proves:
- composition across subsystems moves to `integration/`;
- narrow rules are re-asserted at the function that owns them, with tiny inputs;
- true duplicates are deleted;
- oversized parameter sets keep a few fast representatives.

A coverage ledger maps every removed or moved test to the test that now carries its assertion.

**Tech Stack:** pytest, pytest-xdist, DuckDB, Ibis, PyArrow.

**Spec:** `AGENTS.md` "Tests and validation"; inventory `docs/superpowers/plans/2026-10-19-terrine-fast-tests-inventory.md`; baseline durations recorded in kata `sxmz`.

## Global Constraints

- Every behavior a removed test asserted must still be asserted by a named test in some collected tier. No silent coverage loss.
- Fast tier: no test phase above 2.0 s in an uncontended single-worker run; full run at most 90 s wall at `-n 8` on the 12-core dev machine (median of 3).
- Integration tier (`uv run pytest -q -n 4 integration`) may grow, but stays at most 4 min wall.
- Tests assert consumer-visible behavior: stable refusal codes, values, metadata, invariants. No assertions on source text, wiring, or mocks echoing their own input.
- Deterministic: no sleeps, no wall-clock assertions (shared `tests/_timing.py` hang guards stay).
- xdist-safe: no shared mutable state between tests. Session fixtures may share only immutable inputs (Arrow tables, catalog specs).
- CI must still collect every moved test. `.github/workflows/ci.yml` selectors are updated in the same commit as each move.
- Only test files, conftest/fixtures, CI and the ledger change. Library code changes only if a rewrite reveals a function that can't be called at its owning level. Record that as a divergence first.

## Review Focus

1. **A rewrite that asserts a weaker property.** The new test checks a helper's return value instead of the consumer-visible result the original pinned (refusal code, metadata on the published table). Each rewrite step requires a red check: break the rule and the new test must fail.
2. **A "duplicate" deleted although its integration twin asserts something different.** Each delete requires citing the twin's exact assertion lines in the ledger.
3. **Moved tests that CI no longer collects.** For example, an integration file not in any job, or a test needing an optional extra that only a non-extra job runs.
4. **Session fixtures leaking state across xdist workers.** A shared DuckDB connection or a shared destination path.
5. **Removing the parallel slowdown by hiding work.** For example, skipping lease renewal or snapshot copies in tests, rather than giving each worker its own resources.

---

### Task 1: Find and remove the parallel-run slowdown

The top tests run about 6.7x slower inside the `-n 8` suite than alone. For example, `tests/dataset/test_export.py::test_parquet_export_preserves_pinned_arrow_schema_and_refuses_overwrite` takes 12.6 s alone and 84 s in the suite. Limiting DuckDB to `threads=1` made it worse (dataset directory 111 s vs 86 s). Bare DuckDB, fsync and test collection are not the cause.

Execution ruling (2026-10-19): the 84 s outlier came from concurrent background
runs, not the suite alone. The diagnosis found CPU contention rather than a
shared wait. Task 1 acceptance is therefore: cause identified and recorded;
per-worker resource isolation done; default worker count and DuckDB thread
setting chosen from the measured curve by best total wall time, not a per-test
ratio. Apply a test-only resource setting only when it measurably improves
full-suite wall time. Record the observed ratio without gating on it. Task 7's
90 s full-suite gate is unchanged.

**Files:**
- Modify: `tests/runtime/test_control_recovery.py:39,52` (fixed `/tmp/terrine-recovery-test.sock` → `tmp_path`-scoped or pid+uuid path)
- Modify: other offenders found in Step 2
- Create: `docs/superpowers/plans/2026-10-19-terrine-fast-tests-ledger.md` (findings section)

- [ ] **Step 1: Measure the worker-count curve.** Run `uv run pytest -q -p no:warnings tests/dataset tests/test_common_stack_interop.py -n {1,4,8,12}` under `/usr/bin/time -p`. Record wall, user and sys for each. If user+sys stays flat while wall grows, the tests are waiting (locks or I/O). If user+sys grows with workers, they are competing for CPU.
- [ ] **Step 2: Profile one top test under load.** Run the full `-n 8` suite. In parallel, profile `test_parquet_export_preserves_pinned_arrow_schema_and_refuses_overwrite` with `uv run python -m cProfile -o /tmp/loaded.prof -m pytest -p no:xdist <test>`, then compare the top `tottime` entries with the standalone profile. Name the dominant extra cost with file:line: a lock wait, a shared path, DuckDB checkpointing, or the Ibis compile.
- [ ] **Step 3: Fix the cause.** Give each worker its own resources (paths, sockets, lease names, DuckDB temp directories via `tmp_path`/`worker_id`). If the cause is CPU oversubscription, set the default worker count in `pyproject.toml` `addopts` to the measured best (`-n auto` or a fixed number). Fix the fixed socket path.
- [ ] **Step 4: Verify.** Record the cause, resource isolation, selected worker/thread counts, full-suite wall before and after, and the observed loaded/standalone test-time ratio in the ledger. Acceptance is the execution ruling above, not a per-test timing ratio.
- [ ] **Step 5: Commit** `test: isolate per-worker test resources`.

### Task 2: Shared immutable fixtures and the ledger format

**Files:**
- Create: `tests/_worlds.py` — session-scoped builders for immutable inputs only:
  - `events_world() -> WorldInputs`, a frozen dataclass of `pa.Table`s plus a `Catalog`, replacing repeated setup in `tests/dataset/test_staging.py::_durable_frame`, `tests/dataset/test_export.py::_staged_dataset`, `tests/dataset/test_winsorization_recipe.py::model/training` and `tests/test_common_stack_interop.py::common_frame`;
  - `bind_model(world, tmp_path) -> Model`, which builds a fresh DuckDB per test from the shared tables.
- Modify: `tests/conftest.py` — register `events_world` as a `scope="session"` fixture.
- Modify: the ledger file — one row per inventoried test: `test id | disposition (DELETE/MOVE/REWRITE/SLIM/KEEP) | carrier test id | red-check evidence`.

- [ ] **Step 1:** Add `tests/_worlds.py` and convert `_durable_frame`, `_staged_dataset` and the winsorization `model` fixture to it. Behavior is unchanged.
- [ ] **Step 2:** Run `uv run pytest -q tests/dataset tests/test_common_stack_interop.py`. Same pass count as before.
- [ ] **Step 3:** Commit `test: share immutable dataset worlds across tests`.

### Task 3: Delete verified duplicates (10 tests, about 390 worker-s)

Candidates are the `DELETE` rows in the inventory, for example:
- `tests/test_common_stack_interop.py::test_dataset_batches_and_parquet_preserve_typed_metadata[typed_rows]` duplicates `integration/warehouse_execution/test_backend_capabilities.py::test_duckdb_typed_dataset_arrow_parquet_and_generation_parity`;
- `tests/runtime/test_incremental_equivalence.py::...[insert|update|delete|late_arrival|roster_exit|readiness_advance]` duplicate `integration/warehouse_execution/test_incremental_equivalence.py::test_saved_runtime_correction_matches_clean_rebuild_of_new_generation`;
- `tests/dataset/test_export.py::test_export_discards_unfinished_shard_and_rewrites_on_retry` duplicates `integration/warehouse_execution/test_ml_staging_duckdb.py::test_export_recovers_durable_shard_and_discards_interrupted_shard`.

- [ ] **Step 1:** For each candidate, put the original's assertions side by side with the twin's and cite both sets of line ranges in the ledger. If the twin is missing any assertion, re-classify the test as MOVE (Task 4) or add the missing assertion to the twin.
- [ ] **Step 2:** Delete the confirmed duplicates. Run `uv run pytest -q -n 4 integration` and confirm the twins pass.
- [ ] **Step 3:** Commit `test: drop fast-tier duplicates of integration coverage`.

### Task 4: Move composition tests to integration (52 tests, about 1500 worker-s)

These are the `KEEP-E2E` rows. Group them by the target file the inventory names:
- `integration/warehouse_execution/test_ml_staging_duckdb.py`: export durability, refresh, runtime staging, scoring snapshot;
- `integration/warehouse_execution/test_saved_plan_restart.py`: persisted-layer provenance from `tests/runtime/test_layers.py`;
- `integration/warehouse_execution/test_incremental_equivalence.py`: ratio, event-anchor, float64 and nested-selector cases;
- `integration/agent/test_dataset_tool_runtime.py`: `tests/agent/test_materialize.py` pipeline cases;
- `integration/warehouse_execution/test_ml_staging_examples.py`: `tests/test_documentation.py::test_readme_builds_and_reopens_first_dataset`.

- [ ] **Step 1:** Move the tests, one target file per commit. Merge parameter sets that now duplicate an existing test in that file, so there is one test per behavior. Update the ledger.
- [ ] **Step 2:** Any moved test that needs an optional extra goes into the CI job that installs that extra. Run `uv run pytest -q --collect-only integration` and confirm every moved test id is collected.
- [ ] **Step 3:** Run `uv run pytest -q -n 4 integration`. All pass.
- [ ] **Step 4:** Commit per target, e.g. `test: run staging composition checks in the integration tier`.

### Task 5: Rewrite narrow rules at their owning functions (161 tests, about 1000 worker-s)

These are the `REWRITE` rows. Work one area per commit, in this order (largest seconds first):
1. **dataset:** export metadata and overwrite refusal (`terrine.dataset.export.export_parquet` on a tiny `pa.Table`), winsorization fit/refusals, splits, training/scoring view hashes, staging metadata ownership;
2. **interop:** `MaterializedFrame` Arrow, pandas and Polars fidelity on tiny typed and empty tables;
3. **compile:** cohorts, LTV, time-to-event, retention, panels, persisted layers, on an in-memory DuckDB with fewer than 10 rows;
4. **agent:** preview and budget refusals at `terrine.agent.tools.DatasetTools` with a stub pinned reader;
5. **runtime and execution:** api, cli, backend capabilities.

For every test:
- [ ] **Step 1:** Write the replacement at the owning function. Assert the same consumer-visible outcome: the same refusal `code`, the same values, the same `ColumnMeta`/`Provenance` fields.
- [ ] **Step 2: Red check.** Temporarily break the rule in production code (e.g. remove the overwrite refusal) and confirm the replacement fails. Revert, and record "red: <how>" in the ledger.
- [ ] **Step 3:** Delete the original. Its row in the ledger now names the carrier test.
- [ ] **Step 4:** Run the area's tests with `--durations=0 --durations-min=2.0` uncontended. Nothing above 2.0 s.
- [ ] **Step 5:** Commit per area, e.g. `test: check export metadata and overwrite rules directly`.

If a test can't be rewritten at a lower level without library changes, re-classify it as MOVE (Task 4) and note why in the ledger.

### Task 6: Slim oversized parameter sets (7 tests)

These are the `SLIM` rows (`tests/runtime/test_incremental_equivalence.py` `definition_change`, `dimension_correction`, `readiness_retraction`, and others in the inventory).

- [ ] **Step 1:** Keep at most 3 representative cases in the fast tier, chosen to cover different code paths. Mark the full set `@pytest.mark.slow` and make sure the CI slow step selects that file.
- [ ] **Step 2:** Run `uv run pytest -q <file>` and `uv run pytest -q <file> -m slow`. Both pass.
- [ ] **Step 3:** Commit `test: keep representative incremental cases in the fast tier`.

### Task 7: Guard and verify

**Files:**
- Modify: `pyproject.toml` `addopts` (`--durations=25`; the worker default from Task 1)
- Modify: `.github/workflows/ci.yml` (fast job reports durations; slow and integration selectors cover everything moved)
- Modify: the ledger (final timings)

- [ ] **Step 1:** Run `uv run pytest -q -n 8 tests` three times. The median wall time must be at most 90 s.
- [ ] **Step 2:** Run uncontended `uv run pytest -q -p no:xdist tests --durations=0 --durations-min=2.0`. No entries.
- [ ] **Step 3:** Run `uv run pytest -q -n 4 integration` (at most 4 min), `uv run pytest -q tests -m slow`, and the optional-extra CI selectors. All pass.
- [ ] **Step 4:** The ledger has a disposition and carrier for all 230 inventoried tests, with no empty carrier except for verified deletes that cite their twin.
- [ ] **Step 5:** Commit `ci: report test durations and keep the fast tier fast`.

## Execution notes

- Tasks 1 and 2 run first, in order. Task 1 makes timings trustworthy; Task 2 provides the shared fixtures.
- Tasks 3–6 can run in parallel by directory (dataset, compile, agent, runtime, interop), each in its own commits. One integrator owns `tests/conftest.py`, `.github/workflows/ci.yml` and the ledger.
- One whole-branch review at the end, against the Review Focus list and the ledger.
