# Materialization runtime implementation report

## Completed commits and evidence

### Task 1 — Durable state and dependency graph
- Commit: `815ebc4` (`feat: add materialization runtime state and graph`).
- `uv run pytest tests/runtime/test_state.py tests/runtime/test_graph.py -q`: 4 passed before review fixes.
- Findings reproduced red and fixed in `d7edb98` (`fix: address generation and graph review 8461`): transactionally inserts/supersedes generations, rejects self-supersession, materializes one-shot graph partition iterators. Re-test: 7 passed.
- Task gates `uv run ruff format ...`, `uv run ruff check terrine tests examples`, `uv run ty check terrine tests`, `uv run tach check`: passed.
- Ruling: persist timestamps as ISO-8601 strings because installed DuckDB required absent `pytz` to fetch timezone-aware values; UTC offsets are retained.

### Task 2 — Change evidence and plan serialization
- Commit: `6a82ff9` (`feat: plan source-aware materialization invalidations`).
- `uv run pytest tests/runtime/test_invalidation.py tests/runtime/test_planner.py -q`: 7 passed.
- Review findings reproduced red and fixed in `95f9494` (`fix: address planner review 8463`): unknown-time key-only deletes rebuild conservatively; task IDs hash structured JSON fields; budget limits are retained/serialized/fingerprinted. Re-test: 10 passed.
- Ruff/ty/tach gates passed.
- Focus implemented: half-open support intersection, update/delete support rules, watermark advances not treated as change evidence, stable plan/task serialization, task-count cap, strict unknown-scan refusal.

### Task 3c — Lease fencing and bounded task executor
- Commit: `741e32e` (`feat: execute materialization plans with fencing`).
- Red: `uv run pytest tests/runtime/test_leases.py tests/runtime/test_execute.py -q` initially failed collection because modules were absent.
- Green: same command passed (4 tests), covering lease renewal/fencing, persisted idempotent Arrow task outputs, run retry, cancellation before publish, and task cap before work.
- Ruff/ty/tach gates passed.

### Task 4 — DuckDB publication, crash recovery, and GC
- Commit: `a5f93a4` (`feat: publish DuckDB generations atomically`).
- `uv run pytest tests/runtime/test_duckdb_publication.py tests/runtime/test_gc.py integration/warehouse_execution/test_duckdb_runtime_recovery.py -q`: 9 passed.
- Integration uses spawned independent processes and temporary local DuckDB files, crashing via `os._exit` immediately before and after manifest commit; recovery observed the previous complete generation before commit and the new complete generation after commit.
- Ruff/ty/tach gates passed.
- GC is dry-run by default and removes only old, explicitly superseded, unpinned runtime records and their associated registered DuckDB partition tables.

## Reviews
- 8461 (815ebc4): Fail findings fixed in d7edb98; review commented and closed. Follow-up 8466 on d7edb98: Pass; commented and closed.
- 8463 (6a82ff9): Fail findings fixed in 95f9494; review commented and closed. Follow-up 8467 on 95f9494: Pass; commented and closed.
- 8470 (741e32e) and 8471 (a5f93a4) completed automatic reviews with findings; dispositions are recorded below.

### Review follow-up — Task 3c namespace safety
- Commit: `c52951f` (`fix: validate runtime namespace before SQL`).
- Added a failing injection regression test, validated namespace via `LeaseStore` before SQL interpolation, then `uv run pytest tests/runtime/test_execute.py -q`: 4 passed.
- Required gates passed: `uv run ruff format terrine/runtime/execute.py tests/runtime/test_execute.py`; `uv run ruff check terrine tests examples`; `uv run ty check terrine tests`; `uv run tach check`.
- Review 8470 finding 1 (namespace SQL injection) fixed in c52951f. Findings 2–5 fixed by 8d78a43, a669d8f, e8ebc28 and e3b5da3; review and all follow-up jobs 8484, 8487, 8488 and passing 8489 were commented with `roborev-fix` and closed after the pass.
- Review 8471 finding 1 reproduced and fixed in `532e137` (`fix: protect published generations from GC`): deletion now checks manifest reachability inside its deletion transaction. `uv run pytest tests/runtime/test_gc.py -q`: 3 passed; Ruff/ty/tach gates passed.
- Review 8471 finding 6 reproduced and fixed in `39a8e6c` (`fix: bind publication to dataset lease`): the lease resource must match the staged dataset before publication. `uv run pytest tests/runtime/test_duckdb_publication.py -q`: 6 passed; Ruff/ty/tach gates passed.
- Review 8471 finding 4 reproduced and fixed in `71938ce` (`fix: prevent published generation reactivation`): historical generation identities cannot be reactivated after publication. `uv run pytest tests/runtime/test_duckdb_publication.py -q`: 7 passed; Ruff/ty/tach gates passed.
- Review 8470 is resolved and closed. Review 8471 findings 2, 3 and 5 remain assigned elsewhere; its other dispositions are recorded above.

### Review follow-up — Task 3c executor fencing and budgets
- Commits: `8d78a43` (`fix: fence runtime task attempts and budgets`), `a669d8f` (`fix: fence runtime task completion updates`), `e8ebc28` (`fix: reject stale runtime lease claims`), and `e3b5da3` (`test: cover stale attempt takeover windows`).
- Added failing regressions for stale attempt terminal/task writes, long-task heartbeat renewal, cumulative final-frame row/byte budgeting, reuse of `terrine.budget.*` refusal codes, failed-run retry receipt persistence, and delayed run/task takeover while the newer attempt remains active.
- Heartbeat renews through its own DuckDB cursor/LeaseStore; run and task claims/completions are guarded by monotone attempt fences and the currently active lease; failed runs retry under a fresh fence and no stale update changes terminal receipts.
- Verification: `uv run pytest tests/runtime/test_execute.py -q` (13 passed); `uv run ruff format terrine/runtime/execute.py tests/runtime/test_execute.py`; `uv run ruff check terrine tests examples`; `uv run ty check terrine tests`; `uv run tach check` all passed.
- Review 8470 findings 2–5 and follow-ups 8484, 8487, 8488 were dispositioned with `--commenter roborev-fix`; pass 8489 was recorded; all were closed.

The plan remains incomplete. Task 2 now has source-aware plan task selection, stable fingerprints covering full change support, conservative incomplete-support invalidation, and additional focused acceptance tests. Exact anchor identity, state-panel/Float64 cumulative dependency equivalence and same-snapshot incremental-vs-clean acceptance remain open, as do Tasks 3a/3b, remaining Task 3c concurrency/cancellation publication acceptance, Tasks 5–7, and their public integration surfaces. Findings 2/3/5 of review 8471 were implemented in 9bc57c5; review 8471 and follow-ups 8474/8476 were dispositioned and closed. Timestamp storage migration and compatibility work are committed below. No owner-reserved licensing/adoption decisions were made.
### Review follow-up — 8471 findings 2, 3 and 5

- Findings 2/3/5 have failing regressions in `tests/runtime/test_gc.py` and `tests/runtime/test_duckdb_publication.py`. The implementation serializes pin/delete on a generation `guard_version`, checks pins and manifest within the delete transaction, and uses a resource-guard row written both by lease acquisition and the publication transaction's final fence check. Run-to-publication mapping is stored transactionally by run ID and read by `reconcile`.
- Focused verification: `uv run pytest tests/runtime/test_duckdb_publication.py tests/runtime/test_gc.py -q` (13 passed).
- Timestamp storage is now naive-UTC DuckDB `TIMESTAMP` at microsecond precision (without `pytz`), with legacy ISO/VARCHAR migration for lease expiry and generation creation columns. Commits: `96b9efd` (typed storage), `63f7d57` (legacy schema migration), `d2ae513` (case-insensitive namespace migration). `uv run pytest tests/runtime -q`: 63 passed; required format/ruff/ty/tach gates passed.
- Reviews 8471, 8474, 8476, 8518, 8521 and 8524 were dispositioned and closed. Reviews 8498 (`9bc57c5`), 8519 (`a3d11c3`), 8525 (`d2ae513`), 8526 (`d878d78`) and 8527 (`d24192b`) were Pass and closed.
- Task 2 follow-up: conservative incomplete-support handling, interval coverage, source-level planner selection (including transitive dependents and readiness-only no-op plans), post-filter task caps, complete canonical change-evidence fingerprints, definition fingerprinting, and event-anchor identity rebuild coverage. `uv run pytest tests/runtime/test_invalidation.py tests/runtime/test_planner.py -q`: 23 passed; static gates passed. Exact StatePanel/Float64 cumulative certificate invalidation and full same-snapshot equivalence remain unimplemented and are reported as remaining acceptance.
