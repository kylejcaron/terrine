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
- 8470 (741e32e) and 8471 (a5f93a4): automatic reviews were still running at handoff; disposition pending.

### Review follow-up — Task 3c namespace safety
- Commit: `c52951f` (`fix: validate runtime namespace before SQL`).
- Added a failing injection regression test, validated namespace via `LeaseStore` before SQL interpolation, then `uv run pytest tests/runtime/test_execute.py -q`: 4 passed.
- Required gates passed: `uv run ruff format terrine/runtime/execute.py tests/runtime/test_execute.py`; `uv run ruff check terrine tests examples`; `uv run ty check terrine tests`; `uv run tach check`.
- Review 8470 finding 1 (namespace SQL injection) fixed and noted in `roborev-fix` comment; job closed. Findings 2–5 (stale attempt writes, no heartbeat during long-running task, cumulative output limits, coded budget refusals) are not addressed.
- Review 8471 finding 1 reproduced and fixed in `532e137` (`fix: protect published generations from GC`): deletion now checks manifest reachability inside its deletion transaction. `uv run pytest tests/runtime/test_gc.py -q`: 3 passed; Ruff/ty/tach gates passed.
- Review 8471 finding 6 reproduced and fixed in `39a8e6c` (`fix: bind publication to dataset lease`): the lease resource must match the staged dataset before publication. `uv run pytest tests/runtime/test_duckdb_publication.py -q`: 6 passed; Ruff/ty/tach gates passed.
- Review 8471 finding 4 reproduced and fixed in `71938ce` (`fix: prevent published generation reactivation`): historical generation identities cannot be reactivated after publication. `uv run pytest tests/runtime/test_duckdb_publication.py -q`: 7 passed; Ruff/ty/tach gates passed.
- Reviews 8470 and 8471 are not complete: 8470 findings 2–5 and 8471 findings 2, 3 and 5 remain actionable. These review jobs remain open / not yet given pass dispositions.

## Incomplete plan work
The plan remains incomplete. Task 2 invalidation breadth/planner integration, Tasks 3a and 3b, the remaining Task 3c concurrency/cancellation publication acceptance, Task 5 status/events/cancel and IPC, Task 6 public API/CLI/replay/example/docs/telemetry, and Task 7 same-snapshot incremental-vs-clean equivalence are unfinished. Review findings 8470 (2–5) and 8471 (2, 3, 5) also remain open. Existing commits cover Tasks 1, partial Task 2, Task 3c baseline, Task 4 baseline and follow-up fixes; previously reported focused checks/crash-boundary test remain the only evidence for those baseline tasks. Timestamp storage is still ISO-8601 strings; a lossless typed encoding ruling has not yet been investigated. No owner-reserved licensing/adoption decisions were made.
