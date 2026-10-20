# Terrine test tiers: retire `slow`, add a scheduled `scale` tier

**Goal:** Every test belongs to one of three tiers defined by when it runs, and
push CI drops from ~26 min to roughly 10–12 min without losing any assertion.

**Kata:** parent `1r17`.

## Scope ledger

| Capability | Source | Confirmed |
| --- | --- | --- |
| Retire the `slow` marker and tier | owner: "should slow really exist?"; agent proposal "retire `slow` and add a scheduled scale tier" answered "sounds good!" (2026-10-20) | yes |
| Scheduled/manual `scale` tier for resource-bound scale, oracle and live-warehouse checks | same proposal, item 3 | yes |
| Re-tier the 52 non-scale slow tests: trim to unit budget or move to `integration/`, ledger every disposition | same proposal, item 4 | yes |
| Merge `tests/integration/test_duckdb_scale.py` into the integration tree | kata `1r17` (folded into the proposal) | yes |
| Library performance fixes for slow tiny-input tests | not requested | no: record findings only |

## Tiers after this change

| Tier | Where / marker | Runs | Per-test target |
| --- | --- | --- | --- |
| Unit | `tests/`, unmarked | every local run and every push | ≤ ~2 s standalone locally |
| Integration | `integration/`, unmarked | every push (CI) and before merges | ≤ ~30 s on the 2-CPU CI runner |
| Scale | `@pytest.mark.scale`, under `integration/` | nightly schedule + manual dispatch | bounded by job timeout |

`pyproject.toml` `addopts` deselects `scale` (`-m 'not scale'`); `slow` and
`parameter_recovery` markers are removed (no `parameter_recovery` tests remain).

## Inventory (from `pytest -m slow --collect-only`, CI run 39425651610)

- Scale (→ `scale`): `tests/integration/test_duckdb_scale.py` (3 cases, 140–278 s on
  CI), `integration/warehouse_execution/test_duckdb_scale.py::test_retained_compiler_generations_and_bounded_ranges_match_independent_oracles`
  (84 s), `integration/warehouse_execution/test_ml_staging_duckdb.py::test_growing_dataset_bounds_client_batches_and_arrow_buffers`
  (302 s, at its 300 s subprocess limit), `integration/warehouse_execution/test_live_warehouse_scale.py`
  (owner-approval gated; currently `slow`).
- Re-tier (52): `tests/compile/test_state_panel.py` (29), `tests/temporal/test_window_aggregates.py` (13),
  `tests/temporal/test_sparse_state.py` (4), `tests/compile/test_columns.py` (3),
  `tests/test_frame_runtime.py` (1), `tests/parity/test_increment_frames.py` (1),
  `tests/compat/test_optional_increment.py` (1).

## Disposition rules (Tasks 2–4)

For each slow test, classify by behavioral boundary first, then by cost:
1. **Boundary.** Name the function that owns the rule the test asserts. If the test
   checks one rule that a lower function owns, it is unit-boundary; if it checks
   composition across subsystems (compile → execute → publish/read-back, processes,
   real libraries), it is integration-boundary.
2. **Measure** standalone, setup + call + teardown together:
   `uv run pytest -n 0 -p no:randomly -m "" --durations=0 --durations-min=0 <nodeid>`.
3. Unit-boundary tests:
   - **KEEP-UNIT** if ≤ 2 s standalone: drop the marker only.
   - **TRIM** a parametrized sweep to 2–3 representative cases that still cover each
     boundary the test names (both sides of a cut, both signs); each remaining case
     ≤ 2 s. Cases dropped must be redundant with a kept case; otherwise MOVE them.
   - **REWRITE** at the owning function with smaller inputs. Before deleting the
     original, the replacement must fail when the rule is deliberately broken
     (record the red check).
   - **MOVE** to `integration/<area>/` unchanged if none of the above reaches 2 s
     (e.g. fixed compile/execution overhead on tiny inputs). Record the profiled cause
     as a library-performance finding; do not fix the library here.
4. Integration-boundary tests: **MOVE** to `integration/<area>/` regardless of time.

Every original test gets one ledger row: node id, owning function, boundary,
standalone time, disposition, tier, carrier test (node id), red-check evidence for
REWRITE. No assertion is dropped.

## Tasks

### Task 1 — Scale tier (integrator)
- Add marker `scale`; addopts `-m 'not scale'`. `make test-scale` replaces
  `make test-slow` and defaults to `integration/` via `SCALE_TESTS ?= integration`
  (overridable); verify through the Makefile target, not a raw pytest command.
- Extend `make lint`, `make typecheck` and `make fmt` selectors to cover `integration`
  so relocated tests keep Ruff, ty and formatting checks; fix whatever the wider
  selection surfaces in existing integration modules.
- Move `tests/integration/test_duckdb_scale.py` into `integration/warehouse_execution/`
  (merge with or sit beside the existing scale module; keep helpers importable),
  mark `scale`. Mark the retained-oracle, growing-dataset and live-warehouse tests `scale`.
- CI: delete the `slow` job and the `-m slow` step in `increment-compatibility`; add a
  `scale` job on `schedule` (nightly) and `workflow_dispatch`, `timeout-minutes: 60`.
- Acceptance: `pytest integration -m scale --collect-only` lists exactly the 6 scale
  cases (3 + retained oracle + growing dataset + live); the ordinary integration run
  collects none of them.

### Tasks 2–4 — Re-tier by area (parallel, disjoint files)
- Task 2: `tests/compile/test_state_panel.py` (29).
- Task 3: `tests/temporal/test_window_aggregates.py` (13), `tests/temporal/test_sparse_state.py` (4).
- Task 4: `tests/compile/test_columns.py` (3), `tests/test_frame_runtime.py` (1),
  `tests/parity/test_increment_frames.py` (1), `tests/compat/test_optional_increment.py` (1).
- Moves go to new files `integration/<area>/test_*.py` named per area; shared fixtures
  are imported, not copied, where the source fixture is in a `conftest.py` reachable
  from `integration/` (otherwise move the minimal fixture with the test).
- Each area writes `docs/superpowers/plans/ledger-test-tiers-<area>.md`.
- Acceptance per area: no `pytest.mark.slow` left in its files; every ledger row has a
  carrier; area unit tests ≤ 2 s each standalone; moved tests pass under `pytest integration/<area>`.

### Task 5 — Retire `slow` and verify (integrator)
- Remove the `slow`/`parameter_recovery` markers from `pyproject.toml`; `--strict-markers`
  proves no stragglers.
- Update AGENTS.md tier text, README test section (also drop the stale 10-second
  budget), `docs/guides/temporal-engine.md` slow-tier command, `docs/guides/ml-staging.md`
  (growing-input bounds now run with `make test-scale`), `benchmarks/README.md`
  commands. No CHANGELOG entry: contributor-facing only.
- Verify: unit suite unchanged in count except removals/moves accounted for in ledgers;
  `uv run pytest -q tests` passes with nothing listed ≥ 5 s; `uv run pytest -q integration`
  passes; `uv run pytest -q -m scale integration` passes locally (live test skips without
  approval); push and confirm CI wall time and that no job runs scale tests.

## Measurement hygiene

Area tasks run in parallel, so their timings include contention; they use standalone
`-n 0` runs for decisions and treat ≤ 2 s as the bar. Task 5 re-measures serially on
a quiet machine before anything is accepted.
