# Fast-test coverage and measurement ledger

## Scope and measurement model

Tasks 1 and 2 preserve all test bodies and assertions. Later dispositions below are
not applied by these tasks. Timing observations are wall-clock samples, not
statistical performance guarantees. Compare the same pytest call phase with
`R = loaded_call_seconds / standalone_call_seconds`. The original Task 1 limit
was `R <= 2`; the execution ruling below replaces that gate.
Profiler overhead makes profiled and unprofiled times unsuitable for that ratio.
CPU work is `user + sys`; increasing CPU work at larger worker counts is evidence
of contention, not proof of a cross-process lock.

Ruling: execute only Tasks 1 and 2 inline in the supplied worktree, without kata,
additional worktrees, or subagents, as assigned. The immutable-world interface
from Task 2 must not share a connection or destination with Task 1's workers.

## Task 1: diagnosis before changes

Commands, executed sequentially before editing any files:

```sh
/usr/bin/time -p uv run pytest -q -p no:warnings tests/dataset tests/test_common_stack_interop.py -n 1
/usr/bin/time -p uv run pytest -q -p no:warnings tests/dataset tests/test_common_stack_interop.py -n 4
/usr/bin/time -p uv run pytest -q -p no:warnings tests/dataset tests/test_common_stack_interop.py -n 8
/usr/bin/time -p uv run pytest -q -p no:warnings tests/dataset tests/test_common_stack_interop.py -n 12
```

| workers | passed | real s | user s | sys s | user + sys s |
|---:|---:|---:|---:|---:|---:|
| 1 | 96 | 348.85 | 329.85 | 14.50 | 344.35 |
| 4 | 96 | 99.76 | 367.00 | 15.49 | 382.49 |
| 8 | 96 | 132.96 | 514.79 | 21.80 | 536.59 |
| 12 | 96 | 95.14 | 496.35 | 20.38 | 516.73 |

`sysctl hw.ncpu hw.physicalcpu hw.memsize` returned `12`, `12`, and
`38654705664`. The curve is non-monotonic. Twelve workers had the lowest sampled
wall time; four used less CPU, while eight lost on both measures. This is not the
flat-CPU/increasing-wall signature of a single shared lock.

### Loaded profile

First, uncontended:

```sh
/usr/bin/time -p uv run python -m cProfile -o /tmp/standalone.prof -m pytest -q -p no:xdist tests/dataset/test_export.py::test_parquet_export_preserves_pinned_arrow_schema_and_refuses_overwrite
```

Output: `1 passed, 82 warnings in 8.90s`; call `7.78s`, setup `0.52s`;
real/user/sys `9.97 / 9.38 / 0.50`.

Then start these two commands together, awaiting both processes:

```sh
/usr/bin/time -p uv run pytest -q -n 8 tests --durations=0
/usr/bin/time -p uv run python -m cProfile -o /tmp/loaded.prof -m pytest -q -p no:xdist tests/dataset/test_export.py::test_parquet_export_preserves_pinned_arrow_schema_and_refuses_overwrite
```

Full-suite output: `1293 passed, 6006 warnings in 263.65s`;
real/user/sys `264.37 / 1233.28 / 56.68`. The test's call phase inside that suite
was `9.75s`. Concurrent profile output: `1 passed, 82 warnings in 14.33s`;
call `12.21s`, setup `1.07s`; real/user/sys `15.37 / 10.99 / 0.64`.
Logs: `/tmp/fast-tests-loaded-suite.txt`, `/tmp/fast-tests-loaded-profile.txt`.

Comparison command: `pstats.Stats(path).strip_dirs().sort_stats('tottime').print_stats(15)`.

| function | standalone calls / tottime s | loaded calls / tottime s |
|---|---:|---:|
| Ibis DuckDB `get_schema` | 6 / 1.624 | 6 / 3.306 |
| Terrine `_native._rows` | 383/6 / 1.086 | 380/6 / 2.410 |
| Ibis `graph.toposort` | 7523 / 0.356 | 7522/5824 / 0.444 |
| `gc.collect` | 5 / 0.216 | 5 / 0.248 |

The dominant additional sampled cost is native schema/system-metadata access:
`.venv/lib/python3.13/site-packages/ibis/backends/duckdb/__init__.py:297`
(`con.sql(DESCRIBE ...)` at 328, Arrow retrieval at 332), and
`terrine/execution/_native.py:86-89` (`raw.execute(sql).fetchall()` behind a
per-connection lock). Both cost about twice as much while their call counts are
unchanged. Ibis graph work also increases, but less. There is no evidence here
that snapshot copies or lease renewal should be omitted. The original reported
84-second call was not reproduced in this full-suite sample.

The fixed endpoint at the original
`tests/runtime/test_control_recovery.py:39,52` is a separate verified collision
hazard. Replace it with a shared server/client endpoint containing PID and UUID;
a short `/tmp` path avoids the Unix-socket pathname limit on macOS. The sibling
recovery-copy test already uses a PID-scoped endpoint.

Ruling: worker defaults are configuration, so the measured curve is their test
rather than a permanent timing assertion. No production behavior is changed.
With an explicit `-n` default, use `-n 0` for uncontended runs rather than disabling
the xdist plugin without also clearing addopts.

### Resource experiments and execution ruling

Ruling (parent, 2026-10-19): the original 84 s outlier came from the parent's
concurrent background runs. Task 1 now accepts a recorded cause, isolated worker
resources, and measured worker/thread defaults chosen by total wall time.
Retain a test-only setting only if full-suite wall improves. Record the ratio
but do not gate it. The plan is amended in the same commit; Task 7's 90 s gate
remains. Cost if this sample is unrepresentative: the selected concurrency may
not be optimal on another run or host; timings remain observational.

An unprofiled focused run:

```sh
/usr/bin/time -p uv run pytest -q -n 0 tests/dataset/test_export.py::test_parquet_export_preserves_pinned_arrow_schema_and_refuses_overwrite tests/runtime/test_control_recovery.py
```

Output: `5 passed, 82 warnings in 5.96s`; export call `3.51s`;
real/user/sys `7.39 / 5.07 / 0.74`.

Default native pool probe:

```sh
uv run python -c "import duckdb, pyarrow as pa, polars; print('DuckDB threads:', duckdb.connect().execute(\"select current_setting('threads')\").fetchone()[0]); print('Arrow CPU / I/O:', pa.cpu_count(), pa.io_thread_count()); print('Polars:', polars.thread_pool_size())"
```

Output: DuckDB `12`, Arrow CPU/I/O `12 / 8`, Polars `12`.

A temporary pytest plugin supplying `config={'threads': 2}` to real DuckDB
connections, preserving explicit connection options, ran:

```sh
/usr/bin/time -p env PYTHONPATH=/tmp TERRINE_PROBE_THREADS=2 uv run pytest -q -p no:warnings -p terrine_thread_budget tests/dataset tests/test_common_stack_interop.py -n 8 --durations=0 --durations-min=2
```

Output: `96 passed in 66.81s`; export call `6.24s`;
real/user/sys `67.38 / 447.82 / 17.09`. No query, snapshot, heartbeat, or export
operation was replaced. This config is installed by a session fixture only in
xdist workers, and restored at teardown; non-worker connections are untouched.

Full-suite verification with this setting:

```sh
/usr/bin/time -p uv run pytest -q -n 8 tests --durations=0
```

Output: `1293 passed, 6006 warnings in 245.79s`;
real/user/sys `246.51 / 1204.86 / 51.01`; export call `9.13s`.
Before: wall `264.37s`, export `9.75s`; after: wall `246.51s`, export `9.13s`.
Observed unprofiled ratios are `9.75 / 3.51 = 2.78` before and
`9.13 / 3.51 = 2.60` after. The full-suite baseline included the concurrent
profile as required, so the 17.86 s difference is not a controlled causal
estimate. The isolated focused curve also improved (`132.96s` to `67.38s`).
Full output: `/tmp/fast-tests-after-suite.txt`.

An additional pool experiment bounded Arrow CPU/I/O and Polars to one thread
alongside DuckDB's two threads. Command:

```sh
/usr/bin/time -p uv run pytest -q -p no:warnings tests/dataset tests/test_common_stack_interop.py -n 8 --durations=0 --durations-min=2
```

Output: `96 passed in 88.21s`; real/user/sys `88.81 / 445.87 / 16.97`,
export call `5.92s`. This did not improve total wall over DuckDB-only (`67.38s`),
so those extra pool settings were removed.

The already-running twelve-worker/two-thread comparison completed with:

```sh
/usr/bin/time -p uv run pytest -q -p no:warnings tests/dataset tests/test_common_stack_interop.py -n 12
```

Output: `96 passed in 143.84s`; real/user/sys `145.01 / 518.97 / 22.03`.
Select **eight pytest workers and two DuckDB threads per connection**: the
measured focused wall `67.38s` is the lowest observed setting, and the same
setting completed the full suite in `246.51s`. No further concurrency tuning
is part of these tasks.

### Fixed overhead for library investigation

Command: inspect `/tmp/standalone.prof` with `pstats.Stats(path).stats`, selecting
the named function keys. These are observed main-thread profile counts, not
all-thread totals; runtime tasks also execute in background threads.

| production call site | observed total calls | sampled self time s |
|---|---:|---:|
| `terrine/execution/_native.py:86` `_rows` | 383 | 1.085889 |
| `terrine/execution/_native.py:100` `native_objects` | 35 | 0.000175 |
| `terrine/execution/_native.py:132` `_current_namespace` | 109 | 0.000321 |
| `terrine/execution/_native.py:137` `function_fingerprint` | 87 | 0.001270 |
| `terrine/execution/_native.py:426` `admission_fingerprint` | 2 | 0.000122 |
| `terrine/execution/_native.py:495` `check_native_functions` | 7 | 0.000092 |
| `ibis/backends/duckdb/__init__.py:297` `get_schema` | 6 | 1.624376 |
| `ibis/backends/sql/__init__.py:93` `compile` | 23 | 0.000112 |
| `ibis/backends/sql/compilers/base.py:607` `translate` | 29 | 0.002650 |
| `ibis/backends/sql/rewrites.py:350` `sqlize` | 29 | 0.003205 |
| `ibis/common/graph.py:642` `toposort` | 7523 | 0.356465 |
| `ibis/common/graph.py:724` `bfs` | 631 | 0.233217 |

`_current_namespace` issues the current-database/current-schema query at
`terrine/execution/_native.py:133`; `native_objects` reads tables and views at
`:102-108`; `function_fingerprint` reads the native function catalog at
`:138-142`. Their query costs are attributed to `_rows`, not to the tiny Python
wrappers. Repeated schema and system-catalog queries, plus repeated Ibis graph
passes for a two-row study, are concrete library-level investigation targets.
No library query, admission check, or compilation pass is bypassed here.

Task 1 complete under the amended acceptance. The isolated socket preserves the
same recovery assertions; `tests/runtime/test_control_recovery.py` passed all
four cases in the focused five-test run and in the 1293-case full verification.
The worker fixture changes only native execution resources and restores the
connection factory at session end.

Pre-commit gates:

```sh
uv run ruff format terrine tests examples integration
uv run ruff check terrine tests examples integration
uv run ty check terrine tests
uv run tach check
```

Output: `2 files reformatted, 320 files left unchanged`; both Ruff and ty:
`All checks passed!`; Tach: `All modules validated!`.

## Task 2: immutable worlds and fresh bindings

Ruling: the named fixtures do not have the same source schema. Keep distinct
`events_world`, `winsorization_world`, and `interop_world` session inputs, using
one frozen `WorldInputs` representation and one `bind_model(world, tmp_path)`
factory. Combining their data would change the assertions' inputs. Cost if the
extraction changes a schema: the exact Arrow round-trip probe and existing
typed/decimal/temporal tests must catch it.

`WorldInputs` seals the tables, units, and readiness dictionaries with read-only
mapping proxies; catalogs already recursively seal their fields. Source Arrow
tables and catalog metadata are the only shared data. Alternate worlds reuse
the existing builders once and disconnect the construction connection before
returning. Each bind creates new tables in its own `tmp_path / "study.duckdb"`;
mutations, manifests, snapshots and export destinations remain test-local.
The native in-memory fixture also consumes the same immutable events inputs,
but still creates and disconnects a fresh in-memory connection per test.

Converted `_durable_frame`, all its callers, `_staged_dataset`, winsorization's
`model`, and both `common_frame` variants. Winsorization's `training` helper
retains its design and split construction unchanged. Interoperability's shared
`interop_design` keeps the existing full/small projection in one place.
Empty training changes only that binding's readiness; empty interop deletes
only that binding's roster rows. No test body assertion or parameter case was
removed, weakened, or moved.

Throwaway probe command:

```sh
env PYTHONPATH=. uv run python /tmp/check_terrine_worlds.py
```

Red before implementation: `ModuleNotFoundError: No module named 'tests._worlds'`.
Green after implementation:

```text
events_world immutable inputs; isolated DuckDB mutations; exact Arrow round trip
winsorization_world immutable inputs; isolated DuckDB mutations; exact Arrow round trip
interop_world immutable inputs; isolated DuckDB mutations; exact Arrow round trip
```

The probe attempts frozen field, table-mapping and catalog-mapping mutations;
binds each world into two different directories; deletes one connection's
events; checks the other's rows and all source Arrow tables are unchanged.

### Automatic review of Task 1

`roborev list --repo /Users/kylejcaron/terrine --branch feat/fast-tests --open --json --limit 20`
found job `9112` for `8d117df`. `roborev wait --job 9112` returned two findings:
the worker resource fixture required optional DuckDB even for backend-independent
tests, and serial commands disabling xdist conflicted with the new `-n` default.
Both are valid and addressed with Task 2's fixture changes: an absent DuckDB
leaves only the resource fixture inactive, without skipping any tests; unrelated
import failures still propagate. Executable serial commands now use `-n 0`.

Dependency-isolation regression probe:

```sh
env PYTHONPATH=/tmp:. uv run pytest -q -n 1 -p terrine_without_duckdb tests/test_optional_dependencies.py
```

The temporary plugin rejects `import duckdb` with
`ModuleNotFoundError(name="duckdb")`. Red: both collected tests errored during
the autouse fixture's import. Green: `2 passed in 2.22s`, including the subprocess
check that backend metadata imports without optional drivers or dataframes.

### Task 2 validation

```sh
/usr/bin/time -p uv run pytest -q tests/dataset tests/test_common_stack_interop.py
```

Output: `96 passed, 2661 warnings in 159.42s`;
real/user/sys `160.82 / 462.62 / 19.48`. The same 96 cases passed before and
after conversion. The warnings are the existing Ibis DuckDB Arrow API
deprecations. This observation does not establish a speed improvement; the
fixed pipeline work is unchanged and the sampled wall times vary substantially.

Pre-commit gates used the four exact commands recorded for Task 1:
`3 files reformatted, 320 files left unchanged`; Ruff and ty both
`All checks passed!`; Tach `All modules validated!`.

Coverage table generation parsed every per-test inventory row and asserted
230 unique test IDs and nonempty original carriers. Every current disposition
is KEEP because these tasks change only inputs and resources.


The full-projection caller of `interop_design` was checked separately:

```sh
uv run pytest -q integration/warehouse_execution/test_backend_capabilities.py::test_duckdb_typed_dataset_arrow_parquet_and_generation_parity
```

Output: `1 passed, 59 warnings in 35.65s`. This is a focused composed-call check,
not an additional full integration-suite run.

Task 2 complete: immutable inputs, fresh per-test databases, all named fixture
conversions, unchanged 96-case pass count, complete 230-row coverage ledger,
and the Task 1 review fixes are verified.

## Coverage ledger

All 230 inventory cases remain collected in their original tier after Tasks 1
and 2. `KEEP` records the current disposition, not the inventory's proposed
future change. A later DELETE/MOVE/REWRITE/SLIM must update the carrier and
replace the unchanged-assertion note with its actual red-check or twin-assertion
evidence. No deletion or lower-boundary rewrite is claimed in these rows.

| test id | disposition | carrier test id | red-check evidence |
|---|---|---|---|
| `tests/test_common_stack_interop.py::test_dataset_batches_and_parquet_preserve_typed_metadata[typed_rows]` | KEEP | `tests/test_common_stack_interop.py::test_dataset_batches_and_parquet_preserve_typed_metadata[typed_rows]` | Assertions unchanged; inventory proposes DELETE. |
| `tests/test_common_stack_interop.py::test_dataset_batches_and_parquet_preserve_typed_metadata[empty]` | KEEP | `tests/test_common_stack_interop.py::test_dataset_batches_and_parquet_preserve_typed_metadata[empty]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/test_common_stack_interop.py::test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[typed_rows-polars]` | KEEP | `tests/test_common_stack_interop.py::test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[typed_rows-polars]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/test_common_stack_interop.py::test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[typed_rows-pandas]` | KEEP | `tests/test_common_stack_interop.py::test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[typed_rows-pandas]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/test_common_stack_interop.py::test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[empty-polars]` | KEEP | `tests/test_common_stack_interop.py::test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[empty-polars]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/test_common_stack_interop.py::test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[empty-pandas]` | KEEP | `tests/test_common_stack_interop.py::test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[empty-pandas]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/test_common_stack_interop.py::test_local_arrow_preserves_exact_temporal_values_and_semantic_metadata[typed_rows]` | KEEP | `tests/test_common_stack_interop.py::test_local_arrow_preserves_exact_temporal_values_and_semantic_metadata[typed_rows]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/test_common_stack_interop.py::test_local_arrow_preserves_exact_temporal_values_and_semantic_metadata[empty]` | KEEP | `tests/test_common_stack_interop.py::test_local_arrow_preserves_exact_temporal_values_and_semantic_metadata[empty]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_layers.py::test_saved_panel_publication_preserves_ordinary_provenance[sequential]` | KEEP | `tests/runtime/test_layers.py::test_saved_panel_publication_preserves_ordinary_provenance[sequential]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_layers.py::test_saved_panel_publication_preserves_ordinary_provenance[event_panel]` | KEEP | `tests/runtime/test_layers.py::test_saved_panel_publication_preserves_ordinary_provenance[event_panel]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_layers.py::test_state_panel_composition_matches_full_rebuild` | KEEP | `tests/runtime/test_layers.py::test_state_panel_composition_matches_full_rebuild` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_export.py::test_export_syncs_shards_and_directories_before_checkpoint_and_publication` | KEEP | `tests/dataset/test_export.py::test_export_syncs_shards_and_directories_before_checkpoint_and_publication` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_export.py::test_export_does_not_checkpoint_a_shard_when_fsync_fails` | KEEP | `tests/dataset/test_export.py::test_export_does_not_checkpoint_a_shard_when_fsync_fails` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_export.py::test_empty_training_export_preserves_complete_arrow_metadata[partition_by0]` | KEEP | `tests/dataset/test_export.py::test_empty_training_export_preserves_complete_arrow_metadata[partition_by0]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_export.py::test_empty_training_export_preserves_complete_arrow_metadata[partition_by1]` | KEEP | `tests/dataset/test_export.py::test_empty_training_export_preserves_complete_arrow_metadata[partition_by1]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_export.py::test_export_retry_resyncs_parent_after_interrupted_publication` | KEEP | `tests/dataset/test_export.py::test_export_retry_resyncs_parent_after_interrupted_publication` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_export.py::test_parquet_export_preserves_pinned_arrow_schema_and_refuses_overwrite` | KEEP | `tests/dataset/test_export.py::test_parquet_export_preserves_pinned_arrow_schema_and_refuses_overwrite` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_export.py::test_export_discards_unfinished_shard_and_rewrites_on_retry` | KEEP | `tests/dataset/test_export.py::test_export_discards_unfinished_shard_and_rewrites_on_retry` | Assertions unchanged; inventory proposes DELETE. |
| `tests/dataset/test_refresh.py::test_refresh_nochange_reuses_only_same_definition_and_rejects_stale_base` | KEEP | `tests/dataset/test_refresh.py::test_refresh_nochange_reuses_only_same_definition_and_rejects_stale_base` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_refresh.py::test_refresh_reconstructs_recipe_without_mutating_pinned_reader` | KEEP | `tests/dataset/test_refresh.py::test_refresh_reconstructs_recipe_without_mutating_pinned_reader` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_refresh.py::test_scoring_refresh_preserves_as_of_and_never_reads_unused_labels` | KEEP | `tests/dataset/test_refresh.py::test_scoring_refresh_preserves_as_of_and_never_reads_unused_labels` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_runtime_staging.py::test_saved_stage_replays_exact_generation_and_refuses_changed_view` | KEEP | `tests/dataset/test_runtime_staging.py::test_saved_stage_replays_exact_generation_and_refuses_changed_view` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_runtime_staging.py::test_staged_batches_preserve_frame_metadata_and_verify_recipe_ownership` | KEEP | `tests/dataset/test_runtime_staging.py::test_staged_batches_preserve_frame_metadata_and_verify_recipe_ownership` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_runtime_staging.py::test_stage_renews_lease_during_snapshot_tasks` | KEEP | `tests/dataset/test_runtime_staging.py::test_stage_renews_lease_during_snapshot_tasks` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/test_documentation.py::test_readme_builds_and_reopens_first_dataset` | KEEP | `tests/test_documentation.py::test_readme_builds_and_reopens_first_dataset` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/agent/test_materialize.py::test_typed_dataset_views_survive_preparation_and_staging[training]` | KEEP | `tests/agent/test_materialize.py::test_typed_dataset_views_survive_preparation_and_staging[training]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/agent/test_materialize.py::test_typed_dataset_views_survive_preparation_and_staging[scoring]` | KEEP | `tests/agent/test_materialize.py::test_typed_dataset_views_survive_preparation_and_staging[scoring]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/agent/test_materialize.py::test_stage_refresh_and_export_require_separate_capabilities` | KEEP | `tests/agent/test_materialize.py::test_stage_refresh_and_export_require_separate_capabilities` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/agent/test_materialize.py::test_partitioned_export_projects_no_partition_values[True]` | KEEP | `tests/agent/test_materialize.py::test_partitioned_export_projects_no_partition_values[True]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/agent/test_materialize.py::test_refresh_preparation_replays_before_live_inputs_or_current_base_checks[True]` | KEEP | `tests/agent/test_materialize.py::test_refresh_preparation_replays_before_live_inputs_or_current_base_checks[True]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/agent/test_materialize.py::test_refresh_preparation_replays_before_live_inputs_or_current_base_checks[False]` | KEEP | `tests/agent/test_materialize.py::test_refresh_preparation_replays_before_live_inputs_or_current_base_checks[False]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/agent/test_materialize.py::test_refresh_preserves_owner_approved_destination_alias` | KEEP | `tests/agent/test_materialize.py::test_refresh_preserves_owner_approved_destination_alias` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/agent/test_materialize.py::test_runtime_and_concurrency_limits_are_distinct` | KEEP | `tests/agent/test_materialize.py::test_runtime_and_concurrency_limits_are_distinct` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/agent/test_materialize.py::test_partitioned_export_projects_no_partition_values[False]` | KEEP | `tests/agent/test_materialize.py::test_partitioned_export_projects_no_partition_values[False]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[rows-terrine.budget.rows_exceeded]` | KEEP | `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[rows-terrine.budget.rows_exceeded]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[bytes-terrine.budget.output_bytes_exceeded]` | KEEP | `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[bytes-terrine.budget.output_bytes_exceeded]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/agent/test_materialize.py::test_preview_reads_only_bounded_batches_without_rehashing_output` | KEEP | `tests/agent/test_materialize.py::test_preview_reads_only_bounded_batches_without_rehashing_output` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/agent/test_materialize.py::test_row_preview_byte_budget_counts_exact_projected_arrow_buffers` | KEEP | `tests/agent/test_materialize.py::test_row_preview_byte_budget_counts_exact_projected_arrow_buffers` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[scan-terrine.budget.scan_unknown]` | KEEP | `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[scan-terrine.budget.scan_unknown]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/agent/test_materialize.py::test_preview_is_denied_by_default_and_bounded_redacted_when_granted` | KEEP | `tests/agent/test_materialize.py::test_preview_is_denied_by_default_and_bounded_redacted_when_granted` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_percentile_threshold_fits_training_rows_only_and_freezes_recipe` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_percentile_threshold_fits_training_rows_only_and_freezes_recipe` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_validation_and_test_rows_cannot_change_fit_or_population_identity` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_validation_and_test_rows_cannot_change_fit_or_population_identity` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_independently_fitted_features_compose_and_reopen` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_independently_fitted_features_compose_and_reopen` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_frozen_limits_refuse_rebound_selector_dependencies[dimension-True-aggregate]` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_frozen_limits_refuse_rebound_selector_dependencies[dimension-True-aggregate]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_fitted_transform_refuses_different_feature_definition` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_fitted_transform_refuses_different_feature_definition` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_fitted_limits_are_exact_under_small_decimal_context` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_fitted_limits_are_exact_under_small_decimal_context` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_frozen_limits_refuse_rebound_selector_dependencies[fact-True-aggregate]` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_frozen_limits_refuse_rebound_selector_dependencies[fact-True-aggregate]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_fitting_requires_declared_training_split_and_exact_numeric_feature` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_fitting_requires_declared_training_split_and_exact_numeric_feature` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_frozen_limits_refuse_rebound_selector_dependencies[property-True-attribute]` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_frozen_limits_refuse_rebound_selector_dependencies[property-True-attribute]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_frozen_limits_refuse_rebound_selector_dependencies[source-False-aggregate]` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_frozen_limits_refuse_rebound_selector_dependencies[source-False-aggregate]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_fitted_transform_refuses_same_typed_rebound_metric[approximate_distinct]` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_fitted_transform_refuses_same_typed_rebound_metric[approximate_distinct]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_winsorization_recipe.py::test_fitted_transform_refuses_same_typed_rebound_metric[quantile]` | KEEP | `tests/dataset/test_winsorization_recipe.py::test_fitted_transform_refuses_same_typed_rebound_metric[quantile]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[delete]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[delete]` | Assertions unchanged; inventory proposes DELETE. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[late_arrival]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[late_arrival]` | Assertions unchanged; inventory proposes DELETE. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[definition_change]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[definition_change]` | Assertions unchanged; inventory proposes SLIM. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[roster_exit]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[roster_exit]` | Assertions unchanged; inventory proposes DELETE. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[update]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[update]` | Assertions unchanged; inventory proposes DELETE. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[dimension_correction]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[dimension_correction]` | Assertions unchanged; inventory proposes SLIM. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[readiness_advance]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[readiness_advance]` | Assertions unchanged; inventory proposes DELETE. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[readiness_retraction]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[readiness_retraction]` | Assertions unchanged; inventory proposes SLIM. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[insert]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[insert]` | Assertions unchanged; inventory proposes DELETE. |
| `tests/runtime/test_incremental_equivalence.py::test_ratio_sources_change_independently_match_clean_compilation[numerator]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_ratio_sources_change_independently_match_clean_compilation[numerator]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_incremental_equivalence.py::test_ratio_sources_change_independently_match_clean_compilation[denominator]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_ratio_sources_change_independently_match_clean_compilation[denominator]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_incremental_equivalence.py::test_event_anchor_identity_corrections_match_clean_compilation[first-insert]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_event_anchor_identity_corrections_match_clean_compilation[first-insert]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_compilation_matches_clean_values_and_eligibility` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_compilation_matches_clean_values_and_eligibility` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_incremental_equivalence.py::test_event_anchor_identity_corrections_match_clean_compilation[nth-delete]` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_event_anchor_identity_corrections_match_clean_compilation[nth-delete]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_incremental_equivalence.py::test_historical_1e16_insert_has_identical_float64_refusal` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_historical_1e16_insert_has_identical_float64_refusal` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_incremental_equivalence.py::test_nested_selector_correction_outside_t0_window_matches_clean_frame` | KEEP | `tests/runtime/test_incremental_equivalence.py::test_nested_selector_correction_outside_t0_window_matches_clean_frame` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_scoring_snapshot.py::test_scoring_today_is_resolved_once_at_catalog_day_boundary` | KEEP | `tests/dataset/test_scoring_snapshot.py::test_scoring_today_is_resolved_once_at_catalog_day_boundary` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_scoring_snapshot.py::test_owner_prepares_scoring_view_before_preflight_without_label_inputs` | KEEP | `tests/dataset/test_scoring_snapshot.py::test_owner_prepares_scoring_view_before_preflight_without_label_inputs` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_split_view.py::test_group_split_view_matches_typed_reference_and_reconstruction` | KEEP | `tests/dataset/test_split_view.py::test_group_split_view_matches_typed_reference_and_reconstruction` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_split_view.py::test_training_split_assigns_support_without_reassigning_overlap` | KEEP | `tests/dataset/test_split_view.py::test_training_split_assigns_support_without_reassigning_overlap` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_training_view.py::test_training_view_hashes_selected_feature_definition_and_parent_design` | KEEP | `tests/dataset/test_training_view.py::test_training_view_hashes_selected_feature_definition_and_parent_design` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_training_view.py::test_incomplete_labels_drop_rows_and_keep_null_reasons` | KEEP | `tests/dataset/test_training_view.py::test_incomplete_labels_drop_rows_and_keep_null_reasons` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_training_view.py::test_training_projection_preserves_baseline_treatment_role` | KEEP | `tests/dataset/test_training_view.py::test_training_projection_preserves_baseline_treatment_role` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_training_view.py::test_state_panel_training_view_hashes_baseline_and_prior_state` | KEEP | `tests/dataset/test_training_view.py::test_state_panel_training_view_hashes_baseline_and_prior_state` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_training_view.py::test_feature_definition_hash_tracks_source_timestamp_semantics` | KEEP | `tests/dataset/test_training_view.py::test_feature_definition_hash_tracks_source_timestamp_semantics` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/adapters/test_dagster.py::test_dagster_historical_partitions_publish_distinct_owner_outputs` | KEEP | `tests/adapters/test_dagster.py::test_dagster_historical_partitions_publish_distinct_owner_outputs` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/adapters/test_dagster.py::test_dagster_same_root_failed_retry_reuses_fence_scoped_outputs` | KEEP | `tests/adapters/test_dagster.py::test_dagster_same_root_failed_retry_reuses_fence_scoped_outputs` | Assertions unchanged; inventory proposes DELETE. |
| `tests/compile/test_cohorts.py::test_cohort_collection_observes_fresh_snapshot_and_budget` | KEEP | `tests/compile/test_cohorts.py::test_cohort_collection_observes_fresh_snapshot_and_budget` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_cohort_mean_preserves_fractional_decimal_and_float_values` | KEEP | `tests/compile/test_cohorts.py::test_cohort_mean_preserves_fractional_decimal_and_float_values` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_zero_partial_and_unavailable_cohort_denominators` | KEEP | `tests/compile/test_cohorts.py::test_zero_partial_and_unavailable_cohort_denominators` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_cohort_grouping_bucket_statistic_and_denominators_match_hand_calculation` | KEEP | `tests/compile/test_cohorts.py::test_cohort_grouping_bucket_statistic_and_denominators_match_hand_calculation` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_large_zero_float_cohort_mean_remains_available` | KEEP | `tests/compile/test_cohorts.py::test_large_zero_float_cohort_mean_remains_available` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_float_cohort_mean_avoids_overflow_and_retains_certificate` | KEEP | `tests/compile/test_cohorts.py::test_float_cohort_mean_avoids_overflow_and_retains_certificate` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_float_mean_certificate_covers_many_member_rounding` | KEEP | `tests/compile/test_cohorts.py::test_float_mean_certificate_covers_many_member_rounding` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_cohort_summary_surrogate_export_retains_age_and_denominators` | KEEP | `tests/compile/test_cohorts.py::test_cohort_summary_surrogate_export_retains_age_and_denominators` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_cohorts.py::test_float_cohort_total_refuses_nonfinite_sum_and_certificate` | KEEP | `tests/compile/test_cohorts.py::test_float_cohort_total_refuses_nonfinite_sum_and_certificate` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_cohort_float_total_enforces_final_tolerance` | KEEP | `tests/compile/test_cohorts.py::test_cohort_float_total_enforces_final_tolerance` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_mean_does_not_overflow_intermediate_sum` | KEEP | `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_mean_does_not_overflow_intermediate_sum` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_total_accumulates_cancellation_exactly` | KEEP | `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_total_accumulates_cancellation_exactly` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_exact_integer_values_are_summed_before_cohort_mean_conversion` | KEEP | `tests/compile/test_cohorts.py::test_exact_integer_values_are_summed_before_cohort_mean_conversion` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_bucket_index_normalizes_timestamp_remainders_across_unix_epoch` | KEEP | `tests/compile/test_cohorts.py::test_bucket_index_normalizes_timestamp_remainders_across_unix_epoch` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_cohort_table_reuses_parent_ltv_relation_and_provenance` | KEEP | `tests/compile/test_cohorts.py::test_cohort_table_reuses_parent_ltv_relation_and_provenance` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_cohorts.py::test_repeated_first_entry_events_count_one_cohort_member` | KEEP | `tests/compile/test_cohorts.py::test_repeated_first_entry_events_count_one_cohort_member` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_bucket_index_respects_subsecond_origin_boundaries` | KEEP | `tests/compile/test_cohorts.py::test_bucket_index_respects_subsecond_origin_boundaries` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_total_refuses_unrepresentable_result` | KEEP | `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_total_refuses_unrepresentable_result` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/execution/test_backend_provider.py::test_compiler_and_execution_imports_do_not_depend_on_runtime` | KEEP | `tests/execution/test_backend_provider.py::test_compiler_and_execution_imports_do_not_depend_on_runtime` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_api.py::test_saved_recipe_execution_reopens_retained_snapshot_for_each_task` | KEEP | `tests/runtime/test_api.py::test_saved_recipe_execution_reopens_retained_snapshot_for_each_task` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_api.py::test_custom_saved_recipe_executor_is_not_limited_by_builtin_shape_preflight` | KEEP | `tests/runtime/test_api.py::test_custom_saved_recipe_executor_is_not_limited_by_builtin_shape_preflight` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_stage_normalization.py::test_state_panel_stage_matches_ordinary_normalization_and_support` | KEEP | `tests/dataset/test_stage_normalization.py::test_state_panel_stage_matches_ordinary_normalization_and_support` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_stage_normalization.py::test_stage_never_collects_full_output_through_snapshot_execute` | KEEP | `tests/dataset/test_stage_normalization.py::test_stage_never_collects_full_output_through_snapshot_execute` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_ltv.py::test_cutoff_nulls_period_error_certificate_with_its_value` | KEEP | `tests/compile/test_ltv.py::test_cutoff_nulls_period_error_certificate_with_its_value` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_ltv.py::test_cumulative_float_results_enforce_final_tolerance` | KEEP | `tests/compile/test_ltv.py::test_cumulative_float_results_enforce_final_tolerance` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_ltv.py::test_float_feature_certificates_survive_exact_trajectory_compilation` | KEEP | `tests/compile/test_ltv.py::test_float_feature_certificates_survive_exact_trajectory_compilation` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_ltv.py::test_ltv_period_and_cumulative_values_match_hand_sum` | KEEP | `tests/compile/test_ltv.py::test_ltv_period_and_cumulative_values_match_hand_sum` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_ltv.py::test_trajectory_preserves_event_feature_timing_through_arrow_round_trip` | KEEP | `tests/compile/test_ltv.py::test_trajectory_preserves_event_feature_timing_through_arrow_round_trip` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_ltv.py::test_scale_zero_decimal_cumulative_retains_wide_values` | KEEP | `tests/compile/test_ltv.py::test_scale_zero_decimal_cumulative_retains_wide_values` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_ltv.py::test_cumulative_partitions_repeated_anchors_and_propagates_missing_period` | KEEP | `tests/compile/test_ltv.py::test_cumulative_partitions_repeated_anchors_and_propagates_missing_period` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_staging.py::test_frame_stage_publishes_and_reads_its_pinned_generation` | KEEP | `tests/dataset/test_staging.py::test_frame_stage_publishes_and_reads_its_pinned_generation` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/dataset/test_staging.py::test_reopen_preserves_version_one_manifest_without_frame_metadata` | KEEP | `tests/dataset/test_staging.py::test_reopen_preserves_version_one_manifest_without_frame_metadata` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_staging.py::test_invalid_arrow_batch_size_refuses_before_read` | KEEP | `tests/dataset/test_staging.py::test_invalid_arrow_batch_size_refuses_before_read` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_staging.py::test_reopen_accepts_resolved_owner_scoped_recipe_reference` | KEEP | `tests/dataset/test_staging.py::test_reopen_accepts_resolved_owner_scoped_recipe_reference` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_staging.py::test_manifest_reopen_rejects_tampered_generation_row_count` | KEEP | `tests/dataset/test_staging.py::test_manifest_reopen_rejects_tampered_generation_row_count` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/dataset/test_staging.py::test_roster_only_stage_uses_executor_default_durable_outputs` | KEEP | `tests/dataset/test_staging.py::test_roster_only_stage_uses_executor_default_durable_outputs` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_winsorization.py::test_winsorization_uses_frozen_explicit_bounds_at_declared_stage` | KEEP | `tests/compile/test_winsorization.py::test_winsorization_uses_frozen_explicit_bounds_at_declared_stage` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/test_frame_runtime.py::test_shared_model_concurrent_exports_keep_independent_results` | KEEP | `tests/test_frame_runtime.py::test_shared_model_concurrent_exports_keep_independent_results` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/test_frame_runtime.py::test_unresolved_eligibility_refuses_export_with_only_aggregate_counts` | KEEP | `tests/test_frame_runtime.py::test_unresolved_eligibility_refuses_export_with_only_aggregate_counts` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_cli.py::test_cli_status_events_and_cancel_use_owner_ipc` | KEEP | `tests/runtime/test_cli.py::test_cli_status_events_and_cancel_use_owner_ipc` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_cli.py::test_cli_run_waits_for_long_owner_execution_receipt` | KEEP | `tests/runtime/test_cli.py::test_cli_run_waits_for_long_owner_execution_receipt` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_cli.py::test_cli_plan_refuses_unknown_scan_under_strict_budget` | KEEP | `tests/runtime/test_cli.py::test_cli_plan_refuses_unknown_scan_under_strict_budget` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/test_optional_dependencies.py::test_backend_metadata_imports_without_optional_drivers_or_dataframes` | KEEP | `tests/test_optional_dependencies.py::test_backend_metadata_imports_without_optional_drivers_or_dataframes` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_retention.py::test_retention_composes_with_bounded_repeated_shapes[schedule]` | KEEP | `tests/compile/test_retention.py::test_retention_composes_with_bounded_repeated_shapes[schedule]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_retention.py::test_retention_composes_with_bounded_repeated_shapes[event_panel]` | KEEP | `tests/compile/test_retention.py::test_retention_composes_with_bounded_repeated_shapes[event_panel]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_retention.py::test_named_retention_roundtrip_frame_matches_independent_occurrences[2d]` | KEEP | `tests/compile/test_retention.py::test_named_retention_roundtrip_frame_matches_independent_occurrences[2d]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_retention.py::test_named_retention_roundtrip_frame_matches_independent_occurrences[open_ended]` | KEEP | `tests/compile/test_retention.py::test_named_retention_roundtrip_frame_matches_independent_occurrences[open_ended]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_retention.py::test_calendar_open_retention_history_includes_anticipation[6-False]` | KEEP | `tests/compile/test_retention.py::test_calendar_open_retention_history_includes_anticipation[6-False]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_retention.py::test_calendar_open_retention_history_includes_anticipation[5-True]` | KEEP | `tests/compile/test_retention.py::test_calendar_open_retention_history_includes_anticipation[5-True]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/adapters/test_airflow.py::test_airflow_operator_runs_and_retries_persisted_owner_invocation` | KEEP | `tests/adapters/test_airflow.py::test_airflow_operator_runs_and_retries_persisted_owner_invocation` | Assertions unchanged; inventory proposes DELETE. |
| `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[1d-totals0-fresh_totals0-observed]` | KEEP | `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[1d-totals0-fresh_totals0-observed]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[12h-totals1-fresh_totals1-observed]` | KEEP | `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[12h-totals1-fresh_totals1-observed]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[1d-totals0-fresh_totals0-binned]` | KEEP | `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[1d-totals0-fresh_totals0-binned]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[12h-totals1-fresh_totals1-binned]` | KEEP | `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[12h-totals1-fresh_totals1-binned]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_persisted_layers.py::test_persisted_shape_matrix_preserves_panel_rows_and_metadata[sequential]` | KEEP | `tests/compile/test_persisted_layers.py::test_persisted_shape_matrix_preserves_panel_rows_and_metadata[sequential]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_persisted_layers.py::test_layer_definition_identity_binds_catalog_and_design` | KEEP | `tests/compile/test_persisted_layers.py::test_layer_definition_identity_binds_catalog_and_design` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_persisted_layers.py::test_runtime_and_frame_share_unknown_eligibility_refusal` | KEEP | `tests/compile/test_persisted_layers.py::test_runtime_and_frame_share_unknown_eligibility_refusal` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_persisted_layers.py::test_same_fact_measures_keep_filter_and_aggregation_identity` | KEEP | `tests/compile/test_persisted_layers.py::test_same_fact_measures_keep_filter_and_aggregation_identity` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_persisted_layers.py::test_persisted_daily_prefix_anchor_reuse_matches_ordinary_frame_compiler` | KEEP | `tests/compile/test_persisted_layers.py::test_persisted_daily_prefix_anchor_reuse_matches_ordinary_frame_compiler` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_persisted_layers.py::test_persisted_shape_matrix_preserves_panel_rows_and_metadata[event_panel]` | KEEP | `tests/compile/test_persisted_layers.py::test_persisted_shape_matrix_preserves_panel_rows_and_metadata[event_panel]` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_panels.py::test_sequential_anticipation_extends_control_risk_horizon` | KEEP | `tests/compile/test_panels.py::test_sequential_anticipation_extends_control_risk_horizon` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_panels.py::test_later_initiation_at_trial_specific_exclusive_horizon_is_not_refused` | KEEP | `tests/compile/test_panels.py::test_later_initiation_at_trial_specific_exclusive_horizon_is_not_refused` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_panels.py::test_sequential_roster_history_ignores_ineligible_anchor_rows` | KEEP | `tests/compile/test_panels.py::test_sequential_roster_history_ignores_ineligible_anchor_rows` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_panels.py::test_sequential_roster_history_uses_exclusive_trial_horizon[2025-01-10 00:00:00+00-False]` | KEEP | `tests/compile/test_panels.py::test_sequential_roster_history_uses_exclusive_trial_horizon[2025-01-10 00:00:00+00-False]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_panels.py::test_sequential_later_initiation_uses_reanchored_outcome_support` | KEEP | `tests/compile/test_panels.py::test_sequential_later_initiation_uses_reanchored_outcome_support` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_panels.py::test_sequential_roster_history_uses_exclusive_trial_horizon[2025-01-09 23:59:59+00-True]` | KEEP | `tests/compile/test_panels.py::test_sequential_roster_history_uses_exclusive_trial_horizon[2025-01-09 23:59:59+00-True]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_wide_exit_source_timestamp_is_not_cast_to_nanoseconds` | KEEP | `tests/compile/test_time_to_event.py::test_wide_exit_source_timestamp_is_not_cast_to_nanoseconds` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_wide_exit_coverage_start_is_intersected_before_narrowing` | KEEP | `tests/compile/test_time_to_event.py::test_wide_exit_coverage_start_is_intersected_before_narrowing` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_nanosecond_wide_outcome_and_exit_coverages_do_not_narrow_out_of_range` | KEEP | `tests/compile/test_time_to_event.py::test_nanosecond_wide_outcome_and_exit_coverages_do_not_narrow_out_of_range` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_nanosecond_exit_before_outcome_coverage_is_not_ignored` | KEEP | `tests/compile/test_time_to_event.py::test_nanosecond_exit_before_outcome_coverage_is_not_ignored` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_wide_entry_source_timestamp_is_not_cast_to_nanoseconds` | KEEP | `tests/compile/test_time_to_event.py::test_wide_entry_source_timestamp_is_not_cast_to_nanoseconds` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_nanosecond_event_source_accepts_utc_timestamptz_entry_source` | KEEP | `tests/compile/test_time_to_event.py::test_nanosecond_event_source_accepts_utc_timestamptz_entry_source` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_wide_entry_coverage_start_is_intersected_before_narrowing` | KEEP | `tests/compile/test_time_to_event.py::test_wide_entry_coverage_start_is_intersected_before_narrowing` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_nanosecond_delayed_entry_keeps_entry_before_outcome_coverage` | KEEP | `tests/compile/test_time_to_event.py::test_nanosecond_delayed_entry_keeps_entry_before_outcome_coverage` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_nanosecond_events_survive_time_to_event_compilation_in_any_session_timezone` | KEEP | `tests/compile/test_time_to_event.py::test_nanosecond_events_survive_time_to_event_compilation_in_any_session_timezone` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_wide_roster_observation_end_is_intersected_before_narrowing` | KEEP | `tests/compile/test_time_to_event.py::test_wide_roster_observation_end_is_intersected_before_narrowing` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_life_table_matches_independent_pandas_risk_set_oracle` | KEEP | `tests/compile/test_time_to_event.py::test_life_table_matches_independent_pandas_risk_set_oracle` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_failure_before_non_delayed_entry_is_not_discarded` | KEEP | `tests/compile/test_time_to_event.py::test_failure_before_non_delayed_entry_is_not_discarded` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_survival_preserves_event_feature_timing_through_arrow_round_trip` | KEEP | `tests/compile/test_time_to_event.py::test_survival_preserves_event_feature_timing_through_arrow_round_trip` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_event_before_declared_delayed_entry_is_prevalent` | KEEP | `tests/compile/test_time_to_event.py::test_event_before_declared_delayed_entry_is_prevalent` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_administrative_cutoff_changes_followup_without_changing_readiness` | KEEP | `tests/compile/test_time_to_event.py::test_administrative_cutoff_changes_followup_without_changing_readiness` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_non_delayed_entry_advance_without_prior_event_is_incomplete` | KEEP | `tests/compile/test_time_to_event.py::test_non_delayed_entry_advance_without_prior_event_is_incomplete` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_nanosecond_event_after_python_microsecond_clamp_is_retained` | KEEP | `tests/compile/test_time_to_event.py::test_nanosecond_event_after_python_microsecond_clamp_is_retained` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_time_to_event.py::test_nanosecond_events_support_coverage_outside_nanosecond_range` | KEEP | `tests/compile/test_time_to_event.py::test_nanosecond_events_support_coverage_outside_nanosecond_range` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/agent/test_runtime_feedback.py::test_status_events_and_cancel_require_run_destination_ownership` | KEEP | `tests/agent/test_runtime_feedback.py::test_status_events_and_cancel_require_run_destination_ownership` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/agent/test_runtime_feedback.py::test_dataset_preview_resolves_persisted_destination_before_reading` | KEEP | `tests/agent/test_runtime_feedback.py::test_dataset_preview_resolves_persisted_destination_before_reading` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_all_null_ratio_numerator_is_not_a_zero_denominator` | KEEP | `tests/temporal/test_window_aggregates.py::test_all_null_ratio_numerator_is_not_a_zero_denominator` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_float_window_certifies_ordinary_history_but_refuses_cancellation` | KEEP | `tests/temporal/test_window_aggregates.py::test_float_window_certifies_ordinary_history_but_refuses_cancellation` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_exact_instants_half_open_boundaries_and_named_results[union_window]` | KEEP | `tests/temporal/test_window_aggregates.py::test_exact_instants_half_open_boundaries_and_named_results[union_window]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[avg_calendar_day-mean-expected4-reasons4]` | KEEP | `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[avg_calendar_day-mean-expected4-reasons4]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[avg_event-mean-expected3-reasons3]` | KEEP | `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[avg_event-mean-expected3-reasons3]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[sum-mean-expected0-reasons0]` | KEEP | `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[sum-mean-expected0-reasons0]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_historical_invalid_float_state_is_not_erased_by_a_later_group` | KEEP | `tests/temporal/test_window_aggregates.py::test_historical_invalid_float_state_is_not_erased_by_a_later_group` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_unknown_filter_history_cancels_and_unavailable_suppresses_refusal` | KEEP | `tests/temporal/test_window_aggregates.py::test_unknown_filter_history_cancels_and_unavailable_suppresses_refusal` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_exact_instants_half_open_boundaries_and_named_results[native]` | KEEP | `tests/temporal/test_window_aggregates.py::test_exact_instants_half_open_boundaries_and_named_results[native]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_integer_sum_widens_without_losing_units_above_binary64_precision` | KEEP | `tests/temporal/test_window_aggregates.py::test_integer_sum_widens_without_losing_units_above_binary64_precision` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_count_and_conversion_ignore_unused_float_certificates` | KEEP | `tests/temporal/test_window_aggregates.py::test_count_and_conversion_ignore_unused_float_certificates` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_window_aggregates.py::test_exact_wide_history_retracts_and_retains_decimal_scale` | KEEP | `tests/temporal/test_window_aggregates.py::test_exact_wide_history_retracts_and_retains_decimal_scale` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_control_ipc.py::test_control_returns_full_bounded_event_page` | KEEP | `tests/runtime/test_control_ipc.py::test_control_returns_full_bounded_event_page` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/export/test_surrogate.py::test_surrogate_round_trip_retains_sampled_sequential_roles` | KEEP | `tests/export/test_surrogate.py::test_surrogate_round_trip_retains_sampled_sequential_roles` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/export/test_surrogate.py::test_surrogate_round_trip_retains_selected_event_relative_timing` | KEEP | `tests/export/test_surrogate.py::test_surrogate_round_trip_retains_selected_event_relative_timing` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_state_panel.py::test_binned_state_periods_ignore_covariate_gap` | KEEP | `tests/compile/test_state_panel.py::test_binned_state_periods_ignore_covariate_gap` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_backend_capabilities.py::test_dataset_arrow_gate_precedes_read_and_export_destination_work` | KEEP | `tests/runtime/test_backend_capabilities.py::test_dataset_arrow_gate_precedes_read_and_export_destination_work` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_backend_capabilities.py::test_unknown_snapshot_refuses_only_retained_execution_before_source_work` | KEEP | `tests/runtime/test_backend_capabilities.py::test_unknown_snapshot_refuses_only_retained_execution_before_source_work` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_backend_capabilities.py::test_dataset_export_uses_arrow_without_native_unload_and_checks_scan_policy` | KEEP | `tests/runtime/test_backend_capabilities.py::test_dataset_export_uses_arrow_without_native_unload_and_checks_scan_policy` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_backend_capabilities.py::test_dataset_reopen_refuses_unverified_backend_before_native_attributes` | KEEP | `tests/runtime/test_backend_capabilities.py::test_dataset_reopen_refuses_unverified_backend_before_native_attributes` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compat/test_semantic_interop.py::test_native_catalog_executes_a_frame_without_increment` | KEEP | `tests/compat/test_semantic_interop.py::test_native_catalog_executes_a_frame_without_increment` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/simulate/test_independence.py::test_generator_and_reference_are_independent_of_compiler_and_warehouse` | KEEP | `tests/simulate/test_independence.py::test_generator_and_reference_are_independent_of_compiler_and_warehouse` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_snapshot.py::test_rosterless_event_snapshot_executes_all_required_sources[relative_metric]` | KEEP | `tests/runtime/test_snapshot.py::test_rosterless_event_snapshot_executes_all_required_sources[relative_metric]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_snapshot.py::test_snapshot_captures_only_sources_in_design_dependency_closure` | KEEP | `tests/runtime/test_snapshot.py::test_snapshot_captures_only_sources_in_design_dependency_closure` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_snapshot.py::test_rosterless_event_snapshot_executes_all_required_sources[metric]` | KEEP | `tests/runtime/test_snapshot.py::test_rosterless_event_snapshot_executes_all_required_sources[metric]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_retention.py::test_bounded_retention_matches_exact_path_and_independent_pandas[True]` | KEEP | `tests/temporal/test_retention.py::test_bounded_retention_matches_exact_path_and_independent_pandas[True]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_retention.py::test_bounded_retention_preserves_submicrosecond_anchor_lower_bound[True]` | KEEP | `tests/temporal/test_retention.py::test_bounded_retention_preserves_submicrosecond_anchor_lower_bound[True]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_retention.py::test_bounded_retention_preserves_submicrosecond_anchor_lower_bound[False]` | KEEP | `tests/temporal/test_retention.py::test_bounded_retention_preserves_submicrosecond_anchor_lower_bound[False]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_retention.py::test_bounded_retention_matches_exact_path_and_independent_pandas[False]` | KEEP | `tests/temporal/test_retention.py::test_bounded_retention_matches_exact_path_and_independent_pandas[False]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_retention.py::test_open_ended_retention_requires_complete_source_cutoff` | KEEP | `tests/temporal/test_retention.py::test_open_ended_retention_requires_complete_source_cutoff` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_retention.py::test_finite_retention_preserves_nanosecond_end_and_followup[-1-999999999-True]` | KEEP | `tests/temporal/test_retention.py::test_finite_retention_preserves_nanosecond_end_and_followup[-1-999999999-True]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/semantic/test_rules_import.py::test_rules_import_without_query_or_temporal_packages` | KEEP | `tests/semantic/test_rules_import.py::test_rules_import_without_query_or_temporal_packages` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_columns.py::test_false_conjunct_excludes_unknown_without_hiding_candidate_audit` | KEEP | `tests/compile/test_columns.py::test_false_conjunct_excludes_unknown_without_hiding_candidate_audit` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_columns.py::test_exact_attribute_predicates_do_not_narrow_large_integer_or_decimal` | KEEP | `tests/compile/test_columns.py::test_exact_attribute_predicates_do_not_narrow_large_integer_or_decimal` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_columns.py::test_event_time_selector_metadata_distinguishes_known_absence_and_missing_origin` | KEEP | `tests/compile/test_columns.py::test_event_time_selector_metadata_distinguishes_known_absence_and_missing_origin` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_columns.py::test_reobserved_null_is_known_and_duplicate_inline_observations_dedupe` | KEEP | `tests/compile/test_columns.py::test_reobserved_null_is_known_and_duplicate_inline_observations_dedupe` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/parity/test_increment_frames.py::test_exposure_date_bucket_is_not_an_instant_equality_boundary` | KEEP | `tests/parity/test_increment_frames.py::test_exposure_date_bucket_is_not_an_instant_equality_boundary` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values0]` | KEEP | `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values0]` | Assertions unchanged; inventory proposes SLIM. |
| `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values1]` | KEEP | `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values1]` | Assertions unchanged; inventory proposes SLIM. |
| `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values2]` | KEEP | `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values2]` | Assertions unchanged; inventory proposes SLIM. |
| `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values3]` | KEEP | `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values3]` | Assertions unchanged; inventory proposes SLIM. |
| `tests/agent/test_plan_authorization.py::test_receipt_binds_dataset_recipe_change_evidence_and_base_generation` | KEEP | `tests/agent/test_plan_authorization.py::test_receipt_binds_dataset_recipe_change_evidence_and_base_generation` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/agent/test_plan_authorization.py::test_execute_replays_before_source_freshness_and_resolves_exact_invocation` | KEEP | `tests/agent/test_plan_authorization.py::test_execute_replays_before_source_freshness_and_resolves_exact_invocation` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/integration/test_calendar_panel_quantile.py::test_calendar_comparison_compiles_quantile_outcome_horizon` | KEEP | `tests/integration/test_calendar_panel_quantile.py::test_calendar_comparison_compiles_quantile_outcome_horizon` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/runtime/test_planning.py::test_owner_planner_persists_same_snapshot_and_manifest_by_invocation` | KEEP | `tests/runtime/test_planning.py::test_owner_planner_persists_same_snapshot_and_manifest_by_invocation` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_planning.py::test_roster_only_owner_plan_runs_without_fact_source_work` | KEEP | `tests/runtime/test_planning.py::test_roster_only_owner_plan_runs_without_fact_source_work` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_planning.py::test_owner_planning_refuses_before_staging_on_pinned_inputs[False]` | KEEP | `tests/runtime/test_planning.py::test_owner_planning_refuses_before_staging_on_pinned_inputs[False]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_planning.py::test_plan_request_uses_owner_ipc_from_process_without_database_access` | KEEP | `tests/runtime/test_planning.py::test_plan_request_uses_owner_ipc_from_process_without_database_access` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_planning.py::test_owner_plan_rolls_back_snapshot_pin_when_invocation_persistence_fails` | KEEP | `tests/runtime/test_planning.py::test_owner_plan_rolls_back_snapshot_pin_when_invocation_persistence_fails` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_nonadditive.py::test_approximate_quantile_frame_matches_type7_tolerance_and_is_reproducible` | KEEP | `tests/compile/test_nonadditive.py::test_approximate_quantile_frame_matches_type7_tolerance_and_is_reproducible` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_nonadditive.py::test_approximate_distinct_executes_in_duckdb_and_marks_column_metadata` | KEEP | `tests/compile/test_nonadditive.py::test_approximate_distinct_executes_in_duckdb_and_marks_column_metadata` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_nonadditive.py::test_min_max_and_distinct_windows_preserve_empty_and_null_rows` | KEEP | `tests/compile/test_nonadditive.py::test_min_max_and_distinct_windows_preserve_empty_and_null_rows` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_nonadditive.py::test_quantile_frame_matches_independent_numpy_type7_and_null_reasons` | KEEP | `tests/compile/test_nonadditive.py::test_quantile_frame_matches_independent_numpy_type7_and_null_reasons` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/integration/test_sequential_panels.py::test_all_shapes_build_materialize_and_arrow_round_trip` | KEEP | `tests/integration/test_sequential_panels.py::test_all_shapes_build_materialize_and_arrow_round_trip` | Assertions unchanged; inventory proposes KEEP-E2E. |
| `tests/compile/test_matched_t0.py::test_matched_t0_samples_controls_without_replacement_and_records_probability` | KEEP | `tests/compile/test_matched_t0.py::test_matched_t0_samples_controls_without_replacement_and_records_probability` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_matched_t0.py::test_matched_t0_convenience_lowers_policy_onto_design` | KEEP | `tests/compile/test_matched_t0.py::test_matched_t0_convenience_lowers_policy_onto_design` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_matched_t0.py::test_matched_t0_refuses_insufficient_controls_during_materialization` | KEEP | `tests/compile/test_matched_t0.py::test_matched_t0_refuses_insufficient_controls_during_materialization` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_range_scaling.py::test_shape_outputs_preserve_range_bound_metadata[observed_state]` | KEEP | `tests/compile/test_range_scaling.py::test_shape_outputs_preserve_range_bound_metadata[observed_state]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_range_scaling.py::test_shape_outputs_preserve_range_bound_metadata[binned_state]` | KEEP | `tests/compile/test_range_scaling.py::test_shape_outputs_preserve_range_bound_metadata[binned_state]` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/compile/test_event_sequence.py::test_sequence_compiler_excludes_known_false_eligibility_anchors` | KEEP | `tests/compile/test_event_sequence.py::test_sequence_compiler_excludes_known_false_eligibility_anchors` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_imports.py::test_runtime_metadata_imports_do_not_require_duckdb_backend` | KEEP | `tests/runtime/test_imports.py::test_runtime_metadata_imports_do_not_require_duckdb_backend` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/reference/test_event_sequences.py::test_overlapping_event_anchors_match_independent_pandas_oracle` | KEEP | `tests/reference/test_event_sequences.py::test_overlapping_event_anchors_match_independent_pandas_oracle` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_execute.py::test_lease_heartbeat_keeps_long_task_alive` | KEEP | `tests/runtime/test_execute.py::test_lease_heartbeat_keeps_long_task_alive` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/runtime/test_execute.py::test_cancellation_serializes_with_heartbeat_persistence` | KEEP | `tests/runtime/test_execute.py::test_cancellation_serializes_with_heartbeat_persistence` | Assertions unchanged; inventory proposes REWRITE. |
| `tests/temporal/test_sketches.py::test_tdigest_partition_merge_is_canonical_and_fresh_process_reproducible` | KEEP | `tests/temporal/test_sketches.py::test_tdigest_partition_merge_is_canonical_and_fresh_process_reproducible` | Assertions unchanged; inventory proposes REWRITE. |

## Area dispositions

Per-area ledgers with carrier test ids and red-check evidence: [dataset](ledger-dataset.md), [compile](ledger-compile.md), [runtime/agent](ledger-runtime.md). Totals: dataset 56 rows (MOVE 50, REWRITE 5, KEEP 1); compile 111 (MOVE 20, REWRITE 55, SLIM 4, KEEP 32); runtime/execution/adapters/agent 63 (MOVE 42, REWRITE 8, KEEP 13). DELETE 0: every proposed duplicate turned out to assert something its integration twin does not.
