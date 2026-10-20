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

