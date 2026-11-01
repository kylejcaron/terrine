

## Benchmark repair follow-up

The benchmark was blocked because strict duration parsing rejected windows constructed as integer seconds. Updated every bare-integer benchmark window found in `benchmarks/`: the two workload windows now use `f"-{config.window_days}d"` through `Window`/`agg`, and the overhead window uses `f"-{_WINDOW_SECONDS // 86400}d"`; each has an explicit `"0s"` endpoint.

For the baseline measurement, temporarily restored the pre-task recursive JSON-walk behavior inside `check_native_functions`, ran the benchmark, then restored the committed one-pass implementation before the optimized run. Both benchmark commands completed successfully. Materialization medians (seconds; optimized delta is new minus baseline):

| Case | Recursive baseline | One-pass | Delta |
|---|---:|---:|---:|
| one_row_count | 0.197 | 0.193 | -0.004 |
| one_row_decimal_sum | 0.248 | 0.268 | +0.020 |
| one_row_float_average | 0.397 | 0.347 | -0.050 |
| one_row_ratio | 0.594 | 0.386 | -0.208 |
| one_row_four_columns | 0.971 | 0.867 | -0.104 |
| few_rows_four_columns | 0.881 | 0.866 | -0.015 |
| events_10k_count | 0.191 | 0.185 | -0.006 |
| events_10k_four_columns | 0.995 | 0.981 | -0.014 |
| events_100k_count | 0.418 | 0.226 | -0.192 |
| events_100k_four_columns | 1.468 | 1.417 | -0.051 |

The unweighted mean of these reported medians changed from 0.636s to 0.574s, a 0.062s (9.8%) reduction. Individual case timings are noisy and one case increased by 0.020s; this is a benchmark observation, not a guarantee of per-case improvement. The earlier two failed attempts are superseded by these successful runs.
## Cache-lock review follow-up

Moved DuckDB serialization and JSON token extraction outside `_NATIVE_FUNCTION_NAMES_LOCK`; the lock now covers only cache lookup/LRU refresh and insertion/eviction. A deterministic regression test patches `_rows` and asserts that the cache lock is not held at the serialization boundary.

**RED** — `uv run pytest -n 0 integration/execution/test_native_admission.py` failed the new test at the patched `_rows` assertion: `assert not _native._NATIVE_FUNCTION_NAMES_LOCK.locked()`; observed the lock was `True`.

**GREEN** — `uv run pytest -n 0 integration/execution/test_native_admission.py`: 3 passed. Step 4 selection, `uv run pytest -n 0 tests/execution/test_inputs.py tests/test_frame_runtime.py -k "shadow or macro or function or revalidates or changed_view"`: 9 passed, 35 deselected. `make lint`: passed.
