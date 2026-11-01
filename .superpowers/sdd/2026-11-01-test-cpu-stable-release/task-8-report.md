# Task 8: Extract native function names in one pass

## What changed

- Added `_function_names(raw, statement) -> frozenset[str]`, which serializes SQL once, scans JSON tokens in one regular-expression pass, decodes complete JSON string literals with `json.loads`, and case-folds string values below `function_name`, `function`, and `operator_name` keys at any depth. Non-string values contribute no name.
- Added a lock-protected 256-entry LRU keyed by `(duckdb.__version__, statement)`. Only successful extractions are cached.
- Replaced the recursive decoded-tree walk in `check_native_functions` for both the initial statement and recursively inspected macro definitions. Serialization failures continue to use `_binding_error("native parser could not certify function ownership")`.
- Added compiled README forecast, binned state-panel, and event-sequence study comparisons against a test-local copy of the prior recursive walk. The test also covers operator extraction, macro-body expression names, and JSON-like text inside a SQL string literal.

## TDD evidence

**RED** — `uv run pytest -n 0 integration/execution/test_native_admission.py` before implementation failed: the direct helper test reported `ImportError: cannot import name '_function_names' from 'terrine.execution._native'`. The compiled-study case also exposed its fixture's missing bounded materialization budget, which was corrected before GREEN.

**GREEN** — `uv run pytest -n 0 integration/execution/test_native_admission.py tests/execution/test_inputs.py tests/test_frame_runtime.py -k "shadow or macro or function or revalidates or changed_view"`

Output: `11 passed, 35 deselected` (2.47s; 38 dependency deprecation warnings).

**Operator red check** — Temporarily changed the extraction to drop the operator name `+`, then ran `uv run pytest -n 0 integration/execution/test_native_admission.py::test_function_names_match_recursive_walk_for_compiled_studies`. It failed as intended: the oracle's expected set included `+`, while the extracted set did not. The mutation was reverted; the final focused suite above passed.

## Benchmark

Ran `uv run python benchmarks/run_overhead.py` once at the start and once at the end. Both attempts stopped before measuring any workload while constructing the default design: `TerrineError: window='-604800' is not an elapsed duration ... bare_integer`. No materialization timings were produced; before/after delta is unavailable (N/A), not zero.

## Verification

- `make lint` — passed (`ruff check` and subsequent configured lint stages).
- `uv run ty check terrine` — failed on 9 unresolved optional dependency imports: `google.cloud` (2), `google.cloud.bigquery_storage_v1`, `psycopg`, Airflow modules (3), `dagster`, and `datasketches`. No diagnostic pointed to the changed files.
- The requested focused pytest set passed as reported above.

## Files changed

- `terrine/execution/_native.py`
- `integration/execution/test_native_admission.py`
- `.superpowers/sdd/2026-11-01-test-cpu-stable-release/task-8-report.md`

## Self-review and concerns

- Extraction preserves all three prior key names, matching is case-insensitive at every nesting depth, and non-string key values are ignored. Complete JSON string tokens prevent escaped text inside JSON strings from being mistaken for structural keys.
- The operator omission red check confirms operators are included, rather than being filtered based on `is_operator` metadata.
- No benchmark comparison is available because the benchmark fails during its own design setup. The type-check command is likewise blocked by absent optional dependencies; these are the only outstanding concerns.
