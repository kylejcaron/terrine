# Task 6 implementation report

## Result

Implemented one result-reading API across `Frame`, `MaterializedFrame`, and `Dataset`: `to_pyarrow`, `to_pandas`, and `to_polars`. Migrated the public `MaterializedFrame.to_arrow` callsites without renaming `ArrowSurrogate.to_arrow`. `DatasetManifest.to_json()` now returns JSON text (`str`), and hash/serialization consumers encode explicitly as UTF-8 where bytes are required.

`Frame` and `MaterializedFrame` share the dataframe conversion preflight and preserve the Arrow-backed metadata/provenance; `Dataset` reads its pinned generation through bounded Arrow batches and presents the same conversions, metadata behavior, scan policy, and row/output-byte budget refusals. Conflicting pandas/polars view flags are rejected before materialization or dataset reads. The existing `terrine.dataset.invalid_view` refusal is raised at the single `dataframe_view_preflight` site; its declaration is centralized in `terrine.errors` to respect the module dependency graph and remains re-exported from `terrine.spec.dataset`.

Updated guides, examples, integration/unit callsites and changelog. Kept unrelated `ExportManifest.to_json()` in `terrine/dataset/export.py` returning bytes; that export API is distinct from the `DatasetManifest` JSON-text requirement.

## Test-first evidence

Before implementation, ran:

```text
uv run pytest tests/test_frame.py::test_materialized_frame_dataframes_match_frame_dataframes tests/dataset/test_manifest.py::test_manifest_json_is_text_and_round_trips -q
```

RED: both tests failed as expected: `MaterializedFrame` had no `to_pandas`, and manifest JSON returned `bytes`, not `str`.

## Verification

- `uv run pytest tests -q` — **1995 passed**, 2687 warnings.
- `uv run pytest integration/warehouse_execution/test_ml_staging_duckdb.py -q` — **65 passed**, 1598 warnings.
- `for e in examples/*.py; do uv run python "$e"; done` — all examples completed successfully.
- `uv run python tests/documentation.py` — documented 175 codes from 269 declarations.
- `uv run --group docs python -m tests.documentation --build` — succeeded.
- `make check TESTS="tests/test_frame.py tests/test_frame_runtime.py tests/dataset"` — Ruff, Vulture, pydoclint, Tach, and ty passed; scoped tests **104 passed**.

Warnings in full test runs are existing dependency/deprecation and application warnings; no test failures remained.
