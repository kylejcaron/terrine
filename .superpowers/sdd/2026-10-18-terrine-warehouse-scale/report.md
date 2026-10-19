# Warehouse scale implementation report

## Work completed

- Added immutable `MetricQuantile`, `SketchRequest`, and `ApproxMetricQuantile` expression records; `quantile()` canonicalizes Decimal/string probabilities without context rounding and rejects binary floats or values outside `[0, 1]`.
- Added canonical Design JSON/YAML serialization and root/spec facade exports for quantile declarations. Serialized quantile probabilities and sketch rank-error values must be decimal strings.
- Added exact Hyndman–Fan type-7 window execution for the verified DuckDB backend, using exact Decimal rank/interpolation arithmetic for integer and Decimal facts; unsupported backends, source types and output precision refuse with stable codes before query execution.
- Added exact sparse range-window min/max/distinct-count execution through Catalog/Design/Frame; distinct values deduplicate across dates, and incomplete, empty, and all-null results retain distinct completeness/reason metadata. Nonadditive measure resolution is opt-in so the additive prefix-state resolver continues to refuse these operations.
- Added an independent NumPy type-7 oracle and ordinary Frame integration coverage, including cross-day duplicates, an intraday anchor, and half-open window edges.
- Added the optional DataSketches 5.2.0 extra and adapter-only t-digest/HLL operations in `terrine/temporal/sketches.py`. T-digest records `k`, version, canonical partition merge order, approximate exactness and no guaranteed error bound; HLL records its published relative-error bound. These helpers are not yet wired into Design validation or compilation, which still refuses approximate execution.
- Added `examples/nonadditive_metrics.py`; updated the temporal guide and changelog with supported execution and refusal contracts.

## Acceptance coverage

| Plan Task 1 criterion | Status |
| --- | --- |
| Decimal probability canonicalization, range validation, and binary-float refusal | Covered by `tests/temporal/test_nonadditive.py` |
| Exact type-7 rank/interpolation and exact integer/Decimal arithmetic | Covered by `tests/temporal/test_nonadditive.py` and DuckDB execution test |
| Design JSON/YAML round-trip and canonical hash | Covered by `tests/spec/test_nonadditive_spec.py` |
| Approximation explicit; no fallback; stable owner-gated sketch refusal | Covered by `tests/spec/test_sketch_refusal.py` and temporal refusal test |
| Exact quantile through Catalog/Design/Frame and independent NumPy oracle | Covered by `tests/compile/test_nonadditive.py` and `tests/reference/nonadditive.py`; regression coverage also checks sub-mill probability precision, fractional-cent interpolation, UInt64, nonadditive source facts, and explicit precision refusals |
| DuckDB backend capability gate and coded unsupported-backend refusal | Covered by `tests/spec/test_validate.py`, including mixed-case DuckDB acceptance |
| Sparse exact min/max/count-distinct windows, deduplication, null/empty distinction and half-open intraday edges | Covered by `tests/compile/test_nonadditive.py`, including collision-safe fact projection and naive-UTC behavior under non-UTC session timezone |
| Nonadditive requests do not alter additive sparse-state resolver admission | Covered by `tests/temporal/test_sparse_state.py`; persisted quantile composition uses raw events without additive prefix layers |
| Sequential/calendar outcome horizons and nonadditive metric-kind compatibility | Covered by `tests/spec/test_validate.py` and calendar-comparison quantile integration |
| Task 2 bounded range/last-event/retention/winsorization | Deterministic last-event ordering and temporal-layer retention bands are implemented; bounded shape/range strategy selection and compiler integration remain. Fitted winsorization depends on Task 7.2 dataset recipe interfaces, which are absent. |
| Sketch merge/adapter implementation | Adapter tests cover t-digest accuracy/determinism across a fresh process and HLL bound/merge behavior. Public Design validation/compile execution remains refused; no end-to-end approximate request is complete. |

## Verification

- `uv run ruff format terrine tests examples` — passed.
- `uv run ruff check terrine tests examples` — passed.
- `uv run ty check terrine tests` — passed.
- `uv run tach check` — passed.
- Optional sketch adapter tests (`tests/temporal/test_sketches.py`) — 5 passed, including fixed-seed type-7 accuracy tolerance, canonical t-digest merge across a fresh process, HLL published error bound and partition-union order independence.
- After the adapter commit, `tests/spec/test_sketch_refusal.py` and `tests/spec/test_validate.py` — 52 passed. Ruff format/check, ty and tach gates — passed.
- Focused nonadditive regressions after review fixes (`tests/compile/test_nonadditive.py`, `tests/spec/test_validate.py`, `tests/temporal/test_nonadditive.py`, and calendar-comparison quantile integration) — 67 passed.
- Focused precision regressions (`tests/temporal/test_nonadditive.py` and `tests/compile/test_nonadditive.py`) — 18 passed, including DECIMAL(29,25) refusal at probability 0.12345678.
- Required quality gates on `f78ae9f` and `a663ab3`: `uv run ruff format terrine tests examples`, `uv run ruff check terrine tests examples`, `uv run ty check terrine tests`, and `uv run tach check` — passed.

- Follow-up to review 8914: corrected the stale constructor refusal test, made sketch-adapter tests skip only when DataSketches is absent, added a CI sketches-extra job, capped extreme t-digest tolerances before unsafe conversion, and streamed both single-input and partitioned updates in bounded batches. Review 8915 passed for the verification record.
- Step 1 gates after these changes: `uv run ruff format terrine tests examples`, `uv run ruff check terrine tests examples`, `uv run ty check terrine tests`, `uv run tach check`, and focused sketch/nonadditive tests — passed (12 tests).

## Commits and review dispositions

- `57d1d85` — `Add exact quantile specification contracts`; roborev job `8773` found three medium findings.
- `e6655ab` — `Harden quantile probability specifications`; fixed all three findings with regression tests. roborev job `8778` passed; job `8773` was disposition-commented with the fix SHA and closed.
- `bfbbb9a` — `Execute exact nonadditive metric windows`; roborev job `8796` found ten exactness/temporal/compiler-integration findings.
- `f78ae9f` — `Harden exact nonadditive window execution`; fixed all ten `8796` findings, added regressions, and disposition-commented job `8796` with the fix SHA before closing it. Job `8812` then found an incomplete high-scale interpolation budget.
- `a663ab3` — `Refuse unsafe exact quantile arithmetic`; budgeted every interpolation intermediate and added high-scale precision-refusal tests. Job `8812` was disposition-commented with the fix SHA and closed; job `8815` passed and was disposition-commented/closed.

## Remaining

- Task 1 exact nonadditive execution remains complete on DuckDB. Approximate quantiles and distinct counts now execute end to end from cross-section Design through Frame: DuckDB native HLL for approximate distinct, and a DataSketches t-digest sidecar fed by canonical-ordered Arrow batches for approximate quantiles. Frame Arrow/JSON metadata records algorithm, implementation/version, parameters, and error description. Sketch-extra absence and unsupported shapes refuse before source queries. Runtime composition refuses t-digest sidecars on this branch because the runtime execution-context hook is not yet available; ordinary Frame materialization is the route forward. Other remaining scope: bounded shape/range strategy selection, runtime scan-cost estimates, broader/live benchmark evidence, and fitted winsorization requiring absent Task 7.2 interfaces.

- `ee47d5f` — Added deterministic last-event ordering by timestamp then event ID, with duplicate-ID refusal. Focused last-event tests passed; required quality gates passed.
- `359a53e` — Fixed an internal alias collision; regression failed before fix. Roborev finding 8890 fixed and closed.
- `02b6fb5` — Preserved event fields matching last-event output names. Roborev finding 8891 fixed and closed.
- `f3b3ad9` / `777f025` / `8464111` / `19ec09a` — Added retention threshold bands and review corrections for UTC comparisons, exact missing-follow-up reasons, coded refusal, finite vs open-ended null coverage, and docstring accuracy. Findings 8895, 8901 and 8904 dispositioned/closed.
- `tests/temporal/test_retention.py` (4 passed), `tests/temporal/test_last_event.py` (4 passed), `tests/simulate/test_scale_generator.py` (4 passed); required Ruff format/check, ty and tach gates passed.
- `444627d` added seeded, versioned deterministic event batches, an independent unit-level target-count oracle and the explicit DuckDB benchmark command. Its initial review finding 8903 exposed zero-target units being dropped; fixed in `3524a09`, whose review 8905 passed.
- Final `uv run python -m benchmarks.run_duckdb --config benchmarks/workloads/small.json`: oracle match for 10,000 events and 100 unit counts, 1.078 seconds elapsed and 27,253,251 traced client peak bytes. Scan bytes/cost unknown; no live warehouse selected or run.
- `3c9912a` updates the temporal guide to describe the optional adapter package without implying that approximate requests are supported through Design; required formatting, lint, type, and import-layer gates passed, and review 8921 passed.

- Approximate metric review corrections: sidecar outputs now preserve incomplete-window nulls, register deferred unknown-filter checks, and resolve every public alias of a shared quantile expression. Approximate-distinct expressions also contribute to sequential/calendar outcome horizons and runtime snapshot source discovery. Missing-extra and output-order tests use the actual import cache and explicit key sorting; t-digest execution tests skip when the optional extra is absent.
- Verification after review corrections: Ruff format/check, `uv run ty check terrine tests`, and `uv run tach check` passed. `uv run --extra sketches pytest -q tests/temporal tests/spec tests/compile tests/runtime/test_snapshot.py -n 8` passed (536 tests).
