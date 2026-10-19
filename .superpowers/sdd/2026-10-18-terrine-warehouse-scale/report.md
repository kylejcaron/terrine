# Warehouse scale implementation report

## Work completed

- Added immutable `MetricQuantile`, `SketchRequest`, and `ApproxMetricQuantile` expression records; `quantile()` canonicalizes Decimal/string probabilities without context rounding and rejects binary floats or values outside `[0, 1]`.
- Added canonical Design JSON/YAML serialization and root/spec facade exports for quantile declarations. Serialized quantile probabilities and sketch rank-error values must be decimal strings.
- Added exact Hyndman–Fan type-7 window execution for the verified DuckDB backend, using exact Decimal rank/interpolation arithmetic for integer and Decimal facts; unsupported backends, source types and output precision refuse with stable codes before query execution.
- Added exact sparse range-window min/max/distinct-count execution through Catalog/Design/Frame; distinct values deduplicate across dates, and incomplete, empty, and all-null results retain distinct completeness/reason metadata. Nonadditive measure resolution is opt-in so the additive prefix-state resolver continues to refuse these operations.
- Added an independent NumPy type-7 oracle and ordinary Frame integration coverage, including cross-day duplicates, an intraday anchor, and half-open window edges.
- Kept approximate quantiles explicitly refused (`terrine.metric.sketch_unavailable`); no sketch package or algorithm was introduced because approval is owner-gated.
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
| Task 2 bounded range/last-event/retention/winsorization | Not started; its training-fitted winsorization dependency (Task 7.2 dataset recipe interfaces) is not present in this worktree. |
| Sketch merge/adapter implementation | Not implemented; remains owner-gated by the plan. |

## Verification

- `uv run ruff format terrine tests examples` — passed.
- `uv run ruff check terrine tests examples` — passed.
- `uv run ty check terrine tests` — passed.
- `uv run tach check` — passed.
- Focused nonadditive regressions after review fixes (`tests/compile/test_nonadditive.py`, `tests/spec/test_validate.py`, `tests/temporal/test_nonadditive.py`, and calendar-comparison quantile integration) — 67 passed.
- Focused precision regressions (`tests/temporal/test_nonadditive.py` and `tests/compile/test_nonadditive.py`) — 18 passed, including DECIMAL(29,25) refusal at probability 0.12345678.
- Required quality gates on `f78ae9f` and `a663ab3`: `uv run ruff format terrine tests examples`, `uv run ruff check terrine tests examples`, `uv run ty check terrine tests`, and `uv run tach check` — passed.

## Commits and review dispositions

- `57d1d85` — `Add exact quantile specification contracts`; roborev job `8773` found three medium findings.
- `e6655ab` — `Harden quantile probability specifications`; fixed all three findings with regression tests. roborev job `8778` passed; job `8773` was disposition-commented with the fix SHA and closed.
- `bfbbb9a` — `Execute exact nonadditive metric windows`; roborev job `8796` found ten exactness/temporal/compiler-integration findings.
- `f78ae9f` — `Harden exact nonadditive window execution`; fixed all ten `8796` findings, added regressions, and disposition-commented job `8796` with the fix SHA before closing it. Job `8812` then found an incomplete high-scale interpolation budget.
- `a663ab3` — `Refuse unsafe exact quantile arithmetic`; budgeted every interpolation intermediate and added high-scale precision-refusal tests. Job `8812` was disposition-commented with the fix SHA and closed; job `8815` passed and was disposition-commented/closed.

## Remaining

Task 1 execution is complete on the verified DuckDB backend; exact quantiles refuse on other backends/types/precision limits, and approximate sketches remain refused pending owner approval. Task 2 includes deterministic last-event ordering and temporal-layer retention bands, but bounded shape/range strategy selection and compiler integration remain. The current runtime safely keeps unsupported scan-cost estimates unknown and refuses strict unknown-scan budgets; no backend provides a verified bound/cap here, so estimate authorization beyond that safe refusal remains. The scale generator and an opt-in DuckDB count benchmark were added, but broad compiler-operation benchmarks and a live harness remain. Fitted winsorization depends on absent Task 7.2 training/scoring recipe contracts. No estimator/sampling work or paid warehouse action is in scope.

- `ee47d5f` — Added deterministic last-event ordering by timestamp then event ID, with duplicate-ID refusal. Focused last-event tests passed; required quality gates passed.
- `359a53e` — Fixed an internal alias collision; regression failed before fix. Roborev finding 8890 fixed and closed.
- `02b6fb5` — Preserved event fields matching last-event output names. Roborev finding 8891 fixed and closed.
- `f3b3ad9` / `777f025` / `8464111` / `19ec09a` — Added retention threshold bands and review corrections for UTC comparisons, exact missing-follow-up reasons, coded refusal, finite vs open-ended null coverage, and docstring accuracy. Findings 8895, 8901 and 8904 dispositioned/closed.
- `tests/temporal/test_retention.py` (4 passed), `tests/temporal/test_last_event.py` (4 passed), `tests/simulate/test_scale_generator.py` (4 passed); required Ruff format/check, ty and tach gates passed.
- `444627d` added seeded, versioned deterministic event batches, an independent unit-level target-count oracle and the explicit DuckDB benchmark command. Its initial review finding 8903 exposed zero-target units being dropped; fixed in `3524a09`, whose review 8905 passed.
- Final `uv run python -m benchmarks.run_duckdb --config benchmarks/workloads/small.json`: oracle match for 10,000 events and 100 unit counts, 1.078 seconds elapsed and 27,253,251 traced client peak bytes. Scan bytes/cost unknown; no live warehouse selected or run.
