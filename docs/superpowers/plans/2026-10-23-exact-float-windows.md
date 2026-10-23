# Exact Float Windows on DuckDB Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** DuckDB float windows, averages and ratios use exact state built from each value's shortest round-trip text, rounded to binary64 once, as PostgreSQL and BigQuery already do; the certified-interval machinery (`checked_float64_v1`, absolute-error columns, certificate refusals) is removed.

**Architecture:** A float fact enters exact state as a `BIGNUM` coefficient at one data-derived decimal scale per fact, computed in the same query from the largest number of fractional digits any of the fact's values needs; state is summed and prefix-differenced exactly, then rounded to binary64 through text (DuckDB's string-to-double is correctly rounded). Quotients divide the two correctly rounded operands. DuckDB then shares the exact path (`_rounded_quotient`, the exact arm of `_component`, `exact_to_float`) with PostgreSQL and BigQuery, and every certificate producer and consumer is deleted.

**Tech Stack:** DuckDB 1.5.6 `BIGNUM` (exact `+`, `-`, `SUM`; `*`, `/` and `pow` return `DOUBLE`, so coefficients are built by string padding), Ibis 12, Python 3.13.

**Spec:** owner decision 2026-10-23 on issue `kmc2` ("ok lets do it", accepting the recommendation to switch DuckDB float windows to the exact-from-text method before the first release, in its own reviewed plan); evidence in `wk5v` (numerical-guarantee measurement); `AGENTS.md` § Numerical and data contracts, § Backend portability, § Complete changes.

## Scope ledger

| Capability | Source | Confirmed | Task |
| --- | --- | --- | --- |
| DuckDB float windows/averages/ratios from exact shortest-text state, one rounding | Owner: "ok lets do it" (on the exact-from-text recommendation) | Yes | 1, 2 |
| Remove certificate metadata, error columns and certificate-only refusal kinds | Same; listed as accepted costs in the recommendation (error column and certificate metadata disappear, fixtures regenerate, refusal kinds disappear) | Yes | 3 |
| BigQuery: refuse float inputs needing more than 38 fractional digits instead of rounding them | `AGENTS.md` § Scope of fixes: related defect in the same exact-from-text path (a `1e-300` denominator rounds to zero and reads as a zero denominator) | Repository rule | 1 |
| Compatibility decoding of saved v1 metadata that carries the removed fields | Not requested; package unreleased; clean cutover | No; no task | — |

## Global Constraints

- Measured facts this plan relies on (DuckDB 1.5.6, 2026-10-23): `CAST(DOUBLE AS VARCHAR)` is the shortest round-trip text (1M random values summed exactly matched Python `Fraction(repr(x))` sums); `CAST(VARCHAR AS DOUBLE)` rounds correctly for long and halfway-adjacent inputs; `BIGNUM` `SUM`/`+`/`-` are exact, `*`/`pow` return `DOUBLE`. A per-fact data-derived scale costs ~1.3 s per 1M rows for a prefix of cents-like data (DOUBLE: 0.28 s); a fixed scale of 340 costs 22 s, so the scale must be data-derived.
- Exact or refuse: no input is rounded on entry. DuckDB `BIGNUM` holds every finite binary64 exactly; PostgreSQL `NUMERIC` likewise; BigQuery refuses inputs needing more than 38 fractional digits or magnitude ≥ about 5.7e38.
- Output contract: a window sum is `RN(exact sum of the values' shortest decimals)`; an average/ratio is `RN(RN(p) / RN(q))` on DuckDB (relative error ≤ 3·2⁻⁵³ plus subnormal absolute 2⁻¹⁰⁷⁵) and the existing exact-division rounding on PostgreSQL/BigQuery. Float outputs carry `numeric_policy=None` and no error column on every backend.
- Refusal codes stay `terrine.temporal.float64_precision` (failures `nonfinite_input`, `bound_overflow`, new `unrepresentable_input`) and `terrine.temporal.exact_overflow`; delete failure kinds `tolerance`, `denominator_interval_zero`, `count_limit`, `nonfinite_state`, `uncertifiable_cast` and every producer. Regenerate `docs/reference/refusals.md`.
- No compatibility shim: `ColumnMeta.absolute_error_column` and the `checked_float64_v1` literal are removed outright (package unreleased). Historical plan files under `docs/superpowers/plans/` stay as dated records.
- Study queries stay Ibis expressions; the DuckDB text/scale helpers live in `terrine/backends/numeric.py`.
- Base: branch `feat/exact-float-windows` stacked on `feat/audit-hardening` (04656d5), which awaits the owner's merge approval.

## Review Focus

1. A small window after a long history of large values returns its exact rounded sum instead of refusing (`wk5v` long-history case: k=100,000 events ~U(0,10000), window sum 6.91). Task 2.
2. One tiny value (5e-324) among ordinary values neither loses exactness nor changes other units' results (scale is per fact, results scale-independent). Task 1 (helper), Task 2 (window).
3. A ratio whose exact denominator is nonzero but rounds to 0.0, or whose quotient overflows, refuses `bound_overflow`; an exact zero denominator stays a `zero_denominator` null. Task 1.
4. NaN/±inf anywhere in a non-empty window's history refuses `nonfinite_input`, as today. Task 2.
5. Saved Arrow/JSON metadata and Dataset manifests round-trip without the removed fields; eligibility predicates and state-panel bins on float outputs compare the rounded value. Task 3.

---

### Task 1: Exact float helpers

**Files:** Modify `terrine/backends/numeric.py`; Test `tests/backends/test_numeric.py` (new).

**Interfaces — Produces:**
- `float_scale(value: ir.Column) -> ir.IntegerScalar`: the largest `fractional digits − exponent` over non-null finite values' shortest text, floored at 0; DuckDB only (PostgreSQL/BigQuery return literal 0).
- `exact_float(value: ir.Value, scale: ir.IntegerValue) -> tuple[ir.Value, ir.BooleanValue, ir.BooleanValue]` → `(exact, nonfinite, unrepresentable)`. DuckDB: `BIGNUM` coefficient `sign || digits || repeat('0', scale − need)`; BigQuery: `unrepresentable` also when `need > 38`.
- `exact_to_float(value: ir.Value, scale: ir.IntegerValue | int = 0)`: DuckDB rounds `coef || 'e-' || scale` through text; same `(float, nonfinite, out_of_range)` contract.
- `exact_ratio` on DuckDB: `RN(p)/RN(q)` with a flag when `q` is exactly nonzero but `RN(q) == 0` or the quotient is infinite.

- [ ] **Step 0: Baseline cost.** Before any code change, record `agent-slots -j 1 -- uv run python -m benchmarks.run_overhead --repeats 5 --output /tmp/exactfloat-before.json` at 04656d5; Task 4 compares against it.
- [ ] **Step 1: Failing tests** in `tests/backends/test_numeric.py` on an in-memory DuckDB table: `test_exact_float_sums_match_shortest_decimal_sums` (values `0.1, 0.2, -0.3, 5e-324, 1.7976931348623157e308, -0.0, 1e16, 1.0, -1e16`; per-group `RN(sum)` equals `float(sum(Fraction(repr(x))))`); `test_float_scale_is_largest_fractional_need` (`[1.5, 2.25e-3, 1e20] → 5`); `test_exact_ratio_flags_denominator_that_rounds_to_zero`; `test_exact_to_float_flags_overflow` (sum of two `1.7976931348623157e308` → out_of_range). Run `uv run pytest -n 0 tests/backends/test_numeric.py`; expect failures (`unavailable` refusal on DuckDB).
- [ ] **Step 2: Implement** the DuckDB branches above (regex split of the shortest text into sign, integer digits, fraction digits, exponent); BigQuery `need > 38` → `unrepresentable`.
- [ ] **Step 3:** Tests pass; `make check TESTS="tests/backends/test_numeric.py"`. In `terrine/temporal/aggregate.py` map `float64_unrepresentable_count` to failure `unrepresentable_input` (add it to `Float64Failure` in `prefix.py`). BigQuery live tier `uv run --extra duckdb --extra bigquery pytest integration -m bigquery -n 8 -q` with `test_bigquery_parity.py::test_float_window_failures_are_coded_refusals` extended by a `1e-39` weight → `unrepresentable_input` (the existing `1e39` case also becomes `unrepresentable_input`; red-check the `1e-39` case). Commit `feat: exact shortest-decimal float state on DuckDB`.

### Task 2: Route DuckDB floats through the exact path

**Files:** Modify `terrine/temporal/{daily,prefix,aggregate}.py`; rewrite `tests/temporal/test_window_aggregates.py`, `tests/temporal/test_sparse_state.py` (delete certificate tests), `integration/temporal/test_window_quotient_composition.py`, `integration/compile/test_column_runtime_contracts.py`, `integration/warehouse_execution/test_incremental_equivalence.py`, `integration/warehouse_execution/test_backend_capabilities.py`.

**Interfaces — Consumes:** Task 1 helpers. **Produces:** float window outputs on every backend from `_rounded_quotient` / the exact arm of `_component`; daily/prefix state for float facts = exact sum + `exact_scale` (constant per fact) + `float64_nonfinite_count`/`float64_unrepresentable_count`.

- [ ] **Step 1: Failing tests** (DuckDB, `tests/temporal/test_window_aggregates.py`): `test_long_history_small_window_is_exact` (1,000 history values `x_i` ~ seeded U(0,10000) one day before a window of `[1.25, 2.5, 3.16]`; expect `RN(Fraction sum of reprs)` exactly, `numeric_policy is None`, no error column); `test_tiny_value_does_not_change_other_units` (unit A gets `5e-324`; unit B's window equals its value without A); `test_nonfinite_history_refuses` (`nan` before window → `float64_precision`/`nonfinite_input`); `test_float_average_and_ratio_within_three_ulp` against Fraction references. Observe failures (`tolerance` refusal / error column present).
- [ ] **Step 2:** In `daily.py` route all float facts through `exact_float` with `float_scale` over the fact's filtered source values; carry `exact_scale` through `prefix.py`; in `aggregate.py` remove `_certifying` and its DuckDB arm, `_float_quantity`, `_check_quantity`, the floating arm of `_component`, the final tolerance check and error-column emission; pass `exact_scale` to `exact_to_float`.
- [ ] **Step 3:** Delete now-dead certificate code in `prefix.py` (`outward_*`, `float64_certificate`, `validate_float64`, `float64_rounding_radius`, `float64_tolerance`, `_float64_conforms`, native float cumulative state) and `numeric.py` (`next_toward`, `ieee_*`); keep `float_is_finite` where filters use it. Reduce `Float64Failure` to `nonfinite_input | bound_overflow | unrepresentable_input`.
- [ ] **Step 4:** `make check TESTS="tests/temporal tests/backends"`; `uv run pytest integration -q -m 'not scale and not postgres and not bigquery and not snowflake'`; PostgreSQL live tier (`eval "$(uv run python -m tests.integration.postgres_scratch start)" && make test-postgres`); BigQuery live tier. Commit `feat: compute DuckDB float windows from exact state`.

### Task 3: Remove certificate metadata and consumers

**Files:** Modify `terrine/data.py` (`ColumnMeta`: drop `absolute_error_column`; `numeric_policy: Literal["exact"] | None`), `terrine/compile/_predicates.py`, `terrine/compile/shapes/{cohorts,ltv,state_panel,_observations,time_to_event}.py`, `terrine/compile/columns.py`, `terrine/frame.py`, `terrine/export/surrogate.py`; tests `tests/compile/{test_cohorts,test_ltv,test_state_panel}.py`, `tests/fixtures/test_baseline.py`, `tests/temporal/test_prefix_lookup.py` (rename fixture columns).

- [ ] **Step 1: Failing tests:** `tests/compile/test_cohorts.py::test_float_cohort_mean_matches_exact_reference` (cohort mean of float windows, no error column, value within 3 ulp of Fraction reference); `tests/compile/test_state_panel.py::test_float_bin_edge_uses_rounded_value` (value exactly on an edge bins by the rounded value); extend the existing MaterializedFrame Arrow/JSON round-trip test with a float window column and assert the restored `ColumnMeta` equals the original.
- [ ] **Step 2:** Delete every `checked_float64_v1` branch, error-column propagation and certificate tolerance check in the listed files; update `ColumnMeta.__post_init__` validation.
- [ ] **Step 3:** `make check TESTS="tests"`; non-live integration tier; grep proves no `checked_float64_v1|absolute_error_column|outward_|float64_certificate` remains in `terrine/ tests/ integration/ benchmarks/ examples/ docs/guides docs/reference`. Commit `refactor: remove certified float metadata`.

### Task 4: Documentation, benchmark oracle and measured cost

**Files:** `docs/guides/temporal-engine.md` (replace the checked-Float64 section with the exact-from-text contract and bound), `docs/guides/backend-compatibility.md` (one float contract; BigQuery 38-digit refusal), `docs/reference/refusals.md` (regenerate), `benchmarks/run_overhead.py` (float oracle: sums equal `RN(Fraction sum)` exactly; quotients within 3 ulp), `benchmarks/README.md` (before/after table), `CHANGELOG.md`.

- [ ] **Step 1:** Run the harness after Task 3 (`--output /tmp/exactfloat-after.json`) and compare with Task 1's baseline: warm medians and ranges per case; no wall-clock CI gate.
- [ ] **Step 2:** Update docs and changelog (breaking: `absolute_error_column` removed, `checked_float64_v1` gone, float results may differ by up to ½ ulp per input from earlier DuckDB results, e.g. `0.1 + 0.2 − 0.3` is `0.0`); `uv run python -m tests.documentation`; `uv run --group docs python -m tests.documentation --build`.
- [ ] **Step 3:** Full verification: unit, non-live integration, PostgreSQL and BigQuery live tiers, examples, lint, typecheck. Commit `docs: describe exact float windows`.
