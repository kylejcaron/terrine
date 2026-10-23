# Exact Float Windows on DuckDB Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** DuckDB float windows use exact state built from each value's shortest round-trip text, rounded to binary64 once, and averages/ratios divide those results within a stated error bound, as PostgreSQL and BigQuery already do; the certified-interval machinery (`checked_float64_v1`, absolute-error columns, certificate refusals, runtime certificate nodes) is removed.

**Architecture:** A float fact enters exact state as a `BIGNUM` coefficient at a data-derived decimal scale per prefix partition (unit), computed in the same query from the largest number of fractional digits any of that unit's values needs, so one tiny value only widens its own unit's coefficients; state is summed and prefix-differenced exactly, then rounded to binary64 through text (DuckDB's string-to-double is correctly rounded). A quotient normalizes each operand to a signed mantissa (1 ≤ |m| < 10) and a decimal exponent before dividing, so subnormal operands and sums beyond binary64 range still give an accurate quotient. Downstream cohort and LTV reductions keep their native binary64 arithmetic and gain explicit finite-result checks in place of the certificates. DuckDB then shares the exact path with PostgreSQL and BigQuery, and every certificate producer and consumer is deleted.

**Tech Stack:** DuckDB 1.5.6 `BIGNUM` (exact `+`, `-`, `SUM`; `*`, `/` and `pow` return `DOUBLE`, so coefficients are built by string padding), Ibis 12, Python 3.13.

**Spec:** owner decision 2026-10-23 on issue `kmc2` (below); evidence in `wk5v` (numerical-guarantee measurement); `AGENTS.md` § Numerical and data contracts, § Backend portability, § Complete changes, § Scope of fixes.

## Scope ledger

The owner was given this recommendation: *"switching DuckDB float windows to the exact-from-text method, and doing it before the first release … What it costs: The absolute-error column and certificate metadata go away. That touches the ColumnMeta invariant, Arrow metadata, benchmark result digests and about 5 test modules. Results shift by up to half a unit in the last place per input … Saved fixtures need regenerating. Very small values need a rule … The tolerance and denominator_interval_zero refusal kinds disappear."* The owner replied: **"ok lets do it"**.

| Capability | Source | Confirmed | Task |
| --- | --- | --- | --- |
| DuckDB float windows from exact shortest-text state with one rounding; averages and ratios within a stated bound | Owner reply to the recommendation above | Yes | 1, 2 |
| Rule for very small values on DuckDB: held exactly, no refusal | Recommendation item "very small values need a rule", accepted | Yes | 1 |
| Remove certificate metadata, error columns, certificate-only refusal kinds and their runtime nodes | Recommendation cost list, accepted | Yes | 3 |
| Keep cohort/LTV float reductions native, with explicit finite-result refusals replacing the certificate checks | Needed so removing certificates keeps today's overflow refusals (`AGENTS.md` § Complete changes); no semantic change | Yes (part of the accepted removal) | 2 |
| BigQuery: refuse float inputs needing more than 38 fractional digits instead of rounding them | Agent-found related defect | No; tracked as a separate owner question, no task | — |
| Compatibility decoding of saved v1 metadata carrying the removed fields | Not requested; package unreleased | No; no task | — |

## Global Constraints

- Measured facts (DuckDB 1.5.6, 2026-10-23): `CAST(DOUBLE AS VARCHAR)` is the shortest round-trip text (1M random values summed exactly matched Python `Fraction(repr(x))` sums); `CAST(VARCHAR AS DOUBLE)` rounds correctly for long and halfway-adjacent inputs; `BIGNUM` `SUM`/`+`/`-` are exact, `*`/`pow` return `DOUBLE`. A data-derived scale costs ~1.3 s per 1M-row prefix of cents-like data (DOUBLE 0.28 s); a fixed scale of 340 costs 22 s.
- DuckDB holds every finite binary64 exactly (`BIGNUM`); no input is rounded on entry. PostgreSQL and BigQuery entry rules are unchanged.
- Output contract on DuckDB: a window sum is `RN(exact sum of the values' shortest decimals)` (one rounding). An average or ratio `r` of exact quantities `p/q` satisfies `|r − p/q| ≤ 6·2⁻⁵³·|p/q| + 2⁻¹⁰⁷⁵` (Task 1 derives the constant for its algorithm and documents the derivation; tests use this form, which covers normal and subnormal results). PostgreSQL/BigQuery keep their exact-division rounding. Float outputs carry `numeric_policy=None` and no error column on every backend.
- Exact float state is never added across prefix partitions: cohort and LTV reductions consume published binary64 values.
- Refusal codes stay `terrine.temporal.float64_precision` (failures `nonfinite_input`, `bound_overflow`) and `terrine.temporal.exact_overflow`. Failure kinds `tolerance`, `denominator_interval_zero`, `count_limit`, `nonfinite_state`, `uncertifiable_cast`, the state-panel reason `ambiguous_state_bin`, and all their producers are deleted. Regenerate `docs/reference/refusals.md`.
- No compatibility shim: `ColumnMeta.absolute_error_column` and the `checked_float64_v1` literal are removed outright. Historical plan files under `docs/superpowers/plans/` stay as dated records.
- Study queries stay Ibis expressions; DuckDB text/scale/normalization helpers live in `terrine/backends/numeric.py`.
- Every commit passes its own gates: helpers keep backward-compatible signatures until their last caller migrates; nothing is deleted while a caller remains; tests of a changed representation migrate in the task that changes it.
- Base: branch `feat/exact-float-windows` stacked on `feat/audit-hardening` (04656d5), which awaits the owner's merge approval.

## Review Focus

1. A small window after a long history of large values returns its exact rounded sum instead of refusing (`wk5v` case: 1,000 history values ~U(0,10000), window `[1.25, 2.5, 3.16]`). Task 2.
2. One tiny value (5e-324) in one unit neither loses exactness nor slows or changes another unit's result. Tasks 1, 2.
3. Ratios with subnormal operands (`4.4e-323 / 5e-323` ≈ 0.88), subnormal results (`8.957174075507802e-308 / 4.44329165384767`), negative operands, and averages of values whose sum exceeds binary64 (mean of two `1.7976931348623157e308`) meet the bound, not refused. Task 1.
4. Cohort totals and cumulative LTV that overflow binary64 (two windows of `1e308`) refuse `bound_overflow`. Task 2.
5. Saved Arrow/JSON metadata and Dataset manifests round-trip without the removed fields; eligibility predicates and state-panel bins compare the rounded value. Task 3.

---

### Task 1: Exact float helpers

**Files:** Modify `terrine/backends/numeric.py`; Test `tests/backends/test_numeric.py` (new).

**Interfaces — Produces** (existing callers keep working: new parameters default to today's behavior):
- `float_scale(value: ir.Column, by: Sequence[ir.Column]) -> ir.IntegerColumn`: per partition `by`, the largest `fractional digits − exponent` of the non-null finite values' shortest text, floored at 0; literal 0 on PostgreSQL/BigQuery.
- `exact_float(value, scale: ir.IntegerValue | None = None) -> (exact, nonfinite, unrepresentable)`: DuckDB requires `scale` and returns the `BIGNUM` coefficient `sign || digits || repeat('0', scale − need)`, nonfinite flagged; `unrepresentable` always false on DuckDB.
- `exact_to_float(value, scale: ir.IntegerValue | int = 0)`: DuckDB rounds `coef || 'e-' || scale` through text; same `(float, nonfinite, out_of_range)` contract.
- `normalized(value, scale) -> (mantissa: float64, exponent: int64)` with `1 ≤ |mantissa| < 10` (0 for zero), sign kept; an already rounded binary64 operand normalizes from its shortest text. `rounded_quotient(n_mantissa, n_exponent, d_mantissa, d_exponent) -> (float64, overflow: bool)` divides mantissas in binary64 and applies the exponent difference by rounding text `{q:.17e}` with the adjusted exponent.

- [ ] **Step 0: Baseline cost.** Before any code change, record `agent-slots -j 1 -- uv run python -m benchmarks.run_overhead --repeats 5 --output /tmp/exactfloat-before.json` at 04656d5.
- [ ] **Step 1: Failing tests** (`tests/backends/test_numeric.py`, in-memory DuckDB): `test_exact_float_sums_match_shortest_decimal_sums` (`0.1, 0.2, -0.3, 5e-324, 1.7976931348623157e308, -0.0, 1e16, 1.0, -1e16` in groups; `RN(sum) == float(sum(Fraction(repr(x))))`); `test_float_scale_is_per_partition` (partition A `[1.5, 2.25e-3]` → 5, partition B `[1e20]` → 0); `test_quotient_meets_bound` parametrized over the Review Focus 3 cases plus `-7.5 / 2.5`, `7.5 / -2.5`, `-1e-320 / 3.0`, each asserting `|r − Fraction(p)/Fraction(q)| ≤ 6·2⁻⁵³·|p/q| + 2⁻¹⁰⁷⁵` and sign; `test_quotient_overflow_is_flagged` (`1e308 / 1e-10`); `test_exact_to_float_flags_overflow` (exact sum of two `1.7976931348623157e308`). Observe failures (`unavailable` on DuckDB).
- [ ] **Step 2: Implement** the DuckDB branches; document the quotient error derivation in the `rounded_quotient` docstring.
- [ ] **Step 3: Outlier cost check.** Throwaway script: 1M float events over 1,000 units through public `Design.build(model).materialize()` with and without one `5e-324` value in one unit (prototype the per-unit scale in a disposable copy of the daily state if Task 2 is not yet wired; otherwise run after Task 2 and record here). Record medians; if the outlier case is more than 3× the clean case, stop and report before Task 2 removes anything.
- [ ] **Step 4:** Tests pass; `make check TESTS="tests/backends/test_numeric.py"`. Commit `feat: exact shortest-decimal float helpers`.

### Task 2: Route DuckDB floats through the exact path

**Files:** Modify `terrine/temporal/{daily,prefix,aggregate}.py`, `terrine/compile/shapes/{cohorts,ltv}.py`; tests `tests/temporal/{test_window_aggregates,test_sparse_state}.py` (state-representation tests), `tests/compile/{test_cohorts,test_ltv}.py`, `integration/temporal/test_window_quotient_composition.py`, `integration/compile/test_column_runtime_contracts.py`, `integration/warehouse_execution/{test_incremental_equivalence,test_backend_capabilities}.py`.

**Interfaces — Consumes:** Task 1. **Produces:** `_Quantity` carries `scale: int | str` (a literal for decimals, the state column name for float facts) and `rounded: bool`; `_rounded_quotient` uses `normalized`/`rounded_quotient` on DuckDB for any mix of exact (`coefficient, scale`) and already rounded operands (float/count, float/float with different scales, decimal/count, ratios of `avg_event` measures). Float fact state = exact sum + `exact_scale` (per unit) + `float64_nonfinite_count`.

- [ ] **Step 1: Failing tests:** `test_long_history_small_window_is_exact` (Review Focus 1; `numeric_policy is None`, no error column); `test_tiny_value_does_not_change_other_units`; `test_nonfinite_history_refuses` (`nan` before the window → `nonfinite_input`); `test_mixed_scale_ratio_and_average_meet_bound` (float/count, float/float with scales 2 and 7, decimal/count, ratio of an `avg_event` measure, negative totals); `test_float_cohort_total_overflow_refuses` (two `1e308` windows → `bound_overflow`) in `test_cohorts.py` and the cumulative analogue in `test_ltv.py`. Observe failures.
- [ ] **Step 2:** `daily.py` routes every float fact through `exact_float(value, float_scale(value, by=[unit]))`; `prefix.py` carries `exact_scale`; `aggregate.py` drops the `_certifying` branch so every backend uses the exact arms; cohorts/LTV add a finite-result check (nonfinite total/mean/cumulative → `bound_overflow`) to their native reductions. The certified code stays in place, unused, until Task 3.
- [ ] **Step 3:** Migrate tests that consume the changed state or output representation (in the files above, including `test_sparse_state.py` state tests); keep standalone certificate-helper tests until Task 3. `make check TESTS="tests/temporal tests/backends tests/compile"`; non-live integration `uv run pytest integration -q -m 'not scale and not postgres and not bigquery and not snowflake'`; PostgreSQL live tier (`eval "$(uv run python -m tests.integration.postgres_scratch start)" && make test-postgres`); BigQuery live tier. Commit `feat: compute DuckDB float windows from exact state`.

### Task 3: Remove certificate machinery, metadata and documentation

**Files:** `terrine/data.py` (`ColumnMeta`: drop `absolute_error_column`; `numeric_policy: Literal["exact"] | None`), `terrine/temporal/{prefix,aggregate,daily}.py`, `terrine/backends/numeric.py` (`next_toward`, `ieee_*`), `terrine/compile/_predicates.py`, `terrine/compile/shapes/{cohorts,ltv,state_panel,_observations,time_to_event}.py` (incl. `ambiguous_state_bin`), `terrine/compile/columns.py`, `terrine/frame.py`, `terrine/export/surrogate.py`, `terrine/runtime/{graph,api}.py` (`prefix_certificate` nodes; prefixes connect straight to anchors), `benchmarks/run_overhead.py` (float oracle: sums equal `RN(Fraction sum)`; quotients meet the bound), `docs/guides/{temporal-engine,backend-compatibility,design-validation,state-panels}.md`, `docs/reference/refusals.md`; tests `tests/temporal/{test_sparse_state,test_prefix_lookup}.py`, `tests/compile/test_state_panel.py`, `tests/fixtures/test_baseline.py`, runtime planner tests.

- [ ] **Step 1: Failing tests:** `tests/compile/test_state_panel.py::test_float_bin_edge_uses_rounded_value`; extend the existing MaterializedFrame Arrow/JSON round-trip test with a float window column and assert the restored `ColumnMeta` equals the original and has no error column; runtime planner test asserting a design's graph has no `prefix_certificate` node.
- [ ] **Step 2:** Delete every certificate producer and consumer listed above, then the helpers (`outward_*`, `float64_certificate`, `validate_float64`, `float64_rounding_radius`, `float64_tolerance`, `_float64_conforms`, native float cumulative state, `next_toward`, `ieee_*`), keeping `float_is_finite` where filters use it; reduce `Float64Failure` to `nonfinite_input | bound_overflow` (BigQuery keeps its existing `bound_overflow` mapping). Update the benchmark oracle and the four guides (exact-from-text contract, quotient bound, rounded-value comparisons and bin edges); regenerate refusals with `uv run python -m tests.documentation`.
- [ ] **Step 3:** `rg -i 'checked_float64_v1|absolute_error_column|outward_|float64_certificate|prefix_certificate|denominator_interval_zero|uncertifiable_cast|ambiguous_state_bin|certif' terrine tests integration benchmarks examples docs/guides docs/reference` returns nothing that describes the removed machinery; `make check TESTS="tests"`; non-live integration; `uv run --group docs python -m tests.documentation --build`. Commit `refactor: remove certified float machinery`.

### Task 4: Measured cost, changelog and full verification

**Files:** `benchmarks/README.md`, `CHANGELOG.md`.

- [ ] **Step 1:** `agent-slots -j 1 -- uv run python -m benchmarks.run_overhead --repeats 5 --output /tmp/exactfloat-after.json`; table of warm medians and ranges vs Task 1 Step 0, plus Task 1 Step 3's outlier measurement, in `benchmarks/README.md`; no wall-clock CI gate.
- [ ] **Step 2:** CHANGELOG (breaking): DuckDB float windows are exact from shortest decimal text with one rounding; averages and ratios meet the stated bound; `absolute_error_column`, `checked_float64_v1` and `ambiguous_state_bin` removed; long histories no longer refuse; each input now enters as its shortest decimal (at most half an ulp from its binary value), and results no longer carry earlier floating-point accumulation error, so they can differ from earlier DuckDB results (e.g. `0.1 + 0.2 − 0.3` is `0.0`).
- [ ] **Step 3:** Unit, non-live integration, PostgreSQL and BigQuery live tiers, examples, lint, typecheck, docs build. Commit `docs: record exact float window cost and changes`.
