# PostgreSQL Averages, Ratios and Float Windows Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Averages, ratios and float-valued window metrics materialize on PostgreSQL and agree with the DuckDB reference within a stated tolerance, instead of refusing with `terrine.temporal.float64_unsupported`.

**Architecture:** PostgreSQL keeps every window quantity exact in `NUMERIC` and rounds to binary64 once, at output. Float facts enter exact state as the decimal value of their shortest round-trip text, so prefix differences cancel out-of-window history exactly. Quotients divide in `NUMERIC`. DuckDB's certified `checked_float64_v1` path is unchanged; PostgreSQL float outputs carry `numeric_policy=None` and no absolute-error column.

**Tech Stack:** Ibis 12, PostgreSQL 16 (`NUMERIC` accepts `NaN`/`Infinity`), DuckDB 1.5.6 reference.

**Spec:** owner direction 2026-10-21 ("they don't need to be byte for byte exact as long as they're within a reasonably tight tolerance"); `AGENTS.md` § Backend portability.

## Global Constraints

- DuckDB behavior, metadata and certificates do not change.
- Dialect-specific expressions live in `terrine/backends/numeric.py` (ruff TID251 enforces it).
- Refusal codes reuse existing ones: non-finite inputs and values outside binary64 range refuse with `terrine.temporal.float64_precision` (`failure=nonfinite_input` / `bound_overflow`).
- Error bound (stated in docs): a float-fact window sum is exact over values each within half an ulp of the source float, then rounded once, so its error is at most 2^-53·Σ|xᵢ| over the window plus one rounding; a quotient is rounded to at least 16 significant digits in `NUMERIC`, then once to binary64 (relative error below 1e-15).
- Parity tests compare to DuckDB with `math.isclose(rel_tol=1e-12, abs_tol=1e-300)`; counts and decimal sums stay exactly equal.
- Values whose magnitude is below 2.4703282292062328e-324 convert to 0.0 (PostgreSQL's float8 input rejects underflow to zero).

## Review Focus

1. A non-finite float before the window must refuse (DuckDB refuses on any non-finite history at or before the window end), not vanish through cancellation. Owned by Task 2.
2. A long history of large values with a small later window keeps its small window sum accurate (prefix differences must not cancel catastrophically). Owned by Task 2.
3. Zero denominators (empty average window, zero ratio denominator) yield NULL with the existing reason, never a division error. Owned by Task 1.
4. A quotient beyond binary64 range refuses rather than aborting the PostgreSQL transaction. Owned by Task 1.
5. A caller session with `extra_float_digits=0` cannot shorten the float text used for exact state. Owned by Task 2.

---

### Task 1: Exact NUMERIC quotients on PostgreSQL

**Files:** `terrine/backends/numeric.py`, `terrine/temporal/aggregate.py`, `terrine/temporal/prefix.py` (`_require_backend`), `integration/warehouse_execution/test_postgres_windows.py`

**Interfaces:**
- Produces (`terrine.backends.numeric`): `exact_decimal(coefficient, scale: int) -> ir.Value` (exact NUMERIC from a scaled coefficient); `exact_ratio(numerator, denominator) -> ir.Value` (NUMERIC quotient, NULL for a zero denominator); `exact_to_float(value) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]` = (float or NULL, nonfinite flag, out-of-range flag). PostgreSQL only; DuckDB raises `unavailable`.
- `_Quantity` gains `rounded: bool = False`: the value is an exact NUMERIC decimal that becomes float64 once, at output.

- [ ] Replace `test_quotients_and_floats_refuse_on_postgres` with `test_averages_and_ratios_match_duckdb_within_tolerance` (metrics `average_spend`, `spend_per_order`, windows including an empty and a zero-denominator window) and `test_out_of_range_quotient_is_a_coded_refusal`; run `make test-postgres`, expect failure.
- [ ] On non-DuckDB dialects `_quotient` returns a `rounded` quantity from `exact_ratio`; `aggregate_window` converts `rounded` quantities with `exact_to_float`, adding the two flags as `float64_precision` checks, and emits `numeric_policy=None`. `_require_backend(floating=True)` admits PostgreSQL.
- [ ] `make test-postgres`, `uv run pytest tests -q`, lint, typecheck pass; commit `feat: compute PostgreSQL averages and ratios exactly and round once`.

### Task 2: Float facts on PostgreSQL

**Files:** `terrine/temporal/daily.py` (`_sparse_group`), `terrine/temporal/aggregate.py` (`_component`), `terrine/execution/_postgres.py` (`owned_connection` sets `extra_float_digits = 1`), `integration/warehouse_execution/test_postgres_windows.py`

- [ ] Add `test_float_windows_match_duckdb_within_tolerance` (metric `weight`, with a history of `1e15` values before a window of `0.1` values) and `test_nonfinite_float_history_refuses` (a `NaN` row before the window); expect failure.
- [ ] PostgreSQL float facts aggregate `exact_sum(value.cast("string").cast(exact_type()))`; `_component` marks quantities of floating measures `rounded` on PostgreSQL. Owned connections set `extra_float_digits = 1`.
- [ ] All tiers pass; commit `feat: accumulate PostgreSQL float facts exactly`.

### Task 3: Documentation

**Files:** `docs/guides/backend-compatibility.md`, `CHANGELOG.md`

- [ ] Move averages, ratios and float windows from PostgreSQL's refused list to supported, state the error bound and `numeric_policy=None`; strict docs build passes; commit `docs: PostgreSQL float results and their tolerance`.
