# Ibis-Built Study Queries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Study queries in `terrine/compile/` and `terrine/temporal/` contain no raw SQL and no native-function bindings; every operation Ibis lacks or mistranslates lives in one backend-dispatched package, `terrine/backends/`.

**Architecture:** Three focused modules in `terrine/backends/` own all dialect-specific expression code: dialect resolution, numeric primitives and timestamp primitives. Where Ibis already translates exactly on DuckDB and PostgreSQL (truncating `%`, `Timestamp.delta(..., unit=...)`, `ibis.range(...).unnest()`), the native binding is replaced by plain Ibis. Raw SQL in anchors and state panels becomes Ibis expressions, `Model._sql` is deleted, and ruff bans `ibis.udf`/`ibis.to_sql` outside the allowed packages so the rule stays enforced.

**Tech Stack:** Python 3.13, Ibis 12, DuckDB 1.5.6, PostgreSQL 16, ruff (TID251), tach.

**Spec:** `AGENTS.md` § Backend portability (owner direction 2026-10-21, kata `0qm5`).

## Global Constraints

- Behavior-preserving on DuckDB and PostgreSQL: every existing unit, integration and `make test-postgres` test passes unchanged except for import paths.
- Refusal codes and their context keys do not change; `uv run python -m tests.documentation` regenerates `docs/reference/refusals.md` if a spec moves.
- DuckDB float certification (`nextafter`, IEEE builtins, `checked_float64_v1`) stays DuckDB-only; it moves, it is not ported.
- Dialect fallthrough in a helper raises `terrine.backend.capability_unavailable` (existing spec in `terrine/backends/capabilities.py`) with `operation=<helper name>`.
- Measured facts this plan relies on (probed 2026-10-22): Ibis `%` truncates (`-7 % 2 == -1`) on DuckDB/PostgreSQL; Ibis `//` floors through doubles (inexact, not usable); DuckDB `delta(epoch, unit="microsecond")` equals `epoch_us` exactly through year 290000, `unit="nanosecond"` equals `epoch_ns`; `ibis.range(0, n).unnest().name("ordinal").as_table()` runs on DuckDB and PostgreSQL.

## Review Focus

1. Non-UTC session time zone on the caller's DuckDB connection: UTC wall-clock/instant conversions and anchor identity text must not change. Owned by Task 1 (unit test under `SET TimeZone='America/New_York'`).
2. Naive `timestamp` source columns on PostgreSQL (all current live probes use `timestamptz`): anchors and windows must match DuckDB. Owned by Task 2 (parametrize the live shapes probe).
3. Pre-1970 and late-year instants after replacing `epoch_us`/`mod` with Ibis `delta`/`%`: struct keys keep truncated seconds and signed remainders. Covered by existing `tests/temporal` key tests and `test_postgres_timestamps.py`.
4. Sub-microsecond `TIMESTAMP_NS` values still refuse with `terrine.compile.anchor_timestamp_precision` after the precision check moves to Ibis. Covered by existing `tests/compile/test_anchors.py`.
5. Unbound literal operands (cohort origin, Schedule bounds) resolve to the compiling dialect, not DuckDB, when the other operand is bound to PostgreSQL. Covered by the live cohort and schedule probes.

---

### Task 1: Backend-dispatched expression seam

**Files:**
- Create: `terrine/backends/dialect.py`, `terrine/backends/numeric.py`, `terrine/backends/timestamps.py`
- Delete: `terrine/temporal/_timestamps.py`
- Modify: `terrine/temporal/prefix.py`, `terrine/temporal/daily.py`, `terrine/temporal/aggregate.py`, `terrine/compile/shapes/time_to_event.py`, `terrine/compile/columns.py`, every importer of `terrine.temporal._timestamps` (31 files incl. tests), `tach.toml`
- Test: `tests/backends/test_expressions.py`

**Interfaces:**
- Produces (`terrine.backends.dialect`): `dialect_scope(dialect: str) -> ContextManager[None]`; `dialect_of(*values: ir.Value) -> str` (the enclosing scope's dialect when one is set, else the first bound backend's name inferred from the operands, default `"duckdb"`); `unavailable(operation: str, dialect: str) -> TerrineError`.
- Produces (`terrine.backends.numeric`): `exact_type()`, `is_exact_type(dtype)`, `exact_literal(value: int)`, `exact_sum(value)`, `exact_subtract(left, right)`, `truncating_quotient(value, divisor, *, like: ir.Value) -> ir.IntegerValue`, `float_is_finite(value) -> ir.BooleanValue`, DuckDB-only `next_toward(value, direction)`, `ieee_add/ieee_subtract/ieee_multiply/ieee_divide(left, right)`.
- Produces (`terrine.backends.timestamps`): everything `_timestamps.py` exported (`timestamp_parts`, `timestamp_order_key`, `compare_timestamps`, `_struct_key`, `_backend` → replaced by `dialect_of`), plus `timestamp_is_finite(value)`, `utc_wall_clock(value: aware) -> naive`, `utc_instant(value: naive) -> aware`. `epoch_us_sql` is removed in Task 2.

- [ ] **Step 1: Write the failing test** `tests/backends/test_expressions.py::test_utc_conversions_ignore_the_session_time_zone`: DuckDB connection with `SET TimeZone='America/New_York'`, table of aware `2025-03-09 06:30:00+00` and naive `2025-03-09 06:30:00`; assert `utc_wall_clock(aware)` executes to naive `2025-03-09 06:30:00` and `utc_instant(naive)` to aware `2025-03-09 06:30:00+00:00`.
- [ ] **Step 2: Run** `uv run pytest tests/backends/test_expressions.py -q` — expect ImportError.
- [ ] **Step 3: Create the three modules** by moving code: dialect ContextVar from `prefix.exact_dialect`; exact/float primitives from `prefix.py`; integer quotient and timestamps from `_timestamps.py`; `timezone` builtins from `daily.py`/`time_to_event.py` as `utc_wall_clock`/`utc_instant` (DuckDB and PostgreSQL both spell it `timezone('UTC', x)`). Replace `_remainder` with Ibis `%`, and `_epoch_us`/`_epoch_ns` in `_struct_key` with `value.delta(epoch, unit="microsecond"|"nanosecond")`. `compile.columns.exact_scope` uses `dialect_scope`.
- [ ] **Step 4: Rewrite imports** with `xd://ast_edit` (`from terrine.temporal._timestamps import $$$A` → `from terrine.backends.timestamps import $$$A`) and delete `_timestamps.py`; add `"terrine.backends"` to tach `depends_on` of `terrine.temporal`, `terrine.compile`, `terrine.compile.shapes` (and any other module tach reports).
- [ ] **Step 5: Run** `uv run pytest tests -q`, `make lint`, `make typecheck`, and with the scratch server `make test-postgres`; all pass.
- [ ] **Step 6: Commit** `refactor: move backend-specific study expressions into terrine.backends`.

### Task 2: Raw SQL out of anchors and state panels

**Files:**
- Modify: `terrine/compile/anchors.py` (`_utc_sql`, `_precision_check`, `_utc_column`, Schedule series, event identity text), `terrine/compile/shapes/_observations.py` (on-grid flag, `gap`), `terrine/model.py` (delete `_sql`), `terrine/backends/timestamps.py` (delete `epoch_us_sql`)
- Test: `integration/warehouse_execution/test_postgres_shapes.py`

**Interfaces:**
- Consumes: `utc_instant`, `utc_wall_clock`, `dialect_of` from Task 1.

- [ ] **Step 1: Extend the live probe**: parametrize `_both`/`test_design_matches_duckdb` over `timestamp_type in ("timestamptz", "timestamp")` (DuckDB `TIMESTAMPTZ`/`TIMESTAMP`) for designs `schedule`, `first_event_anchor`, `observed_state_panel`; assert PostgreSQL rows equal DuckDB rows.
- [ ] **Step 2: Run** `make test-postgres` — record whether the naive cases pass before the rewrite (they exercise the old `AT TIME ZONE` path); either result is kept as the baseline.
- [ ] **Step 3: Rewrite** with Ibis only: `_utc_column` → `table.mutate(**{output: utc_instant(table[column])})`; `_precision_check` → `table.filter(col.notnull() & (col.delta(epoch_ns, unit="nanosecond") % 1000 != 0)).aggregate(invalid_count=ibis._.count())` (drop the DuckDB-only refusal there: the check is now portable); Schedule → `ibis.range(0, stop).unnest().name("ordinal").as_table()`; identity text → `utc_wall_clock(column).cast("string")`; state panels → `point_time.delta(start, unit="microsecond") % period_us == 0` and `gap = to_time.delta(from_time, unit="microsecond")`. Delete `_utc_sql`, `_quote` if unused, `Model._sql`, `epoch_us_sql`.
- [ ] **Step 4: Run** `uv run pytest tests -q`, `uv run pytest integration -q`, `make test-postgres`; all pass.
- [ ] **Step 5: Commit** `refactor: build anchors and state panels from Ibis expressions`.

### Task 3: Enforce the rule

**Files:**
- Modify: `pyproject.toml` (ruff `TID251` banned-api `ibis.udf`, `ibis.to_sql`; per-file-ignores for `terrine/backends/**`, `terrine/execution/**`, `terrine/export/**`), `AGENTS.md` (§ Backend portability names `terrine/backends/` and the lint), `docs/reference/refusals.md` if regenerated

- [ ] **Step 1: Red check**: add the ban, temporarily re-add one `ibis.udf.scalar.builtin` in `terrine/temporal/daily.py`, run `uv run ruff check terrine` — expect TID251; remove it.
- [ ] **Step 2: Run** `make lint`, `make typecheck`, `uv run python -m tests.documentation`, strict docs build; all pass.
- [ ] **Step 3: Commit** `build: ban native bindings and raw SQL outside backend packages`.
