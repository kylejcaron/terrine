# Snowflake Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Frame.materialize()` runs on an Ibis Snowflake connection with the PostgreSQL and BigQuery slices' coverage, matching DuckDB (exact for counts and decimal sums, within the documented tolerance for averages, ratios and floats), validated by a live CI job.

**Architecture:** Snowflake's READ COMMITTED isolation is statement-level even inside a transaction, so a transaction gives no multi-statement snapshot. Instead each materialization reads `CURRENT_TIMESTAMP()` once and pins every table and view reference with Time Travel `AT(TIMESTAMP => <t>)`, rewriting the compiled SQL through sqlglot in `terrine/execution/_snowflake.py`; because Time Travel follows the object rather than the name, the session re-reads every admitted relation's `CREATED`/`LAST_DDL` after each query and refuses on any change. Queries are plain `SELECT`s on the caller's connection: no `USE`, `ALTER SESSION`, transaction or temporary object is issued, and the session's `TIMEZONE` must be `UTC` (Ibis sets it at connect). Dialect differences live in `terrine/backends/` helpers. Exact state is `NUMBER(38, 0)` (Snowflake errors on overflow); float facts follow the DuckDB design (a coefficient at a per-unit data-derived scale, built from the shortest round-trip text, with quotients of 19-digit normalized significands). Staging, publication and retained inputs stay DuckDB-only.

**Tech Stack:** `ibis-framework[snowflake]` 12, snowflake-connector-python, key-pair service user; secrets `TERRINE_TEST_SNOWFLAKE_{ACCOUNT,USER,PRIVATE_KEY,WAREHOUSE,DATABASE,SCHEMA}` (verified in CI run 39782618657). No local credentials: live probes run through `warehouse-backends.yml` dispatch.

**Spec:** owner goal "set up warehouse validation the same way increment does it" (PostgreSQL, Snowflake, BigQuery); owner tolerance direction 2026-10-21; `AGENTS.md` § Backend portability.

## Scope ledger

| Capability | Source | Confirmed |
| --- | --- | --- |
| Live Snowflake probe tier and weekly/dispatch CI job | Owner: "set up warehouse validation the same way increment does it" | Yes |
| Frame materialization on Snowflake matching DuckDB | Same owner goal; BigQuery and PostgreSQL precedent | Yes |

## Global Constraints

- Live probes are `@pytest.mark.snowflake`, deselected by default, and fail (never skip) without the six settings. Each probe owns a uniquely named scratch schema `TERRINE_CI_<worker>_<hex>` in the test database and drops it; CI serializes the job (`concurrency: live-snowflake-backend`).
- Refusal codes are reused: unsupported operations `terrine.backend.capability_unavailable`; snapshot problems `terrine.execution.snapshot_unavailable`; changed relations `terrine.execution.invalid_input_binding`; float failures `terrine.temporal.float64_precision`; exact overflow `terrine.temporal.exact_overflow`.
- Study queries stay Ibis expressions. `AT(...)`, `TO_VARIANT`, `TRY_TO_NUMBER`, `TRY_TO_DOUBLE`, `CONVERT_TIMEZONE` and any other raw function binding live only in `terrine/backends/` or `terrine/execution/_snowflake.py`.

## Design decisions

1. **Snapshot by Time Travel.** One snapshot timestamp per materialization; every base table reached (directly, through views, or in admitted SQL) must be a permanent or transient table with `RETENTION_TIME >= 1`, else `snapshot_unavailable` naming the route (`ALTER TABLE ... SET DATA_RETENTION_TIME_IN_DAYS = 1`). External tables, materialized views, secure views (definition hidden) and views calling a routine refuse. A relation created or altered after the snapshot time refuses (`invalid_input_binding`), and so does one whose `CREATED`/`LAST_DDL` changed by the time a query's rows are returned: a swap, rename, replace or view redefinition between admission and the read (measured: every such DDL advances `LAST_DDL`, and `AT` on a swapped-in table answers from that object's own history).
2. **Caller's connection, read-only.** A private connection would need the caller's private key, which the connector does not expose. Terrine issues only `SELECT`/`SHOW`/`DESCRIBE`/`INFORMATION_SCHEMA` reads; it checks `TIMEZONE = 'UTC'` in the session and refuses otherwise (route: `ALTER SESSION SET TIMEZONE = 'UTC'`).
3. **Identifier rules.** `TableRef` names, admitted SQL and view definitions resolve with Snowflake's rules through sqlglot (`normalize_identifiers`): unquoted parts fold to upper case, quoted parts are exact and may contain dots; unqualified names resolve in the connection's (or the referencing view's) database and schema. Column names are exact as the connection reports them.
4. **Floats as on DuckDB.** The previous DECFLOAT-with-guard design is obsolete: main now holds floats exactly from their shortest round-trip text on every backend. Snowflake's `TO_VARCHAR(FLOAT)` keeps at most 15 significant digits (measured: `0.3333333333333333 → '0.3333333333'`), but `TO_VARIANT(x)::VARCHAR` renders the shortest round-trip decimal (`'0.3333333333333333'`, `'1.2345678901234568E17'`, `'4.9E-324'`); that text, lower-cased with a `.0` fraction dropped, feeds the shared coefficient construction. Coefficients are `NUMBER(38, 0)` built with `TRY_TO_NUMBER(text, 38, 0)` (NULL beyond 38 digits → `float64_precision`/`bound_overflow`); prefix sums and window differences that overflow raise Snowflake's `Value overflow in a SUM aggregate` / `Number out of representable range`, mapped to `exact_overflow`. Quotients divide 19-digit normalized significands in `NUMBER(38, 0)`: `(a − a % b) / b` is exact (measured: plain `a / b` rounds to six fractional digits and `TRUNC` of it can round up). Output rounding uses `TRY_TO_DOUBLE`, measured correctly rounded on halfway cases and subnormals (`'9007199254740993' → 9007199254740992`, `'4e-324' → 5e-324`, `'2e-324' → 0`) and returning `inf` beyond the range (flagged `bound_overflow`).
5. **Timestamps.** Snowflake timestamps are exact at their scale (9 by default) from year 1 to 9999 with no infinities; `DATEDIFF(NANOSECOND, epoch, ts)` is exact over the whole range (measured `-62135596800000000000` for year 1), so parts split from it; a `TIMESTAMP_TZ` keeps its stored offset through a plain cast (measured), so `utc_wall_clock` is `TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('UTC', ts))`. The connector returns microsecond Arrow timestamps when a result holds an instant outside 1677-09-21..2262-04-11 (measured), and the cast to the nanosecond output type then fails: such outputs refuse with `terrine.execution.arrow_timestamp_range` (route: declare `TIMESTAMP_TZ(6)` or cast in a SQL relation); in-range results keep nanoseconds.
6. **As-of joins.** Ibis lowers `asof_join` to Snowflake's native `ASOF JOIN ... MATCH_CONDITION`, whose semantics match (measured for `>=`, `>`, timestamps and negated decimal keys).

## Review Focus

1. No statement other than reads is issued on the caller's connection. Task 2.
2. A row inserted after the snapshot timestamp is invisible to every later query of the same run, including through a view and through admitted SQL. Task 2.
3. A replaced table, a changed view and a zero-retention table refuse. Task 2.
4. Float windows match DuckDB's single rounding; coefficient, sum and quotient overflows are coded refusals. Task 4.

---

### Task 1: Live Snowflake harness

**Files:** `pyproject.toml` (extra `snowflake` = `ibis-framework[snowflake]>=12,<13`, marker, addopts `not snowflake`), `uv.lock`, `Makefile` (`test-snowflake`), `integration/warehouse_execution/snowflake_fixtures.py`, `.github/workflows/warehouse-backends.yml` (job `snowflake`, modes `snowflake`/`bigquery` for targeted dispatch), `AGENTS.md`

- [x] Fixture `snowflake_scratch` yields `(con, database, schema)` for a fresh schema and drops it; key-pair auth as in increment's `integration/warehouse_execution/conftest.py::snowflake_con`.
- [x] Probe `test_scratch_schema_is_isolated` (create, insert, read back `NUMBER(12,2)` and `TIMESTAMP_TZ`); dispatch passes (run 40098575403); commit.

### Task 2: Provider, Time Travel snapshot and admission

**Files:** `terrine/execution/_snowflake.py`, `terrine/execution/backend.py` (`_SnowflakeProvider`), `terrine/execution/inputs.py` (`_open_snowflake_execution`), `terrine/backends/capabilities.py` (`SNOWFLAKE_CAPABILITIES`), `integration/warehouse_execution/test_snowflake_admission.py`

- [x] Probes: concurrent insert after open invisible via table, view, SQL relation and Arrow batches; a materialization issues only `SELECT`/`DESCRIBE`/`SHOW` (`QUERY_HISTORY_BY_SESSION`); replaced table / changed view refuse after binding; swap, replace, add-column and view redefinition between resolution and the read refuse; zero-retention table, secure view and routine-calling views refuse; non-UTC session refuses; Snowflake identifier rules; nanosecond outputs beyond Arrow's range refuse; `Dataset.open_generation` refuses. Red checks against a live warehouse are still owed (see Status).
- [x] Implement identity (table type, `CREATED`, `LAST_DDL`, retention, columns, view text), view closure with Snowflake identifier rules, sqlglot `AT` rewrite, post-read identity re-verification, overflow and lost-history error mapping; commit.

### Task 3: Dialect helpers for Snowflake

**Files:** `terrine/backends/{numeric,timestamps,joins,extrema,dialect}.py`, dialect gates (`semantic/rules.py`, `temporal/{first,prefix}.py`, `compile/anchors.py`), `integration/warehouse_execution/test_snowflake_parity.py`

- [x] Probes: parity with DuckDB for every PostgreSQL window and shape design (reuse `test_postgres_windows`/`test_postgres_shapes` data and helpers), event-id tie breaking, cohort summaries, nanosecond timestamp parts and comparisons over years 1-9999, exact overflow boundary.
- [ ] Implement until parity passes; commit (committed; the live run after the last fix is pending, see Status).

### Task 4: Float facts on Snowflake

**Files:** `terrine/temporal/aggregate.py`, `terrine/backends/numeric.py` (landed with Task 3: the float state reuses the same helpers)

- [x] Probes: float windows match DuckDB bit for bit (one rounding of the same decimal sums, history with `1e15` cancelling exactly); non-finite history refuses `nonfinite_input`; coefficients beyond 38 digits refuse `bound_overflow` (including a dropped float denominator and a unit whose smallest value sets a wide scale); a prefix sum beyond 38 digits refuses `exact_overflow`.
- [ ] Implement scaled `NUMBER(38, 0)` state from `VARIANT` text and normalized-significand quotients; committed, live verification pending (see Status).

### Task 5: CI, example and docs

**Files:** workflow, `examples/snowflake/dataset.py`, `docs/guides/{backend-compatibility,capabilities,common-stack-interop}.md`, `README.md`, `CHANGELOG.md`

- [x] Weekly + dispatch job runs `make test-snowflake` and `examples/snowflake/dataset.py`; docs state the snapshot model, identity re-verification, retention and session requirements, identifier rules, the 38-digit exact bound, the Arrow nanosecond range and the refusals; commit. The example's own live run is pending.

## Status (resumed 2026-10-24)

Billing was reactivated and `CREATE SCHEMA` granted: the scratch probe passed in
run 40098575403 (37 s). Main (2d26739) was merged without conflicts; the plan's
DECFLOAT float design was replaced by main's exact-from-text design (Design
decision 4).

**Measured on Snowflake (throwaway branch `probe/snowflake-measure`, runs
40099700857 and 40100385039, since deleted):** everything listed under Design
decisions 1-6, plus: `INFORMATION_SCHEMA.TABLES.LAST_DDL` advances on `ADD
COLUMN`, `RENAME TO`, `SWAP WITH` (both tables) and `CREATE OR REPLACE VIEW`
(which also advances `CREATED`); a zero-retention table errors under `AT`
(`000707`) even for an instant after its creation; `AT` beyond the current time
errors (`000708`); `QUERY_HISTORY_BY_SESSION` showed every completed statement
on the first read; `SHOW PARAMETERS LIKE 'TIMEZONE'` rows are `(key, value,
default, level, ...)`; `NUMBER(38, 0)` sum overflow is `100058 Value overflow in
a SUM aggregate`, subtraction overflow `100046 Number out of representable
range`; `NUMBER(38,0) / NUMBER(38,0)` is `NUMBER(38,6)` and overflows for
38-digit dividends with small divisors; `LEAST(1, NULL)` is NULL; `REPEAT('0',
-1)` is `''`; `TO_VARCHAR(NUMBER(12,2))` keeps the full scale (`'1.50'`); the
connector returns `timestamp[ns]` Arrow columns for in-range results and raises
`252005 ... overflows int64 range` (or returns truncated `timestamp[us]`) for
results holding an out-of-range instant; `DECFLOAT(38)` exists and
`TO_DECFLOAT(FLOAT)` is the exact binary expansion to 38 digits (unused);
`'1e20'` in `INSERT ... VALUES` parses as a NUMBER and fails on a FLOAT column
(tests write `'1e20'::FLOAT`).

**Live results so far:** run 40102138510 (admission + parity, before the last
fixes): 23 passed, 10 failed, 1 skipped. Admission: all passed except
`test_nanosecond_outputs_beyond_arrow_range_refuse` (the connector raised
`252005` instead of returning a microsecond column; now mapped to
`arrow_timestamp_range` as well). Parity: counts, decimals, averages, ratios
and three shape designs failed with `exact_overflow` because `lookup_prefix`
filled unmatched state with `CAST(0 AS DECIMAL(38, 2))` — Ibis types a decimal
`SUM` with scale 2 — so Snowflake widened the prefix column to `NUMBER(38, 2)`
and the output's 38-digit range literal no longer fit; `exact_sum` now casts
the sum to `NUMBER(38, 0)` to keep Ibis's type. `test_float_windows` matched
DuckDB within tolerance but a hard-coded expectation was wrong (replaced by
bit-for-bit comparison with DuckDB's rows). `time_to_first_order`,
`time_to_order_with_entry_and_exit`, `observed_state_panel`, event-id ties,
timestamp parts/comparisons, the nonfinite and 38-digit float refusals and the
float sum overflow passed. Run 40103027788 (same selection, with the fixes)
gave 29 passed, 3 failed, 1 skipped: (a) `test_design_matches_duckdb[schedule]`
fails with Snowflake `000904 invalid identifier '_U.POS'`: Ibis lowers the
Schedule anchor's range/unnest to `TABLE(FLATTEN(...))` with a `_u` alias whose
`POS` column Snowflake rejects (an Ibis Snowflake lowering gap to work around
in `terrine/backends/` or `compile/anchors.py`); (b) `weight_week` came back
`0.6` where DuckDB gives `0.6000000000000001` for the decimal
`0.60000000000000004`: `TRY_TO_DOUBLE('60000000000000004e-17')` is one ulp
off although the measured halfway and subnormal cases were correct, so the
guide now claims within-one-ulp agreement and the test asserts that; a
correctly rounded route (for example `TO_DOUBLE(TO_DECFLOAT(text))`) is
unmeasured; (c) two `5e37` coefficients summed to `1e38` without an error:
Snowflake's `NUMBER(38, 0)` window sum holds values to about `1.7e38` (its
128-bit accumulator) and raises beyond, so the test now sums `9e37` twice.

**Remaining before this plan is done:**

- Fix the Schedule-anchor `FLATTEN` lowering (a) and decide the float output
  conversion (b); then dispatch
  `backend_mode=snowflake` on `feat/snowflake-execution` (full
  `make test-snowflake` plus `examples/snowflake/dataset.py`) and record it.
- Red checks for the live probes (one dispatch per file with deliberate
  one-line breaks: drop the `AT` pin, make `verify()` a no-op, skip the
  retention and routine checks, issue a write in `SnowflakeSession.open`, skip
  coefficient padding, flip the as-of inclusivity) and their recorded failures.
- The quotient-overflow refusal (`spend_per_weight` with a `1e-320` weight
  → `bound_overflow`) did not raise in run 40102138510; the case was removed
  from `test_float_window_failures_are_coded_refusals` pending a measurement
  of `TRY_TO_DOUBLE('1000000000000000000e303')` and of the inserted value.
- PostgreSQL and BigQuery live tiers for the shared changes (`numeric.py`,
  `timestamps.py`, `first.py` reverse key now in nanoseconds for all three
  warehouses, `time_to_event.py` nanosecond gating), `make lint`,
  `make typecheck`, `uv run pytest tests -q` (passed locally before the last
  fixes: 1251), the non-live integration tier, and the docs build.
- `SNOWFLAKE_CAPABILITIES` marks `dialect_admission`, `transaction_view`,
  `arrow_batches`, `exact_timestamps` and `exact_decimals` verified; the
  admission probes exercised the first three in run 40102138510, and the
  parity run that exercises the last two is the pending one.
