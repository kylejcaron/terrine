# Snowflake Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Frame.materialize()` runs on an Ibis Snowflake connection with the PostgreSQL and BigQuery slices' coverage, matching DuckDB (exact for counts and decimal sums, within the documented tolerance for averages, ratios and floats), validated by a live CI job.

**Architecture:** Snowflake's READ COMMITTED isolation is statement-level even inside a transaction, so a transaction gives no multi-statement snapshot. Instead each materialization reads `CURRENT_TIMESTAMP()` once and pins every table reference with Time Travel `AT(TIMESTAMP => <t>)`, rewriting each bound relation (table, view, admitted SQL) through sqlglot in `terrine/execution/_snowflake.py`. Queries are plain `SELECT`s on the caller's connection: no `USE`, `ALTER SESSION`, transaction or temporary object is issued, and the session's `TIMEZONE` must be `UTC` (Ibis sets it at connect). Dialect differences live in `terrine/backends/` helpers. Decimal sums stay in `NUMBER(38, s)` (Snowflake errors on overflow); float state and quotients use `DECFLOAT` (38 significant digits). Staging, publication and retained inputs stay DuckDB-only.

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
- Study queries stay Ibis expressions. `DECFLOAT`, `AT(...)` and any raw function binding live only in `terrine/backends/` or `terrine/execution/_snowflake.py`.

## Design decisions

1. **Snapshot by Time Travel.** One snapshot timestamp per materialization; every base table reached (directly, through views, or in admitted SQL) must be a permanent or transient table with `RETENTION_TIME >= 1`, else `snapshot_unavailable` naming the route (`ALTER TABLE ... SET DATA_RETENTION_TIME_IN_DAYS = 1`). External tables, materialized views, secure views (definition hidden) and views calling a database- or schema-qualified routine refuse. A relation created or replaced after the snapshot time refuses (`invalid_input_binding`).
2. **Caller's connection, read-only.** A private connection would need the caller's private key, which the connector does not expose. Terrine issues only `SELECT`/`SHOW`/`INFORMATION_SCHEMA` reads; it checks `TIMEZONE = 'UTC'` in the session and refuses otherwise (route: `ALTER SESSION SET TIMEZONE = 'UTC'`).
3. **Float tolerance.** `DECFLOAT` additions round to 38 significant digits relative to the running prefix, not the window. A guard keeps the owner contract (error relative to the window's absolute sum): with `A` the absolute-value prefix and `N` the row-count prefix at the window end and `W` the window's absolute sum, a non-empty window refuses with `float64_precision` (`failure=tolerance`) when `N · 10^-37 · A > 2^-60 · W`. Floats enter `DECFLOAT` within half an ulp; quotients divide in `DECFLOAT` and round once to binary64.
4. **Timestamps.** Snowflake timestamps default to scale 9. Arrow nanosecond timestamps cover 1677–2262, so outputs and comparisons must not route a year-1 or year-9999 value through a nanosecond Arrow column; measure, then pick microsecond outputs or the existing timestamp-domain refusal.

## Review Focus

1. No statement other than reads is issued on the caller's connection. Task 2.
2. A row inserted after the snapshot timestamp is invisible to every later query of the same run, including through a view and through admitted SQL. Task 2.
3. A replaced table, a changed view and a zero-retention table refuse. Task 2.
4. Float guard refuses adversarial history (1e30 before, 1.0 in window) and passes ordinary windows. Task 4.

---

### Task 1: Live Snowflake harness

**Files:** `pyproject.toml` (extra `snowflake` = `ibis-framework[snowflake]>=12,<13`, marker, addopts `not snowflake`), `uv.lock`, `Makefile` (`test-snowflake`), `integration/warehouse_execution/snowflake_fixtures.py`, `.github/workflows/warehouse-backends.yml` (job `snowflake`, modes `snowflake`/`bigquery` for targeted dispatch), `AGENTS.md`

- [ ] Fixture `snowflake_scratch` yields `(con, database, schema)` for a fresh schema and drops it; key-pair auth as in increment's `integration/warehouse_execution/conftest.py::snowflake_con`.
- [ ] Probe `test_scratch_schema_is_isolated` (create, insert, read back `NUMBER(12,2)` and `TIMESTAMP_TZ`); dispatch passes; commit.

### Task 2: Provider, Time Travel snapshot and admission

**Files:** `terrine/execution/_snowflake.py`, `terrine/execution/backend.py` (`_SnowflakeProvider`), `terrine/execution/inputs.py` (`_open_snowflake_execution`), `terrine/backends/capabilities.py` (`SNOWFLAKE_CAPABILITIES`), `integration/warehouse_execution/test_snowflake_admission.py`

- [ ] Probes (each red-checked): concurrent insert after open invisible via table, view and SQL relation; no non-read statement reaches the caller's connection (`QUERY_HISTORY_BY_SESSION` for the run); replaced table / changed view refuse after binding; zero-retention table, secure view and routine-calling view refuse; non-UTC session refuses; `Dataset.open_generation` refuses.
- [ ] Implement identity (table type, `CREATED`, `LAST_DDL`, retention, columns, view text), view closure, sqlglot `AT` rewrite, overflow error mapping; commit.

### Task 3: Dialect helpers for Snowflake

**Files:** `terrine/backends/{numeric,timestamps,joins,extrema,dialect}.py`, dialect gates (`semantic/rules.py`, `temporal/{first,prefix}.py`, `compile/anchors.py`), `integration/warehouse_execution/test_snowflake_parity.py`

- [ ] Probes: parity with DuckDB for every PostgreSQL window and shape design (reuse `test_postgres_windows`/`test_postgres_shapes` data and helpers), cohort summaries, timestamp boundaries.
- [ ] Implement until parity passes; commit.

### Task 4: Float facts on Snowflake

**Files:** `terrine/temporal/{daily,prefix,aggregate}.py`, `terrine/backends/numeric.py`

- [ ] Probes: float windows within the absolute-sum bound; non-finite history refuses `nonfinite_input`; the adversarial-history guard refuses `tolerance`; ratios with float denominators.
- [ ] Implement `DECFLOAT` state, absolute and count prefixes, guard; commit.

### Task 5: CI, example and docs

**Files:** workflow, `examples/snowflake/dataset.py`, `docs/guides/{backend-compatibility,capabilities,common-stack-interop}.md`, `README.md`, `CHANGELOG.md`

- [ ] Weekly + dispatch job runs `make test-snowflake` and the example; docs state the snapshot model, retention requirement, session requirement, DECFLOAT bound, refusals; commit.
