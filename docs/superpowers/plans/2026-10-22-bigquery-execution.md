# BigQuery Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Frame.materialize()` runs on an Ibis BigQuery connection with the PostgreSQL slice's coverage, matching DuckDB (exact for counts and decimal sums, within the documented tolerance for floats), validated by a live CI job.

**Architecture:** Each materialization opens a private BigQuery session on a new client built from the caller's credentials and runs `BEGIN TRANSACTION` there, so every query sees one snapshot (measured 2026-10-22: a commit from another client after `BEGIN` stays invisible to later jobs in the session). Dialect differences live in `terrine/backends/` helpers; exact state uses `BIGNUMERIC`. Staging, publication and retained inputs stay DuckDB-only.

**Tech Stack:** `ibis-framework[bigquery]` 12, google-cloud-bigquery 3.46, project `increment-warehouse-ci`, dataset `terrine_ci` (US), service account `terrine-ci`.

**Spec:** owner goal "set up warehouse validation the same way increment does it" (PostgreSQL, then Snowflake and BigQuery); owner direction 2026-10-21 on tolerance; `AGENTS.md` § Backend portability.

## Global Constraints

- Live probes are `@pytest.mark.bigquery`, deselected by default, and fail (never skip) without `TERRINE_TEST_BIGQUERY_PROJECT`, `TERRINE_TEST_BIGQUERY_DATASET` and credentials (`GOOGLE_APPLICATION_CREDENTIALS`, application-default, or `TERRINE_TEST_BIGQUERY_ACCESS_TOKEN` for local runs).
- Each probe owns a uniquely named scratch dataset and drops it; CI serializes the BigQuery job (`concurrency: live-bigquery-backend`), as increment does.
- Measured BigQuery facts this plan relies on: `CAST(FLOAT64 AS STRING)` is round-trip (`0.33333333333333331`); `SAFE_CAST(... AS BIGNUMERIC)` is NULL beyond range and rounds past 38 fractional digits; Ibis renders aware↔naive timestamp casts as `CAST(... AS DATETIME/TIMESTAMP)`, which BigQuery evaluates in UTC; `NUMERIC` is `(38, 9)`.
- Refusal codes are reused: unsupported operations `terrine.backend.capability_unavailable`; float failures `terrine.temporal.float64_precision`; exact output overflow `terrine.temporal.exact_overflow` (including BigQuery's own `BIGNUMERIC` overflow errors, mapped in the session).
- A user SQL relation or view that calls a dataset-qualified routine refuses with `terrine.execution.snapshot_unavailable` (remote functions can have side effects); BigQuery built-ins cannot be shadowed by unqualified names.

## Review Focus

1. The caller's own BigQuery client and any session it holds are never used or changed. Task 2.
2. A view whose definition changes after Model binding refuses (identity fingerprint covers view text). Task 2.
3. Timestamps at 0001-01-01 and 9999-12-31 order and compare exactly; DATETIME sources denote UTC. Task 3.
4. Decimal coefficients come from arithmetic, not text: BigQuery prints `NUMERIC` 1.50 as `1.5`. Task 3.
5. A float above BIGNUMERIC range (|x| ≥ 5.7e38) or non-finite refuses rather than becoming NULL. Task 5.

---

### Task 1: Live BigQuery harness

**Files:** `pyproject.toml` (extra `bigquery`, marker, addopts `not bigquery`), `Makefile` (`test-bigquery`), `integration/warehouse_execution/bigquery_fixtures.py`, `AGENTS.md`

- [ ] Fixture `bigquery_scratch` yields `(con, dataset)` for a fresh dataset `terrine_ci_<worker>_<hex>` and drops it with contents; `connect_bigquery(dataset=...)`.
- [ ] Probe `test_scratch_dataset_is_isolated` (create, insert, read back a NUMERIC(12,2) and a TIMESTAMP); `make test-bigquery` passes locally with a gcloud access token; commit.

### Task 2: Provider, private snapshot and admission

**Files:** `terrine/execution/_bigquery.py`, `terrine/execution/backend.py` (`_BigQueryProvider`), `terrine/execution/inputs.py` (`_open_bigquery_execution`), `terrine/backends/capabilities.py` (`BIGQUERY_CAPABILITIES`), `integration/warehouse_execution/test_bigquery_admission.py`

- [ ] Probes: snapshot isolation against a concurrent insert; caller client untouched; replaced table and changed view refuse after binding (`terrine.execution.invalid_input_binding`); SQL relation and view calling `dataset.fn(...)` refuse; `BIGNUMERIC` source column refuses `terrine.semantic.decimal_scale_required`; `Dataset.open_generation` refuses `runtime_reader`.
- [ ] Implement: identity = (table type, creation time, schema, view query) per relation with view closure from parsed view text; session over a new `bigquery.Client(credentials, project, location)` with `create_session=True` + `BEGIN TRANSACTION`, `ROLLBACK` and session abort on close; `BigQuerySession.execute/batches` through `ibis.bigquery.from_connection(private_client)`; map BigQuery overflow errors to `terrine.temporal.exact_overflow`.
- [ ] Probes pass; commit.

### Task 3: Dialect helpers for BigQuery

**Files:** `terrine/backends/{numeric,timestamps}.py`, `terrine/semantic/rules.py`, `terrine/temporal/{first,prefix,daily}.py`, `terrine/compile/{anchors,shapes/cohorts}.py`, `integration/warehouse_execution/test_bigquery_parity.py`

**Interfaces:** `decimal_coefficient(value, scale) -> exact` replaces every `cast("string").replace(".", "")` (DuckDB/PostgreSQL keep text; BigQuery multiplies by `10**scale` in `BIGNUMERIC`); `exact_type` → `BIGNUMERIC`; `truncating_quotient` → `DIV`; `timestamp_is_finite` → true; `utc_wall_clock`/`utc_instant` → Ibis casts; timestamps are their own order key, parts from `delta(..., "microsecond")`.

- [ ] Probes: parity with DuckDB for the PostgreSQL window and shape designs (counts, decimal sums, averages, ratios, as-of, schedule, event anchor, time-to-event, state panel, cohort summaries); timestamp parts/comparisons at 0001-01-01 and 9999-12-31.
- [ ] Implement until parity passes; commit.

### Task 4: Float facts on BigQuery

**Files:** `terrine/temporal/daily.py`, `terrine/temporal/aggregate.py`, `terrine/backends/numeric.py`

- [ ] Probes: float windows within the absolute-sum bound of DuckDB; non-finite history refuses `nonfinite_input`; |x| ≥ 5.7e38 refuses `bound_overflow`.
- [ ] Float facts enter `BIGNUMERIC` from their round-trip text; a non-finite count field rides the prefix state so non-finite history refuses as on DuckDB; out-of-range text (`SAFE_CAST` NULL) counts as overflow. Bound documented: half-ulp per value plus 5e-39 per value (38 fractional digits) plus one rounding. Commit.

### Task 5: CI, docs and example

**Files:** `.github/workflows/warehouse-backends.yml` (mode `cloud`, job `bigquery` with concurrency group, key written to `$RUNNER_TEMP`), `.github/workflows/ci.yml` (weekly schedule + dispatch for `cloud`), `docs/guides/backend-compatibility.md`, `README.md`, `CHANGELOG.md`, `examples/bigquery/dataset.py`

- [ ] Workflow dispatch run on the branch passes the BigQuery job; strict docs build passes; commit.
