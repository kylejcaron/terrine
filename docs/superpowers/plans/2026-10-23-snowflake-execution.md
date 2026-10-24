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
4. **Floats as on DuckDB, from 18-digit text.** The previous DECFLOAT-with-guard design is obsolete: main now holds floats exactly from their shortest round-trip text on every backend. Snowflake has no row-wise shortest text (measured in run 40106855757 on 143 vectors read back through Arrow): `TO_VARCHAR(FLOAT)` and `TO_VARIANT(x)::VARCHAR` keep 15 significant digits (10 for negative exponents; the shortest `'1.2345678901234568E17'` form seen earlier was constant folding), `TO_JSON` and the `TM`/`TME` formats 16, and `TO_VARCHAR(x, '9.99…EEEE')` prints `##` beyond two exponent digits; `TO_DECFLOAT(x)::VARCHAR` is the one text that always round-trips: the binary value correctly rounded to 18 significant digits (max relative error 4.5e-18, e.g. `0.1 → '0.100000000000000006'`, `5e-324 → '4.94065645841246544e-324'`, `1e23 → '99999999999999991600000'`). That text, lower-cased, feeds the shared coefficient construction. Coefficients are `NUMBER(38, 0)` built with `TRY_TO_NUMBER(text, 38, 0)` (NULL beyond 38 digits → `float64_precision`/`bound_overflow`); prefix sums and window differences that overflow raise Snowflake's `Value overflow in a SUM aggregate` / `Number out of representable range`, mapped to `exact_overflow`. Quotients divide 19-digit normalized significands in `NUMBER(38, 0)`: `(a − a % b) / b` is exact (measured: plain `a / b` rounds to six fractional digits and `TRUNC` of it can round up). Output rounding uses `TRY_TO_DOUBLE`, correctly rounded on all 143 vectors (halfway, subnormal and 17-19-digit near-halfway cases; `TO_DOUBLE(TO_DECFLOAT(text))` double-rounds); row-wise it returns NULL beyond binary64's range (constant folding gives `Infinity`) and 0.0 below it, so a NULL conversion of a non-NULL operand is flagged `bound_overflow`. Float windows therefore match DuckDB within `2⁻⁵¹ · Σ|x| + (n + 1) · 2⁻¹⁰⁷⁴` for `n` subnormal inputs (each value's shortest text and 18-digit rounding both lie within half an ulp of it: `2⁻⁵³ · |x|` for a normal value, `2⁻¹⁰⁷⁵` for a subnormal one, plus one rounding of each result), not bit for bit: 200 values of `5e-324` sum to `1e-321` from the shortest texts and `9.88e-322` from the 18-digit ones.
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

- [x] Probes: concurrent insert after open invisible via table, view, SQL relation and Arrow batches; a materialization issues only `SELECT`/`DESCRIBE`/`SHOW` (`QUERY_HISTORY_BY_SESSION`); replaced table / changed view refuse after binding; swap, replace, add-column and view redefinition between resolution and the read refuse, a streamed read before its first batch; zero-retention table, secure view and routine-calling views refuse; non-UTC session refuses; Snowflake identifier rules, inside admitted SQL (a cased CTE reference) too; nanosecond outputs beyond Arrow's range refuse; `Dataset.open_generation` refuses. Red-checked live (Status).
- [x] Implement identity (table type, `CREATED`, `LAST_DDL`, retention, columns, view text), view closure with Snowflake identifier rules, sqlglot `AT` rewrite, post-read identity re-verification, overflow and lost-history error mapping; commit.

### Task 3: Dialect helpers for Snowflake

**Files:** `terrine/backends/{numeric,timestamps,joins,extrema,dialect}.py`, dialect gates (`semantic/rules.py`, `temporal/{first,prefix}.py`, `compile/anchors.py`), `integration/warehouse_execution/test_snowflake_parity.py`

- [x] Probes: parity with DuckDB for every PostgreSQL window and shape design (reuse `test_postgres_windows`/`test_postgres_shapes` data and helpers), event-id tie breaking, cohort summaries, nanosecond timestamp parts and comparisons over years 1-9999, exact overflow boundary.
- [x] Implement until parity passes; commit (Schedule anchors use `terrine/backends/sequences.py` on Snowflake; integer filters compare `NUMBER(p, 0)` columns against their 38-digit range; green in run 40107656757).

### Task 4: Float facts on Snowflake

**Files:** `terrine/temporal/aggregate.py`, `terrine/backends/numeric.py` (landed with Task 3: the float state reuses the same helpers)

- [x] Probes: float windows match DuckDB within one ulp (the same decimal sums rounded once, history with `1e15` cancelling exactly, the `0.1 + 0.2 + 0.30000000000000004` window equal bit for bit); non-finite history refuses `nonfinite_input`; coefficients beyond 38 digits refuse `bound_overflow` (including a dropped float denominator, a quotient beyond binary64's range and a unit whose widest text sets a 38-digit scale); a prefix sum beyond 38 digits refuses `exact_overflow`.
- [x] Implement scaled `NUMBER(38, 0)` state from `DECFLOAT` text and normalized-significand quotients; green in run 40107656757.

### Task 5: CI, example and docs

**Files:** workflow, `examples/snowflake/dataset.py`, `docs/guides/{backend-compatibility,capabilities,common-stack-interop}.md`, `README.md`, `CHANGELOG.md`

- [x] Weekly + dispatch job runs `make test-snowflake` and `examples/snowflake/dataset.py`; docs state the snapshot model, identity re-verification, retention and session requirements, identifier rules, the 38-digit exact bound, the 18-digit float text, the Arrow nanosecond range and the refusals; commit. Both steps green in run 40107656757.

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
results holding an out-of-range instant; `DECFLOAT(38)` exists (its text of
a `FLOAT` was later measured as 18 significant digits, see below);
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
`0.6` where DuckDB gives `0.6000000000000001`, at first attributed to
`TRY_TO_DOUBLE` (wrong, see below); (c) two `5e37` coefficients summed to
`1e38` without an error: Snowflake's `NUMBER(38, 0)` window sum holds values
to about `1.7e38` (its 128-bit accumulator) and raises beyond, so the test
now sums `9e37` twice.

Run 40103934563 (full `make test-snowflake` on `feat/snowflake-execution`,
head 7f2776d): 32 passed, 1 failed (`test_design_matches_duckdb[schedule]`),
1 skipped. Push CI run 40103924095 on the same head passed.

**Finished 2026-10-24 (head ae0ac93).** Fixes after the review of 7f2776d:
Schedule anchors on Snowflake build their sequence from the cross product of
decimal-digit relations (`terrine/backends/sequences.py`) instead of Ibis's
`TABLE(FLATTEN(...))` lowering; a streamed read verifies relation identity
once the statement completes and before its first batch (it used to verify
after the last, and a one-batch reader never did); `pin()` normalizes
identifiers before scope traversal, so an unquoted `RECENT` names the CTE
`recent` and gets no `AT`; integer filters compare `NUMBER(p, 0)` columns,
which Ibis reads as `int64`, against the 38-digit physical range through
`integer_bounds`, and a threshold outside the column's range settles without
a literal (a 20-digit value used to pass a 21-digit `gt` threshold clipped to
`2**63`).

**Measured on Snowflake (throwaway branch `probe/snowflake-measure-2`, runs
40106215765 and 40106855757, since deleted):** row-wise `TO_VARIANT(x)::VARCHAR`
and `TO_CHAR(x)` keep 15 significant digits (10 with a negative exponent),
`TO_JSON` and `TM`/`TME` 16, so the earlier one-ulp deviation came from lossy
input text (`0.30000000000000004` entered as `0.3`); the shortest form seen in
the first measurement was constant folding. `TO_DECFLOAT(x)::VARCHAR` is 18
significant digits, correctly rounded, round-tripping on all 143 vectors
(max relative error 4.5e-18) and is now the float text (Design decision 4).
`TRY_TO_DOUBLE`, `TO_DOUBLE` and `::DOUBLE` were correctly rounded on all 143
vectors read back through Arrow; `TO_DOUBLE(TO_DECFLOAT(text))` double-rounds.
Row-wise `TRY_TO_DOUBLE` returns NULL beyond binary64's range (`TO_DOUBLE`
errors `100038`; constant folding returns `Infinity`) and `0.0` below it;
`1e-320` is stored as the subnormal `9.99988867182683e-321`. The quotient
case `10 / 1e-320` was therefore a silent NULL; a NULL conversion of a
non-NULL operand is now `bound_overflow` and the parity case is restored.

Run 40107656757 (full `make test-snowflake` plus `examples/snowflake/dataset.py`
on head ae0ac93): 36 passed, 1 skipped, both steps green (10 m 51 s). Push CI
run 40107656532 on the same head passed. Red checks on throwaway branches
from ae0ac93 (since deleted): run 40108635983 (`probe/snowflake-red-1`) with
no `AT` pin, batch verification moved after the last batch, retention,
routine and UTC checks skipped, a `CREATE TEMPORARY TABLE` in `open()`, the
Arrow range mapping and the overflow mapping dropped, `int64` filter bounds,
Ibis's range unnest, 15-digit float text and nanoseconds dropped from
timestamp parts: 17 failed, 19 passed, each break caught by its own probe
(the pin and CTE tests saw 2 rows, the four `during_execution` cases failed
at the batch assertion only, retention and both routine cases and the UTC
case did not raise, `only_reads` saw `CREATE_TABLE`, the Arrow and overflow
tests raised raw connector errors, the beyond-int64 filter counted `[0, 0, 0,
3]`, `[schedule]` failed with `000904`, `weight_week` came back `0.6`, the
timestamp parts mismatched). Run 40108666238 (`probe/snowflake-red-2`) with
`verify()` a no-op, coefficient padding skipped, as-of inclusivity flipped in
`backends/joins.py`, NULL conversions passing and the `first_occurrence` tie
order reversed: 8 failed, 28 passed: the swap, add-column and view cases of
`during_execution` did not raise (the replace case still refuses through the
`time travel data is not available` mapping, by design), the `1e39`, dropped
`1e39` denominator and `1e-20`/`1e20` cases and the float window test failed
from the missing padding, and the `1e-320` quotient did not raise. Two breaks
went undetected: the as-of inclusivity flip (no parity row sits exactly on a
window boundary) and the reversed tie order in `first_occurrence` (the tie
probe exercises another path). Both are probe-sensitivity gaps, not product
faults; a boundary-instant row in the shapes data and a tie case through
`first_occurrence` were added (`test_boundary_and_tied_first_events_match_duckdb`)
and run with both breaks applied in run 40111552297 (`probe/snowflake-red-3`,
deleted): they passed, so neither break is consumer-visible through the
TimeToEvent shapes (events are confined to `[lower, upper)` before the as-of
join, and the tie-ranked event id is not an output); the as-of inclusivity is
observable only through as-of attributes, so the probe now also runs
`as_of_interleaved_units` with events exactly at `t0` and `t0 − 1d` (unrun on
Snowflake: covered by the next full or weekly run). The repeated-subnormal
probe (200 × `5e-324`) passed in the same run, within
`2⁻⁵¹ · Σ|x| + (n + 1) · 2⁻¹⁰⁷⁴`.

Local gates on ae0ac93: `uv run pytest tests -q` 1266 passed; the non-live
integration tier 197 passed; `make lint`, `make typecheck`,
`python -m tests.documentation` and the strict docs build green; PostgreSQL
live tier 45 passed; BigQuery live tier 23 passed. Dispatches used in this
pass: 5 (two measurements, the full run, two red checks); 12 in total.

**Branch review fixes (2026-10-24, head 614fe1b).** Six findings on fa5e06c:
(1) relations carrying a row access, masking or other policy, anywhere in the
closure, refuse `snapshot_unavailable` (policies and their mapping tables are
not pinned); attachments are read through `INFORMATION_SCHEMA.POLICY_REFERENCES`
(`REF_ENTITY_NAME`/`REF_ENTITY_DOMAIN`), which worked for the CI role on tables
and views it owns, and an uninspectable relation refuses too; measured: `ALTER
TABLE ... ADD ROW ACCESS POLICY` advances `LAST_DDL`, so an attachment during a
run refuses through re-verification. (2) Every physical reference in admitted
SQL is rewritten fully qualified at admission (`qualify`, sqlglot, Snowflake
rules), so `USE SCHEMA` after resolution no longer redirects a pinned read
(measured: the broken build read the other schema's 2 rows). (3) Resolved
schemas keep `NUMBER(p > 18, 0)` as `decimal(p, 0)` (`DESCRIBE TABLE`, and
`DESCRIBE RESULT` of an empty admitted query); `dtype: int` accepts scale-zero
decimals; filter bounds come from `coefficient_bounds` (DuckDB's HUGEINT
special case is DuckDB-only). (4) `timestamp_text` renders anchor identities
with `FF9` on Snowflake. (5) Scales other than 6/9 promote losslessly in
`first_occurrence`/`last_event_value` and on the time-to-event axis. (6) NULL
`CREATED`/`LAST_DDL` refuses `snapshot_unavailable` (unit test with a stubbed
metadata row).

Run 40120538048 (throwaway `probe/snowflake-fixes-focused`, deleted): green
pass of the 7 new and 5 affected probes, 12 passed; red pass with one-line
breaks (policy check, qualification, wide-integer typing, anchor text, scale
promotion plus the old scale check): 6 failed as intended (policy cases did
not raise, the schema-change case counted 2, the wide key failed the Arrow
cast, the millisecond case refused `timestamp_order_unsupported`; the
microsecond case failed through the helper's unavailability rather than a
lossy cast, so the collision itself is unmeasured live). Run 40120861209 (full
`make test-snowflake` plus the example on 614fe1b): 45 passed, 1 skipped,
both steps green (11 m 14 s). Push CI run 40120572886 passed. Local gates:
`uv run pytest tests -q` 1287 passed; non-live integration tier 197 passed;
lint, typecheck, documentation and the strict docs build green; PostgreSQL
live tier 45 passed; BigQuery live tier 23 passed. Dispatches used in this
pass: 2; 14 in total.

**Re-review fixes (2026-10-24, head 0bf3159).** Two findings on 614fe1b:
(1) view definitions are admitted under the SQL relation's read-only function
whitelist (`terrine/execution/_views.py`, shared by Snowflake, PostgreSQL and
BigQuery, refusing `snapshot_unavailable` with a materialize-first route), so
`RANDOM()`/`CURRENT_TIMESTAMP()`/unlisted or user functions in a view refuse;
the sibling providers had the same gap (PostgreSQL refused only user
functions, BigQuery only qualified routines) and now share the policy
(PostgreSQL tier 47 passed locally with `random()` and `clock_timestamp()`
cases, BigQuery tier 25 passed with `RAND()` and `CURRENT_TIMESTAMP()`
cases). (2) Relations whose columns carry any tag, direct or inherited
(`INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS`, which the CI role reads on
its own tables and views), refuse `snapshot_unavailable`, since `ALTER TAG
... SET MASKING POLICY` changes values between reads without DDL; an
uninspectable relation refuses naming the APPLY grants.

Run 40123899141 (throwaway `probe/snowflake-fixes-focused-2`, deleted): green
pass 14 passed (view `RANDOM()`, view `CURRENT_TIMESTAMP()`, column, table,
schema-inherited and view-column tags all refuse); red pass with the view
policy replaced by a plain parse and the tag check skipped: 8 failed as
intended (the two nondeterministic views, the four tag cases and the two
user-routine views, now covered by the same policy). Run 40124072495 (full
`make test-snowflake` plus the example on 0bf3159): 51 passed, 1 skipped,
both steps green (12 m 26 s); push CI run 40123931273 passed. Local gates on
0bf3159: unit 1294 passed, non-live integration 197 passed, lint, typecheck,
documentation and strict docs build green. Dispatches used in this pass: 2;
16 in total.

**Routine-name fix (2026-10-24, head 556c3d8).** The whitelist judges a view
by its parse, and sqlglot maps `STR_POSITION` to `CHARINDEX`, so a user
routine of that name passed while Snowflake would run it. The routine check
is restored on the stored text as written (`_called_names`: every token
directly before `(`, quoted names exact), matched against
`INFORMATION_SCHEMA.FUNCTIONS` of the view's database and the session's
current database (every schema of each, a superset of the search path), in
addition to the whitelist. The alias bypass cannot apply to the siblings:
PostgreSQL's check follows `pg_depend` from the view's stored rule to
`pg_proc` OIDs, independent of any name or rendering; BigQuery persistent
UDFs and remote functions must be dataset-qualified in a view, and the
whitelist refuses every `Dot`-qualified call and backticked path before
looking at the rendered name.

Verified refusals, exactly (all `terrine.execution.snapshot_unavailable`
unless noted): zero-retention table; secure view; view calling an unqualified
user routine `same`, a schema-qualified `{schema}.same`, or a user routine
`STR_POSITION` whose name parses as a built-in; view selecting `RANDOM()`;
view filtering on `CURRENT_TIMESTAMP()`; table with a row access policy;
table with a masking policy on a column; view over such a table; table with
a tag on a column, on the table, or inherited from its schema; view with a
tag on a column; table or view with NULL `CREATED`/`LAST_DDL` (unit test,
stubbed metadata); relation whose policy, tag or routine inspection errors
(unit test, stubbed error); non-UTC session. `invalid_input_binding`: a row
access policy attached to an admitted table during execution; a table
swapped, replaced or column-extended, or a view redefined, between
resolution and the read; a fresh materialization after `USE SCHEMA` to a
schema holding a same-named table. Not verified live: `ALTER TAG ... SET
MASKING POLICY` between reads (the tag refusal happens at admission, so the
case cannot be reached), and a reader role lacking the APPLY grants.

Run 40125792491 (throwaway `probe/snowflake-fixes-focused-3`, deleted) was
to run the green and red passes of
`test_relations_without_a_usable_history_refuse` (red: routine match
disabled), but every case failed at connect: Snowflake error 90073,
"Warehouse cannot be resumed because resource monitor 'TERRINE_MONTHLY_CAP'
has exceeded its quota". No test ran; the `STR_POSITION` case and its red
check are unverified live (the unit test `test_views_calling_a_user_routine_named_like_a_built_in_refuse`
fails with the match disabled and passes on 556c3d8), and the final full run
is pending the monitor's reset or a raised quota. Dispatches used in this
pass: 1; 17 in total.
