# PostgreSQL execution and warehouse validation (slice 1)

**Goal:** A Model built on an Ibis PostgreSQL connection materializes correct
datasets (exact rows equal to the existing DuckDB/reference oracle) for the
supported operation set, verified against a real PostgreSQL 16 server locally and
in CI, with the same workflow shape increment uses for warehouse validation.

**Kata:** parent `nws1`; decision record `ak9y`.

## Scope ledger

| Capability | Source | Confirmed |
| --- | --- | --- |
| Warehouse validation set up the same way as increment (PostgreSQL service container; Snowflake/BigQuery scratch resources via secrets, manual + weekly) | owner: "I think I want the same setup as increment for warehouse validation"; "should I not use the increment warehouse that already exists?"; "go" | yes |
| PostgreSQL execution support, first | agent proposal "I'd begin with PostgreSQL, which needs nothing from you", answered "go" | yes |
| Snowflake and BigQuery execution | same setup; separate later slices once secrets exist | yes, not this slice |
| Runtime staging/publication on PostgreSQL (generations, manifests, retained snapshots) | not requested | no: stays DuckDB |
| Warehouse scale benchmarks / spend ceilings | superseded by owner's choice of increment's probe model | no |

## What exists today (code facts)

- `terrine/execution/backend.py`: one `_DuckDBProvider`; every other connection
  refuses `dialect_admission` with `terrine.backend.capability_unavailable`.
- `terrine/backends/capabilities.py`: `BackendCapabilities` (True/False/None per
  operation); `DUCKDB_CAPABILITIES` is the only verified record.
- Verified on PostgreSQL 16.15 (local): `0.1::float8::numeric` returns `0.1`
  (shortest decimal, not the exact binary value), so float8→NUMERIC is not an
  exact conversion; `'…00.1234567+00'::timestamptz` is silently rounded to
  `.123457`, so sub-µs values must be refused before they reach the server.
- `terrine/execution/_native.py`: metadata admission and native function ownership
  read DuckDB system tables (`duckdb_tables/views/functions`); dialect hardcoded.
- `terrine/temporal/prefix.py`: exact window state uses DuckDB `BIGNUM` plus
  builtins `sum/add/subtract/multiply/divide/mod/isfinite/nextafter/epoch_ns/epoch_us`;
  `_require_backend` refuses anything but DuckDB 1.5.6.
- `terrine/temporal/first.py`: first-occurrence point lookup refuses non-DuckDB.
- `terrine/compile/anchors.py`, `terrine/compile/shapes/_observations.py`:
  `ibis.to_sql(..., dialect="duckdb")`, Schedule expansion requires DuckDB `range`.
- Reference oracles: `tests/_worlds.py`, `terrine/simulate` (expected rows).

## Design decisions (slice 1)

1. **Provider.** Add `_PostgresProvider` (matches `ibis.backends.postgres`), dialect
   `postgres`, with its own capability record: `dialect_admission`,
   `transaction_view` (one `REPEATABLE READ READ ONLY` transaction per
   materialization: a real consistent snapshot), `exact_decimals` (NUMERIC),
   `exact_timestamps` (covered domain: `timestamp` and `timestamptz` at
   microsecond precision; sub-µs requests refuse), `arrow_batches`.
   Publication/retained-snapshot operations stay unverified, so runtime staging
   refuses with a route to DuckDB.
2. **DDL-free schema discovery.** Ibis 12's PostgreSQL `con.sql` discovers a query
   schema by creating a temporary view, which a read-only transaction forbids.
   Compiler-produced queries pass their known Ibis schema to `con.sql(..., schema=)`;
   SQL-backed source relations get their schema from a `LIMIT 0` cursor
   description (no DDL). Acceptance runs SQL-backed sources and Schedule
   materialization inside the actual read-only transaction.
3. **Exact state: sums and counts only.** Represent the exact window state as
   PostgreSQL `NUMERIC` through a per-backend primitive table in `prefix.py`
   instead of DuckDB `BIGNUM` builtins. Supported: counts and integer/decimal
   sums (additive windows, as-of attributes). Averages and ratios need the binary64
   quotient certification (`checked_float64_v1` metadata, outward rounding,
   certified predicates), so they **refuse** on PostgreSQL in this slice with route
   "use DuckDB, or request sum and count and divide downstream".
4. **Overflow refusal without try_cast.** Ibis 12 lowers `TryCast` to a plain
   `CAST` on PostgreSQL, which would abort the transaction with a driver error.
   Narrowing to the output type is guarded in NUMERIC (`CASE WHEN value BETWEEN
   min AND max THEN CAST(...) END`) so `terrine.temporal.exact_overflow` is still
   raised. Acceptance: an overflowing decimal sum refuses with that code; a value
   at the representable boundary succeeds.
5. **Float windows refuse** (`FLOAT64_UNSUPPORTED`) with route "cast to DECIMAL or
   use DuckDB": PostgreSQL has no `nextafter`, and float8→NUMERIC is not exact
   (verified above).
6. **Timestamps and event admission.** Port the shared timestamp lowering in
   `terrine/temporal/_timestamps.py` (`timestamp_order_key`, `compare_timestamps`,
   currently DuckDB `epoch_us`/`divide`) to per-backend lowering with shared
   consumers (prefix lookups, timestamp comparisons, SCD2 checks), and extend
   `semantic.rules.supports_event_resolution` to PostgreSQL. Acceptance: strict
   half-open boundaries, pre-epoch fractional instants, nulls, EventAnchor designs.
7. **Dependency floor.** Ibis 11's PostgreSQL as-of lowering applies unit
   predicates after choosing the nearest row (wrong results with interleaved
   units). The `postgres` extra requires `ibis-framework[postgres]>=12,<13`, and
   admission refuses a PostgreSQL connection on Ibis < 12 with a route. Acceptance
   includes an interleaved-unit as-of case.
8. **Metadata/identity.** Relation identity and function ownership via
   `pg_catalog` (relation oid, `pg_proc` for user functions); read-only SQL
   admitted with the `postgres` sqlglot dialect.
9. **Unported operations refuse per operation** with routes (sketches, quotients,
   floats, publication). The plan ports Schedule expansion (`generate_series`)
   and first-occurrence lookup because common designs need them.

## Test harness

- Marker `postgres`, deselected by default (like `scale`). Tests live in
  `integration/warehouse_execution/test_postgres_*.py` and read
  `TERRINE_TEST_POSTGRES_{HOST,PORT,USER,PASSWORD,DB}`; selected but unset settings
  **fail** (increment's rule), never skip.
- Each test owns a unique schema (worker id + uuid) and drops it; no shared namespace.
- `make test-postgres` runs them; locally a helper script starts a throwaway
  PostgreSQL 16 cluster (`initdb` into a temp dir) and exports the settings.
- **Differential oracle:** the existing expected-row worlds are loaded into both
  DuckDB and PostgreSQL; supported Designs must produce identical rows (exact
  decimals, µs timestamps, null reasons, completeness).

## Tasks

1. **Harness.** `postgres` marker + addopts, `make test-postgres`, a helper that
   starts a throwaway PostgreSQL 16 cluster (`initdb` into a temp dir), fixtures
   providing an Ibis PostgreSQL connection + per-test schema loader for worlds.
   Add the `postgres` optional extra (`ibis-framework[postgres]>=12,<13`) and
   notices. Acceptance: selected-without-settings fails; with settings, a trivial
   probe passes.
2. **Provider, admission, metadata, schema discovery.** Decisions 1, 2, 7, 8.
   Unit tests for refusal routing (no server); integration probes for identity,
   admission, SQL-backed sources inside the read-only transaction, and Arrow
   results with exact NUMERIC→decimal and timestamptz→UTC µs.
3. **Shared timestamp lowering and event admission.** Decision 6, before windows.
4. **Exact sums/counts on NUMERIC.** Decisions 3, 4, 5. Differential tests vs the
   DuckDB oracle, overflow refusal and boundary cases, quotient/float refusals.
5. **Shapes.** Remove hardcoded `dialect="duckdb"` in `anchors.py` and
   `_observations.py`; Schedule expansion via `generate_series`; first-occurrence
   lookup; as-of attributes with interleaved units. Differential tests per shape
   (cross-section, scheduled panel, event anchors).
6. **CI workflow.** `.github/workflows/warehouse-backends.yml` mirroring increment
   (`backend_mode` input; postgres job with `postgres:16` service; required
   settings fail), and a `postgres` job in `ci.yml` on every push once green.
7. **Docs + example + finish.** Runnable `examples/postgres_dataset.py` (install
   the extra, connect, PostgreSQL Catalog, Model → Design → `Frame.materialize()`,
   inspect rows) linked from the README with the DuckDB-only publication boundary
   explained; `backend-compatibility.md` PostgreSQL column (verified/refused
   operations and routes); AGENTS test commands; CHANGELOG; full local
   verification and green CI.

## Out of scope / follow-ups

Averages/ratios and float window certificates on PostgreSQL; nanosecond
timestamps; runtime publication on PostgreSQL; Snowflake (slice 2) and BigQuery
(slice 3).
