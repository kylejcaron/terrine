# Engine Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Centralize engine admission, capabilities and expression differences without changing supported behavior, except registry-derived refusal routes.

**Architecture:** A lightweight, immutable `ENGINES` registry describes the four currently verified engines; it lazily resolves dialect classes and identity callbacks so semantic metadata stays importable without Ibis or optional drivers. `Dialect` owns existing portable-correct expression implementations and capability refusals, while one subclass per engine owns only necessary overrides. Existing backend helper functions remain the expression API; execution providers retain their existing session implementations and are selected from registry records rather than a second engine list.

**Tech Stack:** Python 3.13, frozen dataclasses, contextvars, Ibis 11–12 (PostgreSQL/BigQuery/Snowflake require 12), pytest, Ruff, tach, DuckDB 1.5.6, PostgreSQL 16.

**Spec:** `/Users/kylejcaron/terrine/.superpowers/sdd/2026-10-26-public-readiness/task-9-brief.md` (the assigned scope overrides its background discussion of later generic-engine features).

## Global Constraints

- Public API changes update production callers, exports, docstrings, guides, examples, fixtures and CHANGELOG in the same change; clean cutover, no aliases.
- Refusals keep stable package-wide codes; renamed codes get a breaking CHANGELOG entry; every new refusal survives pickle and deepcopy.
- Null versus zero and per-cell null reasons survive Arrow, manifests and exports unchanged.
- Package version stays `0.1.0.dev0`; no tag, push, publish or GitHub mutation (session runs with GitHub publishing disabled; owner-reserved).
- Tests belong in the cheapest tier. Record red-first evidence for every new test, assert behavior rather than wording/wiring, and run `make check TESTS="<affected paths>"` before implementation commits.
- Work only in `/Users/kylejcaron/terrine/.worktrees/engine-registry`, branch `refactor/engine-registry`, based on `1755172`. Use absolute paths for file operations. Do not modify the main checkout or another worktree; do not stash, change branches, merge, rebase or push.
- Scope is a clean reorganization. Preserve admitted engine/build combinations, SQL semantics, validation order, session ownership, capabilities, serialization, schemas, numerical algorithms and tolerances. Only refusal `route` text may change; derive it from the registry and document it in CHANGELOG.
- Do not add `GENERIC`, `allow_unverified`, warnings, provenance fields, a conformance framework, new backend support or generic DECIMAL state. These are later work. An operation lacking an existing portable-correct implementation keeps the existing `terrine.backend.capability_unavailable` refusal.
- Study/compiler/temporal code remains Ibis expressions. Native bindings belong in engine modules. The existing qualified server-identity read moves with engine admission; no new SQL probes, writes or session settings are introduced.
- No tooling/workflow vocabulary in product code, comments, docstrings, docs, CHANGELOG or commit subjects. This implementation document under `docs/superpowers/` is exempt.
- No local BigQuery/Snowflake execution. One owner-dispatched `warehouse-backends` run with `backend_mode=all` at the end is required evidence for both warehouses; its green results are an exit gate, not an assumed pass.

## Review Focus

1. Literal-only operations inside nested or exceptional compile scopes must use the enclosing engine, restore the outer scope, and preserve the existing unscoped DuckDB default; exercise in Task 1.
2. Metadata imports and `backend_capabilities()` must not import optional drivers, query a server, or make semantic rules depend on Ibis; exercise in Task 1, including an unregistered object claiming a verified name.
3. PostgreSQL-wire look-alikes, unmeasured major versions and malformed/empty version results must refuse before source schema access, without weakening the qualified-version check or changing code/context; exercise in Task 1 and retain its live shadowing probe.
4. Moving numeric operations must retain nonfinite/null flags, physical coefficient bounds, cancellation and overflow behavior rather than accidentally inheriting a less exact generic operation; exercise in Task 2 using existing numeric oracles.
5. Timestamp scale, session timezone, pre-epoch remainders, mixed-precision half-open event windows, and an unbound cohort-origin literal must continue to agree across helper and shape paths; exercise in Tasks 3–4 and existing PostgreSQL probes.

---

## File and interface map

Paths below are relative to the assigned worktree. Existing line numbers refer to base `1755172` and are navigation aids, not patch coordinates.

| File | Responsibility after the change |
| --- | --- |
| `terrine/backends/registry.py` (new) | Immutable `Engine`/`SessionStrategy` metadata, `ENGINES`, lazy type/callback lookup, engine admission and supported-route construction. Importable without Ibis or any optional driver. |
| `terrine/backends/dialect.py` | `Dialect`, compile-scope selection, cached registered dialect instances, portable implementations and existing unavailable-operation refusal. No list of supported engines. |
| `terrine/backends/engines/{__init__,duckdb,postgres,bigquery,snowflake}.py` (new) | One concrete dialect per engine; native types/UDF bindings and identity callbacks. Empty package initializer; do not eagerly import engine modules. |
| `terrine/backends/{numeric,timestamps,joins,extrema,sequences}.py` | Existing signatures plus shared engine-independent arithmetic; delegate differences to a dialect, not engine-name conditions. |
| `terrine/backends/capabilities.py` | Existing capability model/refusal factories; concrete capability values move into registry records. Also owns the existing shared snapshot refusal declaration needed by identity admission without an execution-layer import. |
| `terrine/execution/backend.py` | Existing `BackendProvider` protocol and public selectors; no provider tuple, per-engine selection branches or concrete providers. |
| `terrine/execution/{_native,_postgres,_bigquery,_snowflake}.py` | Existing relation/session implementations plus the moved concrete provider and a module-local `PROVIDER`; the registry stores their module paths. `_postgres.py` no longer classifies server identity. |
| `terrine/execution/{inputs,_publish}.py` | Consume the relocated snapshot refusal directly; execution lifetimes otherwise unchanged. |
| `terrine/{compile/anchors,temporal/first,temporal/prefix,semantic/rules}.py` | Replace the four supported-engine lists with registry/dialect decisions while keeping their owning refusal codes/context. |
| `terrine/{temporal/aggregate,compile/shapes/time_to_event,compile/shapes/cohorts}.py` | Delegate narrowing, axis policy and explicit literal/column dialect selection. |
| `terrine/spec/validate.py`, `terrine/export/preflight.py`, `tach.toml` | Replace the additional existing DuckDB-only checks with registry feature predicates; admit only the existing feature subset. Add only the required `terrine.backends` dependency edges. |
| `tests/backends/test_engine_registry.py` (new), existing test modules below | Cheap admission/scope/feature contracts and existing expression oracles. |
| `integration/backends/test_engine_registry.py` (new) | Subprocess-based registry import isolation; process orchestration stays out of the new unit tests. |
| `tests/test_engine_dispatch.py` (new) | Explicit architecture guard for engine selection; this is the requested exception to the ordinary ban on source-structure tests. |
| `AGENTS.md`, `docs/guides/backend-compatibility.md`, `docs/reference/refusals.md`, `CHANGELOG.md` | Registry extension policy and the sole user-visible route correction. |

### Shared contracts (pin these names before implementation)

- `SessionStrategy` is a `StrEnum` with `NO_SNAPSHOT = "no_snapshot"`, `TRANSACTION = "transaction"`, `SESSION_TRANSACTION = "session_transaction"`, `TIME_TRAVEL = "time_travel"`. Registered values are DuckDB/PostgreSQL `TRANSACTION`, BigQuery `SESSION_TRANSACTION`, Snowflake `TIME_TRAVEL`. This describes the existing provider behavior; it does not replace the session implementations with a generalized executor.
- Frozen, slotted `Engine` has `name: str`, `display_name: str`, `ibis_module: str`, `session_strategy: SessionStrategy`, `status: Literal["verified", "unverified"]`, `capabilities: BackendCapabilities`, `features: frozenset[str]`, `provider_module: str`, `identity_requires_query: bool`, and private string paths for the dialect class and identity callback. `Engine.dialect -> type[Dialect]` is a lazy property; `Engine.identify(con: Any) -> None` invokes the registered callback lazily. Use `TYPE_CHECKING` for Ibis/dialect annotations and no eager engine/driver imports.
- `ENGINES: Mapping[str, Engine]` is a `MappingProxyType` containing exactly the current four entries, in existing provider order: `duckdb`, `postgres`, `bigquery`, `snowflake`; all have status `verified`. Each record is the sole owner of its current capability values and registration paths. Remove the four concrete `*_CAPABILITIES` constants and migrate every import, including runtime consumers; retain `BackendCapabilities` and `UNVERIFIED_CAPABILITIES`.
- `get_engine(name: str) -> Engine | None` performs case-insensitive metadata lookup only. `engine_supports(name: str, feature: str) -> bool` returns false for unknown names. Feature keys are `naive_anchor_utc`, `first_occurrence`, `event_resolution` (all four), `quantile`, `approx_distinct`, `warehouse_preflight` (DuckDB only). These are existing feature boundaries, not new promises.
- `resolve_engine(con: Any) -> Engine` matches the exact Ibis class module against `Engine.ibis_module`, performs identity admission, and returns the registered record. It never admits a `.name` claim alone or a raw driver connection. Unknown connections keep code `terrine.backend.capability_unavailable` and context `{backend: casefolded reported name/class name, operation: "query", status: "unverified", route: ...}`. There is no opt-in parameter in this change.
- `supported_engine_route(operation: str) -> str` builds a route from registered verified implementations of that operation, using display names and deterministic registry order. Unsupported-operation routes must not recommend an engine lacking that operation (for example, `exact_ratio` is PostgreSQL/BigQuery, not DuckDB/Snowflake). Keep an engine-local immutable `operations: frozenset[str]` on each dialect class; inspect it lazily for expression-operation routes, and use metadata features/capabilities for admission routes. Do not add a second supported-engine list or infer support from method presence, because inherited refusal methods exist.
- `Dialect(name: str)` is an immutable expression policy with `name: str` and class-level `operations: frozenset[str]`; concrete class names are `DuckDBDialect`, `PostgresDialect`, `BigQueryDialect`, `SnowflakeDialect` in their correspondingly named engine modules. `dialect_scope(dialect: str) -> Generator[None]` retains its current string input, case folding and context restoration. `current_dialect(*values: ir.Value) -> Dialect` chooses scope, otherwise the first bound operand, otherwise the existing DuckDB default. `dialect_for_name(name: str) -> Dialect` provides explicit expression selection without admission queries. Cache only the four registered immutable instances; an unknown name creates a refusal-capable base dialect, not an executable generic backend. Replace all `dialect_of` consumers and delete that helper after cutover.
- Preserve existing helper signatures, including `integer_sequence(stop: int, name: str, *, dialect: str)`, `timestamp_parts(value: ir.TimestampValue, *, backend: str | None = None)`, and `narrow_exact(coefficient, scale: int, target: str | dt.DataType)`. These are useful existing APIs, not compatibility aliases. A supplied timestamp backend chooses the dialect explicitly, including for an unbound literal.
- Registry admission must not allocate an executor or open a session. `backend_provider(con)` resolves the engine then imports its `provider_module` and returns `PROVIDER`. Provider `matches`/`dialect` remain protocol methods backed by their record. `backend_capabilities(con)` remains observational: PostgreSQL is unverified without an identity query; DuckDB validates its native handle without querying; BigQuery/Snowflake retain their current module-based static evidence. Do not call querying admission from inspection.
- Record paths are exact: dialects `terrine.backends.engines.<name>:<ConcreteDialect>`; identity callbacks `terrine.backends.engines.<name>:identify`; provider modules `terrine.execution._native`, `terrine.execution._postgres`, `terrine.execution._bigquery`, `terrine.execution._snowflake`, respectively. DuckDB identity preserves its existing native-handle validation: an invalid native handle on an Ibis-module look-alike still produces the existing query/unverified capability refusal, not a newly exposed snapshot refusal. PostgreSQL performs the moved Ibis-floor/driver/version checks; its malformed-driver refusal remains `terrine.execution.snapshot_unavailable` with the existing `reason`. BigQuery/Snowflake retain existing module-based admission without introducing a new server probe. Engine modules must not import their wrapper modules at module initialization: use deferred imports for shared arithmetic so moving native declarations cannot create a numeric/timestamp/registry import cycle.

### Evidence and acceptance policy

This document specifies future checks; it does not claim that they have run. For a pure move, run existing owning tests before and after and record unchanged assertions/results; do not invent a test that merely verifies forwarding. For new contracts, record an actual failing assertion/import against the old behavior, then its passing result. An added edge test that already passes needs a narrowly scoped in-memory mutation of the owning operation to demonstrate its assertion fails; never commit the mutation or a helper script.

All five tasks share this acceptance contract: full unit tier, full ordinary integration tier, `make test-postgres`, and the completed architecture guard green; refusal codes and every context field other than `route` unchanged, with immutable/pickle/deepcopy behavior retained. Intermediate commits are not final task acceptance. Run focused red/green checks at each coherent task boundary, then run the expensive common acceptance commands **once, serially on the complete branch**; the same final-revision evidence satisfies every task. Do not launch concurrent or redundant full suites on the loaded host.

Common acceptance commands, from the assigned worktree. Export the settings printed by `uv run python -m tests.integration.postgres_scratch start`, then run `make test-postgres`; the scratch helper chooses a free port dynamically:

```bash
uv run pytest tests -n 0
uv run pytest integration -n 0
eval "$(uv run python -m tests.integration.postgres_scratch start)"
PYTEST_ADDOPTS='-n 0' make test-postgres
uv run pytest tests/test_engine_dispatch.py -n 0 -q
```

Expected: each exits 0; no selected PostgreSQL check skips; no engine-name comparison violations. Ordinary tiers retain the repository's live-warehouse/scale exclusions. Record exact commands, exit status and pytest summaries; do not reinterpret deselection as live warehouse evidence.

## Task 1: Introduce the registry and cut over provider/identity admission

**Files:**
- Create: `terrine/backends/registry.py`, `terrine/backends/engines/{__init__,duckdb,postgres,bigquery,snowflake}.py`, `tests/backends/test_engine_registry.py`, `integration/backends/test_engine_registry.py`.
- Modify: `terrine/backends/{dialect,capabilities,numeric}.py` (`exact_type` at numeric:73–88).
- Modify: `terrine/execution/{backend,_native,_postgres,_bigquery,_snowflake,inputs,_publish}.py` (provider classes/selection and `_postgres.py:27–99`).
- Modify: `terrine/runtime/backends/duckdb.py:15,38` (the remaining concrete-capability consumer); remove old constant exports in `capabilities.py`, with no re-exports.
- Modify: `AGENTS.md:238–259`, `docs/guides/backend-compatibility.md:3–22,38–49`, `CHANGELOG.md`.
- Test: `tests/execution/test_backend_provider.py`, `tests/test_optional_dependencies.py`, `tests/semantic/test_rules_import.py`, `integration/warehouse_execution/test_postgres_admission.py`.

**Interfaces:**
- Consumes: existing `BackendCapabilities`, `RefusalSpec`, `BackendProvider`, context scope and native session functions.
- Produces: all shared registry/selection contracts above, usable concrete `Dialect.exact_type(self) -> dt.DataType`, and each engine's `identify(con: Any) -> None` callback. The PostgreSQL callback owns the **only** classifier and qualified version read.

- [ ] **Step 1: Add admission/scope tests and migrate the classifier tests to admission behavior.** In `tests/backends/test_engine_registry.py`, add `test_scope_selection_restores_after_nested_error` asserting an outer `postgres` scope survives an inner mixed-case `SNOWFLAKE` scope and exception, and the previous unscoped default is restored. Assert effective `exact_type` changes rather than merely checking a private context variable. Keep new in-process lookup, scope and admission checks in that unit module. Add `test_registry_import_is_metadata_only` in `integration/backends/test_engine_registry.py`, using the existing subprocess import-blocking pattern to call `get_engine`/feature lookup while Ibis, all optional drivers, compiler and temporal imports are blocked. Extend existing unknown-backend and raw-driver tests to use `resolve_engine` and assert the full pre-existing code/context except route.

```python
def test_scope_selection_restores_after_nested_error():
    baseline = exact_type()
    with dialect_scope("postgres"):
        outer_type = exact_type()
        with pytest.raises(RuntimeError), dialect_scope("SNOWFLAKE"):
            assert exact_type() == dt.Decimal(38, 0)
            raise RuntimeError("stop inner scope")
        assert exact_type() == outer_type
    assert exact_type() == baseline
```

- [ ] **Step 2: Replace direct pure-classifier tests with `test_postgres_engine_identity_preserves_classification`.** Reuse the existing driver cursor fixture and version cases in `test_backend_provider.py:160–272`; call `ENGINES["postgres"].identify(con)`. Assert PostgreSQL 16 succeeds; 15/17 refuse as PostgreSQL/unverified; Redshift/Greenplum/CockroachDB refuse with their existing server/status. Add empty rows, empty text and unknown text (`"unknown"` or first token/unverified, as today), and an incompatible Ibis build that refuses before the version query. For rejected identities assert code, exact non-route context, immutability, deepcopy and pickle. Keep `test_postgres_admission_refuses_lookalike_before_source_schema_access` at the Model boundary and the no-query inspection assertion.
- [ ] **Step 3: Record red/invariance evidence.** Run `uv run pytest tests/backends/test_engine_registry.py integration/backends/test_engine_registry.py tests/execution/test_backend_provider.py tests/test_optional_dependencies.py tests/semantic/test_rules_import.py -n 0 -q`. New registry imports fail before implementation; migrated refusal cases need actual assertion-sensitive red evidence, not only a missing symbol. Record the old provider/lifetime invariance tests separately before moving them.
- [ ] **Step 4: Implement the immutable registry, lazy dialect selection and admission cutover.** Give each concrete subclass its existing exact-state type and make `numeric.exact_type` delegate. Move capability **values**, not just another tuple of their names, into the four records. Move each concrete provider to its existing engine execution module and remove `_PROVIDERS`/the engine selection chain. Preserve private connection creation, borrowed owner handles, release on errors/cancellation and retained-snapshot gates. Move the existing PostgreSQL classifier, version constants, Ibis-floor helper and identity read into its registered callback; delete `ServerIdentity`, `classify_server_version`, `verify_server_identity` and the now-unused `ibis_supported` from `_postgres.py`, not aliases. Keep the Ibis-floor refusal before the identity SQL. Reuse one PostgreSQL raw-handle validator: move it with identity admission and update its internal and external consumers, including `integration/warehouse_execution/test_postgres_admission.py`'s import. Lift `_SNAPSHOT_UNAVAILABLE` to `backends/capabilities.py` and change all six existing execution consumers to import it directly, so callbacks never import execution/runtime. Preserve its factory template/context verbatim.
- [ ] **Step 5: Pin routes and static-inspection behavior.** Add `test_unavailable_route_names_only_verified_operation_providers`: an unsupported `exact_state` request names all four registered verified providers; `exact_ratio` names only PostgreSQL/BigQuery. Change the in-memory registry entry status/support in the test and assert the offered destinations change, without asserting prose wording. For unavailable operations preserve `{backend, operation, status}` and the existing code; assert reconstructed errors preserve the complete new context. Initially define each dialect's operation declarations for its current implementation; do not expose a route to an implementation that still refuses. Document registry ownership and the moved read-only identity exception in AGENTS/guide, and add the CHANGELOG correction for stale DuckDB/PostgreSQL-only routes.
- [ ] **Step 6: Run focused green checks and commit.** Run the Step 3 command again; expected PASS, including no-query inspection and subprocess import isolation in its integration module. Run `make check TESTS="tests/backends/test_engine_registry.py integration/backends/test_engine_registry.py tests/execution/test_backend_provider.py tests/test_optional_dependencies.py tests/semantic/test_rules_import.py"`. Expected exit 0. Stage the changed files and commit `refactor: centralize engine admission and capabilities`. Acceptance also requires the common four final-branch commands above and unchanged PostgreSQL identity/lifetime behavior; this task is not accepted without them.

## Task 2: Move numeric differences into concrete dialects

**Files:**
- Modify: `terrine/backends/numeric.py:58–657`, `terrine/backends/dialect.py`, `terrine/backends/engines/{duckdb,postgres,bigquery,snowflake}.py`.
- Test: `tests/backends/test_numeric.py`, `integration/backends/test_numeric.py`, existing numeric/window PostgreSQL probes under `integration/warehouse_execution/`.

**Interfaces:**
- Consumes: Task 1 `current_dialect`, `dialect_for_name`, engine operation declarations and exact-state types.
- Produces: `Dialect.has_scaled_float_state: bool`; `decimal_coefficient(self, value: ir.Value, scale: int) -> ir.Value`; `coefficient_bounds(self, value: ir.Value) -> tuple[int, int]`; `finite_text(self, value: ir.Value) -> tuple[ir.FloatingValue, ir.StringValue]`; `binary64(self, text: ir.StringValue) -> ir.FloatingValue`; `exact_float(self, value: ir.Value, scale: int | ir.IntegerValue | None = None) -> tuple[ir.Value, ir.BooleanValue, ir.BooleanValue]`; `exact_ratio(self, numerator: ir.Value, denominator: ir.Value) -> ir.Value`; `exact_is_nonfinite(self, value: ir.Value) -> ir.BooleanValue`; `exact_to_float(self, value: ir.Value, scale: int | ir.IntegerValue = 0) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]`; `wide_type(self) -> dt.DataType`; `wide_abs(self, value: ir.Value) -> ir.Value`; `wide_quotient(self, value: ir.Value, divisor: ir.Value) -> ir.Value`; `exact_sum(self, value: ir.Value) -> ir.Value`; `exact_subtract(self, left: ir.Value, right: ir.Value) -> ir.Value`; `truncating_quotient(self, value: ir.IntegerValue, divisor: int | ir.IntegerValue) -> ir.IntegerValue`; `float_is_finite(self, value: ir.Value) -> ir.BooleanValue`.

- [ ] **Step 1: Record existing numeric invariance.** Run `uv run pytest tests/backends/test_numeric.py integration/backends/test_numeric.py -n 0 -q`. Retain existing exact quotient, nested quotient, overflow, NULL/nonfinite, partition-scale and coefficient-range assertions. Their source derivations are the specification; no new numerical constants or tolerance relaxation is authorized.
- [ ] **Step 2: Add the missing default-refusal contract.** In `test_engine_registry.py`, add `test_unknown_dialect_refuses_native_numeric_operations` covering `exact_type`, `exact_float`, `exact_ratio`, `exact_is_nonfinite`, `wide_type`, `exact_sum`, `exact_subtract` and `truncating_quotient`. Assert each existing operation label (`exact_state`, `exact_float`, `exact_ratio`, `exact_is_nonfinite`, `normalized`, `integer_quotient`) and `{backend: "unregistered", status: "unsupported"}`; assert deepcopy/pickle context preservation. Run `uv run pytest tests/backends/test_engine_registry.py -k native_numeric -n 0 -q`; record failure before implementing these methods, not a deliberately incorrect expected value.

```python
def test_unknown_dialect_refuses_native_numeric_operations():
    with pytest.raises(TerrineError) as caught:
        dialect_for_name("unregistered").exact_type()
    assert caught.value.code == "terrine.backend.capability_unavailable"
    assert {k: v for k, v in caught.value.context.items() if k != "route"} == {
        "backend": "unregistered", "operation": "exact_state", "status": "unsupported",
    }
    assert copy.deepcopy(caught.value).context == caught.value.context
    assert pickle.loads(pickle.dumps(caught.value)).context == caught.value.context
```

Extend this test's parameterization with the remaining named operations and their required operands; preserve the asserted operation labels rather than inferring them from Python method names.

- [ ] **Step 3: Implement the method interfaces by moving existing branch bodies.** Move native numeric types/UDF declarations to their owning engine module. Shared parsing, scale text and normalization arithmetic remain in `numeric.py`; they call the selected dialect's primitives instead of carrying a dialect-name argument. Remove `_SCALED`, all numeric engine-name conditions, `_wide_type(dialect)`, `_wide_abs(..., dialect)`, `_wide_quotient(..., dialect)` and `_binary64(..., dialect)` dispatch. Preserve the DuckDB physical integer-coefficient exception, Snowflake decimal-text/TRY_TO_NUMBER/TRY_TO_DOUBLE and remainder-before-division behavior, PostgreSQL nonfinite/underflow handling and BigQuery SAFE arithmetic. Base methods either hold an already portable-correct implementation or raise the same existing capability refusal. Share existing scaled-state arithmetic once through engine-independent helpers; do not copy large algorithms into both engine subclasses.
- [ ] **Step 4: Re-run the oracle selection.** Run `uv run pytest tests/backends/test_numeric.py integration/backends/test_numeric.py tests/backends/test_engine_registry.py -n 0 -q`. Expected PASS with the same output/flag/bound assertions as before. Extend existing parametrization only where a moved primitive lacks coverage; do not introduce a second conformance suite or assert compiled SQL text.
- [ ] **Step 5: Run static/focused gates and commit.** Run `make check TESTS="tests/backends/test_numeric.py tests/backends/test_engine_registry.py integration/backends/test_numeric.py"`. Expected exit 0. Commit `refactor: encapsulate engine numeric expressions`. Acceptance is the common four final-branch commands, existing numeric tolerance/oracle assertions unchanged, and refusal/context invariance other than routes.

## Task 3: Move timestamp operations and their shape policies

**Files:**
- Modify: `terrine/backends/timestamps.py:42–421`, `terrine/backends/dialect.py`, all four concrete dialect modules.
- Modify: `terrine/temporal/first.py:63–85`, `terrine/temporal/aggregate.py:203–237`, `terrine/compile/shapes/{time_to_event,cohorts}.py`.
- Test: `tests/backends/test_timestamps.py`, `tests/temporal/test_first_occurrence.py`, `tests/compile/{test_time_to_event,test_cohorts}.py`, `integration/compile/test_time_to_event.py`, existing PostgreSQL timestamp/shape probes.

**Interfaces:**
- Consumes: Task 2 numeric primitives and Task 1 explicit/scope dialect selection.
- Produces: `Dialect.timestamp_is_finite(self, value: ir.TimestampValue) -> ir.BooleanValue`; `timestamp_extremes(self, dtype: dt.Timestamp) -> tuple[ir.TimestampScalar, ir.TimestampScalar]`; `utc_wall_clock(self, value: ir.Value) -> ir.TimestampValue`; `utc_instant(self, value: ir.Value) -> ir.TimestampValue`; `timestamp_text(self, value: ir.TimestampValue) -> ir.StringValue`; `timestamp_parts(self, value: ir.TimestampValue) -> tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue]`; `timestamp_order_key(self, value: ir.TimestampValue) -> ir.Value`; `reverse_time_key(self, value: ir.TimestampValue) -> ir.Value`; `bounded_nanoseconds(self, scale: int | None) -> bool`; `narrow_exact(self, coefficient: ir.Value, scale: int, target: str | dt.DataType) -> ir.Value`. Existing free helper signatures remain unchanged.

- [ ] **Step 1: Run timestamp/shape invariance tests.** Run `uv run pytest tests/backends/test_timestamps.py tests/temporal/test_first_occurrence.py tests/compile/test_time_to_event.py tests/compile/test_cohorts.py integration/compile/test_time_to_event.py -n 0 -q`. Record the existing session-timezone, mixed precision, strict boundary, pre-epoch bucket, overflow and nanosecond-axis results before moving code.
- [ ] **Step 2: Add explicit dialect/literal and refusal edge tests.** Keep `test_explicit_timestamp_backend_overrides_literal_default` in `tests/backends/test_timestamps.py` for the PostgreSQL scale rejection below. Add `test_explicit_timestamp_backend_preserves_nested_parts_and_scope` in the same module: evaluate an unbound microsecond positive infinity with `backend="duckdb"` inside `dialect_scope("snowflake")`, assert `(rank, seconds, nanos) == (1, 0, 0)`, and assert the ambient Snowflake scope and prior outer scope are both restored. This admitted-scale case must reach nested timestamp helpers; the early PostgreSQL scale rejection alone is insufficient. Require the new case to fail when a nested helper is changed in memory to reselect the ambient Snowflake dialect instead of using the explicit DuckDB dialect; record that failure and the restored passing run. Add `test_timestamp_default_refusals_keep_operation_context` to `tests/backends/test_engine_registry.py` for unknown-name UTC conversion/finiteness/text. Run `uv run pytest tests/backends/test_timestamps.py tests/backends/test_engine_registry.py -k 'explicit_timestamp or timestamp_default' -n 0 -q`; record red evidence using the owning selection/operation if existing behavior already passes.

```python
def test_explicit_timestamp_backend_overrides_literal_default():
    dtype = dt.Timestamp(scale=9)
    value = ibis.literal("2025-01-01 00:00:00", type=dtype)
    with dialect_scope("snowflake"), pytest.raises(TerrineError) as caught:
        timestamp_parts(value, backend="postgres")
    assert caught.value.code == "terrine.temporal.timestamp_order_unsupported"
    assert dict(caught.value.context) == {"dtype": str(dtype)}


def test_explicit_timestamp_backend_preserves_nested_parts_and_scope(connection):
    baseline = current_dialect().name
    value = ibis.literal("infinity").cast(dt.Timestamp(scale=6))
    with dialect_scope("snowflake"):
        rank, seconds, nanos = timestamp_parts(value, backend="duckdb")
        assert current_dialect().name == "snowflake"
        query = ibis.struct(
            {"rank": rank, "seconds": seconds, "nanos": nanos}
        ).name("parts").as_table()
        actual = connection.to_pyarrow(query)["parts"][0].as_py()
        assert (actual["rank"], actual["seconds"], actual["nanos"]) == (1, 0, 0)
        assert current_dialect().name == "snowflake"
    assert current_dialect().name == baseline
```

- [ ] **Step 3: Move native timestamp code and dispatch.** Move `_NATIVE`/`_FINITE` decisions, UDFs, PostgreSQL epoch extraction, each engine's parts/order/extremes policy and Snowflake text/UTC conversion into concrete dialect modules. Keep engine-independent precision promotion and timestamp comparison in `timestamps.py`. Native infinity handling, timestamp-rank null handling, precision refusals and timezone operations remain byte-for-byte algorithmically equivalent. `timestamp_parts(..., backend=...)` must invoke the explicit dialect without losing it in nested helper calls; bind that dialect once throughout its body.
- [ ] **Step 4: Migrate temporal and shape consumers.** Move `first._reverse_time_key`'s scalar-versus-struct operation into dialect methods. Move `aggregate.narrow_exact`'s safe-cast-versus-range-check operation into dialect methods; keep `_scaled_text` shared without creating a backends→temporal dependency (move it to `numeric.py` and migrate both aggregate/cohort imports). Delegate `_bounded_nanoseconds` to the selected dialect. For cohort summaries establish scope with `dialect_scope(current_dialect(*bound_columns).name)` from the bound table after the original scope has ended; apply the same selected instance to the unbound origin literal. Replace `dialect_of` imports/calls here; retain the existing outer-scope restoration and cohort timestamp remainder algorithm.
- [ ] **Step 5: Run green checks and commit.** Run the Step 1 selection plus `tests/backends/test_engine_registry.py`; expected PASS. Run `make check TESTS="tests/backends/test_timestamps.py tests/backends/test_engine_registry.py tests/temporal/test_first_occurrence.py tests/compile/test_time_to_event.py tests/compile/test_cohorts.py integration/compile/test_time_to_event.py"`. Expected exit 0. Commit `refactor: encapsulate engine timestamp semantics`. Acceptance includes the common four final-branch commands, unchanged timestamp/shape refusal context and PostgreSQL precision/timezone probes.

## Task 4: Centralize relational operations and remove supported-engine lists

**Files:**
- Modify: `terrine/backends/{joins,extrema,sequences,dialect}.py`, all concrete dialect modules.
- Modify: `terrine/compile/anchors.py:146–177`, `terrine/temporal/first.py:29–34,125–132`, `terrine/temporal/prefix.py:34–44`, `terrine/semantic/rules.py:62–68`.
- Modify: `terrine/spec/validate.py:451–463`, `terrine/export/preflight.py:174–200`, `tach.toml`.
- Test: `tests/backends/{test_sequences,test_engine_registry}.py`, `tests/temporal/{test_prefix_lookup,test_first_occurrence}.py`, `tests/semantic/{test_metric_rules,test_rules_import}.py`, `tests/compile/test_anchors.py`, `tests/spec/test_nonadditive_spec.py`, `tests/export/test_warehouse_preflight.py`.

**Interfaces:**
- Consumes: `get_engine`, `engine_supports`, `current_dialect`, `dialect_for_name` and all earlier dialect operations.
- Produces: `Dialect.has_native_asof: bool`; `asof_join(self, left: ibis.Table, right: ibis.Table, *, left_unit: str, left_key: str, right_unit: str, right_key: str, inclusive: bool) -> ibis.Table`; `least(self, *values: Any) -> Any`; `greatest(self, *values: Any) -> Any`; `integer_sequence(self, stop: int, name: str) -> ibis.Table`; `supports_exact_prefix(self, backend_name: str) -> bool` (metadata/build checks only). No new public package-root exports.

- [ ] **Step 1: Add/extend owning feature-admission tests.** In `test_metric_rules.py`, assert case-insensitive `event_resolution` admits the four current engines and rejects an unknown engine without importing query packages. Extend `test_first_occurrence.py` with unbound and different-connection rejection, preserving `terrine.temporal.first_occurrence_unsupported_backend` and its sole `backend` context. Add a cheap `_require_backend` test in `test_engine_registry.py` for wrong DuckDB build and a registered non-DuckDB backend outside its matching scope, preserving `terrine.temporal.exact_unsupported`/`{backend}` and issuing no capability SQL. Extend existing spec/export tests so non-DuckDB sources still get the existing quantile/approx-distinct/export-snapshot refusals before execution; assert their existing context without widening admission.

```python
@pytest.mark.parametrize(
    ("name", "expected"),
    [("DUCKDB", True), ("postgres", True), ("bigquery", True),
     ("snowflake", True), ("unregistered", False)],
)
def test_event_resolution_retains_registered_feature_boundary(name, expected):
    assert supports_event_resolution(name) is expected
```

- [ ] **Step 2: Record red/invariance evidence.** Run `uv run pytest tests/backends/test_sequences.py tests/backends/test_engine_registry.py tests/temporal/test_prefix_lookup.py tests/temporal/test_first_occurrence.py tests/semantic/test_metric_rules.py tests/semantic/test_rules_import.py tests/compile/test_anchors.py tests/spec/test_nonadditive_spec.py tests/export/test_warehouse_preflight.py -n 0 -q`. Capture existing assertion results and assertion-sensitive red evidence for new cases; no new network tests in `tests/`.
- [ ] **Step 3: Move relational defaults and native overrides.** Move the current non-native ASOF union/window implementation into the base dialect; DuckDB/PostgreSQL/Snowflake override with the existing native form. Do not replace it with a newly designed range join. Base extrema use the current coalesce-before-LEAST/GREATEST implementation; DuckDB/PostgreSQL override only to skip the unnecessary coalescing. Keep the existing native unnest sequence implementation as the base and move the decimal-digit cross-product override to Snowflake. Preserve aliases, duplicate request handling, NULL states, strict/inclusive cutoffs and sequence type/cardinality; remove `_NATIVE_ASOF`/`_SKIPS_NULLS` and selection branches from wrappers.
- [ ] **Step 4: Replace the four remaining engine lists and extra DuckDB checks.** Anchor naive-UTC admission, first-occurrence admission and semantic event-resolution query registry features while retaining their owning refusal factories/context. Exact-prefix admission selects its dialect implementation, preserving DuckDB `1.5.6` and the current matched-scope requirement for PostgreSQL/BigQuery/Snowflake; unknown/unbound stays refused. Replace spec quantile/approx-distinct and warehouse-preflight DuckDB string comparisons with their corresponding features, not the broad `transaction_view` capability (PostgreSQL is transaction-capable but remains ineligible here). Add `terrine.backends` dependencies for `terrine.semantic`, `terrine.spec` and `terrine.export` in tach; do not add a backends dependency on execution/temporal/compiler. Delete `dialect_of` after its last production/test consumer migrates.
- [ ] **Step 5: Run green/static checks and commit.** Re-run Step 2; expected PASS. Run `make check TESTS="tests/backends tests/temporal/test_prefix_lookup.py tests/temporal/test_first_occurrence.py tests/semantic tests/compile/test_anchors.py tests/spec/test_nonadditive_spec.py tests/export/test_warehouse_preflight.py"`. Expected exit 0. Commit `refactor: centralize engine expression and feature dispatch`. Acceptance requires the common four final-branch commands, unchanged half-open/null/alias behavior and all five duplicated engine lists removed.

## Task 5: Enforce the engine boundary and close documentation/evidence

**Files:**
- Create: `tests/test_engine_dispatch.py`.
- Modify: `AGENTS.md:238–259`, `docs/guides/backend-compatibility.md`, `docs/reference/refusals.md`, `CHANGELOG.md` as needed to finish the same cutover.
- Test: all existing unit/integration tiers and live PostgreSQL probes; `.github/workflows/warehouse-backends.yml` is consumed, not changed.

**Interfaces:**
- Consumes: the complete registry/dialect implementation from Tasks 1–4.
- Produces: `test_no_engine_selection_outside_engine_modules`, backed by `engine_selection_violations(source: str, *, path: str) -> tuple[int, ...]` in the same test module (offending one-based source lines), with no baseline/debt allowlist; one final evidence set for all tasks.

- [ ] **Step 1: Write the architecture guard and prove it detects regressions.** Scan every Python file under `terrine/`, excluding only `terrine/backends/registry.py` and the concrete engine files `terrine/backends/engines/{duckdb,postgres,bigquery,snowflake}.py`, `terrine/execution/{_native,_postgres,_bigquery,_snowflake}.py`, `terrine/runtime/backends/duckdb.py`. Report path/line for `==`, `!=`, `in`, `not in`, `.isin()` or match-case engine selection against engine-name literals, module strings `ibis.backends.<engine>`, or names assigned engine-literal collections. Detect comparisons in either operand order and `.casefold()` forms. Derive the engine literal set from `ENGINES`; do not exempt all of `backends/`, `execution/`, compiler or temporal code. Driver-module authenticity checks against connector roots, SQL dialect arguments, imports and docstrings are not engine selection. Keep the scanner specifically about engine identities, not ordinary product fields coincidentally named `name`/`dialect`.
- [ ] **Step 2: Test the guard's own boundary.** Parameterize tiny in-memory snippets for direct/reversed comparisons, membership through a named set, `.casefold()`, `type(con).__module__`, `.isin()` and match-case; each must report a violation outside an engine file and none in an exact allowed engine file. SQL dialect arguments and `type(raw).__module__.split(...)` driver checks produce no violation. Run `uv run pytest tests/test_engine_dispatch.py -n 0 -q` first with one owning snippet check disabled in memory to record a genuine failing assertion, then restored. The package scan must also catch an in-memory reintroduction of `dialect == "duckdb"` in a non-engine module. Do not create a permanent source rewrite utility or commit temporary files.

```python
@pytest.mark.parametrize(
    "source",
    ['if dialect == "duckdb": pass',
     'if "postgres" != backend.casefold(): pass',
     'NAMES = {"bigquery", "snowflake"}\nif dialect in NAMES: pass'],
)
def test_guard_rejects_engine_selection(source):
    assert engine_selection_violations(source, path="terrine/temporal/example.py")
    assert not engine_selection_violations(
        source, path="terrine/backends/engines/duckdb.py",
    )
```

- [ ] **Step 3: Finish evergreen documentation.** AGENTS names the registry plus one dialect override module as the extension points, keeps study SQL/UDF prohibitions and explains the narrow identity-introspection exception. The compatibility guide names `Engine`, `SessionStrategy`, identity admission and capabilities without claiming generic or newly verified engines. Update the refusal reference only for moved declaration locations and registry-derived routes; no code/context rename. CHANGELOG states that capability routes now include all applicable verified engines instead of stale DuckDB/PostgreSQL-only guidance, and engine execution behavior is unchanged. Leave examples, saved fixtures, schemas and package-root exports untouched unless inspection finds a real old import; no new provenance stamp is authorized.
- [ ] **Step 4: Run the complete common acceptance once.** Run the four commands in Evidence and acceptance policy, serially, and record their actual output. Then run `make check TESTS="tests/test_engine_dispatch.py tests/backends/test_engine_registry.py"` for full static gates and focused tests before the final implementation commit. Expected all exit 0. Check code/context invariance through the migrated owning refusal tests and existing serialization tests, not message snapshots. Commit `refactor: enforce centralized engine dispatch` after a staged vocabulary check excluding `docs/superpowers/`.
- [ ] **Step 5: Obtain the one reserved warehouse run.** The owner makes the complete reviewed revision available to CI and dispatches `warehouse-backends` with `backend_mode=all` exactly once after the local gates. Record the revision, run URL and PostgreSQL/BigQuery/Snowflake job conclusions. Do not dispatch from this worktree, run either cloud warehouse locally, or claim acceptance from an earlier revision. A failed job requires fixing its owning regression and fresh authorized evidence; a missing owner dispatch is an explicit remaining exit-gate blocker, not a waiver.

## Self-review and handoff

- Spec coverage: Task 1 owns registry/provider/capability consolidation, single PostgreSQL classifier, lazy admission and route correction; Task 2 owns all numeric dispatch; Task 3 owns timestamps, reverse keys, narrowing and both named shapes; Task 4 owns joins/extrema/sequences, all four remaining duplicated lists and the additional spec/export comparisons; Task 5 owns the permanent guard, documentation and final warehouse evidence. No generic-engine or provenance change is included.
- Step scan: each code step identifies exact owning files/signatures and current bodies to move; existing oracle tests are invariance checks, new edge contracts require actual red evidence. Intermediate tasks are coherent review units, but expensive whole-branch acceptance is shared rather than repeatedly executed.
- Type consistency: one `Engine.dialect` lazy class property, one `Engine.identify(con)` method, one metadata-only lookup API, one string-based scope API and one explicit `dialect_for_name` expression path. No eager dialect imports in registry/semantic metadata, no dynamic generic opt-in, no provider or classifier compatibility alias.
- Review Focus coverage: scope restoration/import isolation/admission-before-schema are Task 1 tests; numeric flags/ranges/oracles are Task 2; timezone/precision/remainders/literal-origin composition are Task 3 and existing cohort tests; strict relational lookups/unsupported feature boundaries are Task 4. The guard itself has detection and non-violation tests in Task 5.
- Proportion: preserve the original algorithms and existing test oracles instead of transcribing bodies or building another abstraction/conformance framework. The method list is the shared contract needed to separate the changes safely.
- Execution is deliberately **not started** by this plan-only assignment. The implementation owner should execute tasks in order because they share registry/dialect interfaces, review the complete branch, and preserve the owner-reserved warehouse gate. No merge, publication or owner action is authorized here.
