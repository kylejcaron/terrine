# Generic Ibis Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable explicitly opted-in, read-only Ibis backends through the portable base Dialect, with durable unverified provenance and coded operation refusals.

**Architecture:** Extend the existing immutable registry with a separate `GENERIC` record and carry its admission through Model and execution rather than storing connection-global permission. Complete the base expression policy, add a NoSnapshot provider, and stamp the existing result metadata without changing its wire versions. Registered dialect overrides, identity checks and provider lifetimes stay authoritative.

**Tech Stack:** Python 3.13+, frozen dataclasses, existing pydantic Catalog models, Ibis 11–12 (PostgreSQL requires 12), PyArrow, DuckDB 1.5.6, PostgreSQL 16, pytest, Ruff and tach; no new dependency or extra.

**Spec:** `docs/superpowers/specs/2026-10-26-generic-backend-design.md`.

## Global Constraints

- Public API changes update production callers, exports, docstrings, guides, examples, fixtures and CHANGELOG in the same change; clean cutover, no aliases.
- Refusals keep stable package-wide codes; renamed codes get a breaking CHANGELOG entry; every new refusal survives pickle and deepcopy.
- Null versus zero and per-cell null reasons survive Arrow, manifests and exports unchanged.
- Package version stays `0.1.0.dev0`; no tag, push, publish or GitHub mutation.
- Tests belong in the cheapest tier; record red-first evidence for every new test, assert behavior rather than wording/wiring, and run `make check TESTS="<affected paths>"` before implementation commits.
- Study/compiler/temporal code remains Ibis expressions; native bindings belong in engine modules. No engine-name selection outside the registry and engine-specific modules.
- Verification for this change is local DuckDB plus local PostgreSQL 16, including both with the generic dialect forced. No new CI jobs, services, paid-warehouse runs or dependency extras are introduced.

Implementation is confined to `/Users/kylejcaron/terrine/.worktrees/generic-backend`, branch `feat/generic-backend`; use absolute paths for file operations and that directory as command cwd. Do not edit another worktree or run merge, rebase, stash or push. The registry prerequisite plan at `/Users/kylejcaron/terrine/.worktrees/engine-registry/docs/superpowers/plans/2026-10-26-engine-registry.md` is read-only. Before execution, the implementation owner must make its completed registry interfaces available on this branch; do not reconstruct a competing registry from the pre-refactor files here. The conformance prerequisite is already present at `integration/conformance/test_engines.py`.

## Review Focus

1. `allow_unverified=True` must not admit raw drivers, name-only objects, a rejected registered server or a second Model without consent; Task 1 pins admission identity and scope.
2. SQL dialect, portable expression policy and actual backend identity differ on generic Models; nested/literal scopes, source feature admission and cohort summaries must preserve the right one; Tasks 1 and 2 pin this composition.
3. Fixed scale must not erase source decimal precision, overflow prefixes silently, or leave an uncorrected floating integer quotient above `2**53`; Task 2 pins coefficients, quantization, flags and exact signed quotients.
4. NoSnapshot must neither change/close the caller connection nor imply a consistent multi-statement view; lazy batches and early close must preserve coded failures and release readers; Task 1 pins lifecycle and forbidden effects.
5. Legacy unstamped verified metadata must remain readable, while a partial unverified stamp must never be upgraded to verified; Task 3 pins Arrow/JSON compatibility, immutable refusal context and every producer.

---

## File and interface map

Paths below are relative to the assigned worktree. Names already promised by the registry plan are consumed as written; its private implementation details are not assumed.

| Files | Responsibility |
| --- | --- |
| `terrine/backends/{registry,dialect,capabilities}.py`, `terrine/errors.py` | Generic record/admission, base expression selection, read-only unverified capability policy, warning category. Registry imports stay metadata-only. |
| `terrine/model.py`, `terrine/semantic/{bind,rules}.py` | Three constructor flags, retained immutable admission, real Catalog SQL dialect versus derived source expression policy. |
| `terrine/backends/{numeric,timestamps,joins,extrema,sequences}.py` | Existing wrappers/shared algorithms; base implementations live in `dialect.py`, not another dispatch chain. |
| `terrine/compile/{columns,anchors,cost,dispatch}.py`, `terrine/compile/shapes/cohorts.py`, `terrine/temporal/{first,prefix,aggregate}.py`, `terrine/spec/validate.py` | Consume admitted policy/capabilities, preserve summaries and exact-state type distinctions. |
| `terrine/execution/{backend,inputs}.py`, new `terrine/execution/_generic.py` | Engine-bound provider selection, optional query preflight, portable read-only relation resolution and NoSnapshot lifetime. |
| `terrine/{frame,data}.py`, `terrine/runtime/{api,snapshot}.py`, `terrine/dataset/reference.py` | Admission/capability propagation; complete result stamps and unchanged protected runtime gates. |
| `terrine/__init__.py`, `docs/guides/public-api.md` | Only new root export: `UnverifiedBackendWarning`; no public engine/provider knob. |
| New `tests/backends/test_generic.py`, `tests/execution/test_generic.py`, `integration/backends/test_generic.py`, `integration/execution/test_generic.py` | Cheapest owning admission/capability tests, batched numerical/relational oracles and real backend/lifecycle compositions. |
| Existing `tests/test_metadata.py`, `tests/fixtures/baseline.py`, `tests/export/conftest.py`, `tests/test_frame_runtime.py`, `tests/dataset/test_winsorization_definition.py` | Stamp round trips/refusals and explicit new constructor fields in existing builders; migrate every additional `Provenance(...)` caller discovered before Task 3. |
| Existing `integration/conformance/test_engines.py`, `integration/conftest.py` | Same seven known answers; test-only `--generic-dialect` selector, no duplicate suite. |
| `AGENTS.md`, `docs/guides/{backend-compatibility,capabilities,public-api}.md`, `docs/reference/refusals.md`, `CHANGELOG.md`, new `examples/unverified_backend.py` | Support tiers, API/warning workflow, numerical limits backed by probes, new metadata refusal and runnable opt-in. |

### Shared interface decisions

- `resolve_engine(con: Any, *, allow_unverified: bool = False) -> Engine`; registered identity failures never fall through. Lazy `_identify_generic(con: Any) -> None` in `registry.py` checks genuine Ibis ancestry and rejects registered-module impersonation. `GENERIC.ibis_module=""`, `identity_requires_query=False`; its lazy dialect path is `terrine.backends.dialect:Dialect`, identity path `terrine.backends.registry:_identify_generic`, provider module `terrine.execution._generic`. No new `Engine` fields are required.
- `get_engine("generic") -> GENERIC`; unknown actual names still return `None`. Existing `engine_supports(name: str, feature: str) -> bool` consumes the separate record for the explicit policy name only. `ENGINES` and supported routes contain verified registrations, not GENERIC.
- `backend_name(con: Any) -> str` in `registry.py` centralizes actual casefolded connection identity. `expression_dialect(engine: Engine) -> str` returns the registered name or `"generic"` for the generic record. These are internal metadata helpers, not root exports.
- `backend_provider(con: Any, *, engine: Engine | None = None) -> BackendProvider` reuses a supplied admitted engine after module/identity validation; absent admission keeps default-deny resolution. An admitted GENERIC cannot be substituted for a registered connection. Provider `.dialect(con)` continues to return the **real SQL dialect**.
- `Model._engine: Engine` is a frozen internal field. `Model.__init__`, `_bind` and `_for_execution` retain it; `normalize_source(..., engine: Engine | None = None)` and its fallback `_relation(..., engine: Engine | None = None)` consume it. `BoundSource.dialect=expression_dialect(engine)`; `Catalog.dialect` remains the provider SQL dialect and mismatch refusal remains unchanged.
- `BackendCapabilities.require(operation: str, *, backend: str, route: str, allow_unverified: bool = False) -> None` admits `None` only for `dialect_admission`, `exact_timestamps`, `exact_decimals`, `arrow_batches`; never `False`. Callers derive this permission from admitted engine status, not an independently set boolean. Observational `backend_capabilities(con)` stays query-free and does not imply opt-in.
- `bind_resolver`, `open_execution` and `required_relations` retain their existing positional/keyword contracts and add keyword-only `engine: Engine | None = None`. Preserve existing signatures of every expression wrapper and registry-plan Dialect method.
- Generic provider exports `PROVIDER`; `NoSnapshotSession` implements `ExecutionSession`. Add optional `ExecutionContext._preflight: Callable[[Any], None] | None`, invoked before query compilation. Only generic execution installs `NoSnapshotSession.admit(query: Any) -> None`, which lazily validates required decimal operations and translates unsupported query compilation at the boundary without invoking a native dialect.
- Add required `Provenance.engine_name: str`, `.dialect: str`, `.engine_status: Literal["verified", "unverified"]`; shared `data._decode_engine_stamp(value: Mapping[str, Any]) -> tuple[str, str, Literal["verified", "unverified"]]` returns those fields in that order and owns complete-stamp validation/all-absent historic decoding. New writers never omit them. Arrow v2/key `terrine.materialized_frame.v2`, JSON v1 and outer manifest versions do not change.

## Task 1: Admit and execute explicitly opted-in read-only backends

**Files:** Modify `terrine/backends/{registry,capabilities,dialect}.py`, `terrine/{errors,model,frame,__init__}.py`, `terrine/execution/{backend,inputs}.py`, `terrine/semantic/{bind,rules}.py`, `terrine/compile/{columns,anchors,cost}.py`, `terrine/spec/validate.py`, `terrine/dataset/reference.py`, `terrine/runtime/snapshot.py`; create `terrine/execution/_generic.py`, `tests/backends/test_generic.py`, `tests/execution/test_generic.py`, `integration/execution/test_generic.py`; extend `tests/execution/test_backend_provider.py`, `tests/semantic/test_rules_import.py`, `tests/test_optional_dependencies.py`. Update `docs/guides/public-api.md`, `docs/guides/backend-compatibility.md` and CHANGELOG for this API commit.
Add only the required `terrine.model` → `terrine.backends` edge in `tach.toml` for admitted engine metadata; preserve metadata-only imports.

**Interfaces:** Consumes registry `Engine`, `SessionStrategy`, `ENGINES`, lazy identity/dialect properties, supported routes, existing `BackendProvider`/`ExecutionSession`/`ExecutionContext` and capability/numerical refusals. Produces all admission/Model/capability interfaces above, root `UnverifiedBackendWarning`, generic `PROVIDER`, `NoSnapshotSession` and its query preflight. This task includes the real provider so the public opt-in never ships pointing at a missing module; Task 2 expands its portable expression coverage.

- [ ] **Step 1: Add cheap admission and capability tests.** In `test_generic.py`, define a genuine `BaseBackend` test double whose table/SQL/schema access raises immediately; it is an unregistered Ibis backend, not a `.name` echo. Reuse registered identity fixtures from `test_backend_provider.py`. Add these tests with the following assertions (test setup supplies the named connection and no sources are read):

```python
# test_unregistered_admission_requires_explicit_choice
with pytest.raises(TerrineError) as caught:
    resolve_engine(unregistered)
assert caught.value.code == "terrine.backend.capability_unavailable"
assert {k: v for k, v in caught.value.context.items() if k != "route"} == {
    "backend": "unregistered", "operation": "query", "status": "unverified",
}
assert "allow_unverified=True" in caught.value.context["route"]
with pytest.warns(UnverifiedBackendWarning):
    assert resolve_engine(unregistered, allow_unverified=True) is GENERIC
assert dict(copy.deepcopy(caught.value).context) == dict(caught.value.context)
assert dict(pickle.loads(pickle.dumps(caught.value)).context) == dict(caught.value.context)
with pytest.raises(TypeError):
    caught.value.context["backend"] = "changed"
```

Parameterize `test_opt_in_never_bypasses_connection_identity` over raw driver/name-only fake/rejected PostgreSQL look-alike and both flags; retain their owning code/non-route context and zero source access. Add `test_unverified_read_permission_cannot_enable_writes`, checking each of the four `None` read capabilities accepts the explicit permission while all seven `False` capabilities still refuse; `scan_estimate=None` still refuses under its existing strict-budget contract. Add `test_generic_feature_metadata_is_driver_free` and `test_model_binding_flag_is_boolean` (all three constructors; `"true"` refuses with `terrine.model.invalid_binding`). Use no source-text/warning-message snapshots.

- [ ] **Step 2: Add boundary/lifetime tests.** Cheap tests in `tests/execution/test_generic.py` use a backend double that actually raises at compile/execute/lazy iteration; assert capability code, actual backend, operation, original `__cause__`, and that `TerrineError`, cancellation/process-control exceptions are preserved. Test failures from Terrine-owned code outside the boundary are not relabeled. In `integration/execution/test_generic.py`, own a real in-memory SQLite backend and tables via the Ibis base dependency. Define a tiny count-only Catalog and integer relation; exercise default denial, opt-in warning, a successful `context.execute` integer/count read, public Model→Frame count attempt and a second default-deny Model. Materialization must produce the count or an operation-specific coded timestamp refusal, not an uncoded exception. Separate boundary/read evidence from the public count result: an unsupported full workflow cannot turn every test into an accepted refusal.

```python
# test_generic_context_is_statement_scoped_and_borrowed
assert context.owner_con is con and context.con is con
assert context.snapshot.snapshot_id is None
assert context.snapshot.backend == "sqlite"
assert context.snapshot.replay_equivalence == "unverified"
assert first_read == 1
# Fixture changes its own source to 2 between reads, outside Terrine's provider.
assert second_read == 2
assert con.to_pyarrow(con.table("numbers"))["value"].to_pylist() == [2]
```

Add `test_decimal_unavailability_refuses_only_decimal_query`: on the same connection a count succeeds, then a fixed-decimal arithmetic query either passes all scalar admission witnesses or refuses with capability code/`operation="exact_state"`; if SQLite lacks exact decimal arithmetic, assert refusal, never affinity-rounded success. Add a session double proving the scalar decimal witness is checked only once per context that needs it, not for count-only reads. Witnesses must include scale-9 low digits at large offsets, arithmetic/cancellation, signed quotient and near-domain boundaries; compilation-only support is insufficient. Finally parameterize protected requests (stage, retained capture, publication/reopen) and assert capability code, `status="unsupported"`, operation, zero writes/destination artifacts and caller still usable; strict unknown scan budgets still refuse.

- [ ] **Step 3: Record assertion-sensitive red evidence.** Run `uv run pytest tests/backends/test_generic.py tests/execution/test_generic.py integration/execution/test_generic.py tests/execution/test_backend_provider.py tests/test_optional_dependencies.py tests/semantic/test_rules_import.py -n 0 -q`. Expected before implementation: missing interfaces plus failing meaningful admission/read assertions. Record command, exit status and failing assertions; do not count collection failure alone as proof of every behavior.
- [ ] **Step 4: Implement shared admission and constructor forwarding.** Add keyword-only `allow_unverified: bool = False` to `Model.from_catalog`, `from_tables`, `from_increment`; validate before relation access and call `resolve_engine` exactly at public admission. Retain `_engine` through Model creation/rebinding; reuse it via the named engine arguments. Validate Catalog agreement using real SQL dialect, emit the warning from explicit generic admission with caller-facing stacklevel, and lazily expose it at the root. Implement GENERIC's exact capability values/features from the spec. Source feature metadata, preview, anchors and batch gates use admitted policy; strict scan/protected runtime gates never acquire permission.
- [ ] **Step 5: Implement the generic provider/session.** `PROVIDER` implements the existing protocol, resolving tables through Ibis and SQL through existing parser admission in the actual dialect. Refuse retained bindings before metadata/source access. Build `ExecutionContext` on the borrowed caller connection, unknown generation receipts and `_preflight=session.admit`, with no private connection/transaction. Implement local `_call_backend(operation: str, action: Callable[[], Any]) -> Any` only around actual backend calls; preserve coded/numeric failures and chain other backend errors into capability refusals. Preflight lazily inspects graph types and compiles/executes tiny decimal scalar witnesses only when needed, caching witnessed operations per session. No backend lists, raw SQL probes, native-policy retries or binary64 state. Generic operation refusals must name their primitive rather than leaking the compile-inspection snapshot refusal.
- [ ] **Step 6: Implement resource-safe batches and execution rebinding.** Wrap reader construction and lazy iteration; close only owned readers and invalidate the context on close, never reset the caller. Retain Engine through `_for_execution` so schema checks/cost/materialization do not revert to default denial. Add early-close and late-failure cases asserting reader release, coded failures and subsequent caller query success. Generic capability gates remain centralized; direct runtime calls without Model admission remain denied.
- [ ] **Step 7: Run focused green/static gates.** Re-run Step 3; expected PASS, including successful generic reads and unchanged protected/runtime lifetimes. Run `make check TESTS="tests/backends/test_generic.py tests/execution/test_generic.py integration/execution/test_generic.py tests/execution/test_backend_provider.py tests/runtime/test_backend_capabilities.py tests/test_optional_dependencies.py tests/semantic/test_rules_import.py"`; expected exit 0.
- [ ] **Step 8: Commit the admitted read workflow.** Stage the files listed for this task and run `git commit -m "feat: add explicit read-only unverified backend admission"`.

## Task 2: Complete and prove the portable base expression policy

**Files:** Modify `terrine/backends/{dialect,numeric,timestamps,joins,extrema,sequences}.py`, `terrine/temporal/{first,prefix,aggregate}.py`, `terrine/compile/{columns,dispatch}.py`, `terrine/compile/shapes/cohorts.py`, `terrine/frame.py`; extend `tests/backends/test_generic.py`, `tests/backends/{test_numeric,test_timestamps,test_sequences}.py`; create `integration/backends/test_generic.py`; modify `integration/conformance/test_engines.py`, `integration/conftest.py`, AGENTS backend testing commands and CHANGELOG.

**Interfaces:** Consumes Task 1 policy selection and every numeric/timestamp/relational method in the registry plan's Interfaces blocks. Produces complete base `Dialect` operations, `has_scaled_float_state=False`, `has_native_asof=False`, fixed-scale `exact_type() -> dt.Decimal(38, 9)`, independent `wide_type() -> dt.Decimal(38, 0)`, and test-only `--generic-dialect`.

- [ ] **Step 1: Add the forced-policy test selection.** Register `--generic-dialect` in existing `integration/conftest.py` so pytest sees the option before collection for both owning integration paths. Extend the existing conformance `engine` fixture to override only central expression selection with `Dialect("generic")`, including Model/nested/literal/summarization lookups. Restore caches/scopes after each case; retain actual connections, native providers and registered identity checks. Add `test_forced_generic_selection_reaches_nested_literals`: a float `0.1234567899` enters state as exactly `Decimal("0.123456789")` under the flag, unlike native state, including nested literal scope. Do not duplicate/weaken the seven existing answers, change SQL dialect or replace the providers in dialect-only runs.
- [ ] **Step 2: Add one batched numerical oracle and relational edge table.** In `integration/backends/test_generic.py`, use its own parametrized DuckDB/PostgreSQL fixture, with PostgreSQL marked and existing `postgres_scratch` cleanup. Force the same central base selector as Step 1. Define independent Python `Decimal`/integer oracles before writing expressions. Cover these named tests and assertions:

```python
# test_fixed_scale_entry_and_coefficient_domain
assert quantized == [Decimal("0.123456789"), Decimal("-0.123456789"), Decimal("0"), None]
assert source_decimal_difference == Decimal("0.03")
assert exact_type() == dt.Decimal(38, 9)
assert wide_type() == dt.Decimal(38, 0)
# test_signed_integer_quotient_is_exact_after_residual_correction
assert quotients == [3002399751580331, -3002399751580331, -2, 2, None, 9223372036854775807, -9223372036854775808]
# inputs: (9007199254740993, 3), (-9007199254740993, 3), (-7, 3), (-7, -3), (None, 3), (INT64_MAX, 1), (INT64_MIN, 1)
# test_range_asof_preserves_request_multiplicity
assert strict_payloads == ["old", "old", None, None]
assert inclusive_payloads == ["new", "new", None, None]
```

Quantization inputs are `0.1234567899`, `-0.1234567899`, `1e-10`, NULL; decimal cancellation is the existing scale-2 conformance fixture, not the quantized float route. ASOF inputs are duplicate left requests at key 2 plus one unmatched unit and one NULL key; right keys 1/2 carry old/new; include case-colliding candidate alias names. Extend the numeric table with values immediately inside/outside the declared decimal domain, overflowing prefix/difference, NULL/zero/nonfinite distinctions, zero divisor, signed-int64 minimum divided by `-1`, nested ratio, partition/order invariance and cancellation after large history. Assert existing `terrine.temporal.exact_overflow` or `terrine.temporal.float64_precision` and exact failure flags, not a backend exception or float fallback. Add pre-epoch microsecond, unsupported precision/infinity and non-UTC session cases to the timestamp owner tests; preserve the existing timestamp-order code/context. Check schedule cardinality at stop 0 and whole-day/non-unit steps.

- [ ] **Step 3: Run red tests before implementing defaults.** Run `uv run pytest tests/backends/test_generic.py integration/backends/test_generic.py integration/conformance --generic-dialect -n 0 -q`. Expected: current base refusals or incorrect/unimplemented portable behavior; record which assertion detects each operation. Start PostgreSQL with `uv run python -m tests.integration.postgres_scratch start`, export its printed settings in this shell, then run `uv run --extra postgres pytest integration/backends/test_generic.py integration/conformance --generic-dialect -m postgres -n 0 -q`; expected corresponding failing known answers, with no selected case skipped.
- [ ] **Step 4: Implement base numeric/relational methods.** Implement all registry-plan numeric signatures using existing shared decimal-text parsing/normalization. Quantize by text truncation at scale 9, not double multiplication. Preserve scale-0 wide/coefficient types for counts/source decimals, `is_exact_type` recognition and correctly typed zero fills; `exact_literal(value, *like)` must retain an existing exact operand's state type, not coerce every state to the float default. Implement integer quotient with the spec's normalized decimal remainder, rounded seed/residual correction, exact multiplication certificate and final bounds; derive why the residual correction rounds to the exact small integer. Add range-join ASOF, coalesced extrema and integer-seconds sequences. `supports_exact_prefix` accepts bound generic policy but preserves registered build checks and unbound/different-connection refusals. No backend-name dispatch or native numerical changes.
- [ ] **Step 5: Implement base timestamp/UTC methods and scope composition.** Implement registry-plan timestamp methods through Ibis with lossless domain checks, native timestamp order and signed parts. Use typed UTC epoch/delta reconstruction when a cast depends on session zone; never change session settings. `exact_scope(model)` uses `expression_dialect(model._engine)`. Extend `compile_cohort_summary(study: CompiledStudy, *, grouping, value_column: str, dialect: str | None = None) -> CompiledStudy` and its `compile_cohorts` callee with the same `dialect` keyword; Frame passes its admitted policy instead of reselecting from a native bound table. Explicit timestamp selection and nested scopes restore the outer policy on exceptions.
- [ ] **Step 6: Run green oracles and record the error model.** Re-run both Step 3 selections serially; expected PASS with the seven conformance assertions unchanged, both quotient signs exact and no selected PostgreSQL skip. Record actual decimal-text/truncation/ratio/conversion outputs against the independent oracle, including `abs(Q9(t)-t) < 1e-9` and cancellation bounds. Do not claim universal correctly rounded text conversion. Run `make check TESTS="tests/backends/test_generic.py tests/backends/test_numeric.py tests/backends/test_timestamps.py tests/backends/test_sequences.py integration/backends/test_generic.py"`; expected exit 0.
- [ ] **Step 7: Commit portable expressions.** Stage this task's files and run `git commit -m "feat: implement portable Ibis expression defaults"`; numerical policy in CHANGELOG states only derived bounds and exercised evidence.

## Task 3: Persist complete stamps without another metadata cutover

**Files:** Modify `terrine/{data,frame}.py`, `terrine/runtime/api.py`, every `Provenance(...)` fixture/caller in the file map, `tests/test_metadata.py`, `integration/execution/test_generic.py`, existing Arrow/export/manifest round-trip tests, `docs/reference/refusals.md`, `docs/guides/backend-compatibility.md`, CHANGELOG.
Add only the required `terrine.data` → metadata-only `terrine.backends` edge in `tach.toml`; do not make codecs import Ibis or optional drivers.

**Interfaces:** Consumes admitted Engine/policy and existing frozen `Provenance`, `_encode`/`_decode`, Arrow v2 and JSON v1 codecs. Produces the three required fields and `_decode_engine_stamp` interface above; new `RefusalSpec` in `data.py` owns `terrine.metadata.invalid_engine_stamp` with context `{reason, fields}` and literal rematerialization route.

- [ ] **Step 1: Extend existing metadata tests at their owning boundary.** In `test_metadata.py`, parameterize verified/native and unverified/generic stamps on the existing tiny frame builder; assert Arrow and JSON round-trip fields and unchanged table/roles/columns, including existing NULL-reason assertions. Retain old fixture payloads by removing all three fields from otherwise current Arrow v2/JSON v1 metadata, then decode and assert historical verified values from `execution_snapshot.backend`:

```python
# test_engine_stamp_round_trips_and_historic_verified_metadata
assert restored.provenance.engine_name == "sqlite"
assert restored.provenance.dialect == "generic"
assert restored.provenance.engine_status == "unverified"
assert historical.provenance.engine_name == "duckdb"
assert historical.provenance.dialect == "duckdb"
assert historical.provenance.engine_status == "verified"
assert json_payload["version"] == 1
assert arrow_payload["version"] == 2
```

Add `test_partial_or_inconsistent_engine_stamp_refuses`, covering each omitted field in an unverified stamp, partial verified stamp, generic+verified contradiction, registered-name/dialect mismatch, invalid status and all-absent unknown historical backend. Assert `terrine.metadata.invalid_engine_stamp`, exact reason (`partial`, `inconsistent`, `unknown_legacy_engine`), sorted present-field tuple, immutable context, pickle/deepcopy and rematerialization route. Existing unsupported-version checks remain unchanged. Add one generic materialization→Arrow IPC read-back composition with unverified stamp, and extend existing staged/export metadata assertions for verified writers rather than duplicating their workflows.

- [ ] **Step 2: Record red evidence.** Run `uv run pytest tests/test_metadata.py integration/execution/test_generic.py -n 0 -q`; expected missing stamp/validation behavior while old-version assertions remain valid. Record the behavioral failures, not just new required-constructor errors.
- [ ] **Step 3: Implement immutable fields, one decoder and every writer.** Require all three fields at Provenance construction and validate complete stamps consistently with the decoder. Only all-absent legacy stamps use the already recorded verified execution dialect; official pre-change writers had no unverified route. Use registered metadata, not another historical engine list, to reject unknown legacy identities. Invoke `_decode_engine_stamp` from Arrow/JSON decoding before constructing Provenance; never upgrade partial metadata. Update Frame/runtime/batch/empty outputs and every fixture/positional constructor explicitly; derived views/cohorts/life-table/export/manifest metadata retain the fields. Keep versions/keys unchanged. Regenerate the refusal reference with the existing command `uv run python -m tests.documentation` (no arguments).
- [ ] **Step 4: Run green/static checks.** Re-run Step 2 and the existing metadata/export/manifest tests changed by the caller migration. Run `make check TESTS="tests/test_metadata.py tests/test_frame_runtime.py tests/export tests/dataset/test_winsorization_definition.py integration/execution/test_generic.py tests/test_documentation.py"`; expected exit 0, unchanged metadata versions and successful refusal serialization.
- [ ] **Step 5: Commit stamped provenance.** Stage this task's files and run `git commit -m "feat: preserve engine verification in result provenance"`.

## Task 4: Close support tiers and local acceptance

**Files:** Modify `AGENTS.md`, `docs/guides/{backend-compatibility,capabilities,public-api}.md`, `docs/reference/refusals.md`, CHANGELOG; create `examples/unverified_backend.py`. Consume existing `Makefile`, `.github/workflows/{ci,warehouse-backends}.yml` and conformance configuration; do not create CI jobs.

**Interfaces:** Consumes Tasks 1–3 public flag/warning, complete portable policy/provider, stamps and conformance option. Produces documented tiers, one real runnable opt-in workflow and one final local evidence set.

- [ ] **Step 1: Write the real example and finish evergreen documentation.** The example binds an in-memory SQLite backend, demonstrates default refusal before source access, opts in with a visible warning and attempts a tiny count dataset. Print its unverified stamp on success or the precise coded unsupported operation; separately execute an actual generic-provider read so an all-refusal demonstration is not presented as support. Do not catch arbitrary exceptions or suppress warnings. Explain verified engines versus opted-in unverified operations versus refused connections/effects; actual engine name, expression policy and real SQL dialect; NoSnapshot consistency limits; fixed-scale truncation/error terms/domain; unchanged native guarantees; warning filtering; legacy verified metadata decoding and invalid-stamp refusal. AGENTS names the registry/base policy and one engine override as extension points, and preserves the study-expression/native-binding boundary. Update API/docstrings/CHANGELOG for all entry points and the new warning; no tooling vocabulary in product documentation or commit subjects.
- [ ] **Step 2: Run complete acceptance once, serially, from the assigned worktree.** Start a local PostgreSQL server with `uv run python -m tests.integration.postgres_scratch start` and export its printed settings; use the same configured server for all PostgreSQL commands below. Run:

```bash
uv run pytest tests -n 0
uv run pytest integration -n 0
uv run pytest integration/conformance integration/backends/test_generic.py --generic-dialect -n 0 -q
uv run --extra postgres pytest integration/conformance integration/backends/test_generic.py --generic-dialect -m postgres -n 0 -q
PYTEST_ADDOPTS='-n 0' make test-postgres
uv run python examples/unverified_backend.py
uv run pytest tests/test_documentation.py -n 0 -q
```

Expected: all exit 0; all seven named known answers pass unchanged in each forced backend; no selected PostgreSQL case skips; real generic read succeeds; unsupported operations emit their required codes; full native tiers remain green. Record exact commands, revision, exit statuses, summaries and the numeric probe inputs/outputs/error comparisons. Stop the owned scratch server with `uv run python -m tests.integration.postgres_scratch stop`. Do not reinterpret default marker deselection as live PostgreSQL evidence, run cloud warehouses, or claim a future planned command has passed.
- [ ] **Step 3: Decide the cheap existing-command extension from measured evidence.** Record forced-run duration and collected cases alongside native duration. Default decision for this assignment is local-only forced validation: no CI edit, new service or matrix. If both forced variants add only the existing tiny seven-case module and fit the current job timeouts without a dependency/service change, add them to the existing integration/PostgreSQL command steps (not new jobs), retain native runs, and document the measured added work. Otherwise keep the documented local commands. This resolves the brief's older CI-job wording under the owner's local-cost ruling; missing new CI is not a blocker or an unreported promise.
- [ ] **Step 4: Run the required focused commit gate.** Run `make check TESTS="tests/test_documentation.py tests/test_metadata.py tests/backends/test_generic.py tests/execution/test_generic.py"`; expected exit 0.
- [ ] **Step 5: Commit documented support tiers.** Stage the example/docs and any justified existing-command change, then run `git commit -m "docs: describe verified and unverified backend tiers"`. Obtain the implementation owner's ordinary review gate; no push/merge is authorized by this plan.

## Self-review and handoff

- **Spec coverage:** Task 1 owns flag/refusal/warning, admission propagation, capabilities, actual generic reads, NoSnapshot, decimal admission and coded/resource boundaries; Task 2 owns all base operations, numerical domain/error model, UTC, range ASOF and forced known answers; Task 3 owns every producer, round trips, historic verified decoding and the new stamp refusal; Task 4 owns docs/example and serial local acceptance. No requirement is deferred to new CI or a second engine list.
- **Step scan:** Test steps name inputs/assertions; code steps pin owners/signatures/domain choices rather than transcribing implementations. Red evidence is required for behavior, not only missing imports. Final expensive checks are shared once across the completed branch; coherent commits have focused gates.
- **Type consistency:** One Engine record, one optional admitted-engine argument at internal boundaries, one actual backend-name helper, one expression-policy selector, the existing real Catalog SQL dialect, one warning category and one three-field provenance model. No public dialect-force switch, connection-global opt-in or alternate serialization path.
- **Review Focus:** Identity/consent isolation and caller ownership/protected effects/lazy failures are Task 1; nested/literal/cohort policy, coefficients and exact large signed quotients are Task 2; legacy/partial stamps and every writer are Task 3. Named tests for these risks appear above.
- **Proportion:** The plan defines the decisions and required oracles; it reuses the registry method contracts and seven conformance cases instead of duplicating native implementation bodies or creating another framework.
- **Execution handoff:** This is a documentation-only assignment. Implementation is deliberately not started. A later implementation owner should execute natively in task order because admission, expression policy, provider and metadata share interfaces; preserve the owner-approved legacy metadata rule and local-only verification policy. All verification above is future acceptance, not evidence that generic execution already works.
