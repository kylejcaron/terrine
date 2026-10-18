# Terrine Materialization Planner and Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Incrementally materialize customer-owned Terrine datasets with reproducible source snapshots, exact dependency invalidation, bounded and fenced execution, recoverable publication, and honest live run feedback.

**Architecture:** Add one customer-schema runtime store for immutable definition/source generations, layer dependencies, work state, leases, and publication manifests; planner/executor operations consume the existing Model/Design/Frame compiler, temporal layer interfaces, and native execution authority rather than duplicating compilation. DuckDB is the first backend with an explicitly verified atomic publication protocol; other backends must advertise only capabilities they prove. CLI, Python, and optional OpenTelemetry are projections of the same runtime decisions, while status/events/cancellation reuse durable state and never become execution authority.

**Tech Stack:** Python `>=3.13,<3.15` target subject to Task 0.1 compatibility evidence; Ibis, Pydantic 2, Arrow, DuckDB reference backend, pytest/Hypothesis, uv. Optional OpenTelemetry adapter uses an application-configured SDK/exporter and is disabled by default; no mandatory telemetry service or extra.

**Spec:** `docs/superpowers/plans/2026-10-16-terrine-build.md` Task 7.1, including its acceptance and operational constraints; `docs/superpowers/plans/2026-10-16-terrine-dataset-workflows.md` “Tasks 7.1 / 12.1 amendment: live operational feedback”; shared contracts in `2026-10-16-terrine-study-api.md`, `2026-10-16-terrine-temporal.md`, `2026-10-16-terrine-estimation.md`; repository `AGENTS.md`. These companion plans remain authoritative for Model/Design/Frame/MaterializedFrame, readiness, temporal semantics, execution snapshots, and test/refusal rules.

## Global Constraints

- Customer rows, derived data and warehouse state stay in the customer's execution environment; no hosted data service or outbound collection by default.
- No increment edits or mandatory increment dependency. Native core, runtime and tests work with increment absent.
- Watermarks/readiness represent declared completeness, never change detection. Use authoritative revisions/change feeds/checksums where available; otherwise operator-declared restatements or conservative rebuild. Never claim detection of undetectable mutations.
- Retain inserts, updates, deletes/tombstones, source partition/revision lineage and immutable source snapshots. Definition/schema/codec changes establish a new version; reuse requires equivalent dependency fingerprints.
- Windows are half-open `[start,end)`, exact time zero is not floored, and prefix subtraction is only for supported additive states. No anchor×events or hidden unit×day densification.
- Exact invalidation may rebuild extra work but MUST NOT omit affected outputs. Pinned generations never mutate; incremental output must equal a clean rebuild at the same source snapshot.
- Float64 prefix/certificate dependencies are tracked separately from mathematical output-window support. Changes that may alter cumulative state revalidate affected endpoint certificates, precision refusals and dependent eligibility even when the output window's event support is unchanged.
- Reopenable execution snapshots must be captured and retained by runtime execution authority; process-local `ExecutionSnapshot` metadata or hashes alone do not pin/reopen source inputs.
- Saved plans are executable only through a versioned reconstruction recipe with canonical Design and verified owner-bound Catalog/Model, roster, event-ID, destination, and expected-base-manifest references.
- Partition by bounded date ranges/hash buckets with explicit task/byte/concurrency caps; a billion input rows MUST NOT create a billion scheduler objects. Unknown cardinality/scan cost remains unknown.
- Leases bind resources (tables/intervals), not plan IDs. Heartbeats, fencing, compare-and-publish and overlap serialization prevent stale writers from publishing.
- Do not claim publication atomicity except where a backend-specific protocol is implemented and crash-tested. DuckDB is first; unproven backends refuse/advertise unsupported publication guarantees.
- GC only explicitly superseded customer-owned generations and preserve generations referenced by readers/manifests or retention policy.
- `Budget` scan/output/row limits remain distinct; unknown scan estimates cannot satisfy strict scan budgets. No live warehouse/network in unit tests; live backend tests belong under `integration/warehouse_execution/`.
- Runtime status/events/cancellation reuse authoritative state. Telemetry is a best-effort projection only; no payloads, raw SQL, keys, secrets, prompts or raw backend errors in events/metrics.
- State-panel support registers Task 6.2 point/period dependencies in the existing single `terrine/compile/dispatch.py` shape dispatch and `Design.shape: "cross_section" | StatePanel`; it MUST NOT establish a second dispatcher. If Task 6.2 is not landed, this task's state-panel tests and integration follow that shared contract and explicitly depend on its merge.
- Owner-reserved choices are gates, not plan decisions: whether to adopt/vendor Callaway–Sant'Anna or other licensed material, and any licensing/release decision. This runtime does not include or settle them.

## Review Focus

1. A watermark advances while an old partition is corrected/deleted: detect only via revision/change evidence or explicit restatement; test `test_watermark_advance_is_not_change_detection` in Task 2.
2. A changed event lands exactly on a window edge or changes first/nth/each anchor identity: half-open intersection and anchor identity rules must match clean rebuild; test `test_window_boundary_equality_and_event_anchor_identity_invalidation` in Task 2.
3. A writer loses its lease immediately before publication, or an overlapping interval races: old fencing token MUST NOT publish and overlaps serialize; test `test_expired_writer_cannot_publish_and_overlaps_serialize` in Task 3.
4. Process crash occurs on either side of the DuckDB manifest publication boundary: recovery yields one authoritative result, not mixed generations; test `test_duckdb_crash_boundary_retry_reconciles_manifest` in Task 4.
5. Historical Float64 prefix mutation can change a prior accepted result into a precision refusal: revalidate cumulative certificates, eligibility and output against clean rebuild; test `test_historical_prefix_insertion_revalidates_float_certificate_and_refusal` in Task 2. Unknown scan estimates under strict budgets are also pinned by `test_unknown_scan_estimate_fails_strict_budget_before_execution` in Task 2.

---

## File and Interface Map

- `terrine/runtime/state.py`: customer-schema versioned runtime tables and immutable generation/task records.
- `terrine/runtime/graph.py`: stable dependency graph for daily → prefix → anchor → frame layers and optional export/state-panel nodes.
- `terrine/runtime/snapshot.py`: capture, retention, verification, owner-bound reopening and restart recovery of immutable execution inputs.
- `terrine/runtime/recipe.py`: versioned canonical plan reconstruction recipe and owner-bound reference resolution.
- `terrine/runtime/leases.py`: resource leases, heartbeat/renewal, monotone fencing tokens and serialization of overlapping resources.
- `terrine/runtime/planner.py`: definition/source fingerprinting, change evidence, invalidation and bounded partition plan serialization.
- `terrine/runtime/backends/base.py`: narrow capability contract for snapshots, change evidence, limits, locks and publication; no generic false atomicity promise.
- `terrine/runtime/backends/duckdb.py`: DuckDB locking, generation staging and atomic manifest/swap protocol with recovery.
- `terrine/runtime/execute.py`: idempotent bounded execution, durable task/run transitions and fenced compare-and-publish.
- `terrine/runtime/control.py`: owner-local IPC server/client for cross-process status/events/cancel/request-key lookup; the connection-owning runtime remains authoritative.
- `terrine/runtime/status.py`, `events.py`: run read/control projections and bounded resumable event paging per dataset amendment.
- `terrine/runtime/cli.py`: `terrine plan`, `terrine run`, `terrine status`, `terrine events`, `terrine cancel`; owner configuration supplies connection/namespace.
- `terrine/adapters/opentelemetry.py` (optional): best-effort projection of safe lifecycle signals, never state authority.
- `terrine/runtime/__init__.py`: explicit stable public runtime API exports; keep everyday Model/Design/Frame surface unchanged.
- `terrine/compile/layers.py` (compiler-owned): logical `LayerKey` and substitution contracts independent of runtime storage/partition records.
- `terrine/compile/materialize.py` (compiler-owned): plan dependency layers and compose a Frame using compiler-owned logical keys and verified persisted-relation substitutions with the same validation/checks as ordinary `compile_columns`.
- Tests in `tests/runtime/`, `tests/compile/`; DuckDB crash/retry and real separate-process tests under `integration/warehouse_execution/`.
- Update `README.md`, `CHANGELOG.md`, and a runnable `examples/materialization_runtime.py` with commands, consistency guarantees and backend limitations.

## Task 1: Durable Runtime State, Schema, and Dependency Graph

**Files:**
- Create: `terrine/runtime/state.py`
- Create: `terrine/runtime/graph.py`
- Create: `terrine/runtime/__init__.py`
- Test: `tests/runtime/test_state.py`
- Test: `tests/runtime/test_graph.py`

**Interfaces:**
- Consumes: `Model.catalog`, canonical `Design.spec_hash`, `ExecutionSnapshot(snapshot_id, backend, source_generations, input_binding_id, replay_equivalence)`, and `Budget` from `terrine.data`; never serialize live connections or `RelationHandle` objects.
- Produces: immutable records `DefinitionVersion(definition_hash: str, schema_hash: str, codec_version: str)`, `SourceSnapshot(snapshot_id: str, source_revisions: Mapping[str, str | None], readiness: Mapping[str, BoundCoverage], captured_at: datetime, owner_reference: str)`, `GenerationRef(namespace: str, dataset: str, generation: str)`, and `LayerNode(layer: Literal["daily","prefix","anchor","frame","export","state_point","state_period"], source: str, partition: str, definition_hash: str, generation: str)`; `RuntimeStore(con, *, namespace: str)`, `RuntimeStore.initialize() -> None`, `RuntimeStore.read_generation(ref: GenerationRef) -> GenerationRecord | None`, and `dependency_graph(model: Model, design: Design, *, include_exports: bool = False) -> DependencyGraph`. The runtime store holds references, not portable credentials.
- Snapshot capture/reopen/retention is produced by Task 3: a snapshot authority captures the resolved Design's required source, dimension, roster and integrity-input closure for one run, persists an owner-local reopenable receipt, verifies/reopens the same immutable inputs after process restart, and retains them through all partitions/retries. Whole-Catalog definition/model hashes remain bound, but unrelated source rows are not captured. A source mutation/revision mismatch or unrecoverable snapshot abandons the entire staged generation and requires replanning; never mix partitions from separate snapshots.

- [ ] **Step 1: Write failing state and graph tests.** Add `test_runtime_schema_is_customer_namespace_scoped`, `test_generation_records_definition_and_snapshot_separately`, `test_generation_identity_is_immutable_and_supersession_explicit`, and `test_dependency_graph_orders_daily_prefix_anchor_frame`; assert stable serialization and no handles/credentials in persisted records.
- [ ] **Step 2: Run focused tests.** Run `uv run pytest tests/runtime/test_state.py tests/runtime/test_graph.py -q`. Expected: FAIL because runtime records/store/graph are absent.
- [ ] **Step 3: Implement typed durable state and graph.** Use versioned customer-schema tables and canonical JSON-compatible records; link graph edges by immutable fingerprints and generation refs. Keep DAG construction independent of execution.
- [ ] **Step 4: Verify focused tests.** Run `uv run pytest tests/runtime/test_state.py tests/runtime/test_graph.py -q`. Expected: PASS.
- [ ] **Step 5: Commit.** `git add terrine/runtime tests/runtime/test_state.py tests/runtime/test_graph.py && git commit -m "feat: add materialization runtime state and graph"`.

## Task 2: Source Change Evidence and Exact Invalidation Planner

**Files:**
- Create: `terrine/runtime/changes.py`
- Create: `terrine/runtime/planner.py`
- Test: `tests/runtime/test_invalidation.py`
- Test: `tests/runtime/test_planner.py`

**Interfaces:**
- Consumes: Task 1 `DefinitionVersion`, `SourceSnapshot`, `LayerNode`, `DependencyGraph`; `BoundCoverage`; `Design.anchor` (`FixedDate`/`Schedule`/`EventAnchor`); `Window`; `Design.shape`; Task 6.2 `StatePanel` registered only through the common `compile/dispatch.py`.
- Produces: `Change(kind: Literal["insert","update","delete","restatement","readiness","definition","roster"], source: str, before_key_range: tuple[str, str] | None, after_key_range: tuple[str, str] | None, before_time_range: tuple[datetime, datetime] | None, after_time_range: tuple[datetime, datetime] | None, revision: str | None, tombstone: bool = False)`, `ChangeSet(changes: tuple[Change, ...], evidence: Literal["revision","change_feed","checksum","operator","conservative_rebuild"])`, `PartitionSpec(source: str, start: datetime | None, end_exclusive: datetime | None, bucket: int | None, bucket_count: int | None)`, `Invalidation(nodes: tuple[LayerNode, ...], partitions: tuple[PartitionSpec, ...], reasons: tuple[str, ...])`, `PlanRecipe(schema_version: Literal[1], canonical_design: Mapping[str, JsonValue], design_hash: str, model_ref: OwnerBoundModelRef, catalog_hash: str, roster_hash: str, event_ids_hash: str, destination: DestinationRef, expected_base_manifest: str | None)`, `TaskSpec(schema_version: Literal[1], task_id: str, node: LayerNode, partition: PartitionSpec, depends_on: tuple[str, ...], input_fingerprint: str, output_fingerprint: str, estimated_scan_bytes: int | None, measured_scan_bytes: int | None, max_output_bytes: int | None)`, `RefreshPlan(plan_id: str, definition: DefinitionVersion, snapshot: SourceSnapshot, recipe: PlanRecipe, invalidation: Invalidation, tasks: tuple[TaskSpec, ...], budget: Budget, serialized_tasks: tuple[Mapping[str, JsonValue], ...])`, and `plan_refresh(model: Model, design: Design, *, con: Any, namespace: str, destination: DestinationRef, changes: ChangeSet | None = None, budget: Budget | None = None) -> RefreshPlan`.
- Update records must carry both old and new key/time support; deletes must carry old support. If support is absent or incomplete, planner selects a conservative enclosing range or wider rebuild, never only the replacement location.
- `TaskSpec` has a stable schema/version, bounded partition identity, dependency IDs and explicit unknown cost values (`None`) as specified above. Planning is metadata-only except explicit backend snapshot/revision discovery; no customer row scans to infer changes. If authoritative change evidence is unavailable, require declared restatement ranges or conservative rebuild. Strict budget evaluation rejects unknown scan costs.

- [ ] **Step 1: Write failing invalidation tests.** Add `test_watermark_advance_is_not_change_detection`, `test_window_boundary_equality_and_event_anchor_identity_invalidation`, `test_update_timestamp_move_invalidates_old_and_new_support`, `test_update_unit_key_move_invalidates_old_and_new_units`, `test_delete_uses_previous_support`, `test_missing_change_support_forces_conservative_rebuild`, `test_dimension_correction_invalidates_effective_interval`, `test_strict_asof_change_invalidates_matching_lookup_range`, `test_roster_exit_and_readiness_advance_or_retraction_invalidate_candidates`, `test_ratio_sources_change_independently`, `test_definition_change_starts_new_fingerprint`, `test_state_panel_correction_invalidates_adjacent_uses_and_terminal_suffix`, `test_unknown_scan_estimate_fails_strict_budget_before_execution`, and `test_historical_prefix_insertion_revalidates_float_certificate_and_refusal`. Assert boundary equality, first/nth/each key additions/removals, readiness maturity/retraction, baseline changes affecting all unit rows, and SAME-snapshot semantics. The numerical case inserts historical `1e16` before an unchanged window containing `1.0`; compare rebuilt cumulative certificate, endpoint subtraction result, precision refusal and dependent eligibility with a clean rebuild, never retaining an earlier accepted certificate by window-only invalidation.
- [ ] **Step 2: Run focused tests.** Run: `uv run pytest tests/runtime/test_invalidation.py tests/runtime/test_planner.py -q`. Expected: FAIL because planner/invalidation is absent.
- [ ] **Step 3: Implement exact conservative invalidation.** For support `[l,r)` and window `[t0+a,t0+b)`, invalidate iff `l < t0+b AND t0+a < r`. For updates, invalidate union of both before and after key/time supports; for deletes, invalidate the before support. If exact supports are absent, conservatively invalidate the enclosing known range or rebuild the dependent source/dataset. Recompute affected event-anchor identity/order (`first`,`nth`,`each`) before range filtering; remove obsolete keys and rebuild dependent values. Invalidate dimensions, strict-asof lookup ranges, roster candidates, readiness completeness, independent ratio sources, and state-panel adjacent transitions/terminal suffix/baseline dependencies. Separately track cumulative numerical-prefix dependencies: any modified prefix state invalidates downstream endpoint certificates/refusal checks and eligibility even when finite output-window support is disjoint. Rebuild prefix suffixes separately from output windows; changed prefix generation alone does not imply every window result changed. Enforce bounded date/hash partitions and explicit task/byte/concurrency maxima.
- [ ] **Step 4: Verify planner tests.** Run: `uv run pytest tests/runtime/test_invalidation.py tests/runtime/test_planner.py -q`. Expected: PASS; canonical plan serialization and budget refusal included.
- [ ] **Step 5: Commit.** `git add terrine/runtime/changes.py terrine/runtime/planner.py tests/runtime/test_invalidation.py tests/runtime/test_planner.py && git commit -m "feat: plan source-aware materialization invalidations"`.

## Task 3a: Reopenable Snapshots and Saved-Plan Reconstruction

**Files:**
- Create: `terrine/runtime/snapshot.py`
- Create: `terrine/runtime/recipe.py`
- Modify: `terrine/execution/_publish.py` for bounded/backend-native runtime snapshot capture; avoid changing the existing short-lived API unless safely shared
- Test: `tests/runtime/test_snapshot.py`
- Test: `tests/runtime/test_recipe.py`
- Integration: `integration/warehouse_execution/test_saved_plan_restart.py`
- Integration: `integration/warehouse_execution/test_snapshot_partition_consistency.py`
- Integration: `integration/warehouse_execution/test_snapshot_capture_bounds.py`

**Interfaces:**
- Consumes: Task 1 `RuntimeStore`, Task 2 `RefreshPlan`/`PlanRecipe`; `open_execution(catalog: Catalog, con: Any, *, units: Mapping[str, UnitSource] | None = None, input_binding: VerifiedInputBinding | None = None) -> ExecutionContext`, `publish_native_inputs(catalog: Catalog, *, con: Any, units: Mapping[str, UnitSource] | None = None) -> PublishedInputs`, Model/Design and verified input-binding contracts. Do not rely on process-local `PublishedInputs` lifetime or context-only `ExecutionSnapshot`.
- Produces: `SnapshotAuthority.capture(model: Model, *, design: Design, con: Any, namespace: str, run_id: str, budget: Budget) -> ReopenableSnapshot`, `SnapshotAuthority.reopen(snapshot: ReopenableSnapshot, *, con: Any, namespace: str) -> ExecutionContext`, `SnapshotAuthority.release(snapshot: ReopenableSnapshot) -> None`, and `resolve_plan(recipe: PlanRecipe, *, con: Any, namespace: str) -> tuple[Model, Design, DestinationRef]`.
- Snapshot receipt binds retained immutable inputs, source revisions, the resolved Design's physical relation closure, Catalog/model/roster/event-ID hashes and owner namespace. Every partition/retry/restart reopens the same receipt and revalidates binding/schema. Mutation, missing retention or changed identity abandons all staged partitions and requires replan; never mix snapshots.
- Snapshot capture MUST NOT collect all source rows into client memory. Replace the execution-owned publisher's whole-root `fetch_arrow_table()` path with backend-native copy inside the source snapshot transaction (e.g. DuckDB `ATTACH`/`CREATE TABLE AS`) or bounded streaming. Capture only dependencies reachable from the resolved Design (including expression facts, properties, dimensions, roster and integrity inputs); retain whole-model definition hashes without reading unrelated rows. Apply scan/resource budget before capture, account capture scan/output/elapsed costs in run status; unsupported cost remains unknown and strict scan budget refuses before capture.
- `PlanRecipe` contains canonical tagged Design JSON and hash, owner-bound Model/Catalog reference (not credentials), Catalog/roster/event-ID hashes, destination/namespace and expected base manifest. Resolver verifies all hashes under owner config. Saved plans run in fresh process without planning-process objects.

- [ ] **Step 1: Write failing snapshot/recipe tests.** Add `test_snapshot_is_reopened_for_every_partition_and_retry`, `test_source_mutation_between_partitions_abandons_generation`, `test_snapshot_reopens_after_process_restart_or_refuses`, `test_saved_plan_runs_in_fresh_process_from_verified_recipe`, and `test_recipe_rejects_catalog_roster_event_ids_destination_or_manifest_mismatch`. Assert same source/dimension/roster closure across partitions; unrecoverable/mismatched authority publishes no generation.
- [ ] **Step 2: Run focused tests.** Run: `uv run pytest tests/runtime/test_snapshot.py tests/runtime/test_recipe.py -q`. Expected: FAIL because reopenable snapshots and plan reconstruction are absent.
- [ ] **Step 3: Implement durable capture and recipe resolution.** Reuse native backend copy or bounded streaming, retain owner-local inputs under configured policy, reject budgets before copy, preserve honest capture costs and canonical data-only recipe references.
- [ ] **Step 4: Verify focused tests.** Run: `uv run pytest tests/runtime/test_snapshot.py tests/runtime/test_recipe.py -q`. Expected: PASS.
- [ ] **Step 5: Verify fresh process, restart and bounded capture.** Run: `uv run pytest integration/warehouse_execution/test_saved_plan_restart.py integration/warehouse_execution/test_snapshot_partition_consistency.py integration/warehouse_execution/test_snapshot_capture_bounds.py -q`. Expected: PASS on local DuckDB. Include `test_duckdb_snapshot_capture_uses_bounded_client_buffer_as_input_grows` and `test_capture_budget_applies_before_copy`; mutate fact/dimension/roster across partitions and restart, proving same snapshot or abandon-and-replan.
- [ ] **Step 6: Commit.** `git add terrine/runtime/snapshot.py terrine/runtime/recipe.py terrine/execution/_publish.py tests/runtime/test_snapshot.py tests/runtime/test_recipe.py integration/warehouse_execution/test_saved_plan_restart.py integration/warehouse_execution/test_snapshot_partition_consistency.py integration/warehouse_execution/test_snapshot_capture_bounds.py && git commit -m "feat: add reopenable runtime snapshots and plans"`.

## Task 3b: Compiler-Owned Persisted-Layer Composition

**Files:**
- Create: `terrine/compile/layers.py`
- Create: `terrine/compile/materialize.py`
- Modify: `terrine/compile/columns.py` to share ordinary relation composition/validation
- Modify: `tach.toml` to enforce compiler/runtime dependency direction
- Test: `tests/compile/test_persisted_layers.py`

**Interfaces:**
- Consumes: `Design`, `Model`, existing `compile_columns` contract, and verified persisted Ibis relations supplied through compiler-owned logical keys. This unit has no runtime-store, runtime-partition or `LayerNode` dependency.
- Produces: `LayerKey(layer: Literal["daily","prefix","anchor","state_point","state_period"], source: str, measure: str | None, unit: str, grain: str, resolution: Literal["day","event","point","period"], definition_hash: str, snapshot_id: str)`, `LayerSubstitution(key: LayerKey, relation: ibis.Table)`, `plan_layers(design: Design, model: Model) -> LayerPlan`, and `compose_materialized_frame(design: Design, model: Model, *, persisted: Mapping[LayerKey, ibis.Table], budget: Budget | None = None) -> CompiledStudy`. Runtime-owned code later translates verified physical records to these logical keys; compiler never imports runtime.
- Key identity includes source/measure/unit/grain, day-vs-event-vs-point-vs-period resolution, definition and snapshot identity. Substitution delegates to the same ordinary compiler composition/validation and retains integrity, eligibility, Float64 certificates/refusals, strict budgets and output semantics. `Frame.materialize` full Arrow collection is not the warehouse partition staging path. `tach.toml` permits `terrine.runtime -> terrine.compile`, never the reverse.
- Runtime composition MUST enter through `compile_design`. Supported runtime-shape matrix: `cross_section` uses verified persisted anchor/prefix substitutions; `StatePanel` uses the same dispatcher as a full rebuild; every other shape is refused before source work with `terrine.compile.runtime_shape_unsupported`, structured `shape` and `route` context, and a forward route to ordinary Frame materialization or full rebuild. Add matched-input parity for each supported shape; every refused shape has a stable-code test.
- An execution-owned gate shared by Frame and runtime validates required shape budgets, compiler integrity, and unknown eligibility before output can be exported or published. Runtime staging MUST NOT consume compiler-retained tables without this gate.

- [ ] **Step 1: Write failing persisted-layer parity tests.** Add `test_persisted_daily_prefix_anchor_reuse_matches_ordinary_frame_compiler`, `test_layer_substitution_checks_measure_unit_grain_and_resolution_identity`, and `test_layer_substitution_preserves_integrity_eligibility_precision_and_budget_checks`. Assert equal matched-input results/metadata and the same stable refusal for invalid history, precision and strict budgets.
- [ ] **Step 2: Run focused tests.** Run: `uv run pytest tests/compile/test_persisted_layers.py -q`. Expected: FAIL because compiler-owned layer contracts are absent.
- [ ] **Step 3: Implement shared compiler composition.** Derive required layers from ordinary `compile_columns`; provide verified substitutions by `LayerKey`, retaining one composition/validation path rather than a second runtime compiler.
- [ ] **Step 4: Verify parity and dependency direction.** Run: `uv run tach check && uv run pytest tests/compile/test_persisted_layers.py -q`. Expected: PASS; `terrine.compile` has no runtime dependency.
- [ ] **Step 5: Commit.** `git add terrine/compile/layers.py terrine/compile/materialize.py terrine/compile/columns.py tach.toml tests/compile/test_persisted_layers.py && git commit -m "feat: compose frames from verified persisted layers"`.

## Task 3c: Bounded Fenced Runtime Execution

**Files:**
- Create: `terrine/runtime/leases.py`
- Create: `terrine/runtime/backends/base.py`
- Create: `terrine/runtime/execute.py`
- Test: `tests/runtime/test_leases.py`
- Test: `tests/runtime/test_execute.py`

**Interfaces:**
- Consumes: Task 1 `RuntimeStore`; Task 2 `RefreshPlan`/`TaskSpec`; Task 3a `SnapshotAuthority` and `PlanRecipe`; Task 3b `LayerKey` and compiler composition; native `ExecutionContext`.
- Produces: `Lease(resource: str, owner: str, fence: int, expires_at: datetime)`, `LeaseStore.acquire(resource: str, owner: str, *, ttl: timedelta, now: datetime) -> Lease`, `LeaseStore.renew(lease: Lease, *, ttl: timedelta, now: datetime) -> Lease`, `LeaseStore.release(lease: Lease) -> None`, `LeaseStore.assert_current(lease: Lease) -> None`; `BackendCapabilities(snapshot: bool, change_evidence: frozenset[str], atomic_manifest_publish: bool, scan_estimate: bool, cancellation: bool)`; `RuntimeBackend.snapshot(model: Model, *, con: Any, namespace: str) -> SourceSnapshot`, `RuntimeBackend.publish_generation(stage: StagedGeneration, *, expected_manifest: str | None, lease: Lease) -> PublicationReceipt`, `RuntimeBackend.reconcile(run_id: str, *, store: RuntimeStore) -> Reconciliation`; `execute_plan(plan: RefreshPlan, *, backend: RuntimeBackend, store: RuntimeStore, clock: Clock) -> RunReceipt`.
- Resource keys represent table/dataset and overlapping interval/bucket scope, not plan IDs; overlapping refreshes serialize and disjoint scopes MAY run concurrently. Runtime translates verified physical generation/partition records to logical compiler `LayerKey`s. Every write/publication checks the fencing token and expected current generation. Unknown capabilities stay unknown; unsupported atomicity is never implied.

- [ ] **Step 1: Write failing lease/execution tests.** Add `test_lease_heartbeat_renewal_and_fencing`, `test_expired_writer_cannot_publish_and_overlaps_serialize`, `test_disjoint_partitions_can_progress_under_bounds`, `test_duplicate_task_completion_is_idempotent`, and `test_cancel_before_publish_never_exposes_generation`. Assert controlled-clock expiry, compare-and-publish, bounded task count/concurrency and idempotent retry.
- [ ] **Step 2: Run focused tests.** Run: `uv run pytest tests/runtime/test_leases.py tests/runtime/test_execute.py -q`. Expected: FAIL because fenced execution is absent.
- [ ] **Step 3: Implement lease and executor state machine.** Persist run/task before expensive work, reopen one retained snapshot, renew leases while work proceeds, record idempotent partition completion, call compiler layer composition, and fence every write/publication; expired writers stop/refuse.
- [ ] **Step 4: Verify focused tests.** Run: `uv run pytest tests/runtime/test_leases.py tests/runtime/test_execute.py -q`. Expected: PASS.
- [ ] **Step 5: Commit.** `git add terrine/runtime/leases.py terrine/runtime/backends/base.py terrine/runtime/execute.py tests/runtime/test_leases.py tests/runtime/test_execute.py && git commit -m "feat: execute materialization plans with fencing"`.
## Task 4: DuckDB Atomic Publication, Recovery, and Generation GC

**Files:**
- Create: `terrine/runtime/backends/duckdb.py`
- Create: `terrine/runtime/gc.py`
- Test: `tests/runtime/test_duckdb_publication.py`
- Test: `tests/runtime/test_gc.py`
- Create: `integration/warehouse_execution/test_duckdb_runtime_recovery.py`

**Interfaces:**
- Consumes: Task 3c `RuntimeBackend`, `Lease`, `PublicationReceipt`, `Reconciliation`; Task 1 generation/manifest/pin records.
- Produces: `DuckDBBackend(con, *, namespace: str) -> RuntimeBackend`; `publish_generation(stage: StagedGeneration, *, expected_manifest: str | None, lease: Lease) -> PublicationReceipt`; `reconcile(run_id: str) -> Reconciliation`; `collect_superseded(store: RuntimeStore, *, retain: timedelta, now: datetime, dry_run: bool = True) -> GCReport`.
- DuckDB is the only backend whose `atomic_manifest_publish=True` may be set here. Stage each immutable generation, verify all bounded partitions/digests, then atomically commit authoritative manifest publication with fencing and expected-current check. Document the actual transaction/crash boundary. Other backends need separate capability evidence and may refuse safe publication.

- [ ] **Step 1: Write failing publication/GC tests.** Add `test_duckdb_manifest_switch_is_atomic_with_generation`, `test_partial_stage_never_becomes_visible`, `test_manifest_conflict_refuses_stale_expected_generation`, `test_retry_reuses_only_matching_generation_partitions`, `test_gc_only_removes_explicit_unpinned_superseded_generations`, and integration `test_duckdb_crash_boundary_retry_reconciles_manifest`. Test injected failure immediately before and after commit; verify readers see either previous complete manifest or new complete manifest, never mixed tables.
- [ ] **Step 2: Run unit tests.** Run: `uv run pytest tests/runtime/test_duckdb_publication.py tests/runtime/test_gc.py -q`. Expected: FAIL because DuckDB protocol and GC are absent.
- [ ] **Step 3: Implement DuckDB protocol and reconciliation.** Keep staged output invisible until committed manifest switch; after ambiguous failure consult durable manifest and fence before retry, never blindly republish. GC requires explicit supersession, no active readers/pins, and elapsed retention; dry-run is default.
- [ ] **Step 4: Verify unit tests.** Run: `uv run pytest tests/runtime/test_duckdb_publication.py tests/runtime/test_gc.py -q`. Expected: PASS.
- [ ] **Step 5: Run actual DuckDB crash/retry smoke.** Run: `uv run pytest integration/warehouse_execution/test_duckdb_runtime_recovery.py -q`. Expected: PASS against a temporary local DuckDB database and injected process/crash-boundary faults; no network/warehouse.
- [ ] **Step 6: Commit.** `git add terrine/runtime/backends/duckdb.py terrine/runtime/gc.py tests/runtime/test_duckdb_publication.py tests/runtime/test_gc.py integration/warehouse_execution/test_duckdb_runtime_recovery.py && git commit -m "feat: publish DuckDB generations atomically"`.

## Task 5: Durable Run Status, Events, and Cancellation

**Files:**
- Create: `terrine/runtime/status.py`
- Create: `terrine/runtime/events.py`
- Create: `terrine/runtime/control.py`
- Test: `tests/runtime/test_run_status.py`
- Test: `tests/runtime/test_run_events.py`
- Test: `tests/runtime/test_cancel_publication.py`
- Test: `tests/runtime/test_control_ipc.py`

**Interfaces:**
- Consumes: Task 1 runtime state and Task 3 lease/publication decisions. Preserve exact amendment contracts below.
- Produces: `get_run(run_id: str, *, con: Any, namespace: str) -> RunStatus`, `read_run_events(run_id: str, *, con: Any, namespace: str, after: str | None = None, limit: int = 100) -> EventPage`, `cancel_run(run_id: str, *, con: Any, namespace: str) -> CancelReceipt`; owner-local `ControlServer(store: RuntimeStore, *, namespace: str, endpoint: ControlEndpoint)`, `ControlServer.start() -> None`, `ControlServer.close() -> None`, and `ControlClient(endpoint: ControlEndpoint, *, namespace: str).get_run(run_id: str) -> RunStatus`, `.read_run_events(run_id: str, *, after: str | None = None, limit: int = 100) -> EventPage`, `.cancel_run(run_id: str) -> CancelReceipt`, `.find_run_by_request_key(request_key: str, *, destination: DestinationRef) -> RunReceipt | None`. IPC requests carry operation, run ID/cursor or request key plus destination; endpoint credentials/owner context are configured locally and authorize destination access.
- `RunStatus` includes `run_id,durable_state,state_version,updated_at,heartbeat_at,lease_expires_at,freshness,phase,progress,cost,readiness,error_code,pinned_result_ref,latest_event_cursor`; times UTC, unknown metrics null. States: `queued,running,cancel_requested,succeeded,failed,cancelled`. `CancelReceipt(run_id, disposition, status)` with disposition `requested|already_requested|already_terminal`. `RunEvent(run_id, sequence, at, kind, payload)` and `EventPage(events, next_cursor, has_more, cursor_gap)`; cursor opaque, exclusive and run-bound; reads cap limit at owner-configured maximum (default 1000).
- For DuckDB, a second process MUST NOT open/query the writer's database file while the writer holds it. The writer-owned server handles status/event/cancel on its existing connection and serializes cancellation with publication. Endpoint exists only while owner process is alive; after crash it is closed/unlinked, status is explicitly unavailable until a recovery owner reacquires the DuckDB file, reconciles manifest/run state and starts a new endpoint. No sidecar writer, lock bypass, guessed terminal state or fallback scan.
- Durable state is authoritative. `failed` runs are retryable only after a newer lease fencing token is acquired; prior task results may be reused idempotently, but an expired writer cannot resume or publish. `succeeded` and `cancelled` are terminal and never regress. Cancellation and publication serialize through the same fence/compare-and-publish protocol: if publication commits first return succeeded/pinned result; if cancellation wins first, do not publish. A request alone does not prove backend query/billing stopped. Stale heartbeat means liveness unknown. Event lifecycle decisions are durable; progress can coalesce to at most once/second/run, never row/unit granularity. Status/event reads inspect runtime store only, never trigger scans or cost probes. Safe allowlist excludes payloads, raw SQL, keys, secrets, credentials, prompts and backend exception text.

- [ ] **Step 1: Write failing run feedback and transport tests.** Add `test_status_disconnect_resume_and_stale_heartbeat`, `test_event_cursor_is_exclusive_bounded_and_run_bound`, `test_event_retention_gap_signals_resync`, `test_duplicate_delivery_deduplicates_by_run_sequence`, `test_absent_cost_metrics_stay_unknown`, `test_cancellation_is_idempotent_and_serializes_both_publication_races`, `test_status_event_reads_do_not_query_customer_sources`, `test_control_endpoint_lifetime_and_crash_recovery`, `test_control_client_never_opens_backend_connection`, and `test_ipc_request_key_lookup_after_lost_response_uses_destination`. Assert missing authoritative status returns typed unavailable rather than fabricated state; actual separate-process scenarios belong to Task 6 integration and use IPC without opening the writer's database file.
- [ ] **Step 2: Run focused tests.** Run: `uv run pytest tests/runtime/test_run_status.py tests/runtime/test_run_events.py tests/runtime/test_cancel_publication.py tests/runtime/test_control_ipc.py -q`. Expected: FAIL because read/control projections and owner transport are absent.
- [ ] **Step 3: Implement durable projections and owner-local control server.** Persist lifecycle decisions in runtime state and bounded event pages; use namespace/run permissions and coded safe errors. Bind endpoint with owner-only local permissions, bounded requests/timeouts and documented lifetime. Preserve publication/cancel compare-and-publish race semantics; recovery after owner crash reopens runtime store only after DuckDB releases the writer lock, reconciles publication, then re-advertises status.
- [ ] **Step 4: Verify focused tests.** Run: `uv run pytest tests/runtime/test_run_status.py tests/runtime/test_run_events.py tests/runtime/test_cancel_publication.py tests/runtime/test_control_ipc.py -q`. Expected: PASS.
- [ ] **Step 5: Commit.** `git add terrine/runtime/status.py terrine/runtime/events.py terrine/runtime/control.py tests/runtime/test_run_status.py tests/runtime/test_run_events.py tests/runtime/test_cancel_publication.py tests/runtime/test_control_ipc.py && git commit -m "feat: expose durable run feedback over owner IPC"`.

## Task 6: Python/CLI Surface, Documentation, and Optional Telemetry Projection

**Files:**
- Create: `terrine/runtime/cli.py`
- Modify: `terrine/runtime/__init__.py`, CLI entry-point configuration in `pyproject.toml`, `README.md`, `CHANGELOG.md`
- Optional Create: `terrine/adapters/opentelemetry.py`
- Test: `tests/runtime/test_cli.py`, `tests/runtime/test_api.py`, `tests/runtime/test_request_replay.py`, optional `tests/runtime/test_opentelemetry.py`
- Create: `examples/materialization_runtime.py`
- Create: `integration/warehouse_execution/test_runtime_cli.py`
- Create: `integration/warehouse_execution/test_request_replay_process.py`
- Integration: `integration/warehouse_execution/test_materialization_example.py`

**Interfaces:**
- Consumes: Tasks 1–5 stable records, `plan_refresh`, `execute_plan`, and status/control functions.
- Produces: `terrine plan` and `terrine run` using owner-configured connection/namespace and stable versioned JSON task schemas; Python `plan_refresh(...) -> RefreshPlan`, `run_plan(plan: RefreshPlan, *, con: Any, namespace: str, request_key: str | None = None, executor: Executor | None = None) -> RunReceipt`, `run_serialized_plan(serialized: bytes, *, con: Any, namespace: str, request_key: str | None = None, executor: Executor | None = None) -> RunReceipt`, `find_run_by_request_key(request_key: str, *, destination: DestinationRef, con: Any, namespace: str) -> RunReceipt | None`, `open_published_generation(receipt: RunReceipt, *, con: Any, namespace: str) -> ibis.Table`, plus Task 5 status/event/cancel functions. `open_published_generation` verifies owner namespace, destination and exact pinned manifest/generation, returning only that published relation (not a mutable latest table or internal runtime records). CLI supports `terrine run --plan PATH`, `terrine run --request-key KEY`, and `terrine run --lookup-request-key KEY --destination DEST`; owner config supplies connection/namespace. `terrine status RUN_ID --json`, `terrine events RUN_ID --after CURSOR --limit N --json`, and `terrine cancel RUN_ID --json` route over owner IPC while DuckDB is held. Request-key scope is `(namespace, destination, key)`. Canonical fingerprint includes versioned recipe, definition/source snapshot, destination, expected base manifest and normalized budget; allocate and atomically bind key+fingerprint to run ID before expensive work. Same fingerprint returns/replays that run/result after response loss; changed fingerprint for the same scope refuses. Reusing a key across different destinations is independent and lookup requires explicit destination plus namespace permission. Synchronous execution is allowed; asynchronous start requires owner executor and returns accepted, never detached completion.
- If OTel adapter is included, app configures SDK/exporter, disabled by default/no outbound endpoint; instrumentation best-effort, low-cardinality, redacted, no auto-instrumented SQL/query capture. Exporter outage/saturation MUST NOT alter results, errors, cancellation, or publication and MUST NOT block indefinitely.

- [ ] **Step 1: Write failing public surface and replay tests.** Add `test_plan_json_schema_is_versioned_and_stable`, `test_request_key_same_fingerprint_replays_existing_run`, `test_request_key_changed_fingerprint_refuses`, `test_same_request_key_isolated_by_destination`, `test_response_loss_then_fresh_process_lookup_returns_same_run`, `test_cli_status_events_cancel_use_owner_context`, `test_cli_rejects_unknown_cost_under_strict_budget`, `test_open_published_generation_uses_exact_receipt_generation`, `test_async_start_requires_owner_executor`, and `test_otel_outage_never_changes_execution_truth` (when optional adapter exists). In `integration/warehouse_execution/test_runtime_cli.py`, include `test_separate_process_status_and_cancel_route_to_duckdb_owner`; in `test_request_replay_process.py`, retry lookup over IPC after response loss before receiving run ID while owner is active. Add `test_materialization_runtime_example` for Model/Design → owner-configured destination → save/run plan → open/query pinned generation → declared correction refresh; no warehouse/network.
- [ ] **Step 2: Run focused tests.** Run: `uv run pytest tests/runtime/test_cli.py tests/runtime/test_api.py tests/runtime/test_request_replay.py -q`. Expected: FAIL because CLI/runtime facade is absent.
- [ ] **Step 3: Implement public runtime/CLI and safe receipt access.** Keep exact Python operations shared by CLI and later 12.1 agent policy; agent receives IDs/cursors only under owner context. Request lookup over IPC requires explicit destination and uses the same namespace authorization and canonical fingerprint binding as run. CLI status/events/cancel/lookup use owner IPC while DuckDB is held, not direct DB access. `open_published_generation` resolves only the immutable receipt's exact published generation under owner context, never exposes internal table names/manifest writes, and enables a consumer query. No arbitrary SQL/callbacks/budget escalation.
- [ ] **Step 4: Add optional telemetry only as projection.** Run: `uv run pytest tests/runtime/test_opentelemetry.py -q`. Expected: PASS when extra is installed; base runtime remains importable and functional without OTel. If no adapter is shipped, omit its optional file/test, not its failure isolation in core lifecycle tests.
- [ ] **Step 5: Verify runtime, replay and worked example.** Run: `uv run pytest tests/runtime/test_cli.py tests/runtime/test_api.py tests/runtime/test_request_replay.py -q`; then `uv run pytest integration/warehouse_execution/test_runtime_cli.py integration/warehouse_execution/test_request_replay_process.py integration/warehouse_execution/test_materialization_example.py -q`. Expected: Python/CLI parity, separate-process status/cancel and request lookup after response loss, destination-isolated keys, exact-generation read, bounded event resumption, changed-request refusal, redaction, no network; telemetry is not required.
- [ ] **Step 6: Document and commit.** Add the runnable DuckDB example and README guide for owner config, destination creation, save/run, exact pinned-generation query access and refresh after an operator-declared correction. Also explain readiness/watermark distinction, generation consistency, DuckDB-only atomicity, unknown budgets, IPC lifetime/recovery, status versus telemetry, cancellation limitations, request-key namespace/destination scope and backend limitations. Commit: `git add terrine/runtime terrine/adapters/opentelemetry.py pyproject.toml tests/runtime/test_cli.py tests/runtime/test_api.py tests/runtime/test_request_replay.py tests/runtime/test_opentelemetry.py integration/warehouse_execution/test_runtime_cli.py integration/warehouse_execution/test_request_replay_process.py integration/warehouse_execution/test_materialization_example.py examples/materialization_runtime.py README.md CHANGELOG.md && git commit -m "feat: add materialization runtime CLI and feedback"` (omit optional paths not created).

## Task 7: Same-Snapshot Incremental Equivalence Acceptance

**Files:**
- Create: `tests/runtime/test_incremental_equivalence.py`
- Create: `integration/warehouse_execution/test_incremental_equivalence.py`
- Modify as needed: prior runtime planner/executor test files only where owning assertions belong.

**Interfaces:**
- Consumes: complete runtime public APIs from Tasks 1–6, existing `simulate` oracle and local fixture generators; all comparison inputs pinned to the identical source snapshot and definition.
- Produces: no new runtime API; one consolidated deterministic acceptance matrix used to prove incremental result equals clean rebuild for insert/update/delete, late arrival outside normal window, earlier first-anchor insertion, deleted nth event, event each identity changes, dimension correction, roster exit/correction, readiness advance and retraction, ratio inputs changing independently, definition/schema change, strict-asof ranges, and state-panel adjacent/terminal/baseline corrections.

- [ ] **Step 1: Add independent clean-rebuild comparisons.** `test_incremental_matches_clean_rebuild_at_same_snapshot` asserts keys, values, completeness/reason metadata, provenance generations and removed-key behavior; parameterize only where it remains traceable. Pin idempotent rerun and unaffected window outputs despite changed prefix generation.
- [ ] **Step 2: Run focused equivalence tests.** Run: `uv run pytest tests/runtime/test_incremental_equivalence.py -q`. Expected: PASS across each listed scenario, with deterministic fixtures and no real warehouse/network.
- [ ] **Step 3: Run backend integration acceptance.** Run: `uv run pytest integration/warehouse_execution/test_incremental_equivalence.py integration/warehouse_execution/test_duckdb_runtime_recovery.py integration/warehouse_execution/test_runtime_cli.py -q`. Expected: PASS for real local DuckDB crash-boundary/retry, overlapping refresh serialization, expired-writer refusal and CLI state parity. Strict unknown-scan budget rejects before any source query.
- [ ] **Step 4: Commit acceptance evidence.** `git add tests/runtime/test_incremental_equivalence.py integration/warehouse_execution/test_incremental_equivalence.py && git commit -m "test: prove incremental rebuild equivalence"`.

## Self-Review

- **Spec coverage:** state/generation/source snapshots and dependency graph (Task 1); source change evidence, exact output invalidation and separate Float64 prefix-certificate/eligibility invalidation (Task 2); versioned plan reconstruction, owner-bound reopenable snapshot capture/retention/restart recovery, shared compiler layer reuse, bounded task/resource policies, leases and fencing (Task 3); DuckDB-only atomic publication, crash recovery and explicit superseded-generation GC (Task 4); authoritative run status/events/cancellation plus owner-local IPC lifetime and crash recovery (Task 5); Python/CLI, request-key scope/fingerprint/atomic run binding/replay, optional OTel and guides (Task 6); same-snapshot incremental/clean equivalence, all listed scenarios and idempotent rerun (Task 7). Task 6.2 extends the existing single dispatch; Task 7.2 pinned references, Task 9.1 scheduler parity and Task 12.1 agent policy consume this runtime, not a parallel authority.
- **Evidence for review findings:** `temporal/prefix.py` carries checked Float64 certificates/refusals; Task 2 tests historical `1e16` and cumulative numerical dependency revalidation. `execution/inputs.py` provides context-scoped snapshots, while `_publish.py` uses `fetch_arrow_table()` and a process-owned temporary artifact; Task 3 now requires budget-first backend-native/bounded capture, retained reopenable inputs and restart tests. `compile_columns` owns composition and `Frame.materialize` collects Arrow; Task 3 separates compiler logical `LayerKey` from runtime physical records, shares ordinary composition/checks, and enforces dependency direction via tach. DuckDB's file lock makes direct second-process status unsupported; Task 5/6 now route status, cancellation and request lookup through owner IPC, including crash/endpoint lifetime. Change records now carry old/new update support and previous delete support. Request lookup requires explicit destination, IPC lookup is authorized, and same keys across destinations are tested. Task 6 includes the runnable DuckDB consumer guide and exact receipt-generation read operation.
- **Type consistency:** Task 1 records feed Task 2 plan; Task 2 feeds Task 3 executor; Task 3 backend receipts feed Task 4; Task 5 reads and controls the same task/run/manifest state; Task 6 exposes those shared signatures; Task 7 tests public behavior without creating an alternate engine.
- **Review focus:** all five risks are pinned to explicit tests in their owning tasks.
- **Proportion:** Plan records contracts and observable assertions without transcribing internal implementation bodies.

## Open Decisions / Gates

- Callaway–Sant'Anna adoption or vendoring, licensing and release authority remain owner-reserved and out of scope; no algorithm/library is selected here. Recommendation: retain an explicit gate before any such dependency or release action.
- Non-DuckDB backend publication atomicity is intentionally undecided until a backend-specific protocol and crash evidence exist. Recommendation: report unsupported/unknown and refuse guarantees rather than advertise generic atomicity.
