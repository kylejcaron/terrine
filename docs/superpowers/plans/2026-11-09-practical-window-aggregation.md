# Practical window aggregation implementation plan

> For agentic workers: use `executing-plans` for this dependency-ordered integration lane. Read this plan and its specification before changing code; preserve the task boundaries and verification evidence.

**Goal:** replace per-output cumulative lowering with shared, window-local Ibis aggregation across existing pathways, retaining useful dataset semantics and practical native/exact arithmetic.

**Architecture:** collect dependency-ready additive requests; share bounded source access and equivalent primitives; feed raw rows or eligible noncumulative daily summaries into one local reducer/finalizer. Retained execution captures inputs and anchors, then uses that same ordinary compiler. It does not persist a replacement aggregate cache.

**Tech stack:** Python, Ibis expressions, existing backend dialect/provider interfaces, DuckDB retained runtime, Arrow metadata, pytest.

**Spec:** `docs/superpowers/specs/2026-11-08-practical-window-aggregation-design.md`.

**Evidence:** `docs/research/2026-11-08-query-strategy-spike.md` and its immutable benchmark records/archive.

## Scope ledger

| Capability area | Source | Disposition |
|---|---|---|
| Improve every existing pathway | Owner: “the goal is to apply this improvement to every pathway” | Confirmed goal; migrate every additive consumer, not unrelated statistical algorithms |
| Keep only useful, practical guarantees | Owner: “only if theyre both useful and more importantly practical” | Confirmed criterion for the aggregation improvement; retain exact declarations and remove lifetime-prefix dependence in source windows |
| Representative performance evidence rather than an expensive benchmark per pathway | Owner: “I imagine benchmarking each might be expensive?” followed by “lets do it” | Confirmed research approach; existing findings constrain the implementation and its integrated remeasurement |
| Produce an implementation plan now | Owner: “great lets make a plan” | Current deliverable only; production execution requires a subsequent instruction |
| Relax magnitude, quotient, product and underflow safeguards in specialized cumulative/cohort reducers | Agent-proposed extension in the earlier spec, not separately named by owner | Unconfirmed; no implementation task. Preserve those existing safeguards pending owner decision |
| New public modes, new backends/shapes, different Catalog defaults, removal of `exact=True` | Not requested | Excluded; no tasks |
| Publish, release, push, merge, or operate on owner datasets | Not authorized | Excluded; tests use isolated disposable data |

The batching, summary representation and execution-revision mechanisms below are implementation choices for those goals, not added public capabilities. Runtime schema version 1 stays unchanged: existing immutable Arrow schema storage carries the new execution marker. No database migration, owner upgrade operation, source rewrite or published-data rewrite is included.

The earlier readiness C1–C4 work remains evidence, not a second plan to execute. This plan supersedes its source-window native prefix-relative certification and physical strategy only. Exact opt-in/default declarations, Catalog versions, specialized cumulative/cohort safeguards, release work and existing cost evidence are not reopened.

## Global constraints and execution shape

- **One integration lane, five tasks, executed in listed order.** True dependencies are 1 → 2, {2, 3} → 4, and 4 → 5. Task 3's retained retirement is independently runnable; do not invent a blocker merely to enforce lane scheduling. Shared files have one owner; no parallel implementation PRs or partial release between Tasks 3 and 5.
- Proposed execution checkout: `/Users/kylejcaron/terrine/.worktrees/practical-window-aggregation`, branch `refactor/practical-window-aggregation`. Create it only when execution is authorized; record its immutable base then. The present research checkout is for the plan, not production edits.
- Read LSP references before removing or changing exported signatures. Preserve existing backend dispatch, source admission and snapshot ownership. All study computation stays in Ibis; no compiler-to-runtime imports or engine-name switches.
- A task's gate is `make check TESTS="<its affected modules>"`, its failing-first evidence, and one actual changed-path smoke. Do not run a complete tier per task or per worker. Run commands that can wait on agent-slots without a timeout. Do not disable its plugin.
- Run the full affected tiers **once on the integrated head**, in CI when published or in the integration checkout for local-only work. Task 5 names the commands. New tests derive expected values from the contract/independent arithmetic, not implementation output. Every new permanent case names its unique consumer-visible bug and shows red before green; for a new kernel, a deliberate one-line semantic break is acceptable. An import failure is not a semantic red.
- Extend owning tests before adding tests. Delete source-text, forwarding-only and obsolete prefix-policy assertions rather than repinning them. Keep exact/timestamp witnesses and relevant refusal tests. No timing assertions, broad new test matrices or performance threshold changes without evidence.
- Update the relevant guide/changelog in the same task after its smoke passes. Documentation is not a separate cleanup project. Task 5 reconciles the complete cross-path account and evidence.
- Before each task closes: commit, reconcile its commit reviews and branch review, record the gate lines and fresh verification. The final plan is complete only after all five acceptance sets pass; an intermediate kernel is not the delivered improvement.

For the implementation parent, set `harness.qa` to allow heavy selections only in the proposed integration checkout. Match `make check` only when no focused `TESTS=` override is present, full `make test`/`TESTS="tests integration"`, `make test-postgres`, and any full conformance selection. Focused task checks must not be mistaken for full-suite runs. Keep the watcher running during execution and check it at each task boundary.

## Review focus

| Likely failure | Required proof and owner |
|---|---|
| Batching changes counts, unknown-filter semantics, availability, or multiplies two sources | Tasks 1/4: owning temporal known answers and compiler two-source/eligibility integration |
| Daily summaries evaluate unused historical units/gaps or quantize after summing | Task 2: matched raw/day answers, unused-bucket poison cases, exact discarded-term case |
| Some generated/relative shape still uses prefixes or joins at the wrong anchor grain | Task 4: dependency-ready collection plus existing LTV/state/panel/selector routes; Task 5 pathway ledger |
| Old executable plans replay/reuse without the new policy, or old data becomes unreadable | Tasks 3/4: pre-side-effect guards, revision mismatch refresh, immutable metadata identity, historical administration and Arrow/manifest round trips |
| Native source-window errors become exact-overflow refusals, or helper retirement removes still-used specialized safeguards | Task 4: provider-boundary checks and LTV/cohort compatibility; retain exact-domain tests and unconfirmed specialized policy |

## Shared internal contracts

### Local aggregation boundary

Add an internal frozen `AggregateRequest` in `terrine/temporal/aggregate.py` (not a package-facade export):

```python
@dataclass(frozen=True, slots=True)
class AggregateRequest:
    metric: BoundMetric
    window: Window
    output: str
    role: str
```

Requests passed to one local aggregation have one resolved anchor relation and grain. Measures are resolved against the normalized/enriched source schemas. The temporal function owns source-local reduction and common finalization:

```python
def aggregate_windows(
    anchors: ibis.Table,
    requests: Sequence[AggregateRequest],
    sources: Mapping[str, tuple[ibis.Table, BoundSource]],
    readiness: Readiness,
    *,
    boundary: str,
    resolution: Literal["event", "day"] = "event",
) -> TemporalResult: ...
```

Task 1 implements the event form without a stub day branch; Task 2 introduces the fully working `resolution` argument. Source relations use normalized `_unit`/`_timestamp`; anchors retain `unit`, `anchor_id`, `t0` and observation bounds. Final output remains one row per full anchor identity, with multiple `ColumnMeta` entries and the existing deferred checks. Each source is reduced before cross-source results are joined.

`compile/numeric.py` owns resolution, representation choice and grouping into calls to this boundary. Introduce `LoweredAggregates(result: TemporalResult, strategies: Mapping[str, str])` and:

```python
def lower_aggregates(
    ctx: CompileContext,
    requests: Sequence[AggregateRequest],
    *,
    anchors: ibis.Table,
    relative: bool,
) -> LoweredAggregates: ...
```

Keep `CompileContext.model`, `unit`, `fixed_anchors` and `events_for`; remove its prefix `states` cache in Task 4. Distinguish source-access compatibility from primitive equivalence. A primitive key includes source/unit binding, value/label semantics, predicate, interval, arithmetic and operation; never use display names or aggregation-specific `measure_identity` as a substitute. Use existing structural expression identities rather than SQL strings or a new public optimizer API.

### Primitive payload

Extract shared contribution logic beside `_predicate`/`resolve_measure` in `temporal/daily.py`. Produce only needed columns: selected `event_count`, `nonnull_count`, native sum or exact coefficient sum, selected nonfinite count, exact-float unrepresentable/discarded-term counts, and applicable unknown-filter provenance. Exact bounded accumulations also carry `sum_withheld`. Native sums have no magnitude certificate. Raw and daily forms reduce these same sufficient primitives.

`selected = predicate.fill_null(False)` contributes values/counts; unknown candidates retain provenance rather than disappearing through `WHERE predicate`. Quantize each exact-float input before any summary. Declaration type/scale/policy belongs to the resolved primitive, not repeated row metadata. Do not carry cumulative `sum_bound`, redundant observed-null counts or native `sum_withheld` into the new payload.

Compute output availability before deciding what numerical inputs are consumed. An unavailable window must not make unused summary arithmetic relevant to another output. Keep candidate-validation semantics: later eligibility filtering must not hide invalid required inputs in otherwise available candidate windows.

### Execution compatibility

Use one internal `EXECUTION_REVISION` in the existing `compile/provenance.py`, derived by ordinary callers, not a new user-authored option:

| Effective revision | Meaning |
|---:|---|
| 0 (absent) | Historical prefix-spool plans/results |
| 1 | Task 3: retained prefixes removed; ordinary inline compiler still used |
| 2 | Task 4: local window compiler |

Revision 1 is an independently runnable preparatory state, not a permanent second engine. Task 4 advances to revision 2 so locally generated revision-1 data/plans cannot masquerade as local-aggregation results. No executable legacy adapters remain. A separately approved future specialized-policy change would need its own revision.

New wire formats are `SavedPlan.schema_version = 2` and `RefreshPlan` definition schema version 3. Include the revision in the execution fingerprint, never in only one side of `plan.definition_hash == saved_plan.design_hash`. Decode and checksum old SavedPlan v1/RefreshPlan v2 documents for inspection/pin release; reject their execution with `terrine.runtime.saved_plan_mismatch` and structured context giving a rebuild route. Decode compatibility is not execution permission.

Add optional `Provenance.execution_revision: int | None = None`. Missing means historical revision 0; leave it absent when re-serializing legacy metadata. New ordinary frames stamp the current revision; retained frames stamp the validated plan revision. Existing `DatasetManifest.frame_metadata` carries it; do not add another manifest revision field or misuse `compiler_version`/`ExecutionSnapshot`.

Use the existing `runtime_partition_metadata.schema_metadata` storage, not new runtime columns. `StagedGeneration` carries an optional revision from the **validated executing plan**, never a default inferred from the installed code or incoming data. The publisher writes a dedicated versioned marker under Arrow schema key `b"terrine.execution"` (JSON `{ "version": 1, "execution_revision": <positive integer> }`) in every partition. It remains separate from Frame provenance so plain Arrow/custom executors and typed empty results work without invented Frame metadata. An existing marker must agree with explicit execution context; conflicting/malformed markers refuse. Where Frame provenance exists, it must also agree; absent historical provenance cannot be silently upgraded. Unattributed publication remains readable but unknown/non-reusable.

Add one metadata-only `generation_execution_revision(dataset, generation)` backend reader over the **exact published generation and complete partition set**. All markers absent means legacy/unknown revision 0. All valid and equal yields that revision. Mixed absence, unequal/malformed markers, missing schema rows or no partitions refuse reuse rather than selecting the first partition. Preserve existing transactional publication, schema-sensitive retry/digest checks and zero-partition refusal. A retry cannot relabel an existing generation.

Only verified current-revision base evidence permits reduced or no-change plans. Bind reuse-base identity/evidence into the execution fingerprint separately from declaration identity. `OwnerPlanner`, dataset planning/staging and direct planning callers derive it from the runtime reader, not a portable manifest's cached provenance; missing evidence conservatively keeps the full graph. Recheck exact base identity/revision under the dataset lease before a reuse-dependent execution invokes callbacks, and inside the publication transaction before a reuse receipt. Cover the separate `Dataset.refresh` early return with the same fenced check. A custom no-op callback returning an arbitrary generation is not verified reuse.

Keep checksum/identity binding (`_bind_saved_plan`) separate from a shared current-revision execution-admission guard. Historical status/events/cancellation and pin cleanup still bind old envelopes. Execution entrypoints invoke admission before replay, authorization, pinning or callbacks. Replay and post-commit recovery also validate the **exact committed generation's** revision; a request-key or run-publication match alone is insufficient. This is integrity within the existing trusted owner/executor boundary, not a new security sandbox for arbitrary callbacks.

Decision after plan review: immutable schema metadata already has transactional publication and metadata-sensitive retry checks; it avoids the owner-wide admission/drain/migration protocol required by new runtime columns. Revisit relational revision storage only if measured partition-metadata lookup cost, SQL-only inspection needs or a new retained backend makes this representation inadequate.

## Task 1 — Implement the local raw reducer and semantic finalizer

**Owns:** `terrine/temporal/daily.py`, `aggregate.py`, reusable symbols currently in `prefix.py`, and exact/native helper use in `terrine/backends/numeric.py`. Tests: `tests/temporal/test_sparse_state.py`, `test_window_aggregates.py`, `integration/temporal/test_window_quotient_composition.py`. Relevant docs: temporal-engine/backend-compatibility guides and changelog.

**Consumes:** normalized source relations, resolved BoundMetric/BoundMeasure, readiness and anchors. **Produces:** working event-form `aggregate_windows`, shared contribution/finalization helpers, multi-output `TemporalResult`. The ordinary compiler remains on its current path until Task 4.

- [ ] Extend existing temporal fixtures to construct local inputs instead of requiring a persisted prefix. Keep tests of still-live prefix functions until their retirement; remove native-history policy assertions when their owning test is migrated, not genuine exact-domain coverage.
- [ ] Add/extend known-answer coverage for one local batch: selected values `2, NULL, 3` yield count 3, nonnull mean 2.5 and sum 5; empty complete windows yield the documented empty value/reason, all-null sums remain distinguishable; start included/end excluded; duplicate anchor instances remain separate. Use output-specific coverage/observation bounds and ratio numerator/denominator availability. Assert values and reason columns, not generated SQL.
- [ ] Prove a semantic red with the existing implementation where applicable, or deliberately change the new reducer's event mean denominator to event count: the expected `2.5` must fail. Restore it before the milestone gate. Name the bug caught by each genuinely new case.
- [ ] Factor `_predicate`, quantization and contribution construction without duplicating label/filter semantics. Move `native_arithmetic` and unknown-field constants to `daily.py`; move `FLOAT64_PRECISION` to `backends/numeric.py` and migrate imports. Keep exact guards/narrowing, generic witnesses and selected-invalid-input indicators.
- [ ] Join bounded raw events to requests, condition each primitive on the exact half-open interval, reduce by full anchor identity, then combine reduced sources. Guard fixed-domain exact accumulations per selected primitive/window, not the entire union of batch inputs. Finalize all outputs through the existing null-reason, note, completeness and check machinery.
- [ ] Preserve exact per-input quantization and discarded-term notes; use ordinary binary64 selected sums/division for native operands. A count metric cannot become a nullable mean's denominator. An available zero denominator remains `zero_denominator` with current reason precedence.
- [ ] Run `make check TESTS="tests/temporal/test_sparse_state.py tests/temporal/test_window_aggregates.py integration/temporal/test_window_quotient_composition.py"`. Smoke the real reducer on DuckDB with two windows and two sources; inspect materialized values/reasons and execute its validation before export. Update owning docs and commit after green.

**Acceptance:** one working raw batch, independent known answers, retained exact domains, no lifetime dependency in the new reducer and no temporary no-op API.

## Task 2 — Add scoped daily inputs and declaration-derived strategy selection

**Owns:** `temporal/daily.py`, `aggregate.py`, `compile/numeric.py`; the same temporal tests plus `integration/compile/test_numeric_lowering.py` for a direct batch-lowering probe. Relevant guide/strategy documentation.

**Consumes:** Task 1 local primitives/finalizer. **Produces:** day-form `aggregate_windows` and complete `lower_aggregates`; both physical inputs use the same finalization. The old single-request compiler call remains until Task 4.

- [ ] Extend matched raw/day cases using fixed offset `UTC+05:30`, half-open day boundaries and duplicate anchors. Preserve count/nonnull/unknown/discarded-term payloads across grouping. Exact inputs `[4e-10, 4e-10]` must yield sum `0` with `below_resolution`, not `1e-9` from quantizing their sum; native mode retains approximately `8e-10`.
- [ ] Add the locality regression: two consumed unit/day pairs with an unused day between them and an unrelated unit containing problematic native values. Neither unused pair may enter summary arithmetic. Also cover an unavailable wider window alongside an available narrow one. Prove red by removing the consumed-pair scope (or using a min/max envelope) and observe the consumer-visible failure.
- [ ] Derive distinct required `(unit, bucket)` identities from active/available requests to bound shared source access. Within that relation, apply **primitive-specific consumption masks before conversion and aggregation**: a bucket needed only by a count must not feed a sum, mean, exact conversion or its invalid-input checks. Sharing the access union does not share numerical relevance. An event serving multiple windows contributes once to each genuinely equivalent primitive/bucket, not once per scope-join match. Carry exact withheld flags only to outputs consuming that primitive/bucket.
- [ ] Add `test_summary_masks_sum_buckets_consumed_only_by_count` in `integration/warehouse_execution/test_postgres_windows.py`: Jan 1 has two `1e308` values, Jan 2 has value `1`, with complete scheduled anchors Jan 3/4. A three-day count and one-day native sum must return `(3, 1)` then `(3, 0)`. Prove red on PostgreSQL by removing the sum's consumption mask; the unused bucket's native sum overflows. DuckDB-only checking can miss this because unused infinite summaries may be discarded. Run `uv run --extra postgres pytest integration/warehouse_execution/test_postgres_windows.py -m postgres -k summary_masks_sum_buckets_consumed_only_by_count` on the disposable scratch service; no full PostgreSQL tier at this task.
- [ ] Use existing fixed-offset alignment rules, not UTC-only rounding. A bucket is usable only when wholly contained. Partial-day/event-relative/irregular requests use raw rows; never reconstruct an edge bucket or claim raw/day bit identity for ill-conditioned native sums.
- [ ] Implement small declaration rules: fixed single-origin requests use raw; statically aligned repeated fixed/scheduled origins may use daily summaries; unproven alignment uses raw. Split incompatible representations without losing output order. No row-count probes, density thresholds, persisted cache or retry strategy. Expose raw/day/native/exact choices through existing strategy metadata.
- [ ] Run the Task 1 selection plus `integration/compile/test_numeric_lowering.py` through one focused `make check`. Smoke a direct `lower_aggregates` batch on real DuckDB in each representation, checking independent values/reasons and physical plans. Record any irreducible planner cost rather than inventing a second engine. Update docs and commit.

**Acceptance:** raw/day parity on matched selected inputs and exact domains; no unused unit/gap arithmetic; general raw fallback and working declaration-derived selection.

## Task 3 — Retire retained prefix intermediates and version execution identity

**Owns:** `compile/layers.py`, `materialize.py`, `columns.py`, `dispatch.py`, `provenance.py`; persisted-prefix propagation in `compile/shapes/{event_panel,sequential,state_panel,_observations}.py`; `runtime/{api,layers,materialize,graph,planner,planning,saved_plan,execute,cost,status}.py`, `runtime/backends/{base,duckdb}.py`; `data.py`, `frame.py`, `dataset/reference.py`, `agent/tools.py` and affected control callers. Runtime schema remains unchanged. Use LSP to resolve all `RefreshPlan`, `StagedGeneration`, `_bind_saved_plan` and persisted-prefix callsites before editing.

**Consumes:** existing ordinary compiler plus retained snapshots/anchors. **Produces:** revision-1 retained execution with no prefix ABI, verified historical readers/admin decoding, and revision-aware computation/reuse. Keep `build_measure_layer` only while the ordinary compiler still calls it; do not move it into a temporary adapter.

- [ ] First add failing consumer regressions in existing runtime owners: old checksummed plans can be inspected/released but not executed; stale revision cannot return a request-key replay; an empty ChangeSet with an old base forces recomputation; reopening the old generation still returns its original data/metadata. Capture a valid legacy plan before changing constructors. Do not fake a failed checksum to test the execution guard.
- [ ] Implement shared execution admission for saved-plan execution, `run_serialized_plan`, direct `run_plan` **before replay**, and `execute_plan` before persistence, authorization, pinning or source callbacks. Validate canonical in-memory/serialized plan identity as well as revision. Keep `_bind_saved_plan` checksum/namespace/design binding available to administration. Validate old checksums in their original encoding; do not checksum a new-format reserialization and reject legitimate historical binding.
- [ ] Migrate `agent/tools.py::_authorize_run` and other binding consumers deliberately. Historical status, event pages and cancellation remain authorized through their old invocation; an older invocation at the same destination must not abort lookup of the matching current invocation. Extend `tests/agent/test_plan_authorization.py` and `integration/agent/test_dataset_tool_runtime.py` without weakening policy/source authorization.
- [ ] Implement the runtime marker and complete-generation metadata reader described above. Extend `tests/runtime/test_duckdb_publication.py` for typed-empty and multipart/plain-Arrow publication, consistent retries, absent/mixed/malformed/mismatched evidence and preserved old schema bytes. Existing version-1 namespaces stay version 1; no upgrade path or owner-level lock is added.
- [ ] Add revision and reuse-base evidence to the execution fingerprint and staged publication. `plan_refresh` must keep the full graph for mismatched/unknown bases even with a nonempty partial ChangeSet, not only an empty one. Recheck reuse-dependent plans at the shared execution boundary before custom callbacks; cover the arbitrary-generation no-op loophole and `Dataset.refresh`'s separate early return. Inside `reuse_generation`, fence and verify the exact base again before recording a receipt. Preserve definition/design hash equality.
- [ ] Extend existing replay/recovery tests: stale request-key plans cannot replay; committed publication revision must agree during post-commit recovery; superseded-generation replay checks that exact generation, not today's current manifest. A base-change/takeover race cannot yield a successful stale reuse receipt. Keep completed-task restoration bound to the revision-bearing run fingerprint.
- [ ] Stamp ordinary and retained provenance, with the retained value taken from the validated plan. In `_metadata_payload`, omit the optional field for legacy provenance; keep existing materialized-frame metadata and DatasetManifest schema versions. Extend `tests/test_metadata.py` and legacy dataset-read cases to prove old metadata/digests are not relabeled.
- [ ] Remove daily→prefix task production and prefix planning/substitution/spooling/restoration together. Remove `persisted_prefixes` from every dispatch/shape signature. `plan_layers`/composition retain real anchor identity validation; `_layers` transports only remaining real retained inputs. Delete prefix-specific HUGEINT/BIGNUM Arrow transport when no remaining consumer needs it. No empty replacement task kind.
- [ ] Update invalidation closure, scan/output estimates and observed intermediate counters for the remaining graph. Keep relevant source/readiness/roster invalidation and budget behavior. Do not merely lower expected task counts in tests; assert correction reaches the right final frames and unknown cost still refuses configured budgets.
- [ ] Replace prefix ABI tests in `tests/compile/test_persisted_layers.py`, `tests/runtime/test_layers.py` and persisted boolean-output cases with anchor/snapshot integrity assertions where still meaningful. Remove prefix blob/hash expectations from incremental-equivalence/restart/staging tests; retain immutable-generation, correction and exact/native result assertions.
- [ ] In addition to the following retained-path gate, run the changed publication/planning/authorization owners: `make check TESTS="tests/runtime/test_duckdb_publication.py tests/runtime/test_planning.py tests/agent/test_plan_authorization.py integration/agent/test_dataset_tool_runtime.py"`. These are focused modules, not another full tier.
- [ ] Focused gate: `make check TESTS="tests/runtime/test_schema.py tests/runtime/test_saved_plan.py tests/runtime/test_planner.py tests/runtime/test_graph.py tests/runtime/test_layers.py tests/runtime/test_api.py tests/runtime/test_execute.py tests/runtime/test_cost_estimates.py tests/compile/test_persisted_layers.py tests/test_metadata.py integration/warehouse_execution/test_saved_plan_restart.py integration/warehouse_execution/test_incremental_equivalence.py integration/warehouse_execution/test_ml_staging_exports.py integration/compile/test_boolean_outputs.py"` (drop a module only if all its obsolete tests were removed; rehomed tests replace its selector). Run `uv run python examples/materialization_runtime.py` against its disposable data as the stage/correct/reopen smoke. Update runtime/staging/cost docs and commit.

**Acceptance:** no retained prefix tasks/substitutions; old final data and pin cleanup work; old execution/replay/reuse refuses or explicitly rebuilds; revision-1 stage/refresh works through the ordinary compiler. This is not yet the local-compiler release.

## Task 4 — Switch every additive compiler invocation to dependency-ready batches

**Owns:** `compile/columns.py`, `numeric.py`, `provenance.py`, remaining inline prefix helpers in `layers.py`; affected shape callers and strategy/cost metadata. Retires `temporal/prefix.py`, `lookup.py` and their obsolete tests after references are exhausted. Existing nonadditive/selector algorithms remain separate.

**Consumes:** Task 2 batch lowering and Task 3 revision-aware runtime. **Produces:** revision-2 ordinary and retained local aggregation everywhere; no general prefix engine.

- [ ] Add a failing public compiler regression for native `[old: 1e16, selected: 1]`: the recent window returns approximately `1` independent of excluded history, with normal native metadata. Update `integration/compile/test_numeric_lowering.py` to use selected-input known answers, not `native_window_bound` or strategy-name snapshots. Delete `tests/compile/test_numeric.py`'s source-text DECIMAL/SQL-length test rather than repin it.
- [ ] In `compile_columns`, collect additive leaves from features, outcomes and eligibility before physical lowering, while keeping outward order. Resolve selector dependencies first; partition ready leaves by actual anchor relation/origin/grain. Do not introduce a global compilation-session cache across distinct calls.
- [ ] Call `lower_aggregates` once per compatible ready group, attach each multi-output relation/check set once, and populate existing expression values, timing metadata and public aliases from it. Preserve candidate check lifting before eligibility filtering, including empty published outputs. Preserve output-specific completeness and unavailable-filter reasons.
- [ ] Extend `integration/compile/test_column_runtime_contracts.py` with a two-source known answer: source A values `2,3`, source B values `4,6,10` produce sum A `5`, sum B `20`, ratio `0.25`, with no 2×3 fan-out. Include mean/count and mixed filters/windows in the same batch where this can extend existing fixtures. Nested selector requests must use the selected origin; two anchor IDs for the same unit/time must both survive.
- [ ] Exercise generated LTV windows and binned/observed state period, endpoint, row and baseline compilations through their existing owners. Check repeated/composite anchor identities in sequential/event panels; sequence/survival/life-table grouping leaves use the same collector while their specialized reductions remain untouched.
- [ ] Remove `lower_aggregate`, `CompileContext.states`, `build_measure_layer`, cumulative/lookup implementation and unconsumed fields/imports. Move surviving helper ownership as Task 1 specified; do not delete exact helpers still needed locally. Retire prefix-only tests including strict lookup behavior in conformance; keep half-open local membership coverage at its owner.
- [ ] Migrate every `native_window_bound` consumer to independent, selected-input known answers with fixture-appropriate tolerances, then remove that obsolete helper from `tests/_float_bound.py`; retain exact `window_sum_bound`/`ratio_bound`. Current consumers are the temporal sparse/window tests, `integration/compile/test_numeric_lowering.py`, `integration/backends/test_numeric.py` and `integration/conformance/test_engines.py`. Consolidate the million-history conformance case into the existing small large-history case with both day/intraday outputs only if all its assertions are retained; no million-row history is needed to prove local membership. Run the focused numeric conformance cases with `uv run pytest integration/conformance/test_engines.py -k 'window_sums or bounded_accumulation or sub_resolution'` and the affected backend numeric module.
- [ ] Bump `EXECUTION_REVISION` from 1 to 2. Extend Task 3's regression using a valid revision-1 plan/generation: execution cannot replay it and no-change refresh must calculate a revision-2 generation, while explicitly opening revision 1 preserves it. The guard is comparison-based, not a growing list of executable adapters.
- [ ] Narrow `execution/_bigquery.py`/`_snowflake.py` error translation for the newly unguarded native source-window path: reliably exact failures retain `EXACT_OVERFLOW`, ambiguous/native overflow preserves its original execution cause, and history mappings stay intact. Add/extend the owning provider-boundary regression and prove red with the broad mapper; run it in the focused gate. Preserve specialized LTV/cohort magnitude/quotient/product/underflow behavior; follow LSP references before deleting any numeric helper still used there.
- [ ] Focused gate: `make check TESTS="tests/compile/test_columns.py tests/compile/test_ltv.py tests/compile/test_state_panel.py tests/compile/test_panels.py tests/compile/test_event_sequence.py tests/compile/test_time_to_event.py tests/compile/test_range_scaling.py tests/temporal/test_window_aggregates.py tests/temporal/test_nonadditive.py integration/compile/test_numeric_lowering.py integration/compile/test_column_runtime_contracts.py integration/compile/test_frame_runtime_window_export.py integration/state_panels/test_state_panel_workflows.py integration/warehouse_execution/test_incremental_equivalence.py"`. Smoke one real multi-metric `Design` through materialize and retained refresh; inspect values/reasons, SQL and physical plan. Update strategy/temporal/shape docs and commit.

**Acceptance:** every internal additive compilation routes to the batch collector; no prefix producer/consumer/shim remains; source fan-out and anchor grain are correct; revisions invalidate old computations.

## Task 5 — Verify the integrated pathway, artifact and performance contracts

**Owns:** affected existing integration/corpus fixtures, `scripts/write_artifact_corpus.py` if required for new metadata, benchmark helpers/evidence, guides/changelog. No new product capability or fallback optimizer.

**Consumes:** final revision-2 implementation. **Produces:** complete coverage ledger, immutable compatibility evidence and honest integrated performance measurements; existing specialized reducer safeguards are unchanged.

- [ ] Walk the following coverage map and extend an existing owning case only for a missing consumer-visible assertion. Record the exact test/case used for every row. The map is not a quota of new tests or benchmarks.

| Pathway | Existing verification home |
|---|---|
| Fixed/scheduled ordinary metrics; relative/nested selectors; eligibility | `tests/compile/test_columns.py`, `integration/compile/test_column_runtime_contracts.py`, `test_numeric_lowering.py` |
| Sequential/trial/calendar panel; event panel | `tests/compile/test_panels.py`, `integration/parity/test_increment_panel_parity.py` |
| Calendar comparison | `tests/compile/test_panels.py`, `integration/test_artifact_corpus_schema.py` recipe cases |
| LTV generated periods and cumulative/cohort results | `tests/compile/test_ltv.py`, `test_cohorts.py`, `integration/compile/test_ltv.py` |
| Binned and observed state panels; baseline/endpoints | `tests/compile/test_state_panel.py`, `integration/state_panels/test_state_panel_workflows.py` |
| Sequence/session/funnel base metrics | `tests/compile/test_event_sequence.py`, recipe/artifact fixtures |
| Survival and life-table additive grouping | `tests/compile/test_time_to_event.py`, `test_survival_bounds.py`, `test_completeness_columns.py`, `integration/compile/test_time_to_event.py`, `integration/test_frame.py`, artifact cases |
| Nonadditives, retention, attribute/as-of lookups | `tests/compile/test_nonadditive.py`, `test_retention.py`, owning temporal tests |
| `.expr`, preview, materialize, dataframe conversions | `integration/test_frame.py` (including pandas/Polars parity), `integration/test_model_from_tables.py`, owning column checks |
| Catalog/from-tables/Increment entry routes | `integration/compat/test_native_catalog.py`, `integration/parity/test_increment_panel_parity.py`, model/artifact cases |
| Stage/capture/refresh, bare/saved-plan execution, restart/replay | `integration/warehouse_execution/test_runtime_api.py`, `test_saved_plan_restart.py`, `test_incremental_equivalence.py`, `test_ml_staging_exports.py` |
| Immutable reopen/export, training/export/orchestrator consumers | `test_ml_staging_reads.py`, `test_artifact_identity.py`, `test_ml_staging_training.py`, `test_orchestrator_adapters.py` under `integration/warehouse_execution/` |
| Generic read-only admission and verified engine policy | `integration/execution/test_generic_provider.py`, `integration/backends/test_generic.py`, `integration/conformance/test_engines.py` |

- [ ] Preserve `tests/artifacts/1` byte-for-byte. Use a new generation (currently `2`) for changed output provenance and current executable-plan examples; keep independently stored legacy plan fixtures for decoding/refusal/release tests. Do not alter `tests/artifacts/RELEASES`, package version or release mappings. Update corpus determinism tests to regenerate the appropriate generation without rewriting old expected bytes. Check old version-1 manifests with and without `frame_metadata`, not only new fixtures.
- [ ] Run the integrated local gates once: `make check TESTS="tests integration" PYTEST_WORKERS=auto`, plus `uv run pytest integration/conformance --generic-dialect`. Run `make test-postgres` once against the repository's disposable scratch service (start/stop it through the documented helper). These are the full-selection commands allowed only in the integration checkout. Do not run the entire scale tier.
- [ ] Verified BigQuery/Snowflake execution uses the existing tiny conformance/parity cases when authorized/configured, not a performance campaign per pathway. Record actual live runs separately from compile-only evidence; unavailable authorization/credentials leaves an explicit warehouse gate, never a claim of conformance. Preserve registry-driven engine selection.
- [ ] Remeasure representative **integrated** workloads: (a) 10k units/300k events, 5 metrics × 10 windows, native and integer; (b) dense aligned overlap, raw versus daily construction+consumption where the internal kernel is directly exercised, plus the ordinary compiler; (c) genuinely poor compression/irregular repeated anchors with an independent oracle. Add a case whose distinct unit/day bucket count is near its event count; the existing sparse case compresses 2.1× and is not that evidence.
- [ ] Report separately compile time, raw/formatted SQL bytes, full-result materialization time, source/intermediate/final row counts where observable, and retained stage/correct/refresh cost. Include summary construction and validation; do not compare prototype server-only numbers with Frame wall time. Use fresh benchmark result directories; preserve the 20261108 baseline/archive. Keep connection credentials out of metadata. Run enough bounded repeats to expose noise and report the method, not a single lucky timing.
- [ ] For new end-to-end ratios, run the same public-API benchmark driver/fixture at the pre-change production baseline `5705ff7fde0f781c7be5f3386ab4e5f1c81425db` in an isolated baseline checkout and at the integrated head. The old prototype timings are context, not a full-Frame baseline. Preserve the recorded PostgreSQL timeout as censored evidence rather than rerunning it merely to confirm failure.
- [ ] Reuse `benchmarks/query_strategy_spike.py`/`run_cost.py` only for their actual contracts; extend the existing helper where full Frame/retained or poor-compression coverage is missing. Do not turn the experimental variants into a second production implementation. Record SQL/physical plans and independent result comparisons. BigQuery compilation is SQL-size evidence only; PostgreSQL timeouts remain censored observations.
- [ ] Acceptance is evidence that wide fixed queries no longer expand per output and that the chosen summary rule avoids the measured dense-overlap raw regression, with practical end-to-end cost including runtime handling. Record any irregular/poor-compression regression honestly. Do not restore prefixes, add density probes or weaken exact semantics to make a chart look better; diagnose within this design, or record a scoped decision if evidence invalidates it.
- [ ] Run `uv run python examples/materialization_runtime.py` and a multi-output ordinary materialization after the final changes. Reconcile all changed docs, refusal descriptions, strategy/cost descriptions and artifact surfaces. Remove throwaway probes; keep only reproducible benchmark assets. Perform the final diff/spec review, reconcile commit and branch reviews, and close the implementation effort only with this ledger and observed evidence.

**Acceptance:** all existing additive routes use the new pipeline; old final artifacts remain readable; current execution cannot reuse stale semantics; native/exact semantics have independent evidence; representative integrated performance is measured without claiming every pathway was benchmarked.

## Plan review and handoff

Self-review this plan from the specification's Practical result contract through Decision boundaries: preserve every confirmed result contract, rollout row, retained compatibility obligation and acceptance item. Review interfaces, dependency order, changed-file ownership, test cost and the five failure modes above. Run the independent plan-review panel before creating implementation task issues; record every disposition. The panel has one rerun after material design changes; resolve remaining findings without an unbounded review loop.

Create five child implementation tasks under the existing improvement parent, each linking its section and only its real prerequisites. Keep the plan path on the parent and label it `active-plan`. Record the unconfirmed specialized-policy question as a standalone `needs-human` issue, not a blocker or child; the recommendation is to preserve those safeguards until explicitly confirmed. The current request ends with the reviewed plan/task graph; do not create the implementation worktree or begin Task 1 until implementation is requested.
