# Safe lazy study frames implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete Task 4.2's native Model → Design → Frame → Arrow workflow, including exact input authority, per-column temporal semantics, eligibility audit, bounded preview/export and honest provenance.

**Architecture:** Rebuild the complete Ibis graph inside one owned execution context. Ordinary native tables use a no-copy transaction; explicit publication creates runtime-owned sealed native inputs and live verified bindings. A column compiler consumes the existing anchor/window primitives; Frame owns execution, limits and conversion.

**Tech Stack:** Python 3.13, Ibis 12, DuckDB 1.5.6, PyArrow, SQLGlot, existing Pydantic records and focused pytest checks.

**Spec:** `docs/superpowers/specs/2026-10-17-terrine-frame-design.md`.

**Companion contracts:** Task 4.2 and signatures in `2026-10-16-terrine-study-api.md`; shared interfaces in `2026-10-16-terrine-build.md`; existing numerical semantics in `2026-10-16-terrine-temporal.md`; later-only amendments in `2026-10-16-terrine-dataset-workflows.md`.

## Global constraints

- No increment package edits, network/warehouse checks, publishing, pushing or merging.
- Preserve all Task 4.2 acceptance; no new weights, estimators, state builders, feature-store clients or plugin framework.
- Default preview and Design.build execute no source-row query; schema/native metadata binding is allowed.
- Ordinary execution does not copy inputs. Exact checks, eligibility and output share the actual pinned context.
- Input authority is live issuer ownership and verified immutable storage or a real transaction, not names, hashes, read-only flags or serialized receipts.
- Preserve exact counts/decimals, checked Float64 certificates, native timestamp instants, nullable values and shared metadata.
- Permanent tests protect consumer-visible semantics, not wording/private wiring; tiny deterministic fixtures, no Cartesian matrices or full-suite run. Expensive native composition belongs in the existing slow tier.
- Parent owns shared Model/binder/anchor integration, tracker changes, checks, commits and review receipts. Disjoint implementation workers skip tests/build/lint/formatters/imports and git.

## Review focus

1. A literal dotted table name and a real schema-qualified SQL reference must not be conflated; Task A exercises both and ambiguous names.
2. A private cursor changes schema/search path/timezone and loses TEMP registrations; Task A proves preservation or a specific refusal, never silent substitution.
3. A nanosecond history one tick before t0 and a microsecond/UTC cutoff outside the nanosecond range must compare exactly; Task B covers strictness and 1600/2500 bounds.
4. Zero-error Float64 values are genuine singleton intervals; Task B covers Eq(0) on an observed empty additive window plus uncertain nonzero-error thresholds.
5. A closed publication, a failed stream or a missing optional dependency must not leak data, reopen revoked authority or return partial success; Tasks A/C exercise lifecycle and conversion errors.

## File ownership and frozen integration interfaces

**Task A:** create `terrine/execution/__init__.py`, `inputs.py`, `_native.py`, `_publish.py`; extend `semantic/sql.py` with a private parsed-query admission helper; add `tests/execution/test_inputs.py`. These files own native input discovery, issuer registry, publication and leases. No Model import at runtime.

**Task B:** create `compile/columns.py`, `_asof.py`, `_predicates.py`, `temporal/_timestamps.py`; modify `temporal/attributes.py` and `temporal/lookup.py`; add `tests/compile/test_columns.py`, extend existing attribute/lookup/window tests. No Frame/input-authority imports. Parent first supplies Model._relation and binder/anchor resolver migration.

**Task C (parent):** integrate `model.py`, `semantic/bind.py`, `semantic/models.py`, `compile/anchors.py`, `spec/design.py`, `__init__.py`, existing optional increment facade; create `compile/cost.py`, `frame.py`, `tests/test_frame.py`, `tests/integration/study_api_smoke.py`; update README, changelog, CI selectors and tach edges. Shared data records stay unchanged unless a demonstrated contract gap requires a complete migration.

Frozen internal boundary:

```python
# execution.inputs: no Model dependency
@contextmanager
def open_execution(catalog: Catalog, con: Any, *,
                   units: Mapping[str, UnitSource] | None = None,
                   input_binding: VerifiedInputBinding | None = None
                   ) -> Iterator[ExecutionContext]: ...

def bind_resolver(catalog: Catalog, con: Any, *,
                  units: Mapping[str, UnitSource] | None = None,
                  input_binding: VerifiedInputBinding | None = None
                  ) -> Callable[[RelationSpec], ibis.Table]: ...
# Metadata-only resolver for initial Model binding. With a supplied binding,
# validate live authority and resolve stored roots under short metadata leases;
# do not execute original SQL or hold a data-execution transaction at build time.
# Without a binding, admit native roots/views/functions before any schema bind,
# reject data-dependent inference, and cache admitted relation expressions.

# ExecutionContext owns .con, .snapshot: ExecutionSnapshot,
# resolve(relation: RelationSpec) -> ibis.Table,
# execute(query: ibis.Table) -> pyarrow.Table,
# and the full lease/transaction lifetime. No detached reader may escape it.

# model.py, parent-owned
Model._relation(relation: RelationSpec) -> ibis.Table
Model._for_execution(context: ExecutionContext) -> Model

# compile.columns: no Frame/execution dependency
compile_columns(design: Design, model: Model) -> CompiledStudy
# CompiledStudy owns:
# table: ibis.Table; audit_table: ibis.Table with __terrine_eligible Boolean;
# roles: Roles; columns: Mapping[str, ColumnMeta];
# feature_cutoffs / eligibility_cutoffs: Mapping[str, FeatureCutoff];
# strategy: Mapping[str, str];
# validate(*, execute: Callable[[ibis.Table], pyarrow.Table]) -> None.
# audit_table is one row per original candidate, not already filtered output.
# table exports retained rows and public/support columns; validate never exports data.
```

No new public generic executor/proof interface. Initial Model binding uses bind_resolver so published roots bind only stored schemas. Ordinary native roots undergo dependency/function admission before any Ibis table/sql schema method, including initial Model construction; a view over read_csv(custom_path_function()) must refuse without invoking it. Reject data-dependent or unproven backend schema inference with an explicit-publication route. Cache admitted relation expressions for later build/preview; otherwise re-admit changed native definitions before schema rebinding. The execution clone resolves every source/dimension/roster/event-ID read in context, preserves declarations/hash and rejects schema drift. Compiler-owned SQL may use only the same backend and resolved graphs.

## Task 1: Native input authority and owned publication (A)

**Produces:** the frozen execution context, the two required execution refusals, and these focused public module interfaces (not root exports):

```python
@dataclass(frozen=True)
class PhysicalRelationId:
    kind: Literal['table', 'sql_result']
    catalog: str | None
    schema: str | None
    name: str
    definition_hash: str | None = None

class RelationHandle: ...       # opaque, only issuer-registry membership grants authority
class VerifiedInputBinding: ... # opaque, not deserializable authority

required_relations(catalog, con, *, units=None) -> tuple[PhysicalRelationId, ...]
verify_immutable_inputs(catalog, con, *, units=None, relations) -> VerifiedInputBinding
publish_native_inputs(catalog, *, con, units=None) -> PublishedInputs
# PublishedInputs.con: managed Ibis backend
# PublishedInputs.relations: immutable exact mapping of IDs to issued handles
# PublishedInputs is a context manager; close revokes new leases.
```

- [ ] **A1: Write focused behavior tests before implementation.** Ordinary bare names, quoted literal `literal.dot`, explicit qualified SqlQuery, nested native view/CTE, empty/WHERE-false source retained in closure; table/dimension/roster all required. Ambiguous partial names refuse with a qualified-SQL/publication route. An unused CTE is admitted only consistently with native binding, never silently treated as a read leaf.

Add a behavioral pre-schema admission regression: create a view over read_csv with a counting custom path function, reset the counter after owner setup, and assert Model construction refuses while counter remains zero. Also replace an admitted view after Model creation and verify build/default preview never invoke the rejected function through a stale resolver. Use only local temporary fixture data; no source-text assertions or network.
- [ ] **A2: Run `uv run pytest tests/execution/test_inputs.py -q`.** Expected red: execution module/interfaces absent. Parent records the actual failure.
- [ ] **A3: Implement `_native.py` discovery and ordinary execution.** Verify DuckDB/Ibis native backend, qualified internal metadata functions and exclusive origin access. Two separate verified `system.main.current_transaction_id()` statements must differ before duplication; equal IDs refuse without transaction control on caller. Snapshot origin semantic settings/catalog/schema; create a private raw connection and its Ibis wrapper first (Ibis resets TimeZone), then reproduce/verify differences on the final runtime (ignore progress display), disable Python replacement scans and BEGIN only on the owned raw connection before source binding.

Use a complete native table/view metadata snapshot. Match TableRef.name as one literal identifier under DuckDB ASCII-insensitive comparison; never split dots. SQL factors use SQLGlot scopes and structural components. Partial names require globally unique table/view metadata matches, including TEMP/internal contenders; qualified names match exact components. Recursively admit stored view query ASTs, record definitions/OIDs, reject cycles/opaque relation functions and shadowing custom functions/macros. Extend existing SQL admission internally, not a second parser convention. Bind unchanged original queries; map native get_table_names leaves through the same resolver and require exact agreement. Do not use optimized EXPLAIN or estimates to erase inputs. Require one mutable native database for table/view closure. All integrity/data reads use this exact context. Revalidate metadata immediately before checks/export; None is honest for unknown snapshot/source versions.

Use qualified native `json_serialize_sql` solely to canonicalize actual native function/operator names for ownership checks; it is parser metadata, not relation identity. COUNT(*) becomes count_star, ordered percentile becomes quantile_cont, and arithmetic operators are function lookups too. Check these names against qualified native function metadata, allowing trusted internal system overloads and rejecting noninternal shadow contenders. Inspect trusted internal macro dependencies recursively if a used builtin expands through them. Continue using the existing SQLGlot read-policy for admission. No source SQL is rewritten for execution.

- [ ] **A4: Implement `_publish.py` explicit publication.** This operation may read/copy inputs because the caller explicitly requests it. On origin, first exclude a caller-owned active transaction; use only a runtime-started capture transaction and never finish a transaction the caller owns. Persistent native roots plus local native TEMP roots may be captured under one controlled origin view, with exclusive local state and at most one mutable persistent database. A genuinely opaque input is supported only as a single whole-result capture boundary, not a collection of sequential mutable upstream reads claimed coherent. Refuse unsupported multi-upstream coherence with a route to stage one owner-defined result/provider snapshot.

Create a runtime-owned temporary DuckDB artifact, CTAS admitted roots/results with all needed columns, finish the capture, detach/close every runtime-owned writer, open fresh read-only backing and register actual native schema/identities/generations. No user file is modified/deleted and no external write target is accepted. Deduplicate identical original relation definitions/captured roots; direct native stored tables have canonical table IDs, SQL/view results have tagged definition-hashed logical IDs plus verified backing tables. The managed root map replaces original roots before discovery/binding. Logical-result substitution removes dependencies used only inside that root, not independently referenced inputs. Verify complete exact frontier; a random receipt is only a label for the real issuance event.

Maintain issuer identity and managed connection, Catalog/roster declaration hashes, root definitions, stored schema/identity/generation and owner/lease state. Fake/missing/extra/foreign/stale/mutable handles refuse with `terrine.execution.invalid_input_binding`. Revalidate before every operation under a retained lease. Owner close disallows new leases even for existing Models; a lease already held keeps storage alive until completion, then closes/removes only its own artifact. Avoid registry ownership cycles and cleanup leaks. No upstream historical-snapshot/replay claim is made. Current source changes after sealing do not invalidate the stored result.

- [ ] **A5: Extend lifecycle/authority regression tests.** Native concurrent writer after first check is invisible through output; caller transaction and TEMP shadow are not lost. USE other + nondefault TimeZone is preserved, including a source SQL cast whose result changes under UTC versus America/New_York, after Ibis wrapping. Native function aliases/operators cannot be redirected by custom macros. Provenance strings/manual handles cannot mint authority; wrong backend, declaration/schema, missing/extra membership and owner close refuse before data. A real live publication succeeds with the transaction route disabled; whole SQL result reads stored output, not mutated originals. Lease retained during owner close finishes; a later lease refuses. Simulate one exceptional stream consumer and assert owned resources close; no private-structure assertions.
- [ ] **A6: Parent runs focused tests/static checks, then a real native context/publication smoke.** Source values and concurrent-write visibility must match; report no portable replay guarantee. Commit only A's complete files and guide/changelog entry, capture actual SHA, finish expected commit review and record receipt. No task closure on a pending review.

## Task 2: Column composition, exact as-of and eligibility (B)

**Consumes:** migrated Model._relation, compile_anchors returning AnchorTable, existing normalize_source/enrich_events, sparse daily/event prefixes, aggregate_window/TemporalResult, attribute_asof/validate_history and typed metadata. Produces CompiledStudy as frozen above.

- [ ] **B1: Add semantic regressions and reproduce before production changes.** One tiny native roster/events/history fixture covers two units and intraday t0, pre/post windows, observed zero, all-null measure, unavailable history and independently incomplete outcome. Assert named values/roles/window metadata/cutoffs and exact null reasons. Strict history and public aggregate windows must preserve t0-1ns, t0 and t0+1ns boundaries with UTC/us anchors, through both native and union_window lookup strategies. Include pre-epoch fractional instants and 1600/2500 cutoffs; no timestamp truncation/overflow. Malformed history validation count results (empty/null/negative/non-integer) must fail closed rather than pass. Consolidate existing attribute/lookup/window tests; expensive public composition uses the slow tier.
- [ ] **B2: Parent runs the focused new tests.** Expected red includes absent column compiler and existing mixed timestamp dtype refusal/history parser defect; record each observed behavior.
- [ ] **B3: Implement the common exact key in `temporal/_timestamps.py`, attributes and prefix lookup.** Define `timestamp_order_key(value: ir.TimestampValue) -> ir.StructValue` and `compare_timestamps(left: ir.TimestampValue, right: ir.TimestampValue, op: Literal['lt','le','gt','ge','eq']) -> ir.BooleanValue`. The key fields are rank:int8, seconds:int64, nanos:int64. For NS coefficient n: seconds=trunc_divide(n,10^9), nanos=mod(n,10^9). For us coefficient u: seconds=trunc_divide(u,10^6), nanos=mod(u,10^6)*1000. Use native integer divide/mod, never Float64 division or full coefficient scaling. Finite rank=0; supported negative/positive infinity ranks=-1/+1 with zero remaining fields. A NULL timestamp produces a NULL struct. Coarser units use exact checked native coefficient conversion; inability raises `terrine.temporal.timestamp_order_unsupported` with a narrow normalization route, never a lossy key. Source/output timestamp columns stay unchanged.

Project the same key on point/prefix sides. Native ASOF uses strict key inequality; fallback unions equal-typed keys, orders by key then existing request/state kind, carries MAX of the complete struct marker and equality-joins that full marker. Do not compute independent per-field maxima. NULL requests retain no-match semantics in both routes, not the final state. Reuse the same comparisons in as-of/enrichment/overlap checks. Strengthen the Arrow history-count parser to require exactly one nonnegative integer count. Native builtin UDF declarations are permitted; Python row callbacks are not.
- [ ] **B4: Implement `_asof.py`.** Resolve dimension histories through Model._relation. Inline event-time/pre-exposure attributes project distinct unit/time/value changes; static attributes project distinct unit/value. Identical observations dedupe, conflicting values at one unit/time (or per static unit) refuse via deferred history checks. Use strict as-of for features and explicit non-strict Treatment.at, without imposing causal restrictions. Missing history is incomplete `missing_history`; found NULL is complete with existing all-null semantics. Intersect source/unit readiness using strict lower<cutoff<=upper or point lower<=cutoff<upper, respecting source coverage precedence. Preserve effective-time knowledge honestly.

Retain the matched observation time for every inline/derived change-point history. In addition to cutoff membership, require source and unit completeness from that match through cutoff. Source lower bound after match gives unavailable missing_history; unit lower bound after match gives incomplete_observation, preserving source precedence. An explicit named valid_from/valid_to record can assert a covering interval; LEAD of incomplete points is not authority. Static declarations keep their explicit invariant semantics. Regression: an old nonnull or NULL observation before either lower bound cannot become a complete as-of feature or a known IsNull/eligibility result; a re-observation within coverage and a genuine explicit validity interval remain usable.
- [ ] **B5: Implement `_predicates.py`.** Define internal compiled predicate records carrying Boolean status, completeness/reason, cutoff and deferred checks. Reuse `_filter_comparison` and typed-literal admission for exact integer/decimal comparisons. IsNull is true only for known observed null; incomplete/missing input remains UNKNOWN. Gt/Eq with checked Float64 use outward certified endpoints, preserving a genuine singleton when error==0. Gt true iff lower>c, false iff upper<=c; Eq true only singleton==c, false if outside interval; else UNKNOWN with reason `uncertain_comparison`. Endpoint overflow is the existing numerical refusal, not UNKNOWN. And implements SQL three-valued false precedence; a known false clears UNKNOWN reason/completeness for that predicate. For an unresolved conjunction preserve the first unknown conjunct's reason in declared order. Exact null predicates never equate unavailable data to an observed null.
- [ ] **B6: Implement `compile_columns` and tests for remaining contracts.** Validate one NFC/casefolded public namespace/reserved labels before SQL compilation; preserve original labels and quote identifiers, including SQL-looking names. Compile candidates and distinct temporal/as-of expressions once. Reuse sparse measure states, fresh lookup identities and exact primitive checks; choose day state only when alignment is established, otherwise event state. No unit×calendar or anchor×window event expansion. Compose named feature/outcome/eligibility/treatment outputs with complete ColumnMeta and support columns. Keep one candidate-status row per anchor for audit; export only retained rows after producer checks. Source/history invalidity is never hidden by false eligibility. Only push exclusion before outcome work when all deferred integrity checks remain global/equivalent.

Cutoffs include every named feature/eligibility/as-of expression; latest contributing time and strictness determine And. Current history provenance is retrospective_effective_time; preserve explicit treatment known_at_t0. Roles has no added weights API. Add focused false+unknown, IsNull, nonzero interval ambiguity, zero-error singleton, exact decimal/large integer predicate and naming-collision cases. No duplicate same-path rows or source-text tests.

- [ ] **B7: Parent runs changed focused tests/static checks and a public-value native smoke, measures durations and keeps expensive cases slow.** Commit B's complete files/temporal documentation/changelog; reconcile expected review IDs before closure. No Frame claim from compiler-only checks.

## Task 3: Shared resolver integration, Frame and public workflow (C)

**Consumes:** A's execution context and B's compiled result. Parent owns this task and shared integration.

- [ ] **C1: Add focused public tests before the relevant production changes.** `tests/test_frame.py` covers no-row build/default preview, bounded diagnostics (uncapped exact summary versus capped unknown totals), full eligibility UNKNOWN refusal with counts only, strict unknown scan budget before integrity/count/output, max_rows abort (never truncated success), cumulative Arrow-byte abort and optional missing dependencies before reads. Reuse A/B fixtures rather than repeat temporal edges. Construct a Model from the original feature+label Catalog with only feature-source readiness: feature-only Design builds, selecting the label still raises the existing missing-source code. Keep complete immutable-binding verification. New model-hash tests change readiness/unit/event-ID declarations independently and assert hash changes; reordering preserves it. Metadata round-trip asserts actual exact/nullable data and cutoff/source-generation/snapshot distinctions.
- [ ] **C2: Parent establishes red focused tests.** `uv run pytest tests/test_frame.py -q`; missing public Frame/facades is the expected initial failure, not proof of later semantics.
- [ ] **C3: Migrate the shared resolver before parallel B implementation.** Add optional input_binding to native Model and existing from_increment forwarding. Bind supplied readiness only; remove eager all-Catalog-metric coverage requirements and preserve selected-source checks in spec.validate. The later scoring helper prunes labels before that selected-design validation; it must accept feature-only readiness on the original Catalog. Immutable bindings still verify the complete Catalog/dimension/roster frontier. `normalize_source` takes an optional internal relation resolver for source/dimension metadata. All Model roster/event-ID and anchor fact/roster reads call Model._relation; compiler-owned SQL uses model.con and resolved graphs. `_for_execution` rebinds metadata in context, rejects schema drift and preserves declarations/hash. Model hash includes canonical Catalog/units/event IDs and supplied UTC readiness. Catalog lazy facades call A's signatures without eager execution imports.
- [ ] **C4: Implement `compile/cost.py`.** Metadata-only Preview contains coverage, chosen strategy, unknown costs and explicit warnings. DuckDB estimates/output bytes are not scan bounds. A positive bounded count first enforces scan policy, then queries capped candidate statuses (cap+1) in one context without exposing rows. Uncapped count fills candidate/false/unknown/retained totals; capped totals stay None, counted_rows=min(observed,cap). Default count=0 makes no integrity/count/output query. Declare the four shared budget codes from the spec once; strict unknown scan_bytes refuses before checks, non-strict records an honest warning. Integrity-probe scans use the same policy.
- [ ] **C5: Implement `frame.py` and facades.** `Design.build(model)` validates/compiles lazily into Frame; Frame.expr is the explicit unsafe Ibis escape hatch. Safe materialize opens A's context, creates execution Model and B's graph, runs all deferred checks and full eligibility audit, raises existing `terrine.study.eligibility_unknown` with aggregate counts if unresolved, and otherwise performs one Arrow output stream. LIMIT max_rows+1 belongs to that output query, not guard+rerun; discard partial output on row/byte overage or reader error and always close reader/context. Keep per-row support columns and shared MaterializedFrame serialization. Source generation keys are canonical JSON `[kind,name]` strings as the spec pins, all declared roots present; native unknowns remain None, publication generations are actual issued stored-input events. Snapshot_id unknown stays None and replay stays unverified.

`Frame.to_pyarrow(*,budget=None)` returns serialized metadata via MaterializedFrame.to_arrow. to_pandas/to_polars check optional dependency before reads; pandas uses nullable Arrow-backed types. Use one shared missing-dependency coded refusal/install route. Frame materialize accepts positional/keyword budget as specified. Root exports add only Frame, Budget and preview plus already-reviewed study constructors; keep execution authority and compiler helpers in focused modules. Do not export internal aliases.

Classify `baseline_binary` only for a fixed-date cross-sectional design with a Boolean treatment at zero and explicit `known_at_t0=True`; preserve effective-time knowledge. Descriptive/nonbaseline treatments remain unclassified. Do not weaken estimator admission or infer assignment. A public producer-to-existing-consumer regression must pass declared treatment and refuse the otherwise identical undeclared-known-at-zero variant.

- [ ] **C6: Run focused contract checks and the real public native smoke.** `make check TESTS='tests/execution/test_inputs.py tests/compile/test_columns.py tests/test_frame.py tests/test_frame_runtime.py'`; run changed slow selectors separately. `uv run python -m tests.integration.study_api_smoke` constructs Catalog/YAML + Model + Design without increment imports, asserts actual named values, intraday exclusion, eligibility totals, per-source incompleteness, provenance and both transaction/publication routes. Exercise a concurrent native writer between check/output. No mocked copies of compiler logic.
- [ ] **C7: Update user docs, changelog, CI and architecture boundaries.** README shows the working native example, lazy preview, explicit budgets/conversions and focused publication workflow. Document partial-name ambiguity route, active transaction requirement, caller resolution-state exclusion, Frame.expr bypass and honest snapshot/replay limits without presenting missing future implementations as inherent limits. CI runs focused fast and explicit slow integration tiers; Tach reflects actual module edges without runtime import cycles.
- [ ] **C8: Commit and verify the committed deliverable.** Review staged diff for scope/accidental jargon (internal plan paths exempt), commit, record SHA and all hook-enqueued review IDs. Run clean-checkout smoke from this commit and build/install wheel into an isolated environment; run the same smoke outside source checkout with increment imports blocked. Both routes/facades must exist in the wheel. Reconcile every owned review/findings and repository-wide inventory before closing C or Task 4.2. Do not push/merge or claim whole-roadmap completion.

## Execution sequence and review receipts

1. Parent completes spec self-review and goal check, then global plan panel including the referenced companions. Address findings before production.
2. Parent starts C1/C2/C3 shared resolver prerequisite; A can implement disjoint execution files concurrently. Once resolver migration is present, B implements its independent compiler/time slice. A/B workers return changes and concerns only; parent integrates, runs each focused milestone, commits and handles reviews.
3. Parent completes C runtime/cost/public integration and end-to-end gates. All three slices get distinct durable child issues and review receipts (worktree/branch/base/tip/imported commits/job IDs/disposition).
4. Existing roadmap Tasks 4.4 onward resume from the durable parent after 4.2 closes; their acceptance is not silently dropped. Specialized/scientific reserved decisions remain separately gated.

## Self-review and decisions

Spec coverage: A owns authority/lifetime; B owns expression semantics; C owns actual resolver use, runtime, preview/budgets, conversions, documentation and clean/wheel proof. Shared metadata has no unowned migration. All five Review Focus cases have owning regression/smoke steps. Interfaces are spelled once above and reused; numerical column/anchor compilers do not import Frame/execution. The positive-count preview orchestrator in compile.cost has a distinct runtime architecture boundary. Stage commits may be dependency-ordered but no incomplete public support claim precedes C's smoke.

Choose the conservative metadata/binder construction rather than make an observed but undocumented bound-plan serializer a new required dependency. It preserves ordinary literal names and has a concrete exact qualified-SQL route for ambiguous partial references. Direct native probes establish that private connection settings must be reproduced and exact mixed timestamp comparisons must be range-aware. Explicit owned publication is adopted over a second borrowed-file authority variant. No user-only authority is assumed or waived.

## Global panel disposition

Panel 8394 returned four medium findings, all accepted after repository/native checks. Restore settings after Ibis wrapping and test a timezone-sensitive source value; validate readiness only for selected Design/view sources; retain existing text serialization functions without Design method aliases and migrate the master example; extend the exact timestamp axis through both prefix strategies. Native probes verified STRUCT ASOF ordering and complete-key MAX in DuckDB/Ibis, signed remainders, ±1ns and 1600/2500 keys. A TRY_CAST-to-NS prototype failed on a wide cutoff and is rejected. The common-key design and readiness integration changed, so request the one permitted re-review before implementation.

Re-review 8406 returned one high and one medium finding, both accepted. Cutoff-only carried-state completeness is replaced with matched-point-to-cutoff continuity or an explicit authoritative validity interval; B owns source/unit lower-gap regressions. A's bind_resolver now performs native view/function admission before initial ordinary Model schema binding, caches admitted relation expressions and prevents stale-view rebinding from invoking rejected functions; A/C own the behavioral counter regression. This addresses the identified gaps without adding public proof controls or weakening any bound-input check. Both panel jobs completed/closed; no passing-panel verdict is claimed. Do not run a third panel under the one-rereview policy; implementation and fresh task/commit reviews still must prove these requirements.
