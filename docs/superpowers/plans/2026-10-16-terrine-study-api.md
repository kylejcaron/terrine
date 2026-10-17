# Terrine Study API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Define and implement Terrine's typed, serializable study-design API, anchor/population compiler, and lazy `Frame` interface for complete and temporally correct analytical datasets.

**Architecture:** Keep expressions, designs, validation, YAML I/O, anchors, and frame metadata pure and independent of Ibis; have the compiler consume the bound model and temporal layer interfaces. A `Frame` carries an expression and column-level role/completeness metadata, while preview separates row/output estimates from scan-cost estimates and never executes an unbounded full materialization.

**Tech Stack:** Python, Pydantic models, PyYAML, Ibis expressions, Apache Arrow (required); Polars and Pandas as optional conversion extras; pytest.

**Spec:** `docs/superpowers/specs/2026-10-16-terrine-handoff.md` and `docs/superpowers/plans/2026-10-16-terrine-build.md`; coordinate interfaces with the temporal and estimation companion plans linked by the master plan.

## Global Constraints

- Primary binding: `Model.from_catalog(path_or_catalog, *, con, readiness: Mapping[str, Coverage], units: Mapping[str, UnitSource] | None = None, event_ids: Mapping[str, str] | None = None, input_binding: VerifiedInputBinding | None = None) -> Model`. `Model.from_increment(...)` is a lazy optional compatibility adapter (`terrine[increment]`), not a core dependency; native core imports and operation MUST work when increment is unavailable. The adapter maps to the same normalized descriptors. A `VerifiedInputBinding` is issued and revalidated by the bound Catalog/backend, never created as proof from caller-supplied strings: it pins backend identity and a complete mapping of every physical relation in the model's dependency closure (facts, dimensions, rosters, and SQL view/table dependencies), each to its actual immutable generation. For SQL whose dependency closure cannot be independently resolved and verified, the binding MUST pin the immutable materialized SQL result itself. No missing, extra, changed, or unverifiable relation is accepted. All relations resolve through that one binding; no sequential copies of mutable sources. A backend may instead provide one transaction snapshot that covers the same complete closure. If neither route is supported, execution MUST refuse before returning data.
- `Coverage(start, through)` declares complete local calendar days and stores date values only. `Model.from_catalog` binds it once through `Catalog.day_boundary` to `BoundCoverage(start: UTC timestamp, end_exclusive: UTC timestamp)`; Readiness stores bound intervals. Never infer readiness from observed maximum timestamps.
- `UnitSource(table, key, entered_at=None, exited_at=None, observed_from=None, observed_until=None)` supplies a real roster for `FixedDate`/`Schedule`. Entry/exit determine candidate membership at `t0` only; observation bounds independently delimit measured history and are carried as `_observed_from`/`_observed_until`. A null bound is unbounded only within explicitly declared source completeness. Without bounds, document the continuous-observation assumption; never substitute membership exit for observation end.
- `Design(unit, anchor, *, features: Mapping[str, Expr] | None = None, outcomes: Mapping[str, Expr] | None = None, eligibility: Mapping[str, Predicate] | None = None, treatment: Treatment | None = None, gap='0d', shape='cross_section', anchor_range: AnchorRange | None = None)`; normalize absent mappings to fresh immutable empty mappings. `AnchorRange(start: date | datetime | None = None, end: date | datetime | None = None)` is pure serializable and half-open; require at least one bound and `start < end` when both are present. Date bounds are converted with the bound Catalog's `day_boundary`; datetime bounds must be timezone-aware and preserve their exact instants after UTC normalization. Range filtering never changes first/nth selection over complete declared source history.
- Expressions/predicates are typed, serializable AST nodes, never arbitrary Python callables, SQL text, or pickle payloads.
- Time zero is UTC-aware and never floored; preserve the Catalog's declared fixed day-boundary offset. Elapsed duration units are integral `s`/`m`/`h`/`d`/`w` using Task 0.1's exact grammar; days/weeks are 24h/168h in that fixed offset. Windows are half-open `[start,end)` and additive prefixes use `P(x)=sum(ts<x)`.
- No hidden unit-by-day densification or anchor-by-event/window multiplication. Explicit bounded panel output is allowed when requested and costed. Arbitrary intraday boundaries use event-resolution state/lookup and report its higher cost; daily prefixes are only for day-aligned boundaries.
- No silent `metric.window_days` override. Reject incompatible windows unless the design explicitly uses a supported rewrite; do not imply this task implements one.
- No customer rows, connection secrets, or payload samples in hosted metadata or preview metadata.
- For gap `g >= 0`, feature and eligibility window ends MUST be `<= -g`; as-of cutoff MUST be `<= -g` and strict at equality. Outcomes start at or after `0d`; never shift explicit windows or outcomes to satisfy gap. Task 4.1 emits candidate anchors only. Task 4.2 applies the named eligibility conjunction: retain all true, exclude any known false, refuse if no known false and any result is unknown/null/incomplete; unavailable values remain unknown even for `IsNull`.
- All-treated event-anchor datasets are valid descriptive datasets. Causal estimator eligibility is a separate later validation and must not be imposed by design validation/build.
- Arrow is core; Polars/Pandas are optional extras. Preserve integer/decimal precision; do not blanket-convert to float or decimal(38,9).
- `Design` has no weight argument in this near-term contract; this plan does not add compiled target-weight support. `Roles.weight` and weight provenance remain optional for later producers; never invent sample/target weights.
- Tests are planned checks only; run the listed focused commands during implementation and report observed output, not assumed results.

## Review Focus

- Equal timestamps and duplicate event IDs can make repeated anchors nondeterministic: test stable-event-ID ordering and duplicate-ID refusal in Task 4.1.
- Strict cutoffs at `t0`, including intraday `t0`, are tested against compiled feature/outcome columns in Task 4.2; Task 4.1 tests anchor times only.
- Partial readiness can make one column complete while another is not: test independent per-column completeness and null reasons in Task 4.2.
- Serialization must remain data-only and stable: test AST round-trip/hash determinism and reject callable/unknown AST nodes in Task 3.1.
- Preview's output-size estimate is not a scan-byte estimate: test unknown scan costs remain unknown and budgets are enforced at materialization in Task 4.2.

Execution commands below run through `uv run` in the assigned worktree; install the DuckDB test extra before focused query tests. Literal pytest commands are selectors, not permission to use a different ambient environment. Each task also runs an actual local smoke of its public behavior and updates its owned guide/changelog before its commit.
---

## File and Interface Map

- `terrine/spec/expressions.py`: immutable pure-Pydantic expression and predicate AST; canonical expression encoding/hash.
- `terrine/spec/anchors.py`: pure anchor constructors and serialized fixed-date/event/schedule parameters, owned by Task 3.1.
- `terrine/spec/design.py`: `Design`, treatment representation, structural invariants, and serialization-facing public fields.
- `terrine/spec/validate.py`: static, metadata-only validation against model descriptors, coverage, and source semantics.
- `terrine/spec/io.py`: canonical JSON/YAML encoding, loading, version checks, and hash support.
- `terrine/compile/anchors.py`: roster/event anchor expansion and time-zero resolution.
- `terrine/compile/columns.py`: feature/outcome/eligibility/treatment expression compilation and metadata assembly.
- `terrine/compile/cost.py`: preview estimates and explicit unknown scan-cost representation.
- `terrine/frame.py`: lazy `Frame`, expression accessor, conversions, materialization and budget enforcement.
- `terrine/data.py`: shared `Coverage`, `UnitSource`, `Roles`, `ColumnMeta`, and `MaterializedFrame` data models.
- Tests under `tests/spec/`, `tests/compile/`, and `tests/test_frame.py`; names and assertions are specified by each task.

The task numbering below is retained from the master roadmap. Native `Model.from_catalog`, Catalog/semantic binding, coverage/readiness, and temporal operations are owned by Tasks 1.1/1.2; the optional `Model.from_increment` bridge is owned by Task 0.2. Consume those shared interfaces rather than defining a parallel model or coverage loader.

## Public API Example (near-term required)

This descriptive adopter example follows the master API's single source coverage and preserves named feature/outcome mappings. A useful preactivity predicate is separate from baseline treatment; the event anchor does not imply treatment assignment. `asof(before='0d')` is strict. The optional cohort range selects anchors after event-order selection, so early users retain their historical first event rather than receiving a replacement first event in range.

```python
import ibis
import terrine as tr

model = tr.Model.from_catalog(
    "semantic.yaml",
    con=ibis.duckdb.connect("product.duckdb"),
    readiness={
        "product_events": tr.Coverage(start="2025-01-01", through="2026-09-30"),
    },
)
design = tr.Design(
    unit="user",
    anchor=tr.EventAnchor("feature_enabled", which="first"),
    anchor_range=tr.AnchorRange(start="2025-03-01", end="2026-08-01"),
    features={
        "sessions_before": tr.agg("sessions", window=("-28d", "0d")),
        "revenue_before": tr.agg("revenue", window=("-28d", "0d")),
        "plan_before": tr.asof("plan_tier", before="0d"),
    },
    outcomes={
        "sessions_after": tr.agg("sessions", window=("0d", "28d")),
        "revenue_after": tr.agg("revenue", window=("0d", "56d")),
    },
    eligibility={
        "prior_activity": tr.gt(tr.agg("sessions", window=("-56d", "-28d")), 0),
    },
)
design.validate(model)  # Metadata-only; no data query.
frame = design.build(model)  # Lazy relation; t0 is not day-floored.
preview = tr.preview(design, model)  # No full count; scan bytes can be unknown.
```
The event at exactly `t0` is excluded from features and included in `[0d,28d)` outcomes. This is an adopter dataset, not an effect estimate; all-treated event anchors remain valid descriptive frames. FixedDate/Schedule additionally bind `units={"user": tr.UnitSource(table="users", key="user_id", entered_at="created_at", observed_from="observed_from", observed_until="observed_until")}` on Model; membership exit never supplies measurement coverage. Existing increment definitions may instead use `tr.Model.from_increment(...)` only with the optional `terrine[increment]` adapter. Public constructors are typed AST builders: `tr.agg`, `tr.asof`, `tr.gt`, and `tr.FixedDate`/`tr.EventAnchor`/`tr.Schedule`; no `FirstEvent` helper is introduced by this plan.

### Task 3.1: Serializable Expression AST and Canonical YAML

**Files:**
- Create: `terrine/spec/expressions.py`
- Create: `terrine/spec/anchors.py`
- Create: `terrine/spec/design.py`
- Create: `terrine/spec/io.py`
- Create: `terrine/spec/__init__.py`
- Test: `tests/spec/test_expressions.py`
- Test: `tests/spec/test_design_io.py`

**Interfaces:**
- Produces: `Expr = MetricAgg(metric: str, window: Window) | AttributeAsOf(attribute: str, before: int)`; `Predicate = Gt(expr: Expr, value: JSON scalar) | Eq(left: Expr, value: JSON scalar) | IsNull(expr: Expr, negate: bool=False) | And(terms: tuple[Predicate,...])`. `tr` exposes `agg`, `asof`, `gt`, `eq`, `is_null`, and `all_of`; `gt(expr, scalar) -> Gt` is a predicate (usable in `eligibility`), not a separate computable expression. Define `FixedDate(date)`, `EventAnchor(fact, which='first'|'each'|'nth', n=None)`, `Schedule(every, start, end)` (exclusive end), and `AnchorRange(start: date | datetime | None = None, end: date | datetime | None = None)` as pure serializable anchor types. `AnchorRange` is an optional `Design.anchor_range`, serialized as tagged data only; require at least one bound, `start < end` when both are present, and reject naive datetimes. Preserve date-versus-aware-datetime tags in canonical serialization because dates resolve to Catalog day boundaries while aware timestamps retain exact instants. Define immutable `Design` with the exact public signature in Global Constraints. `Treatment(attribute: str, at: str='0d')` is only an explicitly declared observed, known-at-`t0` binary baseline attribute; it never infers treatment from a future/event anchor. `Design.to_yaml(path)` and `Design.from_yaml(path) -> Design` use a versioned data-only schema. Canonical representation is sorted-key compact UTF-8 JSON over normalized AST values; `Design.spec_hash` is SHA-256 hex of those bytes, independent of mapping insertion order and YAML formatting. Keep spec hash distinct from model/source provenance hashes.

- [ ] **Step 1: Write failing AST and serialization tests**

Add `test_duration_ast_normalizes_to_seconds`, `test_anchor_yaml_round_trip_preserves_anchor_parameters`, `test_design_yaml_round_trip_preserves_named_feature_and_outcome_maps`, `test_spec_hash_ignores_mapping_order`, `test_canonical_serialization_sorts_mapping_keys`, `test_callable_and_unknown_node_are_rejected`, `test_yaml_refuses_duplicate_keys_and_unknown_schema_version`, `test_anchor_range_yaml_round_trip_preserves_date_and_instant_bounds`, `test_anchor_range_requires_ordered_finite_bounds`, and `test_anchor_range_rejects_naive_datetime`. Assert `tr.asof('plan_tier', before='0d')` serializes with zero-second strict cutoff; FixedDate, EventAnchor, Schedule, and AnchorRange round-trip as tagged values. Canonical serialization distinguishes Catalog-day date bounds from exact aware timestamp bounds, and the spec hash includes the range. The hash is over canonical spec bytes only, not a claim that model/source inputs are unchanged.

Include `test_subday_durations_have_equivalent_canonical_specs`: 60s/1m, 60m/1h and 24h/1d produce identical AST seconds/hashes. Apply the single Task 0.1 parser; reject fractional/compound/month units and overflow rather than introducing a second grammar.

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `pytest tests/spec/test_expressions.py tests/spec/test_design_io.py -q`
Expected: FAIL because the spec AST/serialization API is not implemented.

- [ ] **Step 3: Implement the pure AST and schema**

Implement immutable validated nodes, anchor constructors, and discriminated data-only serialization. Normalize durations using Task 0.1 `Window` rules (`s`/`m`/`h`/`d`/`w`, signed integral seconds, `start < end`); `asof` uses a signed strict cutoff before `t0`. Reject unsupported Python values/callables, unknown node tags, duplicate YAML keys, and extra fields. Use safe YAML loading; canonical JSON sorts every mapping key recursively and emits compact UTF-8 bytes before SHA-256 hashing. `Design.spec_hash` describes the design alone; materialized provenance separately carries design, model-definition, and immutable source-generation identity. Do not infer treatment from the anchor event. Grace initiation remains descriptive (e.g. event-defined cohort/predicate); it is not baseline treatment. Include schema version and pure import/export surface. Serialize `Design.anchor_range` with distinct canonical date/datetime bound tags; do not encode callables, query text, or a precomputed selection result.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/spec/test_expressions.py tests/spec/test_design_io.py -q`
Expected: PASS, including deterministic hash and all refusal cases.

- [ ] **Step 5: Commit the task**

```bash
git add terrine/spec tests/spec
git commit -m "feat: add serializable study expression specs"
```

### Task 3.2: Metadata-Only Static Temporal and Semantic Validation

**Files:**
- Create: `terrine/spec/validate.py`
- Modify: `terrine/spec/design.py`
- Test: `tests/spec/test_validate.py`

**Interfaces:**
- Consumes: `Design`, `Expr`, `Predicate`, `Treatment`, `Model` bound descriptors, `Coverage(start, through)`, immutable semantic descriptors from `terrine.semantic`, and temporal capability metadata from Task 1. No query execution.
- Produces: `Design.validate(model) -> None` (or typed `TerrineError` refusal with stable namespaced code); internal `validate_design(design: Design, model: Model) -> ValidationReport` for static readiness/required-source summaries, with no customer values. Validation does not inspect row counts, observed maxima, or execute SQL. Static validation checks range shape only (at least one bound, ordered bounds, valid date/aware-datetime types); it neither queries data nor estimates event-history completeness. Date-to-instant resolution uses the model's bound Catalog day boundary during anchor compilation.

- [ ] **Step 1: Write failing static validation tests**

Task 3.2 consumes Task 1.1's pure `semantic/rules.py` metric-domain/refusal contract; it never imports `temporal/aggregate.py`. Keep the existing 1.1 prerequisite, not a new 1.3 dependency. Add a subprocess validation smoke with Ibis, temporal and compiler imports blocked, including statically unaligned calendar averages; lazy package exports must not defeat the boundary.

Add tests `test_missing_coverage_declaration_refuses_referenced_source`, `test_window_extending_beyond_declared_coverage_builds_with_runtime_incompleteness`, `test_invalid_or_empty_window_refuses`, `test_feature_and_eligibility_must_end_by_minus_gap`, `test_asof_cutoff_must_be_at_or_before_minus_gap_and_strict_at_boundary`, `test_outcome_window_must_start_at_or_after_zero`, `test_metric_window_days_conflict_is_not_overridden`, `test_named_dimension_ambiguity_refuses`, `test_intraday_anchor_requires_event_resolution_capability`, and `test_validation_does_not_execute_query`. Consume already-bound UTC BoundCoverage; do not normalize the date-only Coverage again in validation. Missing readiness refuses; insufficient declared coverage preserves runtime null/reason. Gap `2d` requires feature/eligibility right edges and strict as-of cutoffs `<= -2d`; outcomes still start `>= 0d`.

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `pytest tests/spec/test_validate.py -q`
Expected: FAIL because static validator is absent/incomplete.

- [ ] **Step 3: Implement metadata-only validation**

Resolve metric/source/dimension names against normalized native descriptors; reject unresolved/ambiguous references, unsupported aggregations, invalid half-open intervals and missing referenced-source readiness. For gap `g >= 0`, feature/eligibility window ends and strict as-of cutoffs must be `<= -g`; outcome starts must be `>= 0d`. Insufficient bound coverage preserves the expression and adds runtime null/reason, not a static refusal. Refuse conflicts with declared `metric.window_days` rather than silently changing either value. Keep validation metadata-only: no Ibis compile, query or materialization. Use module-owned immutable RefusalSpec declarations with stable `terrine.*` codes.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/spec/test_validate.py -q`
Expected: PASS; a spy/raising connection confirms validation makes zero queries.

- [ ] **Step 5: Commit the task**

```bash
git add terrine/spec tests/spec
git commit -m "feat: validate study specs against model metadata"
```

### Task 4.1: Candidate Anchor and Roster Compilation

**Files:**
- Create: `terrine/compile/anchors.py`
- Create: `terrine/compile/__init__.py`
- Test: `tests/compile/test_anchors.py`

Keep the first compiler concrete: do not create a placeholder `shapes.py` that
would later be shadowed by Task 5.1's real `shapes/` package. Candidate construction
is lazy; defer data-dependent integrity checks through an explicit
`AnchorTable.validate(*, execute)` callable under Task 4.2's pinned executor.

**Interfaces:**
- Consumes: `FixedDate(date)`, `EventAnchor(fact, which='first'|'each'|'nth', n=None)`, `Schedule(every, start, end)` (end exclusive), optional `Design.anchor_range`, `UnitSource(table, key, entered_at=None, exited_at=None, observed_from=None, observed_until=None)`, `event_ids: Mapping[str,str]`, and temporal fact/roster binding from `Model` and Task 1 outputs.
- Produces: `compile_anchors(design: Design, model: Model) -> AnchorTable` of candidate anchors, one row per `(unit, anchor_id)` with UTC-aware exact `t0`, `_observed_from`, `_observed_until`, and explicit anchor provenance. Expand and select against complete declared source history, then apply `anchor_range` to selected exact UTC `t0` values; only range-retained anchors reach Task 4.2 eligibility. `first` selects earliest timestamp and `nth` selects one-based `n` over complete declared history, ordered by timestamp and configured stable event ID; range filtering MUST NOT cause a later event to replace an out-of-range first/nth event. Date range bounds resolve with `Catalog.day_boundary`; aware timestamp bounds are exact UTC instants. Entry/exit affect candidate membership at `t0` only. `shape='cross_section'` defines row grain `(unit, anchor_id)` and may repeat units for descriptive anchors; it does not imply one row per unit. Causal/repeated-measures checks are separate downstream gates.

- [ ] **Step 1: Write failing candidate anchor and population tests**

Add `test_fixed_date_uses_declared_roster_not_observed_events`, `test_schedule_end_is_exclusive_and_roster_lifetime_is_half_open`, `test_event_first_is_exact_timestamp_not_day_bucket`, `test_each_tied_events_use_configured_stable_id`, `test_duplicate_event_id_refuses`, `test_each_without_ties_retains_all_anchors`, `test_anchor_rows_carry_observation_bounds`, `test_event_first_range_keeps_original_first_outside_range`, `test_event_nth_range_selects_history_nth_before_filtering`, and `test_event_each_range_filters_each_selected_event`. Assert exact UTC half-open bounds, identity selection before filtering, date-bound conversion using the Catalog day boundary, candidate population, roster membership at `t0`, observation-bound propagation, and that eligibility receives only range-retained anchors. Do not assert feature, eligibility, or outcome values here; those belong to Task 4.2 after temporal dependencies exist. Duplicate configured event IDs refuse rather than deduplicate or pick a winner. Do not manufacture rows by unit×day densification. Pure anchor classes and round-trip tests belong to Task 3.1.

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `pytest tests/compile/test_anchors.py -q`
Expected: FAIL because candidate anchor expansion is not implemented.

- [ ] **Step 3: Implement candidate anchor compiler**

Compile validated serializable anchors against the configured roster or fact source. FixedDate and Schedule require a `UnitSource`; apply entered/exited membership as `[entered_at, exited_at)` at each `t0`, with unknown lifetime explicitly fixed-population semantics. Separately propagate optional observation columns as `_observed_from` and `_observed_until`; an event anchor may use an optional roster to provide bounds, otherwise uses only the documented continuous-observation assumption within declared source completeness. Schedule emits elapsed intervals from `start` through exclusive `end`. EventAnchor `first` selects earliest timestamp; `each` preserves all events; `nth` selects one-based `n` and requires positive `n`. Expand declared complete source history and determine first/nth/each selection before range filtering. Filter the selected rows by `start <= t0 < end`, resolving date bounds with `Catalog.day_boundary` and timestamp bounds as exact UTC instants. Apply roster membership at `t0` as specified, then make only range-retained anchors candidates for Task 4.2 eligibility. An out-of-range first or nth event is discarded, never replaced by a later in-range event. The range bounds cohort anchors only; they do not trim feature/outcome history or alter source readiness. Do not floor UTC `t0`; order ties only by configured stable event ID and refuse duplicate IDs within relevant fact/unit scope. This task emits candidate anchors only: do not compile eligibility or filter candidates based on predicates. Never derive `Treatment` from the anchor event.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/compile/test_anchors.py -q`
Expected: PASS; fixture verifies candidate population, observation-bound propagation, no densification, and duplicate-ID refusal.

- [ ] **Step 5: Commit the task**

```bash
git add terrine/compile tests/compile/test_anchors.py
git commit -m "feat: compile study anchors and populations"
```


### Task 4.2: Lazy Frame, Eligibility, Column Metadata, Conversion, and Preview

**Files:**
- Create: `terrine/compile/columns.py`
- Create: `terrine/compile/cost.py`
- Create: `terrine/frame.py`
- Create: `terrine/execution/inputs.py` (runtime immutable-input discovery/verification; Catalog uses a lazy facade)
- Create: `terrine/execution/__init__.py`
- Modify: `terrine/model.py`, `terrine/semantic/models.py`, `terrine/spec/design.py`, `terrine/__init__.py` (native Model/Catalog/build facades and public exports)
- Conditional integration: `terrine/adapters/increment.py` only if Task 0.2 already exists; otherwise its owner must consume the completed native input-binding facade before claiming adapter support. Core 4.2 never waits for this optional file.
- Modify: `terrine/data.py`
- Test: `tests/compile/test_columns.py`
- Test: `tests/test_frame.py`
- Update: `README.md`
- Test: `tests/integration/study_api_smoke.py`

- Consumes: `Design.build(model) -> Frame`, candidate `AnchorTable`, temporal expression outputs, `Coverage`/`BoundCoverage`, and shared `ColumnMeta`/`MaterializedFrame`.
- Produces these shared contracts (integrated by Task 0.1, implemented by this task's compiler):
  - `Roles(unit: str, anchor: str, t0: str, treatment: str | None, features: tuple[str,...], outcomes: tuple[str,...], weight: str | None = None, cluster_unit: str | None = None)`.
  - `ColumnMeta(window, role, observed_from_column, observed_until_column, complete_column, reason_column, numeric_policy: Literal["exact","checked_float64_v1"] | None = None, absolute_error_column: str | None = None)`. For checked Float64 values, the error-column reference identifies a finite nonnegative per-row absolute error bound in the reserved compiler namespace; exact numeric outputs have no bound column. Temporal Tasks 1.2–1.3 own the numerical contract. Non-numeric columns use neither field. Preserve these through export/import, alongside per-column completeness; no Design weights API is introduced.
  - `FeatureCutoff(latest_seconds: int, strict: bool, knowledge: Literal["recorded_at","retrospective_effective_time"])`.
  - `ExecutionSnapshot(snapshot_id: str | None, backend: str, source_generations: Mapping[str, str | None], input_binding_id: str | None, replay_equivalence: Literal["verified","unverified"])` records the immutable execution view, never live handles or credentials. A transaction may lack portable generation IDs (None); that is consistent execution, not promised replay. Without transactions, require the explicit backend-verified immutable binding described below for all facts/dimensions/rosters/SQL dependencies. Record its stable receipt ID and verified generations, not the runtime token. Revalidate before probes/export; unknown/mutable/missing/stale inputs refuse. Neither serialized receipts nor caller-supplied strings confer execution authority.
  - `EligibilitySummary(candidate: int, excluded_false: int, unknown: int, retained: int)` is a typed audit record; all counts are nonnegative and `candidate == excluded_false + unknown + retained`. Producer materialization refuses unresolved unknowns. The value may preserve unknown counts for loaded/manual frames so downstream causal consumers can refuse with `terrine.estimate.unsupported_design` and retain refusal context; causal use requires `unknown == 0` and `retained` equal to the exported row count. It is optional only when no eligibility predicates exist.
  - `Provenance(feature_cutoffs: Mapping[str,FeatureCutoff], eligibility_cutoffs: Mapping[str,FeatureCutoff], eligibility_summary: EligibilitySummary | None, treatment_knowledge: Literal["recorded_at","retrospective_effective_time"] | None, weight_kind: Literal["baseline_function","sampled_control"] | None, weight_cutoff: FeatureCutoff | None, weight_depends_on_treatment: bool | None, design_hash: str, model_hash: str, source_generations: Mapping[str,str | None], execution_snapshot: ExecutionSnapshot, shape: str, treatment_design: Literal["baseline_binary","grace_initiation"] | None, treatment_known_at_t0: bool = False)`. Record every feature/eligibility cutoff including as-ofs. Preserve model hash, source generations, snapshot, and design hash as distinct provenance; unknown IDs are `None`, never fabricated. Weight metadata is typed and optional for later/manual producers only; `Design` has no weight field and this compiler never creates weights.
  - A safe baseline cutoff is `<0`, or `==0` with strict exclusion. `baseline_binary` treatment needs an explicit known-at-t0 declaration; retrospective effective-time history is labeled honestly. Causal estimator eligibility remains a later distinct gate; descriptive validation/build do not impose causal-treatment restrictions. Weight provenance may be supplied by later producers but is not a `Design` field and never defaults to causal safety.
  - `Budget(max_rows: int | None = None, max_output_bytes: int | None = None, max_scan_bytes: int | None = None, strict_scan: bool = True)`.
  - `Frame.expr`; `Frame.to_pyarrow(*, budget: Budget | None = None) -> pyarrow.Table`, analogous conversions; `Frame.materialize(budget: Budget | None = None) -> MaterializedFrame`.
  - `MaterializedFrame(table: pyarrow.Table, roles: Roles, columns: Mapping[str,ColumnMeta], provenance: Provenance)` is the shared materialized data contract; recursively immutable metadata, Arrow numeric/null preservation. `Frame.expr` is a low-level inspection/composition escape hatch: executing it directly bypasses terrine's snapshot/integrity/budget gates and is not a verified export. Safe application/agent paths use Frame materialization/staging APIs.
  - `CoverageSummary(start: datetime, end_exclusive: datetime)`; `Preview(planned_rows: int | None, output_bytes: int | None, scan_bytes: int | None, scan_reason: str | None, coverage: Mapping[str,CoverageSummary], strategy: Mapping[str,str], budget_warnings: tuple[str,...], candidate_rows: int | None = None, known_false_rows: int | None = None, unknown_rows: int | None = None, eligible_rows: int | None = None, counted_rows: int | None = None, count_capped: bool = False)`.
  - `tr.preview(design, model, budget: Budget | None = None, *, bounded_count: int = 0) -> Preview`: no query by default. A positive bounded count is an explicitly requested capped query, not an exact total or guaranteed scan bound; record `count_capped` and apply scan budget before it. Eligibility counts remain unknown unless explicitly computed; preview distinguishes candidate/known-false/unknown/eligible counts without exposing rows.

- [ ] **Step 1: Write failing frame, eligibility, completeness, snapshot, conversion, and cost tests**

Add `test_output_names_share_one_namespace`, `test_reserved_compiler_names_refuse_for_all_output_roles`, `test_treatment_output_name_is_reserved`, and `test_quoted_or_sql_looking_output_labels_are_identifiers_not_sql`. Feature, outcome, named eligibility, treatment, and role columns share one exact-string namespace; duplicates refuse before SQL compilation with stable `terrine.spec.output_name_conflict`. Reserve role labels `unit`, `anchor_id`, and `t0`, treatment label `treatment`, internal prefix `__terrine_`, and observation metadata labels `_observed_from`/`_observed_until`; user outputs with these names refuse with `terrine.spec.reserved_output_name`. Role and treatment names remain stable in `Roles`, serialization, preview, and materialized output. Labels are opaque identifiers: use backend-safe identifier quoting/escaping, never interpolate labels into SQL. Test supported labels with spaces/quotes and SQL-looking text as literal column names, with no expression execution or injection.

Portability rule: compare names after Unicode NFC normalization and case folding for duplicates/reservations, while preserving original labels in output. Also reject any additional collision under the executing backend's identifier rules. Tests include `revenue`/`Revenue`, `Treatment` and mixed-case reserved prefixes; no implicit renaming. Internal alias allocation uses the reserved prefix even for source properties that resemble public role names.

Test immutable input binding contracts: exact complete physical closure acceptance; missing, extra, stale, mutable, changed, and backend-mismatched relation refusal; actual identity/generation verification rather than caller-supplied snapshot IDs or strings; unresolved SQL closure accepted only when its immutable materialized result is pinned; and one binding shared by probes, eligibility, and export. A transaction snapshot covering the complete closure is the alternate accepted route; absent either capability, execution refuses before returning data.

Add `test_named_columns_keep_role_and_window_metadata`, `test_asof_feature_has_explicit_strict_cutoff_provenance`, `test_retrospective_history_is_not_mislabeled_recorded_at`, `test_provenance_tracks_design_model_and_each_source_generation`, `test_each_source_has_independent_observed_until_and_completeness`, `test_observation_end_makes_crossing_window_incomplete`, `test_empty_observed_additive_window_is_zero`, `test_candidate_anchor_and_eligibility_conjunction`, `test_eligibility_known_false_excludes_candidate`, `test_eligibility_unknown_refuses_materialization`, `test_is_null_only_tests_known_observed_null`, `test_eligibility_unknown_with_known_false_is_excluded`, `test_eligibility_summary_is_required_for_causal_import`, `test_snapshot_shared_by_integrity_probe_eligibility_and_export`, `test_mutation_between_probe_and_export_uses_snapshot_or_refuses`, `test_row_budget_aborts_on_limit_plus_one_without_truncation`, `test_strict_scan_budget_refuses_unknown_cost`, `test_preview_does_not_run_count_by_default`, `test_bounded_preview_count_is_capped`, `test_preview_does_not_treat_output_size_as_scan_bytes`, `test_unknown_scan_cost_stays_unknown`, `test_arrow_conversion_is_core`, `test_optional_conversions_refuse_without_extra`, and `test_preview_metadata_contains_no_customer_payload`. Assert named expressions preserve role/window/cutoff and per-column source completeness; crossing `_observed_until` yields null with `incomplete_observation`, while roster exit affects candidate membership only. Evaluate named eligibility predicates as a conjunction before expensive outcome joins when safe: all true retains; any known false excludes; absent known false with any unknown/null/incomplete refuses materialization with stable code `terrine.study.eligibility_unknown`, never silently drops unknown through SQL `WHERE`. `IsNull` tests only a known observed null, not unavailable data. Counts/refusal context distinguish candidate, false, unknown, retained; preview exposes no rows. Complete empty additive `sum`/`count` yields zero; event count and nonnull count remain distinct; output rows equal retained anchors without unit×day/event-window expansion.

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `pytest tests/compile/test_columns.py tests/test_frame.py -q`
Expected: FAIL because lazy frame compilation, eligibility, conversions, and preview are not implemented.

- [ ] **Step 3: Implement column compiler, input verification and lazy Frame**

Compile each named feature, outcome, eligibility, and treatment expression once against candidate anchors and temporal interfaces, preserving names and attaching role/window/observation-bound/completeness/reason references. Evaluate eligibility predicates as an explicit conjunction before expensive outcome joins when safe: retain all-true candidates, exclude any known-false candidate, and refuse materialization when no predicate is false but any is unknown/null/incomplete. Never rely on SQL `WHERE` to discard unknowns; `IsNull` tests only known observed null values. Keep candidate/false/unknown/retained counts distinct without exposing rows. Add per-expression `feature_cutoffs` and `eligibility_cutoffs`, including as-of attributes; record latest relative time, strictness, and recorded-at versus retrospective knowledge. Carry model hash, per-source immutable generations, and `ExecutionSnapshot` separately from design hash. Begin one transaction/snapshot before exact integrity probes and use the same immutable view for probes, eligibility, and final export; if unavailable, require an immutable input binding or refuse before returning output. Bind all fact, dimension, and roster relations to it. Observation bounds intersect source coverage; a window crossing `_observed_until` is incomplete/null with `incomplete_observation`, never inferred from roster exit. Maintain one completeness state per expression/source and event-count versus nonnull-count metadata. Incomplete interval -> null and reason; observed empty additive interval -> zero; empty mean/min/max or zero ratio denominator -> null and reason. Keep exact integer/decimal dtypes. `Frame` remains lazy; Arrow is core, optional DataFrame methods import extras only when called. `materialize` is the explicit execution boundary and checks row/output budgets before returning `MaterializedFrame`.
Validate all output labels with one namespace/reserved-name policy before SQL compilation, using the stable refusal codes and identifier handling defined in Task 4.2 Step 1.

Eligibility comparisons over `checked_float64_v1` use certified intervals `[value-error,value+error]`, rounded outward, not just point estimates. `Gt(x,c)` is true only if the lower bound exceeds c, false only if the upper bound is <= c; otherwise unknown. `Eq(x,c)` is true only for a certified singleton equal to c, false when c is outside the interval, otherwise unknown. Exact counts/decimals retain ordinary comparisons. Test tiny values near a threshold with nonzero certificates; uncertainty follows the existing eligibility-unknown refusal rather than silently selecting a different cohort. IsNull distinguishes known null values from unavailable/uncertifiable expressions as before.

- [ ] **Step 4: Implement bounded preview and cost report**

Implement metadata/compile-only preview by default. An explicitly requested bounded count applies scan budget first, returns `count_capped`, and never claims an exact total when capped. Output size is not scan size. Materialization/conversions use one capped query/stream (`LIMIT max_rows+1` when configured); exceeding `max_rows` aborts without truncated success. Do not guard then reexecute an uncapped mutable query. Enforce `max_output_bytes` while consuming bounded Arrow batches; abort and discard partial results if cumulative bytes exceed the cap. Limits bound returned rows/buffers, not upstream scan/work. Strict scan budget with unknown scan cost refuses before any validation/count/export query. Exact integrity probes use full-source EXISTS/count, never sampled subsets, and charge their scan to the same budget. Use the same transaction snapshot or verified input binding for exact probes, eligibility and export. Import the execution-owned refusal declarations: absent snapshot capability is `terrine.execution.snapshot_unavailable`; an incomplete, stale, unverifiable or mismatched supplied binding is `terrine.execution.invalid_input_binding`. Assert those same codes at binding verification and pre-export revalidation. Do not accept caller strings/hashes or sequential mutable copies as proof. Unresolved SQL dependencies require a verified immutable materialized result. Preview eligibility counts stay unknown unless explicitly counted.

- [ ] **Step 5: Run focused tests and local runtime smoke**

Run: `pytest tests/compile/test_columns.py tests/test_frame.py -q`
Expected: PASS; optional extras are isolated, metadata is per-column, unknown scan cost remains unknown, and row limits abort rather than truncate.

Run: `uv run python -m tests.integration.study_api_smoke`
Expected: a local DuckDB-backed fixture constructs native `Model.from_catalog` using Catalog/YAML and common-stack dependencies, validates/builds the public example, materializes Arrow under explicit `Budget`, and asserts named feature/outcome values, intraday cutoff, eligibility accounting, observation completeness, and snapshot provenance. It uses fixture data only, makes no network/warehouse calls, and passes with increment imports blocked. Optional adapter behavior belongs to adapter coverage.

- [ ] **Step 6: Update API documentation and changelog**

Update `README.md` with the native public example and conversion/preview/budget behavior; add the user-visible API entry to `CHANGELOG.md`. Document that preview does not count rows by default, strict scan budgets refuse unknown cost, and hosted metadata contains no customer payload.

- [ ] **Step 7: Commit the task**

```bash
git add terrine/compile/columns.py terrine/compile/cost.py terrine/frame.py terrine/data.py terrine/execution/__init__.py terrine/execution/inputs.py terrine/model.py terrine/semantic/models.py terrine/spec/design.py terrine/__init__.py tests/compile/test_columns.py tests/test_frame.py tests/integration/study_api_smoke.py README.md CHANGELOG.md
git commit -m "feat: compile lazy study frames and previews"
```

Include any actual optional-adapter changes in the same commit when that adapter exists; do not require a nonexistent optional file. Verify this commit from a clean checkout with `uv run python -m tests.integration.study_api_smoke`, exercising both transaction and backend-verified immutable-binding routes. The installed-wheel smoke must also resolve the native Model/Catalog/input-verification facades; an uncommitted execution module cannot satisfy this gate.

## Signature Gaps and Explicit Resolutions

This plan resolves predicates as `Gt`/`Eq`/`IsNull`/`And`, explicit baseline treatment as `Treatment(attribute, at='0d')`, and grace initiation as descriptive-only. Anchors are defined/serialized in Task 3.1; Task 4.1 compiles candidate anchors only; Task 4.2 owns eligibility and actual feature/outcome assertions. `Budget`, `Preview`, `FeatureCutoff`, `ExecutionSnapshot`, `EligibilitySummary`, and `Provenance` are pinned here. Materialized provenance keeps design hash, model hash, source generations, and execution snapshot distinct. `cross_section` denotes `(unit, anchor_id)` row grain, not unit uniqueness; causal inference requiring unique units refuses repeated anchors. Native `Model.from_catalog` and Catalog-bound Coverage are primary; `Model.from_increment` is optional compatibility only.
Shared additions for the companion contracts: `AnchorRange(start: date | datetime | None = None, end: date | datetime | None = None)` is a pure serializable value; `Design(..., anchor_range: AnchorRange | None = None)` selects the cohort after complete-history first/nth selection and before eligibility. Date bounds bind through the Catalog's fixed day boundary; aware timestamps preserve exact UTC instants. Output labels across features, outcomes, eligibility, treatment, and roles use one namespace, with stable refusals `terrine.spec.output_name_conflict` and `terrine.spec.reserved_output_name`; compiler names are reserved and labels are always identifiers, never SQL fragments.

`Model.from_catalog(..., input_binding: VerifiedInputBinding | None = None)` accepts a verified runtime binding, not a serialized receipt. Task 4.2 adds this optional capability to Model and the optional increment facade without making it required for normal transaction-backed use.

`Catalog.required_relations(con, *, units: Mapping[str, UnitSource] | None = None) -> tuple[PhysicalRelationId,...]` explicitly discovers the complete source/dimension/roster/SQL closure. `Catalog.verify_immutable_inputs(con, *, units=None, relations: Mapping[PhysicalRelationId,RelationHandle]) -> VerifiedInputBinding` verifies that exact closure. These are explicit backend-metadata operations in `execution/inputs.py`, not hidden data scans during Catalog load or Design validation. UnitSource bindings MUST match those later supplied to Model.

`PhysicalRelationId` is the backend-canonical qualified relation identity, or a tagged logical SQL-result identity including its definition hash. `RelationHandle` is a backend-issued pinned version/time-travel/snapshot reference or runtime-issued immutable-generation handle whose publication can be verified; a mutable table merely named “snapshot” is not one. The provider verifies backend identity, existence, immutable/version semantics, schema, source-definition hash and complete binding membership. Source SQL either resolves every dependency at the pinned versions without changing query meaning, or is replaced by its verified immutable materialized result. Unresolvable closure without such a result refuses; no arbitrary SQL rewriting or sequential mutable copies.

The opaque runtime binding stores catalog/roster hashes and backend identity, remains model-side and cannot be rebuilt from YAML/JSON. Execution revalidates it, pins its references through all checks/export, and verifies it belongs to the model/connection. Frame provenance serializes only its safe receipt ID and source generations; loading that provenance cannot mint a verified binding. Agent tools accept owner-configured binding references, never arbitrary handles. `terrine.execution.snapshot_unavailable` covers absent snapshot capability; `terrine.execution.invalid_input_binding` covers unverifiable/stale/mismatched supplied bindings. Test both routes with transaction snapshots disabled and with changes between verification and execution.

## Later dataset-shape and view amendments

[Dataset workflows](2026-10-16-terrine-dataset-workflows.md) owns Task 6.2's StatePanel shape/source union and structural prior-state/ordinal/baseline-reference metadata, plus Task 7.2's immutable training/scoring view metadata and facade signatures. These are additions at those milestones, not claims that this near-term Expr union already supports them. Integrate their pure shared metadata through the Task 0.1 owner; migrate serialization, validation, preview, exports and consumers together. Generic Expr/Predicate variants remain unchanged. No MOST estimator or new Property.as_of mode is introduced.
