# Table-First Binding and Dataset Constructors Implementation Plan

> Implementation checklist: complete tasks in order; each step uses checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add table-first `Model.from_tables` binding and `terrine.recipes` dataset constructors that compile to ordinary `Design` objects.

**Architecture:** `Model.from_tables` translates typed table declarations into the existing native Catalog and binds through `Model.from_catalog`. Pure constructors in `terrine.recipes` assemble existing anchors, expressions, shapes, and `Design`; they do not execute queries or add a parallel representation.

**Tech Stack:** Python 3.13, dataclasses, Ibis, Pydantic Catalog records, pytest, DuckDB integration examples.

**Spec:** `docs/superpowers/specs/2026-10-26-table-first-binding-and-recipes-design.md`

## Global Constraints

- Keep package version `0.1.0.dev0`.
- Keep `Design`, anchors, expression functions, `shape=`, `Sequential`, and `EventPanel` public and unchanged.
- `Model.from_tables` requires explicit `units` roster and event completeness; never infer either from event data.
- Constructors return plain `Design` and require row-defining choices; only convention defaults such as first occurrence are allowed.
- New refusals use stable package-wide codes, immutable structured context, and survive pickle/deepcopy.
- Preserve null versus zero and per-cell null reasons through Arrow, manifests, and exports.
- Run focused tests in the cheapest tier and `make check TESTS="<affected paths>"` before each commit.
- Update production callers, exports, docstrings, guides, runnable examples, refusal docs, tests, and CHANGELOG in the same change; use clean cutover with no aliases.
- Warehouse behavior must use Ibis expressions; do not add raw SQL or UDF bindings to compiler/temporal code.

## Review Focus

1. A declared unit with no matching event remains in generated output; complete empty count/sum equal `0`, event mean is null with reason `no_observations`, and an incomplete window is null with its existing incompleteness reason. Test in Task 2's integration composition.
2. Missing `Events.complete` refuses rather than inferring a maximum event time; test code `terrine.model.missing_event_completeness` in Task 1.
3. Generated metric-name collisions between tables/value columns (and any explicit catalog names supported by the implementation) refuse with `terrine.model.generated_metric_collision`; test in Task 1.
4. Recipes do not default row-defining unit, event/start event, period, or horizon; event occurrence defaults to first and nth occurrence requires positive `n`. Test required arguments and occurrence contract in Task 3.
5. Each constructor preserves existing low-level shape and returns `Design`, while conditional events remain expressible with `EventAnchor` plus `relative_to=event_time(...)`; test construction and equivalence in Task 3 and runnable examples in Task 4.

---

### Task 1: Add typed event-table descriptors and table-first binding

**Files:**
- Modify: `terrine/data.py` (`__all__`, beside `UnitSource`/`Coverage`)
- Modify: `terrine/model.py` (`__all__`, beside `from_catalog`)
- Modify: `docs/reference/refusals.md`
- Test: `tests/test_semantic_binding.py`

**Interfaces:**
- Consumes: `UnitSource`, `Coverage`, `Catalog`, `Model.from_catalog`, `readiness`, and `event_ids`.
- Produces: exported immutable `Events(table: str, time: str, keys: Mapping[str, str], values: tuple[str, ...] = (), complete: tuple[date | str, date | str] | None = None)`; `Model.from_tables(con, *, units: Mapping[str, UnitSource], events: Mapping[str, Events], attributes: Mapping[str, UnitSource] | None = None, dialect: str | None = None, day_boundary: str = "UTC", readiness: Mapping[str, Coverage] | None = None, event_ids: Mapping[str, str] | None = None) -> Model`; refusal codes `terrine.model.missing_event_completeness` and `terrine.model.generated_metric_collision`.
- Prerequisite: the separate mapping-input change to `Model.from_catalog` must be available before Task 1, because `from_tables` calls it with the generated Catalog and a plain units mapping; if it is not merged into this task branch, the implementer must use its exact merged API rather than add a local shim.

- [ ] **Step 1: Write failing descriptor and admission tests**

Add tests asserting `Events("orders", "occurred_at", {"customer": "customer_id"}, ("amount",), ("2025-01-01", "2025-01-31"))` preserves the table/time/keys/values/completeness values and rejects malformed required fields using the established typed-record conventions. Add a `Model.from_tables` test using a DuckDB connection with explicit units and an event table; assert `model.catalog.metrics` contains `orders_count`, `orders_amount_sum`, and `orders_amount_mean`, and that `model.units` contains the supplied roster unit.

- [ ] **Step 2: Run focused tests to verify failure**

Run: `uv run pytest -q tests/test_semantic_binding.py -k 'events_descriptor or from_tables'`
Expected: FAIL because `Events` and `Model.from_tables` are not implemented; do not accept collection-only success.

- [ ] **Step 3: Implement `Events` and `Model.from_tables`**

Add the frozen typed event descriptor in `terrine.data`. Generate current `Catalog` source/fact/measure/metric records for every event declaration; map values to sum/mean metrics and count to the event count metric. Require completeness per descriptor and explicit `units`; verify required event keys name declared units. Detect generated metric collisions before binding. Refuse missing completeness with `terrine.model.missing_event_completeness`, and collisions with `terrine.model.generated_metric_collision`; include immutable context naming the missing source or colliding metric and associated source names. Delegate final binding to `Model.from_catalog`, forwarding a plain `dict(units)` and the connection and binding options. Do not query event rows.

- [ ] **Step 4: Add failure-contract tests and run the unit tests**

Add tests asserting missing completeness raises `TerrineError` with `.code == "terrine.model.missing_event_completeness"`, collisions across generated names raise `.code == "terrine.model.generated_metric_collision"`, and each error preserves its code and context through both `pickle.loads(pickle.dumps(error))` and `copy.deepcopy(error)`. Run: `uv run pytest -q tests/test_semantic_binding.py -k 'events_descriptor or from_tables'`. Expected: PASS.

- [ ] **Step 5: Document refusal codes and commit**

Add both codes, their trigger, and route forward to `docs/reference/refusals.md`. Run `make check TESTS="tests/test_semantic_binding.py"`. Stage only Task 1 files and commit with `feat: add table-first model binding`.

### Task 2: Prove aggregation parity with native Catalog behavior

**Files:**
- Test: `integration/test_model_from_tables.py` (new, or the existing native Model integration module if that is its established location)

**Interfaces:**
- Consumes: Task 1 `Events` and `Model.from_tables` signatures.
- Produces: end-to-end evidence of exact row parity for generated count, sum, and mean metrics against an equivalent hand-written Catalog.

- [ ] **Step 1: Write the failing composition test**

Create a tiny DuckDB fixture with a roster containing `u1` and zero-activity `u2`, event rows only for `u1`, and distinct complete/incomplete coverage windows. Bind once through `Model.from_tables` and once through an equivalent explicit `Catalog`; build matching designs using `agg("orders_count", ...)`, `agg("orders_amount_sum", ...)`, and `agg("orders_amount_mean", ...)`. Assert the collected rows from both models are equal. Also assert the complete empty window for `u2` has count `0`, sum `0`, mean `None`, and mean reason `"no_observations"`; assert every incomplete-window aggregate is null with its existing incompleteness reason.

- [ ] **Step 2: Run the integration test to verify failure**

Run: `uv run pytest -q integration/test_model_from_tables.py`
Expected: FAIL on missing `Model.from_tables` or a specific value/reason mismatch, rather than skip or collection failure.

- [ ] **Step 3: Correct aggregation mapping only as needed**

Adjust generated Catalog metric/fact/measure declarations in `terrine/model.py` so generated metrics resolve to the existing aggregate implementation and match the reference Catalog exactly. Do not change aggregate semantics to make the test pass; `terrine/temporal/aggregate.py` owns the established empty-window contract.

- [ ] **Step 4: Run the composition test and commit**

Run: `uv run pytest -q integration/test_model_from_tables.py`.
Expected: PASS with the two paths producing identical rows and the asserted zero/null reasons. Run `make check TESTS="tests/test_model.py integration/test_model_from_tables.py"`. Commit: `test: verify table-first aggregation parity`.

### Task 3: Implement pure `terrine.recipes` constructors

**Files:**
- Create: `terrine/recipes.py`
- Modify: `terrine/__init__.py` only if a `recipes` module attribute needs explicit lazy exposure; keep `recipes` as a focused module, not individual constructor aliases.
- Test: `tests/test_recipes.py` (new)

**Interfaces:**
- Consumes: Existing `Design`, `Schedule`, `EventAnchor`, `Sequential`, `EventPanel`, `CohortSpec`, `Trajectory`, `TimeToEvent`, `LifeTable`, `StatePanel`, and `EventSequence` implementations.
- Produces: `time_series`, `trial_panel`, `event_panel`, `ltv`, `survival`, `life_table`, `state_panel`, and `event_sequence` with signatures and defaults exactly as specified in the spec. Every function returns a plain `Design`.

- [ ] **Step 1: Write failing constructor tests**

Add tests for all eight constructors. Assert `isinstance(result, Design)` and these low-level fields: `Schedule` for `time_series`/`state_panel`, `SequentialSpec` for `trial_panel`, `EventPanelSpec` for `event_panel`, cumulative `Trajectory` and `CohortSpec` for `ltv`, `TimeToEvent` for `survival`, `LifeTable` containing `TimeToEvent` for `life_table`, and `EventSequence` for `event_sequence`. Assert `ltv.shape.origin == start_event`, `ltv.shape.trajectory.metric == metric`, and `ltv.shape.trajectory.cumulative is True`. Assert all supplied shared keywords are retained on the Design. Assert omission of `unit`, required event/start-event, period, or horizon/max-events arguments raises `TypeError`; first occurrence is the default; `occurrence="nth", n=2` selects the second occurrence; `occurrence="nth"` without positive `n` raises `ValueError`; first/each with `n` raises `ValueError`. Assert survival follow-up is governed by coverage by default, and the supplied `administrative_cutoff` appears unchanged on the Design. Assert a low-level conditional-event Design using `EventAnchor` and `relative_to=event_time(...)` can still be constructed.

- [ ] **Step 2: Run constructor tests to verify failure**

Run: `uv run pytest -q tests/test_recipes.py`
Expected: FAIL because `terrine.recipes` and its constructors are not yet present.

- [ ] **Step 3: Implement the constructors**

Implement signatures exactly as in the spec. Each constructor validates only what its composed low-level objects require and delegates to them: `trial_panel` must use `Sequential(base, treatment=..., periods=..., period=...)`; `event_panel` must use `EventPanel`; the other constructors retain their corresponding low-level shape on `Design`. Apply shared Design keyword values directly without creating a wrapper type or alternate defaults.

- [ ] **Step 4: Run constructor tests and commit**

Run: `uv run pytest -q tests/test_recipes.py`.
Expected: PASS; all constructors return `Design` with the asserted shape and required-argument behavior. Run `make check TESTS="tests/test_recipes.py"`. Commit: `feat: add dataset design constructors`.

### Task 4: Update API policy, guides, runnable examples, and changelog

**Files:**
- Modify: `AGENTS.md` (“Public API and refusals”)
- Modify: `docs/guides/public-api.md`
- Modify: `docs/guides/trajectories-survival.md`, `docs/guides/sequential-panels.md`, `docs/guides/state-panels.md`, and `docs/guides/temporal-engine.md` (or the current guide sections owning these workflows)
- Modify: `examples/trajectories_survival.py`, `examples/sequential_trials.py`, `examples/state_panel.py`, `examples/event_sequences.py`
- Modify: `CHANGELOG.md`
- Test: example execution test/CI workflow selection that already runs `examples/`

**Interfaces:**
- Consumes: Task 1 and Task 3 public APIs; spec's two-layer policy and exact constructor signatures.
- Produces: constructor-first public docs and executable examples; low-level Design equivalents remain visible under “Under the hood” sections.

- [ ] **Step 1: Update `AGENTS.md` and public API reference**

State explicitly that `terrine.recipes` constructors are the documented starting point for common dataset types and compile to the unchanged low-level `Design` API, which remains the full-flexibility layer and is not an alias. Update the facade table to describe `terrine.recipes`, list the constructor set, and clarify that `Sequential` and `EventPanel` remain available transforms.

- [ ] **Step 2: Write constructor-first guides with low-level equivalents**

For each constructor, document its required row-defining arguments, result row meaning, convention defaults, and an “Under the hood” code example producing the same Design with existing low-level APIs. Document conditional events only with `EventAnchor` and `relative_to=event_time(...)`; do not add a constructor for them.

- [ ] **Step 3: Convert existing examples and add coverage for all constructors**

Use the existing four workflow examples as the runnable recipe examples; include `time_series` and `trial_panel` in sequential/time-series coverage, `event_panel` in the event-panel example, `ltv`, `survival`, and `life_table` in the trajectory/survival example, `state_panel` in the state example, and `event_sequence` in the sequence example. Ensure every one of the eight constructors is exercised by an example, with assertions over observable output rather than print-only demonstration. Register any new example paths in the existing CI example runner if needed.

- [ ] **Step 4: Update the changelog and run the runnable examples**

Add an unreleased entry in the existing style describing the public additions and the two layers; mark breaking changes only if the owning release policy treats these additions as breaking. Run the repository's existing example command/CI selector against all changed examples. Expected: all examples execute successfully. Do not modify package version.

- [ ] **Step 5: Run focused checks and commit**

Run: `make check TESTS="tests/test_model.py tests/test_recipes.py integration/test_model_from_tables.py"` and the documented example runner. Expected: both succeed. Commit only the docs/examples/changelog/policy changes with `docs: document table-first datasets and constructors`.

### Task 5: Final acceptance and handoff

**Files:**
- Test: all paths listed by Tasks 1–4

**Interfaces:**
- Consumes: completed binding, constructors, docs, and examples.
- Produces: verified public behavior and a review-ready branch.

- [ ] **Step 1: Run focused acceptance suite**

Run: `make check TESTS="tests/test_model.py tests/test_recipes.py integration/test_model_from_tables.py"`.
Expected: PASS.

- [ ] **Step 2: Run integration and all runnable constructor examples**

Run: `uv run pytest -q integration/test_model_from_tables.py` and the repository's documented example runner over all examples listed in Task 4. Expected: PASS; each constructor is represented by an executed example.

- [ ] **Step 3: Review changed public surfaces**

Confirm exports, docs, refusal reference, changelog, all examples, and tests agree with the spec; no low-level API was renamed or removed; errors are pickle/deepcopy portable; no null reason was converted to zero or lost.

- [ ] **Step 4: Commit any final corrections**

Run `git diff --check`, then `make check TESTS="tests/test_model.py tests/test_recipes.py integration/test_model_from_tables.py"`. Commit any corrections with a product-term subject. Do not push, tag, publish, or merge.
