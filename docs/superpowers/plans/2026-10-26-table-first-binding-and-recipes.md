# Table-First Binding and Dataset Constructors Implementation Plan

> Implementation checklist: complete tasks in order; each step uses checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add table-first `Model.from_tables` binding and `terrine.recipes` dataset constructors that compile to ordinary `Design` objects.

**Architecture:** `Model.from_tables` translates one-unit event table declarations into the existing native Catalog and binds through `Model.from_catalog`. Pure constructors in `terrine.recipes` assemble existing anchors, expressions, shapes, and `Design`; they do not execute queries or add a parallel representation.

**Tech Stack:** Python 3.13, dataclasses, Ibis, Pydantic Catalog records, pytest, DuckDB integration examples.

**Spec:** `docs/superpowers/specs/2026-10-26-table-first-binding-and-recipes-design.md`

## Global Constraints

- Keep package version `0.1.0.dev0`.
- Keep `Design`, anchors, expression functions, `shape=`, `Sequential`, and `EventPanel` public and unchanged.
- `Model.from_tables` requires explicit `units` roster and event completeness; never infer either from event data.
- Each `Events.keys` mapping has exactly one unit; multi-unit sources use `Model.from_catalog`.
- Constructors return plain `Design`; signatures omit row options unsupported by their low-level shapes.
- New refusals use stable package-wide codes, immutable structured context, and survive pickle/deepcopy.
- Preserve null versus zero and per-cell null reasons through Arrow, manifests, and exports.
- Run focused tests in the cheapest tier and `make check TESTS="<affected paths>"` before each commit.
- Update production callers, exports, docstrings, guides, runnable examples, refusal docs, tests, and CHANGELOG in the same change; use clean cutover with no aliases.
- Warehouse behavior must use Ibis expressions; do not add raw SQL or UDF bindings to compiler/temporal code.

## Review Focus

1. A declared unit with no matching event remains in generated output; complete empty count/sum equal `0`, event mean is null with reason `no_observations`, and an incomplete window is null with its existing incompleteness reason. Test in Task 2's integration composition.
2. Missing `Events.complete` refuses rather than inferring a maximum event time; test code `terrine.model.missing_event_completeness` in Task 1.
3. The generated source and its same-named event fact are a permitted pair; any other source/fact/metric or conflicting readiness-name collision refuses with `terrine.model.generated_metric_collision`. Test in Task 1.
4. Multiple unit keys in one `Events` descriptor refuse with `terrine.model.multiple_event_units`; test in Task 1. Invalid recipe arguments and unsupported keyword use are refused by Python/type/shape validation; test in Task 3.
5. Recipe shapes match the existing contracts: trial schedule dates define trials, LTV age 0 includes its origin event, and constructor arguments are not silently dropped. Test in Tasks 2–3 and demonstrate in runnable examples in Task 4.

---

### Task 1: Add typed event-table descriptors and table-first binding

**Files:**
- Modify: `terrine/data.py` (`__all__`, beside `UnitSource`/`Coverage`)
- Modify: `terrine/model.py` (`__all__`, beside `from_catalog`)
- Modify: `docs/reference/refusals.md`
- Test: `tests/semantic/test_catalog.py`

**Interfaces:**
- Consumes: `UnitSource`, `Coverage`, `Catalog`, `Model.from_catalog`, `readiness`, and `event_ids`.
- Produces: exported immutable `Events(table: str, time: str, keys: Mapping[str, str], values: tuple[str, ...] = (), complete: tuple[date | str, date | str] | None = None)`; `Model.from_tables(con, *, units: Mapping[str, UnitSource], events: Mapping[str, Events], dialect: str | None = None, day_boundary: str = "UTC", readiness: Mapping[str, Coverage] | None = None, event_ids: Mapping[str, str] | None = None) -> Model`; refusal codes `terrine.model.missing_event_completeness`, `terrine.model.multiple_event_units`, and `terrine.model.generated_metric_collision`.
- Prerequisite: the separately planned mapping-input change to `Model.from_catalog` must be available before this task; use its exact merged API rather than adding a local shim.

- [ ] **Step 1: Write failing descriptor and admission tests**

Add tests asserting `Events("orders", "occurred_at", {"customer": "customer_id"}, ("amount",), ("2025-01-01", "2025-01-31"))` preserves its values and freezes/detaches `keys`. Bind a one-unit `orders` table and assert generated source/event fact intentionally shares `orders`, value fact is `orders_amount`, and metrics include `orders_count`, `orders_amount_sum`, and `orders_amount_mean`; assert the supplied unit roster remains bound. Assert an additional readiness key colliding with a generated metric/fact (for example `orders_amount_sum`) raises `TerrineError` with `.code == "terrine.model.generated_metric_collision"`. Assert keys `{"customer": "customer_id", "shop": "shop_id"}` raises `TerrineError` with `.code == "terrine.model.multiple_event_units"` and route context directing multi-unit sources to `Model.from_catalog`.

- [ ] **Step 2: Run focused tests to verify failure**

Run: `uv run pytest -q tests/semantic/test_catalog.py -k 'events_descriptor or from_tables'`
Expected: FAIL because `Events` and `Model.from_tables` are not implemented; do not accept collection-only success.

- [ ] **Step 3: Implement `Events` and `Model.from_tables`**

Add the frozen typed descriptor in `terrine.data`. Require each descriptor to declare exactly one unit key; refuse multiple keys with `terrine.model.multiple_event_units`, naming the source and routing multi-unit sources to `Model.from_catalog`. Require completeness per event and explicit `units`. Generate each source name as its event fact, each value fact as `<source>_<value>`, and the declared count/sum/mean metric names. Permit only the deliberate identity collision between a source and its own same-named event fact; detect all other collisions among generated source/fact/metric names and against distinct explicit readiness keys before binding, refusing with `terrine.model.generated_metric_collision`. Create generated `Coverage` from each `complete` pair, merge it with optional readiness for other sources, assemble the Catalog as a plain mapping, then delegate to `Model.from_catalog` with that mapping and `units` as a plain mapping, forwarding all other binding options. Do not query event rows or bind attributes.

- [ ] **Step 4: Add refusal-contract tests and run focused tests**

Assert missing completeness raises `TerrineError` with `.code == "terrine.model.missing_event_completeness"`; multiple keys raise `.code == "terrine.model.multiple_event_units"`; source/fact/metric name collisions and an explicit readiness key colliding with a generated name raise `.code == "terrine.model.generated_metric_collision"`. The event source and its same-named event fact bind successfully. For all three errors assert code and context survive `pickle.loads(pickle.dumps(error))` and `copy.deepcopy(error)`. Run: `uv run pytest -q tests/semantic/test_catalog.py -k 'events_descriptor or from_tables'`. Expected: PASS.

- [ ] **Step 5: Document refusals and commit**

Add all three codes, triggers, context fields, and routes forward to `docs/reference/refusals.md`. Run `make check TESTS="tests/semantic/test_catalog.py"`. Stage Task 1 files and commit with `feat: add table-first model binding`.

### Task 2: Prove aggregate and LTV semantics against native Catalog behavior

**Files:**
- Test: `integration/test_model_from_tables.py` (new)

**Interfaces:**
- Consumes: Task 1 `Events` and `Model.from_tables` signatures.
Produces: end-to-end evidence for count/sum/mean row parity, zero-activity roster retention, empty/incomplete-window reasons, generated event-fact binding, and LTV origin inclusion through the existing low-level API.

- [ ] **Step 1: Write the failing composition test**

Create a tiny DuckDB fixture with a roster containing `u1` and zero-activity `u2`, an `orders` event source and a value column `amount`. Include a nonzero amount at the selected origin and a distinct later event amount. Bind once through `Model.from_tables` and once through an equivalent hand-written Catalog; assert the generated source/event fact deliberately shares the name `orders` and build matching Designs using `agg("orders_count", ...)`, `agg("orders_amount_sum", ...)`, and `agg("orders_amount_mean", ...)`. Assert collected rows from both paths are equal. For `u2`'s complete empty window assert count `0`, sum `0`, mean `None`, and mean reason `"no_observations"`; for an incomplete window assert all unavailable aggregates are null with the existing incompleteness reason. Against the `from_tables` model, build LTV directly with `Design("user", EventAnchor("orders"), shape=CohortSpec("orders", Trajectory("orders_amount_sum", "30d", 2, cumulative=True)))`; assert age 0's cumulative value includes the nonzero origin amount plus the later contribution in the same half-open `[t0, t0 + period)` window.

- [ ] **Step 2: Run the integration test to verify failure**

Run: `uv run pytest -q integration/test_model_from_tables.py`
Expected: FAIL on missing `Model.from_tables` or a specific value/reason mismatch, not skip or collection failure.

- [ ] **Step 3: Correct generated Catalog mapping only as needed**

Adjust generated Catalog source/fact/measure/metric declarations in `terrine/model.py` so generated metrics and event facts resolve to the existing aggregation and event-anchor implementations. Do not change aggregate or LTV origin-window semantics to make the reference test pass.

- [ ] **Step 4: Run composition test and commit**

Run: `uv run pytest -q integration/test_model_from_tables.py`.
Expected: PASS with row parity, zero/null reasons, and the age-0 origin-inclusion assertion. Run `make check TESTS="tests/semantic/test_catalog.py integration/test_model_from_tables.py"`. Commit: `test: verify table-first aggregation and ltv semantics`.

### Task 3: Implement shape-compatible `terrine.recipes` constructors

**Files:**
- Create: `terrine/recipes.py`
- Modify: `terrine/__init__.py` only if `terrine.recipes` requires explicit lazy exposure; do not add individual root aliases.
- Test: `tests/spec/test_recipes.py` (new)

**Interfaces:**
- Consumes: Existing `Design`, `Schedule`, `EventAnchor`, `Sequential`, `EventPanel`, `CohortSpec`, `Trajectory`, `TimeToEvent`, `LifeTable`, `StatePanel`, and `EventSequence` implementations.
- Produces: `time_series`, `trial_panel`, `event_panel`, `ltv`, `survival`, `life_table`, `state_panel`, and `event_sequence`; exact signatures and accepted keywords are in the spec. Each returns a plain `Design`.

- [ ] **Step 1: Write failing constructor and refusal tests**

Assert each constructor returns `Design` with the expected anchor/shape; omission of every required unit, event/start event, schedule bound/interval, panel period, trajectory period/period count, event-sequence horizon, or `max_events` raises `TypeError`. Assert trial Design has `Schedule(every, start, end)` and `SequentialSpec` fields matching treatment, trial weight, anticipation, and reference period. Assert `ltv.shape.origin`, trajectory metric/period/periods/cumulative match the inputs. For every constructor accepting `features`, `eligibility`, `anchor_range`, or `gap`, assert the returned Design exposes the exact supplied value; for each accepting `outcomes`/`treatment`/`administrative_cutoff`, assert the value is retained on Design or the corresponding shape/cutoff field. For `life_table`, assert `group_by=("region",)` with `features={"region": asof(...)}` produces a Design whose shape names `region`, while an extra feature not named by `group_by` raises `ValueError`; `treatment=...` raises `TypeError`. Unsupported keywords raise `TypeError`: `event_panel(treatment=...)`, `state_panel(outcomes=...)`, `state_panel(treatment=...)`, `ltv(outcomes=...)`. Assert survival/life_table accept `first`/`nth` only; `each` refuses with `.code == "terrine.design.invalid"`, nth needs positive `n`, and first with `n` refuses. Assert event_panel/event_sequence accept `each`, with invalid nth arguments refused. Assert a low-level conditional-event Design using `EventAnchor` and `relative_to=event_time(...)` can still be built.

- [ ] **Step 2: Run constructor tests to verify failure**

Run: `uv run pytest -q tests/spec/test_recipes.py`
Expected: FAIL because `terrine.recipes` and its constructors are not implemented.

- [ ] **Step 3: Implement the constructor signatures and composition**

Implement exact spec signatures. `time_series` uses only `unit`, `every`, `start`, `end` as row-defining arguments and creates `Schedule`. `trial_panel` creates a `Schedule` base Design then calls `Sequential(base, treatment=..., trial_weight=..., anticipation=..., reference_period=...)`; do not pass `periods`/`period` to `Sequential`. `event_panel` creates an event-anchored base and calls `EventPanel` with `periods`, converted `period_seconds`, and `reference_period`; accept no treatment. `ltv` creates `CohortSpec` with cumulative `Trajectory`, accepts `Treatment` but no arbitrary outcomes, and forwards optional administrative cutoff. `survival` accepts `Treatment`; `life_table` takes no treatment and requires `group_by` names to match exactly the keys in `features`, passing them to `LifeTable.group_by`. Both use first/nth start anchors only and pass optional cutoff; neither accepts `each` or arbitrary outcomes. `state_panel` combines Schedule and StatePanel and accepts no outcomes/treatment. `event_sequence` constructs a bounded `Window` and `EventSequence` with required `max_events`. Pass each supported keyword through to a field consumed by that shape; omit unsupported keywords rather than discarding them.

- [ ] **Step 4: Run constructor tests and commit**

Run: `uv run pytest -q tests/spec/test_recipes.py`.
Expected: PASS, including explicit unsupported-keyword and invalid occurrence checks. Run `make check TESTS="tests/spec/test_recipes.py"`. Commit: `feat: add shape-compatible dataset constructors`.

### Task 4: Update public policy, guides, runnable examples, and changelog

**Files:**
- Modify: `AGENTS.md` (“Public API and refusals”)
- Modify: `docs/guides/public-api.md`, `docs/guides/temporal-engine.md`, `docs/guides/trajectories-survival.md`, `docs/guides/sequential-panels.md`, `docs/guides/state-panels.md`
- Create: `examples/table_first_dataset.py`
- Modify: `examples/trajectories_survival.py`, `examples/sequential_trials.py`, `examples/state_panel.py`, `examples/event_sequences.py`
- Modify: `CHANGELOG.md`
- Test: `.github/workflows/ci.yml` `docs-and-examples` job (executes each `examples/*.py`)

**Interfaces:**
- Consumes: Task 1/2 `Events`, `Model.from_tables` and Task 3 constructors; the spec's two-layer policy and exact signatures.
- Produces: constructor-first documentation, table-first binding guidance, and runnable examples whose assertions show behavior; each constructor and `from_tables` has an executed example.

- [ ] **Step 1: Update `AGENTS.md` and public API reference**

State explicitly that `terrine.recipes` constructors are the documented starting point for common dataset types and compile to the unchanged low-level `Design` API, which remains the full-flexibility layer and is not an alias. Update the facade table to describe `terrine.recipes`, list all eight constructors, and clarify that `Sequential` and `EventPanel` remain public transforms. Document that `Model.from_tables` is the one-unit/simple event-table route and `Model.from_catalog` remains the route for multi-unit event sources, attributes, SQL, filters, and ratios.

- [ ] **Step 2: Write constructor and table-first guides**

Add a table-first binding guide section showing `Events`, required roster/completeness, generated fact/metric names, zero-activity roster retention, materialization, and the `Model.from_catalog` route for attributes/multi-unit sources. For each constructor, document its required row-defining arguments, supported keywords, result row meaning, convention defaults, and an “Under the hood” low-level Design example. Document LTV age-0 origin inclusion, survival's coverage/absolute-cutoff follow-up (no relative duration horizon), trial calendar bounds, and conditional events as the low-level expression pattern. Document that `life_table.group_by` must exactly match declared grouping features, extra features refuse rather than disappear, and the materialized roles include grouping columns plus `time_bin` without treatment.

- [ ] **Step 3: Add runnable example coverage and CI execution**

Create `examples/table_first_dataset.py`; the existing CI job in `.github/workflows/ci.yml` automatically executes every `examples/*.py`, so no separate selector is needed. Demonstrate `Model.from_tables` with an explicit roster containing a zero-activity unit, complete coverage, generated names, and an LTV/event-based recipe consuming generated fact `orders`; assert observable values. Update the existing examples so they exercise `trial_panel` (use `every="14d"`, `start="2025-01-01"`, exclusive `end="2025-02-01"` and assert scheduled trial dates are exactly Jan 1, Jan 15, and Jan 29), `event_panel`, `ltv` (origin amount assertion), `survival` and `life_table` (group by a declared feature; assert the materialized grouping column, `roles.features == ("region", "time_bin")`, `roles.outcomes == ("at_risk", "events", "censored", "delayed_entries", "event_rate", "censor_rate")`, and `roles.treatment is None`), `state_panel`, and `event_sequence`. Every constructor appears in a CI-executed example; each accepted recipe keyword shown has a tested observable effect.

- [ ] **Step 4: Update changelog and execute examples**

Add an unreleased entry in the existing style describing public additions and both layers; mark breaking changes only if the release policy treats these additions as breaking. Run the exact CI example command: `for example in examples/*.py; do uv run python "$example"; done`. Expected: all finish successfully. Do not change package version.

- [ ] **Step 5: Run focused checks and commit**

Run `make check TESTS="tests/semantic/test_catalog.py tests/spec/test_recipes.py integration/test_model_from_tables.py"` and the documented example runner. Expected: both succeed. Commit only docs/examples/changelog/policy changes with `docs: document table-first datasets and constructors`.

### Task 5: Final acceptance and handoff

**Files:**
- Test: `tests/semantic/test_catalog.py`, `tests/spec/test_recipes.py`, `integration/test_model_from_tables.py`, and the registered runnable examples

**Interfaces:**
- Consumes: completed binding, constructors, docs, and examples.
- Produces: verified public behavior and a review-ready branch.

- [ ] **Step 1: Run focused acceptance suite**

Run: `make check TESTS="tests/semantic/test_catalog.py tests/spec/test_recipes.py integration/test_model_from_tables.py"`.
Expected: PASS.

- [ ] **Step 2: Run integration and all runnable examples**

Run: `uv run pytest -q integration/test_model_from_tables.py` and the existing documented example runner over the four updated examples plus `examples/table_first_dataset.py`. Expected: PASS; every constructor and table-first binding has an executed example.

- [ ] **Step 3: Review public contracts against the spec**

Confirm exports, docs, refusal reference, changelog, all examples, and tests agree with the spec; no low-level API was renamed or removed; all three refusal codes are pickle/deepcopy portable; no null reason was converted to zero or lost; LTV age 0 includes the origin event; unsupported keywords are not silently accepted.

- [ ] **Step 4: Commit final corrections**

Run `git diff --check`, then `make check TESTS="tests/semantic/test_catalog.py tests/spec/test_recipes.py integration/test_model_from_tables.py"`. Commit corrections with a product-term subject. Do not push, tag, publish, or merge.
