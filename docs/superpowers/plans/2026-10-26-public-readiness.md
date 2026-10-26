# Public Readiness Program Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement each task. Work state lives in the kata issue tree; this file is the design and sequence. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the breaking public-API and usability changes, rewrite the README so the owner can make the repository public, then build the engine registry for 10-15+ warehouses.

**Architecture:** A program of independent work items, each owned by one kata issue. Bounded items execute directly from their issue's acceptance list; two items (T2/T6, T9-T11) are architectural and get their own spec or implementation plan when they start. Items run in parallel lanes chosen from their file footprints; contention is resolved by sequencing inside a lane, not by fake dependencies.

**Tech Stack:** Python 3.13/3.14, Ibis, DuckDB 1.5.6 reference backend, PostgreSQL/BigQuery/Snowflake live probes, pytest tiers per `AGENTS.md`.

**Spec:** No single spec. Sources are the kata issue bodies named per task (each carries its design and acceptance) and the owner decisions recorded in the scope ledger. T2 produces `docs/superpowers/specs/2026-10-26-table-first-binding-and-recipes-design.md`, which T6 implements.

## Scope ledger

| # | Capability | Source | Confirmed |
| --- | --- | --- | --- |
| 1 | Phase order: API/usability, then README, then warehouse work; repo goes public after phase 2 | Owner: "1) focus on public api / usability changes 2) update the readme 3) the the underlying warehouse stuff ... that way I can make the repo public after 2)" | Yes |
| 2 | README rewrite | Owner: "the readme is a nightmare for me" | Yes |
| 3 | Table-first binding (`Model.from_tables`) and dataset constructors in `terrine.recipes`, low-level API kept | Owner question: "should we have an even simpler high level API that is just common operations over our low level API"; reply to the proposed `Model.from_tables` + one-constructor-per-dataset-type design: "I like both of these ideas!"; "I personally thought itd help users understand better instead of having to learn our unique syntax right away" | Yes |
| 4 | Rename saved-recipe family (`DatasetRecipe` -> `DatasetDefinition`, `SavedPlanRecipe` -> `SavedPlan`) to free "recipe" | Reply "i like this" to the proposal "use `tr.recipes` for the dataset constructors and rename the saved-recipe family to `DatasetDefinition` / `SavedPlan`"; "I dont think we need pantry" | Yes |
| 5 | `TimeToEvent.duration` as elapsed time; `EventAnchor` offset for landmarking | Owner question about these two items: "do these make sense to add to the package?"; reply to "yes to both": "great! make kata issues for both" | Yes |
| 6 | `event_time(occurrence="last")`; predicates as feature/outcome values; both in phase 1 | Owner question about recency and yes/no labels: "should we come up to solutions for these now?"; reply "ok can you make a plan for this?" to "If you agree, I'll move k79m and hvhz to priority 1" | Yes |
| 7a | Engine registry and conformance suite for 10-15+ engines | Owner: "assume we'll support 10-15 in the near future and might expand further beyond that. show me pseudocode of your proposed solution"; reply "yes please!" to "I can turn this into kata issues in that order: a parent issue with four children" (registry refactor, conformance suite, generic default, engines) | Yes |
| 7b | Opt-in generic default for unregistered Ibis backends | Owner: "isnt it as easy as seeing which ibis backend is provided and giving a warning?"; "does your proposed solution have a default operation, ie just default unverified ibis?"; same "yes please!" | Yes |
| 7c | Adding new engines (Trino, ClickHouse, SQL Server, Oracle, Spark, ...) | Owner: "we wont add additional engines as part of this work" | **Out of scope** - no tasks; kata `6dmp` stays in the backlog outside this plan |
| 8 | Refuse PostgreSQL-wire look-alikes (Redshift etc. admitted as PostgreSQL), before publication | Owner standing rule (global AGENTS.md): "Do not defer related defects found while working"; placement: "recommendations sounds good to me" (reply to "keep it in phase 1") | Yes |
| 9 | `from_catalog` accepts a mapping; examples import public modules only; reviewed public route for training splits (kata `tcv2`) | Owner: "recommendations sounds good to me" (reply to "include it") | Yes |
| 10 | Readable, consistent completeness columns across shapes, option (a): `<name>__complete` / `<name>__reason` next to each value (kata `12j7`) | Owner: "recommendations sounds good to me" (reply to "include it, and use (a)") | Yes |

Making the repository public, choosing version/license and any push or publish are owner actions (kata `v9yy`), not tasks.

## Global Constraints

- Follow repository `AGENTS.md`: tests in the cheapest tier; red-first evidence for every new test; no wording/wiring tests; `make check TESTS="<affected paths>"` before each commit.
- Public API changes update production callers, exports, docstrings, guides, examples, fixtures and CHANGELOG in the same change; clean cutover, no aliases.
- Refusals keep stable package-wide codes; renamed codes get a breaking CHANGELOG entry; every new refusal survives pickle and deepcopy.
- Null versus zero and per-cell null reasons survive Arrow, manifests and exports unchanged.
- Package version stays `0.1.0.dev0`; no tag, push, publish or GitHub mutation (session runs with GitHub publishing disabled; owner-reserved).
- One worktree and branch per task under `.worktrees/<branch-leaf>`, based on the current `main`; merge to `main` locally after the task's review gate.
- No tooling names (kata, roborev, superpowers, subagent) in code, commits, docs or CHANGELOG.
- Warehouse parity evidence: each task touching compiled output (T3, T4, T5, T8) closes with local PostgreSQL probes, plus local BigQuery probes when `gcloud` credentials are available. Snowflake has no local credentials: before T7 starts, the owner pushes `main` and dispatches `warehouse-backends` with `backend_mode=all`; a green run is the phase 1 exit gate.

## Review Focus

1. Artifacts written before T1 behave predictably after it: a bare definition payload (`schema_version`, `base_design`, `view`, optional `transforms`; unchanged by the rename) loads unchanged with the same digest; a manifest or saved plan carrying renamed fields (`recipe`, `recipe_hash`, `dataset_recipe_json`) refuses with `terrine.dataset.invalid_manifest` (new) / `terrine.runtime.saved_plan_mismatch`, never a partial decode or uncoded crash. Owner: T1.
2. `occurrence="last"` over a complete lookback with no matching event yields null with `complete=True`, distinct from an incomplete lookback (null, reason). Owner: T4.
3. A boolean outcome whose condition is already met inside a window that is not yet complete yields null with a reason, never `True`. Owner: T5.
4. `Model.from_tables` keeps roster units with zero events, with empty-window values by aggregation: count and sum are `0` (complete); event means are null with reason `no_observations`; any incomplete window is null with its incompleteness reason. It refuses a missing completeness declaration instead of inferring it from the latest event. Owner: T6.
5. `from_tables` generated metric names colliding with each other or with an explicit catalog name refuse with a coded error; recipes constructors never default a row-defining argument (unit, start event, period, horizon). Owner: T6.

---

## Sequence and lanes

```mermaid
flowchart LR
  subgraph P1[Phase 1: public API and usability]
    T1[T1 z2nq rename] --> T14[T14 tcv2 public surface] --> T6[T6 jb52 from_tables + recipes]
    T2[T2 vdnx spec + plan] --> T6
    T3[T3 40d3 duration] --> T15[T15 12j7 completeness columns]
    T4[T4 hvhz last occurrence] --> T5[T5 k79m boolean outputs] --> T15
    T8[T8 1ky5 look-alikes]
  end
  subgraph P2[Phase 2]
    WB{{Owner: warehouse-backends run green}} --> T7[T7 x41p README] --> PUB{{Owner: make repo public}}
  end
  subgraph P3[Phase 3: engines]
    T9[T9 j60e registry refactor] --> T11[T11 14ff generic opt-in]
    T9 --> T10[T10 w6pn conformance] --> T11
    T13[T13 9btd anchor offset]
  end
  T15 --> WB
  T6 --> WB
  T8 --> WB
  PUB --> P3
```

| Wave | Parallel lanes | Why this split |
| --- | --- | --- |
| 1 | T1, T2, T3, T4 | Disjoint footprints. T1 and T3 both touch `terrine/frame.py` in different functions (recipe/view references vs `_normalize_output`); merge whichever finishes second with a rebase. T2 is documentation only |
| 2 | T5 (after T4), T14 (after T1), T8 | T4 and T5 share `spec/expressions.py`, `spec/validate.py`, `compile/columns.py`, so they run in one lane. T14 edits the examples T1 renames and `model.py`. T8 touches only `execution/_postgres.py` and its tests |
| 3 | T6 (after T2 + T14), T15 (after T3 + T5) | T6 touches `model.py`, `__init__.py` and a new `recipes` module; T15 touches compile, temporal, `frame.py`, `data.py`, exports and agent schemas. Only guide pages overlap; rebase the second |
| Gate | Owner pushes `main` and dispatches `warehouse-backends` (`backend_mode=all`) | Snowflake parity has no local route; GitHub actions are owner-reserved |
| 4 | T7 | Shows the final API: blocked by T3, T4, T5, T6, T14, T15 |
| Gate | Owner decides publication (`v9yy`) | Owner-reserved |
| 5 | T9, T10 (cases), T13 | T9 is a pure refactor of `terrine/backends/` and `terrine/execution/`. T10 writes its cases against today's DuckDB/PostgreSQL fixtures in parallel, but closes only after T9 merges and its fixture is parametrized from `ENGINES`. T13 touches `compile/anchors.py` near the engine list T9 removes (`anchors.py:154`): rebase whichever lands second |
| 6 | T11 (after T9 + T10) | Needs the registry and the suite |
| - | No new engines | Owner decision; T12 removed |

Concurrency cap: at most four worktrees at once; every wave stays within it.

## Per-task execution protocol

Every task follows the same loop (kata-driven-development "Per-Task Discipline"):

- [ ] `kata show <ref>`; `kata claim <ref>`; comment `Approach:` before code.
- [ ] Create worktree `.worktrees/<leaf>` on branch `<branch>` from `main`.
- [ ] Red: write the failing test(s) the issue acceptance names; run and record the failure.
- [ ] Green: implement; run `make check TESTS="<paths>"` and the named integration/probe commands.
- [ ] Commit (jargon check on the staged diff); run the repository-wide review inventory; resolve owned findings.
- [ ] Merge to `main`; close the issue with `--commit <sha>` and `--evidence "test:<cmd>"`.
- [ ] Any divergence from this plan or the issue: `Divergence:` comment first.

---

### Task 1: Rename the saved-recipe family (kata `z2nq`)

**Path:** bounded, mechanical. **Branch:** `feat/saved-definitions`.

**Files:** `terrine/spec/dataset.py`, `terrine/spec/__init__.py`, `terrine/spec/transforms.py`, `terrine/runtime/recipe.py` (rename module to `terrine/runtime/saved_plan.py`), `terrine/runtime/{api,planning,__init__}.py`, `terrine/dataset/{manifest,reference,transforms}.py`, `terrine/agent/{recipes,schemas,tools}.py` (`recipes.py` -> `definitions.py`), `terrine/frame.py`, `terrine/__init__.py`, refusal registry and `docs/reference/refusals.md`, guides (`ml-staging`, `materialization-runtime`, `orchestrators`, `dataset-agent-tools`, `public-api`), examples, tests, CHANGELOG.

**Interfaces:**
- Produces: `DatasetDefinition`, `DatasetDefinitionStore`, `DatasetDefinitionReference` (`terrine.spec.dataset`); `SavedPlan` (`terrine.runtime.saved_plan`); codes `terrine.dataset.invalid_definition`, `terrine.dataset.invalid_manifest` (new), `terrine.runtime.saved_plan_mismatch`; wire fields `dataset_definition_json`, `plan_hash`; agent tools `get_dataset_definition_schema`, `propose_definition`, `validate_definition`. Persisted runtime SQL identifiers (`runtime_dataset_recipes`, `runtime_dataset_requests.recipe_json`, `dataset/reference.py:156,196,425-426,486-491`) keep their names: renaming them would orphan existing owner databases for no user-visible gain.

- [ ] **Red:** in `tests/dataset/test_recipe.py` (renamed to `tests/dataset/test_definition.py`): `test_pre_rename_definition_bytes_load_with_same_digest` (canonical bytes produced by today's `DatasetRecipe`, checked in as a fixture, load as `DatasetDefinition` with an identical `digest`, so stored definitions stay resolvable); `test_manifest_json_with_renamed_fields_refuses` (an actual pre-rename manifest payload; code `terrine.dataset.invalid_manifest`; today `DatasetManifest.from_json` raises a plain `ValueError`, `manifest.py:174`); in the saved-plan tests, `test_saved_plan_json_with_renamed_fields_refuses` (code `terrine.runtime.saved_plan_mismatch`); in the staging tests, `test_pre_rename_owner_database_refuses_old_generation_and_stages_new` (a pre-T1 runtime database fixture: reopening its generation refuses with `terrine.dataset.invalid_manifest`; staging a new generation into the same database succeeds). Review Focus 1.
- [ ] **Green:** rename; `DatasetManifest.from_json` raises the new coded `terrine.dataset.invalid_manifest` for any malformed or unknown-field payload (registered, pickle/deepcopy-safe, documented); `grep -rniE 'recipe' terrine` matches only the persisted SQL identifiers listed above (the `terrine.recipes` module does not exist yet). No legacy-name constants and no migration (pre-release).
- [ ] **Verify:** `make check TESTS="tests"`; `uv run pytest -q integration`; `uv run python examples/dataset_agent_tools.py`; `uv run python examples/materialization_runtime.py`; `uv run python examples/retention_and_winsorization.py`.

### Task 2: Spec and implementation plan for `from_tables` and `terrine.recipes` (kata `vdnx`)

**Path:** architectural, documentation only. **Branch:** `docs/recipes-spec`.

**Files:** create `docs/superpowers/specs/2026-10-26-table-first-binding-and-recipes-design.md` and `docs/superpowers/plans/<date>-table-first-binding-and-recipes.md`.

**Interfaces:**
- Produces for T6: the spec settles (a) `Model.from_tables` signature and the `Events` description type, (b) generated metric naming and collision refusal code, (c) each constructor's signature with required versus defaulted arguments, (d) the AGENTS.md "Public API and refusals" wording for the two layers.

- [ ] Write the spec from the `vdnx` issue body (rules already decided: constructors return plain `Design`; low-level API unchanged including `Sequential`/`EventPanel`; roster and completeness explicit; conditional events stay a documented expression pattern).
- [ ] Self-review the spec; run the plan review on spec and plan; revise.
- [ ] Bootstrap T6's sub-tasks as children of `jb52` from the reviewed plan.

### Task 3: `TimeToEvent.duration` as elapsed time (kata `40d3`)

**Path:** bounded. **Branch:** `fix/tte-elapsed-duration`.

**Files:** `terrine/compile/shapes/time_to_event.py:479,501,512`, `terrine/frame.py:888-901`, `terrine/compile/shapes/life_table.py:53-61`, `terrine/export/lifelines.py:89-96,181-207`, tests in `tests/compile/test_time_to_event.py`, `tests/export/test_lifelines.py`, `integration/compile/test_time_to_event.py`, `integration/handoffs/test_lifelines.py`, `docs/guides/trajectories-survival.md`, CHANGELOG.

**Interfaces:**
- Produces: `duration` column equal to terminal time minus `t0`, as an Arrow duration whose unit matches the compiled time axis on every backend: `duration("ns")` on nanosecond axes (DuckDB `timestamp[ns]`, `time_to_event.py:43-46`; Snowflake scales 7-9), `duration("us")` on microsecond axes. An elapsed value outside Arrow's int64 nanosecond range (possible on Snowflake, whose nanosecond axis spans years 1-9999) refuses with the existing `terrine.execution.arrow_timestamp_range` hazard code and its route to microsecond precision. LifeTable and lifelines consume it directly.

- [ ] **Red:** `test_duration_is_elapsed_from_t0_for_event_and_censored_rows`: event at `t0 + 10d` gives `timedelta(days=10)`; censored at cutoff `t0 + 21d` gives `timedelta(days=21)`. `test_duration_with_delayed_entry_is_measured_from_t0`: entry at `t0 + 2d`, event at `t0 + 10d` gives `duration = 10d`, `entry_time` stays the entry timestamp, and the lifelines export's entry is `2d`. `test_nanosecond_duration_keeps_submicrosecond_elapsed`: on `timestamp[ns]` sources an event at `t0 + 1d + 1ns` gives `duration("ns")` value `86_400_000_000_001`, and the lifelines duration equals the pre-change export for the same rows. Snowflake: the same submicrosecond case and an over-range elapsed case (refusal) in the Snowflake survival probes, run by the owner-dispatched `warehouse-backends` gate.
- [ ] **Green:** emit `terminal - t0`; LifeTable and lifelines read elapsed values; KM/Cox inputs and LifeTable bins unchanged numerically.
- [ ] **Verify:** `make check TESTS="tests/compile/test_time_to_event.py tests/export/test_lifelines.py"`; `uv run pytest -q integration/compile/test_time_to_event.py integration/handoffs/test_lifelines.py`; `make test-postgres` against the scratch server.

### Task 4: `event_time(occurrence="last")` (kata `hvhz`)

**Path:** bounded. **Branch:** `feat/event-time-last`.

**Files:** `terrine/spec/expressions.py:213-220`, `terrine/compile/columns.py:470-482`, `terrine/agent/design_schema.py`, `terrine/temporal/first.py:242` (decide `last_event_value`: expose as `last_value` or delete), `docs/guides/temporal-engine.md`, tests in `tests/spec/`, `tests/compile/`, `integration/compile/`.

**Interfaces:**
- Produces: `EventTime.occurrence` accepts `"last"`; ordering is the reverse of `(timestamp, event id)`.

- [ ] **Red:** `test_last_occurrence_selects_latest_event_before_t0` (two events at `t0-5d`, `t0-1d` -> `t0-1d`); `test_last_occurrence_empty_complete_lookback_is_null_complete` (Review Focus 2); `test_last_occurrence_ties_mirror_first` (equal timestamps, event ids `a`,`b`: first -> `a`, last -> `b`); `test_last_occurrence_unknown_label_after_latest_match_is_incomplete` (label-filtered selector: known match at `t0-3d`, unknown-label event at `t0-1d` -> null, reason `incomplete_label_history`); `test_last_occurrence_unknown_label_before_latest_match_is_complete` (unknown at `t0-5d`, known at `t0-1d` -> `t0-1d`, complete), plus the same two cases at a tied timestamp ordered by event id.
- [ ] **Green:** accept `"last"`; use one occurrence ordering for both ranking (`compile/columns.py:470-482`) and unknown-label precedence (`columns.py:512-516`, today `<` / `<=`), reversed for `"last"`; update the agent design schema enum.
- [ ] **Verify:** `make check TESTS="tests/spec tests/compile/test_columns.py"`; related integration module; `make test-postgres`.

### Task 5: Predicates as feature and outcome values (kata `k79m`)

**Path:** bounded, with a short design in the issue first (output-value traversal and composite timing, below). **Branch:** `feat/boolean-outputs`. **After:** T4 (shared files).

**Files:** `terrine/spec/design.py:85-87,110-112`, `terrine/spec/validate.py` (incl. `_outcome_bounds`, line 192), `terrine/compile/_predicates.py:44-108` (role parameter instead of hard-coded `"eligibility"` at line 107), `terrine/compile/ranges.py:27-39` (`plan_ranges` collects leaves of predicate-valued outputs, as it already does for eligibility `And`), `terrine/compile/layers.py:210-214`, `terrine/compile/columns.py` (incl. `expression_timings`, line 1090), `terrine/compile/shapes/sequential.py:36-46`, `terrine/frame.py:296-300` (training-view timings), `docs/guides/design-specs.md`, tests.

**Interfaces:**
- Consumes: T4's `occurrence` handling (none directly; shared files only).
- Produces: `Design(features=..., outcomes=...)` accept `Gt | Eq | IsNull | And`; boolean output columns with `ColumnMeta` role `feature`/`outcome`. One helper yields the leaf expressions of any output value; every consumer that walks outputs (range planning, layer planning, Sequential follow-up horizon, expression timings) uses it. A predicate output's timing is the composite of its leaves: support from the earliest leaf start to the latest leaf end, so outcome horizons and time-split assignment see the full window.

- [ ] **Red:** `test_converted_within_7d_true_false_and_unknown` (event day 3 -> True; none in complete window -> False; event day 1 in incomplete window -> null with reason, Review Focus 3); `test_conjunction_with_complete_false_term_is_false_and_complete` (`all_of(complete False, incomplete term)` -> `False`, `complete=True`: three-valued AND, as eligibility already computes at `_predicates.py:56-64`); `test_conjunction_with_true_and_incomplete_terms_is_null`; `test_churned_30d_eq_zero`; `test_predicate_outcome_round_trips_json`; `test_predicate_over_nonadditive_operand_gets_range_plan` (`gt(count_distinct metric over [0s,7d)), 2)` as an outcome materializes, matching the unwrapped value's comparison); `test_sequential_followup_covers_predicate_outcome_horizon` (a 30d predicate outcome on a schedule ending before the window does extends follow-up exactly as the unwrapped 30d aggregate does); `test_time_split_assigns_boolean_label_by_its_window` (a boolean label whose window crosses the split boundary is assigned the same way as the unwrapped aggregate label).
- [ ] **Green:** widen accepted types; add the output-leaf helper and composite timing; route range planning, layer planning, `_outcome_bounds` and expression timings through it; compile predicate values per role; validation reuses per-leaf role checks.
- [ ] **Verify:** `make check TESTS="tests/spec tests/compile"`; `uv run pytest -q integration/compile`; `make test-postgres`; check `frame.training_set(label=...)` accepts a boolean label.

### Task 6: Implement `Model.from_tables` and `terrine.recipes` (kata `jb52`)

**Path:** executes T2's implementation plan (sub-tasks become children of `jb52`). **Branch:** `feat/recipes`. **After:** T1, T2.

**Files (expected; T2's plan is authoritative):** `terrine/model.py`, new `terrine/recipes.py`, `terrine/__init__.py`, `docs/guides/*` (constructor first, "under the hood" Design second), examples, `AGENTS.md` (two-layer API wording), CHANGELOG.

- [ ] Execute T2's plan; Review Focus 4 and 5 tests live there.
- [ ] **Verify:** every new example runs in CI; `from_tables` produces rows identical to the equivalent hand-written Catalog for count, sum and mean metrics (integration composition). `from_tables` passes a plain mapping to `from_catalog` (T14).

### Task 14: Public-surface fixes (kata `tcv2`)

**Path:** bounded. **Branch:** `fix/public-surface`. **After:** T1 (examples renamed there).

**Files:** `terrine/model.py:94-150` (`from_catalog` accepts a mapping and validates it as a `Catalog`), `terrine/dataset/__init__.py` (reviewed public route for `GroupSplit`/`TimeSplit`, used by `Frame.training_set(split=...)`), `docs/guides/public-api.md`, all `examples/**/*.py` imports, `tests/semantic/test_catalog.py`, CHANGELOG.

**Interfaces:**
- Produces: `Model.from_catalog(path_or_catalog: str | Path | Catalog | Mapping[str, Any], *, con, readiness, units=None, event_ids=None, input_binding=None)`; split policies importable from a public facade.

- [ ] **Red:** `test_from_catalog_mapping_binds_same_model_as_catalog` in `tests/semantic/test_catalog.py`: the same definition passed as a dict and as `Catalog.model_validate(dict)` yields equal `model.catalog`; an invalid mapping refuses with `terrine.catalog.invalid_definition`.
- [ ] **Green:** accept the mapping; replace example imports of internals (`terrine.compile.columns`, `terrine.execution.backend`, `terrine.runtime.<submodule>` where `terrine.runtime` exports the name, `terrine.semantic.models.Catalog` where a dict suffices) with public routes; export the split policies from a facade.
- [ ] **Verify:** `make check TESTS="tests/semantic/test_catalog.py tests/test_documentation.py"`; every script under `examples/` runs.

### Task 15: Readable, consistent completeness columns (kata `12j7`)

**Path:** bounded with a short design first (in the issue, before code) covering where observation bounds and `anchor_provenance` go (metadata or opt-in). **Branch:** `feat/completeness-columns`. **After:** T3, T5.

**Files:** `terrine/data.py` (`ColumnMeta` support-column names), `terrine/compile/{columns,_asof,_predicates,retention}.py`, `terrine/compile/shapes/{cohorts,event_sequence,ltv,_observations,state_panel,time_to_event}.py`, `terrine/temporal/{aggregate,daily,nonadditive,sequence}.py`, `terrine/frame.py`, `terrine/dataset/training.py`, `terrine/export/{preflight,surrogate}.py`, `terrine/agent/{schemas,tools}.py`, `terrine/spec/sequence.py`, affected tests, guides, CHANGELOG.

**Interfaces:**
- Produces: for every output value column `<name>` in every per-unit shape, `<name>__complete` (bool) and `<name>__reason` (string) placed immediately after it. Aggregate shapes that report several exact reasons per row keep `list<string>` under the same suffix (LifeTable today: `reason: list<string>`, `life_table.py:169`); the export preflight already accepts both forms (`export/preflight.py:129-132`). `ColumnMeta` points at those names; design-level refusal for user column names ending in `__complete` or `__reason`.

- [ ] **Red:** `test_time_series_columns_are_values_with_named_support` (two values -> `unit, anchor_id, t0, rev_14d, rev_14d__complete, rev_14d__reason, rev_next_7d, ...`, no `__terrine_value_*`); `test_ltv_and_survival_use_the_same_suffixes`; `test_life_table_keeps_list_valued_reasons_under_the_suffix` (two incomplete records with different reasons in one bin -> both reasons listed; preflight exclusion counts unchanged); `test_reserved_support_suffix_refuses`; `test_null_reason_round_trips_arrow_and_manifest_under_new_names`.
- [ ] **Green:** rename support columns at the point each shape publishes them; observation bounds move per the design; exports and training views read names from `ColumnMeta`.
- [ ] **Verify:** `make check TESTS="tests"`; `uv run pytest -q integration`; `make test-postgres`; library hand-off integration tests unchanged in behavior.

### Task 7: README rewrite (kata `x41p`)

**Path:** bounded documentation. **Branch:** `docs/readme`. **After:** T3, T4, T5, T6, T14, T15 and the warehouse-backends gate.

**Files:** `README.md`, `CONTRIBUTING.md` (receives contributor/test-tier prose), guides receiving moved sections (`materialization-runtime.md`, `dataset-agent-tools.md`, `orchestrators.md`, owned-inputs section), `examples/first_dataset.py` (extractor heading).

- [ ] Rewrite to the outline in `x41p`: two-sentence purpose with an output table; 15-line example with `from_tables` + `tr.recipes`; "Datasets you can build"; five rules; warehouse table; links; contributing pointer. Add the "common feature patterns" snippets section (rolling windows, conditional event, active-before eligibility, attributes at t0, recency via `occurrence="last"`, `converted_7d`/`churned_30d` labels). The example output shows values with their `__complete`/`__reason` columns (T15).
- [ ] **Verify:** `uv run python examples/first_dataset.py`; every section removed from the README exists in a linked guide (checked by hand against the old README headings).

### Owner gate: publication

Owner chooses version and license and authorizes making the repository public (kata `v9yy`). No task runs this.

### Task 8: Refuse PostgreSQL-wire look-alikes (kata `1ky5`)

**Path:** bounded. **Branch:** `fix/postgres-identity`. **Phase:** 1, wave 2.

- [ ] **Red:** `test_server_identity_classification` on real `version()` strings: PostgreSQL 16 admitted; Redshift, Greenplum, CockroachDB refused with `terrine.backend.capability_unavailable`.
- [ ] **Green:** check identity at admission in `terrine/execution/_postgres.py`, before schema access.
- [ ] **Verify:** `make check TESTS="tests/execution"`; `make test-postgres`.

### Task 9: Dialect classes and one engine registry (kata `j60e`)

**Path:** architectural refactor; write its implementation plan when started (64 engine checks across 8 files plus the 5 engine lists listed in the issue). Behavior is unchanged except one documented user-visible change: refusal `route` text is derived from the registry (fixes the stale "DuckDB or PostgreSQL" route; CHANGELOG entry). **Branch:** `refactor/engine-registry`.

- [ ] Plan, review, bootstrap sub-tasks under `j60e`; move T8's server-identity classifier into `Engine.identify` and delete the original so exactly one classifier remains; verify with unit + integration tiers, `make test-postgres`, and an owner-dispatched `warehouse-backends` run.

### Task 10: Engine conformance suite (kata `w6pn`)

**Path:** bounded, tests only. **Branch:** `test/engine-conformance`.

- [ ] One known-answer case per regression in the issue; each shown red on a deliberate one-line break of its helper; DuckDB and PostgreSQL on every push. Cases start against today's fixtures; the task closes after T9 with the fixture parametrized from `ENGINES`.

### Task 11: Generic dialect and `allow_unverified` opt-in (kata `14ff`)

**Path:** architectural; short spec from the `7zj3` parent design, then plan. **After:** T9, T10. **Branch:** `feat/generic-backend`.

### Task 12: removed

Adding engines is out of scope for this plan (owner: "we wont add additional engines as part of this work"). The registry, conformance suite and generic opt-in (T9-T11) make a later engine a single dialect file plus a CI job; kata `6dmp` holds that backlog.

### Task 13: `EventAnchor` offset for landmark datasets (kata `9btd`)

**Path:** architectural-light; settle the issue's open decisions in a short design before code. Serialization stays compatible: `offset` is emitted only when nonzero, so existing saved Designs and their `spec_hash` values are unchanged; CHANGELOG entry. **Branch:** `feat/anchor-offset`. Runs in wave 5.

## Open decisions

- Resolved 2026-10-26 ("recommendations sounds good to me"): T8 stays before publication; `tcv2` and `12j7` are in phase 1 as T14 and T15 (option (a)); the warehouse-backends run gates the README. No new engines ("we wont add additional engines as part of this work").
- Plan review finding "allow a README milestone on the current API before T6" was rejected: the owner ordered API changes before the README ("1) focus on public api / usability changes 2) update the readme").
