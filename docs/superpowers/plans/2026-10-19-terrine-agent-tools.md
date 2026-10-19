# Dataset Agent Tools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide a constrained, versioned tool interface for agents to inspect Terrine dataset metadata, propose and validate typed Design edits, estimate/preflight work, and stage or refresh datasets only under explicit owner policy.

**Architecture:** `terrine.agent` is a thin adapter over existing `Model`, `Design`, compiler, preview, owner planning/snapshot and dataset/runtime APIs; it accepts only safe typed Design/`DatasetRecipe` serialization and never introduces an agent-specific compiler or execution path. A trusted `AgentPolicy` separates no-data inspection/validation/preview from owner-authorized source preparation, writes, export, row preview and cancellation. Only `OwnerPlanner` may capture and pin immutable execution inputs; agent-visible receipts are opaque references to owner-persisted `PlanInvocation`s. An optional MCP adapter maps to the same Python operations.

**Tech Stack:** Python 3.13+, existing Pydantic 2, SQLGlot, Ibis and DuckDB reference backend; JSON Schema/result envelopes; pytest. MCP SDK remains an optional extra.

**Spec:** [Task 12.1 in the Terrine build roadmap](2026-10-16-terrine-build.md#task-121-dataset-agent-tools), constrained by the binding [datasets-only scope decision](../specs/2026-10-18-terrine-scope-datasets-only.md). Reuse the dataset, materialization-runtime and ML-staging plans and APIs; do not implement estimation or causal-agent behavior.


## Global Constraints

- Dataset tools only: outputs are typed datasets, metadata and pinned references/manifests; no estimation, causal mode, inferential diagnostics or causal claims.
- No arbitrary SQL, Python expressions, importable callbacks, Catalog edits or caller-selected executable code in agent requests. Catalog SQL comes only from owner-approved definitions and remains subject to `admit_read_query` in the execution dialect.
- Owner-trusted configuration, never agent input, determines source/catalog and destination allowlists, permissions, runtime namespace, executor and budgets. Source credentials and connections stay caller-owned.
- Separate inspection/validation from source preparation, warehouse writes, exports, row preview and cancellation. Execution requires owner-granted capability and an owner-prepared receipt bound to typed Design/`DatasetRecipe`, model, retained source snapshot or authoritative source generation, destination, effective budget, change evidence and expected base generation; recheck authorization at execution. A no-data preview is not an executable receipt.
- Cost/scan unknowns remain unknown. Strict scan constraints fail closed when no verified bound exists; output limits do not imply scan limits. Budget checks cover preview and repeated tool calls; no automatic retries or budget escalation.
- Read-only SQL safety uses the execution dialect; reject SQL when read-only safety cannot be established. Use least-privilege backend credentials and make no promise that resource limits prevent all database-side effects.
- Responses expose metadata, aggregates and references by default, never customer rows, raw SQL, credentials or backend connection details. Bounded/redacted row preview requires a separate explicit permission.
- Prompt content, Catalog descriptions and tool output are untrusted data and cannot grant permissions or alter policy. Refusals use stable codes, immutable structured context and a suggested route forward when one exists.
- Preserve generation pinning, destination-scoped idempotent request-key replay, cancellation/publication behavior and recoverability of unpublished work by calling the existing owner planner, snapshot authority and runtime/dataset operations.

## Scope ledger

- Dataset discovery, typed Design/recipe validation, preview, planning, staging, refresh and pinned dataset references are authorized by the spec's In scope line: “Anchors, windows, aggregates, as-of attributes, expansion, all frame shapes (cross-section, sequential/scheduled, calendar and event panels, cohorts, LTV, time-to-event, state panels, sequences), static temporal validation, null-not-zero censoring, coverage/readiness, materialization runtime, ML staging, orchestrators, dataset agent tools.”
- Role-aware downstream hand-offs and descriptive dataset metadata are bounded by: “Dataset semantics stay: `treatment`, anchor, trial and cluster roles; `Schedule` (described as repeated time zeros, not as target-trial emulation); grace windows (descriptive).”
- Shared export preflight is explicitly authorized by: “**Export preflight (new, shared by every hand-off).** Before any library hand-off: incomplete outcome windows are refused or dropped with excluded counts per exact reason (never zero-filled or imputed); unknown eligibility is refused; repeated rows per independent unit require a cluster column; grace-defined treatment is refused as a baseline treatment. Move these checks out of `terrine/estimate/_data.py` and `panel.py` rather than rewriting them. One refusal code per hazard, shared across entry points.”
- Optional MCP transport is not explicitly authorized by the scope spec; owner-gated, and must remain an adapter over the authorized dataset tools. External object-store destinations are owner-gated under kata `rrsf`; do not treat this plan as authorization to access them.

## Review Focus

1. **Agent-forged permissions or source/destination escalation:** test every operation against a policy that denies the requested capability; ensure inputs cannot widen the trusted allowlists.
2. **SQL/callback injection and untrusted catalog text:** test that operation payloads cannot carry SQL/callbacks, and that owner-approved SQL is rejected by the existing dialect-aware read-only admission whenever safety is uncertain.
3. **Unknown scan cost or unbounded/over-budget preview:** test strict unknown scan refusal, output-vs-scan budget distinction, and count caps before execution.
4. **Stale plan/model/source/destination/budget or replay conflict:** test execution refusal after any binding changes; same request key and same fingerprint returns the same pinned result, while a conflicting fingerprint is refused.
5. **Secret/row leakage and retries/cancellation races:** test error sanitization and metadata-only defaults, separately permissioned redacted row preview, retry idempotency, and that cancellation/publication status never reports a false successful generation.

---

### Task 1: Typed, versioned tool contracts and trusted policy

**Files:**
- Create: `terrine/agent/__init__.py`
- Create: `terrine/agent/schemas.py`
- Create: `terrine/agent/policy.py`
- Create: `tests/agent/test_schemas.py`
- Create: `tests/agent/test_policy.py`
- Modify: `pyproject.toml` (optional MCP extra only when Task 5 adds its adapter)

**Interfaces:**
- Consumes: `terrine.errors.TerrineError`, `RefusalSpec`, `freeze`, `thaw`; `Model`, `Design`, `Budget`, `RefreshPlan`, `DatasetManifest`.
- Produces: `AgentPolicy`; versioned strict request/response records and JSON Schema for the operations in Tasks 2–4. Every response has `schema_version`, `status` (`ok`, `refused`, or `pending`), nullable stable `code`, immutable JSON `context`, nullable `suggested_route`, and nullable typed `output_reference`; successful outputs carry operation-specific typed fields, not arbitrary executable payloads. The contract rejects extra fields and non-JSON values.

- [ ] Add `tests/agent/test_schemas.py::test_tool_schemas_are_versioned_strict_and_round_trip`: assert the schema version is `1`; required fields, enums, and extra-field rejection; response context cannot be mutated; JSON serialization/thaw preserves the same values.
- [ ] Run `uv run pytest tests/agent/test_schemas.py -q` and confirm it fails because the agent contract is not implemented.
- [ ] Add `tests/agent/test_policy.py::test_policy_permissions_are_operation_specific_and_owner_configured`: assert inspect/validate do not confer prepare/write/export/preview/cancel; source and destination requests outside owner allowlists refuse; caller permission overrides are rejected; requested numeric budgets can only tighten owner bounds and cannot disable owner-required strict scan.
- [ ] Run `uv run pytest tests/agent/test_policy.py -q` and confirm it fails on missing `AgentPolicy` behavior.
- [ ] Implement explicit frozen `AgentPolicy` fields for enabled operations, trusted `ModelReference`/`ModelResolver`/`SnapshotAuthority`, separate inspect/validate/prepare/write/export/preview/read/cancel grants, allowed source/catalog identities, allowed destination namespaces/names, design-root, owner `Budget`, separately enforced max run time/concurrency/request counts, redacted-preview permission/column allowlist and owner-provided runtime executor. No agent-supplied policy fields are merged into it; a helper intersects requested and owner `Budget` row/output/scan-byte limits by tighter numeric bound and preserves owner-required `strict_scan=True`.
- [ ] Implement JSON-compatible strict request/response types, generated or hand-authored JSON Schema tied to `schema_version=1`, and explicit stable refusal-envelope conversion from `TerrineError` without message parsing; use `freeze`/`thaw` rather than duplicating error immutability rules.
- [ ] Run `uv run pytest tests/agent/test_schemas.py tests/agent/test_policy.py -q`; expected: PASS.
- [ ] Commit the contract and policy slice: `feat: add typed dataset agent tool policy`.

### Task 2: Catalog inspection and typed Design editing

**Files:**
- Create: `terrine/agent/tools.py`
- Create: `tests/agent/test_inspect_design.py`
- Create: `tests/agent/fixtures/catalog.yaml`

**Interfaces:**
- Consumes: Task 1's `AgentPolicy`, operation schemas and envelope; `Model.catalog`, `Model.sources`, `Model.metrics`; `Design`, `Design.spec_hash`; `DatasetRecipe`, `DatasetRecipe.from_json`; `terrine.spec.io.to_json`, `from_json`, `to_yaml`, `from_yaml`; `terrine.spec.validate.validate_design`, `ValidationReport`; `terrine.compile.dispatch.compile_design`.
- Produces: `DatasetTools(model: Model, policy: AgentPolicy, *, namespace: str, executor: Executor | None = None)` with `inspect_catalog()`, `list_capabilities()`, `get_design_schema()`, `get_dataset_recipe_schema()`, `design_example()`, `propose_design(serialized: str, *, base: str | None = None)`, `propose_recipe(serialized: str, *, base: str | None = None)`, `validate_design(serialized: str)`, `validate_recipe(serialized: str)`, `save_design(name: str, serialized: str)`, and `load_design(name: str)`. The schema/example resources expose versioned tagged Design nodes and one minimal editable Design derived from catalog metadata; recipe operations accept only `DatasetRecipe` with its typed training/scoring views. Proposals return typed values and deterministic field-level diffs without compiling or executing. Saved definitions are restricted to the configured design root and validated simple names.

- [ ] Add `tests/agent/test_inspect_design.py::test_inspect_catalog_returns_only_allowed_semantic_metadata`: verify sources, metrics, properties and dataset capability names are filtered by trusted source allowlist; no connection, credential, customer row or raw SQL appears.
- [ ] Add `test_propose_design_and_recipe_from_discovered_schema`: start with catalog-only inspection plus `get_design_schema()`/`design_example()`, construct a valid Design without a prebuilt serialized fixture, and construct a `DatasetRecipe` using the discovered versioned tagged-node schema; assert stable schema/hash round-trip and field-level diff, including a training split and scoring `as_of`.
- [ ] Add `test_validation_uses_existing_metadata_contracts`: validate a good Design/recipe using `validate_design`, verify required sources/obligations and no source-row execution; assert invalid windows/unknown metric return stable refusal code/context/route.
- [ ] Add `test_design_save_load_is_confined_to_owner_root`: allow a named safe Design/recipe; refuse path traversal, symlink escape and untrusted root override; reload identical canonical content and `spec_hash`/recipe digest.
- [ ] Implement `DatasetTools` inspection using bound Catalog descriptors only; capability discovery uses the existing validator/runtime supported-shape matrix (`ensure_runtime_shape_supported`) and reports dataset-only operations, with no causal-mode names or estimator status.
- [ ] Implement schema/example resources and parse/diff/validate/save/load via `terrine.spec.io`, `DatasetRecipe.from_json`, and `validate_design`; derive a valid example from the permitted Catalog, expose tagged serialization nodes, compute stable diff paths, and constrain atomic writes to policy's design root.
- [ ] Map `TerrineError` to the shared immutable refusal envelope and suggested route by stable refusal code; keep non-refusal programming errors from being disguised as success.
- [ ] Run `uv run pytest tests/agent/test_inspect_design.py -q`; expected: PASS.
- [ ] Commit the inspect/edit slice: `feat: add dataset catalog and design tools`.

### Task 3: Dry-run cost/preflight and owner-prepared execution receipts

**Files:**
- Modify: `terrine/agent/tools.py`
- Modify: `terrine/agent/schemas.py`
- Modify: `terrine/runtime/planning.py` (add typed `PlanRequest.dataset_recipe` while retaining the current Design field for base-design identity)
- Modify: `terrine/runtime/recipe.py` (persist/verify typed DatasetRecipe view in existing SavedPlanRecipe)
- Create: `tests/agent/test_plan_authorization.py`
- Modify: `tests/runtime/test_planning.py`
- Modify: `tests/runtime/test_recipe.py`

**Interfaces:**
- Consumes: Task 2 `DatasetTools`; `terrine.compile.cost.preview`; `Budget`, `Preview`; `terrine.runtime.planning.PlanRequest`, `PlanInvocation`, `OwnerPlanner`; `ModelReference`, `DestinationRef`, `SnapshotAuthority`; `DatasetRecipe`; `ChangeSet`; `run_serialized_plan`; `Dataset.plan_refresh`.
- Produces: `DatasetTools.preview_dataset(recipe: DatasetRecipe, *, budget: Budget | None = None, bounded_count: int = 0) -> PreviewResponse` (metadata-only compilation; counts only when separately permitted and bounded), `prepare_dataset(recipe: DatasetRecipe, *, destination: DestinationRef, budget: Budget | None = None, invocation_id: str) -> PlanReceipt`, and `prepare_refresh(dataset_reference: AgentDatasetReference, *, changes: ChangeSet | None = None, budget: Budget | None = None, invocation_id: str) -> PlanReceipt`. The owner-side immutable `PlanReceipt` references persisted `PlanInvocation`, captured snapshot, typed DatasetRecipe, trusted destination, effective budget and expected base generation; its agent projection contains only an opaque receipt ID and safe hashes/metadata. Preparation authorizes and captures source inputs via `OwnerPlanner.plan` and `SnapshotAuthority`; it is distinct from the no-data dry-run. Refresh resolves the persisted recipe for the authorized dataset reference and binds its expected base generation plus typed `ChangeSet`; absent change discovery uses explicit `conservative_rebuild`. If no owner SnapshotAuthority can retain/reopen inputs and no authoritative source generation is available, preparation refuses before issuing an executable receipt.

- [ ] Add `tests/agent/test_plan_authorization.py::test_preview_is_row_free_and_unknown_scan_cost_stays_unknown`: assert `preview_dataset` uses `terrine.compile.cost.preview`, performs no source-row execution by default, reports unknown scans/output where not measured, and strict unknown scan-byte budget refuses before any snapshot capture.
- [ ] Add `test_effective_budget_never_weakens_owner_limits`: owner `max_rows`, `max_output_bytes` and `max_scan_bytes` are intersected with requested bounds by minimum; owner-required `strict_scan=True` cannot be disabled; missing request bounds inherit owner limits; tighter requests remain tighter; max run time, concurrency and total tool calls are enforced by distinct owner policy fields.
- [ ] Add `test_prepare_uses_owner_planner_and_refuses_without_reopenable_snapshot`: authorized preparation goes through `OwnerPlanner.plan` and retains source snapshot inputs; no snapshot authority and no authoritative source version refuses with actionable route rather than issuing an executable receipt.
- [ ] Add `test_receipt_binds_dataset_recipe_change_evidence_and_base_generation`: training/scoring view identities (including splits and resolved scoring `as_of`), typed changes or conservative rebuild, destination, effective budget, hashes and expected generation are part of the saved invocation fingerprint; changed bindings conflict.
- [ ] Add `test_execute_replays_before_source_freshness_and_resolves_exact_invocation`: after a response-loss replay, same destination/request key and matching fingerprint returns existing run/published generation without reopening current sources; conflicting fingerprint refuses. A new request revalidates authorization and retained snapshot before execution.
- [ ] Add `test_plan_and_run_recheck_destination_source_and_budget_grants`: revoke/change any grant after preparation and assert refuse before write; unchanged grants execute only the exact owner-persisted invocation.
- [ ] Run `uv run pytest tests/agent/test_plan_authorization.py -q` and confirm the tests fail for the missing operations/guarantees.
- [ ] Implement effective-budget intersection, preserving strict owner constraints. Do not treat agent-provided limits as policy or permit a looser caller budget.
- [ ] Keep `preview_dataset` row-free by default through `terrine.compile.cost.preview`; allow only explicitly authorized `bounded_count` and pass the cap unchanged. It must not capture a source snapshot or report output limits as scan bounds.
- [ ] Add `tests/runtime/test_planning.py::test_plan_request_round_trip_preserves_dataset_recipe_view`: construct a typed training `DatasetRecipe` with explicit split metadata and a scoring recipe with resolved `as_of`; round-trip `PlanRequest` JSON and assert the recipe digests, base Design hashes and typed view fields are preserved.
- [ ] Run `uv run pytest tests/runtime/test_planning.py::test_plan_request_round_trip_preserves_dataset_recipe_view -q`; expected: FAIL until `PlanRequest` serializes the optional recipe field.
- [ ] Add `tests/runtime/test_recipe.py::test_saved_plan_recipe_round_trips_typed_dataset_view`: serialize/reopen owner-saved stage and refresh recipes; assert training split and scoring `as_of`, base Design, expected base generation and `ChangeSet` remain bound, and altered view/evidence fails the fingerprint.
- [ ] Run `uv run pytest tests/runtime/test_recipe.py::test_saved_plan_recipe_round_trips_typed_dataset_view -q`; expected: FAIL until `SavedPlanRecipe` carries typed DatasetRecipe views.
- [ ] Extend `PlanRequest` with optional `dataset_recipe: DatasetRecipe`; require its `base_design` to match the existing Design field. Have `OwnerPlanner.plan` resolve the exact recipe view before required-source discovery, preflight compilation, validation and snapshot capture. Use that view's effective Design and capture only its required inputs; feature-only scoring must not require readiness for, or read, an unused label source. Persist the complete DatasetRecipe/view, base Design identity and expected base generation in `SavedPlanRecipe`, together with typed `ChangeSet`. Keep existing Design-only orchestration callers behaviorally unchanged and run their focused planner tests.
- [ ] Extend runtime tests to prove `PlanRequest` JSON round-trip and same invocation id idempotently retain the typed recipe; changed recipe view conflicts. Extend `SavedPlanRecipe` resolution to return the exact recipe view, not only its base Design.
- [ ] Add a scoring preparation regression with no readiness for its unused label source; assert preparation succeeds, the source remains unread and absent from retained inputs, and the saved recipe still binds the complete base Design and resolved scoring view.
- [ ] Run `uv run pytest tests/runtime/test_planning.py tests/runtime/test_recipe.py tests/adapters/test_airflow.py tests/adapters/test_dagster.py tests/adapters/test_dbt_core_recipe.py -q`; expected: PASS with Design-only orchestration requests unchanged and typed DatasetRecipe views preserved.
- [ ] Store internal invocation/recipe material owner-side. Return only a versioned, opaque receipt reference and safe hashes through the agent envelope. Reauthorize source/destination/write/export grants before resolving that receipt.
- [ ] Implement `execute_plan` by resolving the exact owner-persisted invocation and calling `run_serialized_plan`; its destination-scoped request-key replay/fingerprint check must happen before new freshness/reopen checks. Do not recompile or silently construct a new plan during execution, and never accept an agent-provided `Executor` or `SnapshotAuthority`.
- [ ] Run `uv run pytest tests/agent/test_plan_authorization.py -q`; expected: PASS.
- [ ] Commit the plan/preflight slice: `feat: add owner-prepared dataset agent plans`.

### Task 4: Dataset materialization, safe preview and run feedback

**Files:**
- Modify: `terrine/agent/tools.py`
- Modify: `terrine/agent/schemas.py`
- Create: `tests/agent/test_materialize.py`
- Create: `tests/agent/test_runtime_feedback.py`
- Create: `integration/agent/test_dataset_tool_flow.py`
- Create: `examples/dataset_agent_tools.py`

**Interfaces:**
- Consumes: Task 3 `PlanReceipt`/`execute_plan`; `terrine.dataset.reference.stage_frame`, `Dataset.manifest`, `Dataset.plan_refresh`, `Dataset.refresh`, `Dataset.export_parquet`; runtime `run_plan`, `get_run`, `read_run_events`, `cancel_run`, `open_published_generation`; existing DuckDB `DesignExecutor` and `Executor` protocol.
- Produces: owner-authorized `stage(receipt, request_key) -> AgentDatasetReference`, `refresh(dataset_reference, receipt, request_key) -> AgentDatasetReference`, `export_parquet(dataset_reference, *, destination, partition_by, budget) -> ExportManifest`, `preview_rows(reference, *, limit, columns)`, `get_run_status(run_id)`, `get_run_events(run_id, *, after, limit)`, and `request_cancel(run_id)`. `AgentDatasetReference` is a versioned safe projection of the internal `DatasetManifest`: approved dataset/generation refs, hashes, safe column roles/types, completeness summary and opaque recipe reference only; it excludes recipe literals, raw SQL and unapproved metadata. Every run/dataset lookup resolves trusted runtime metadata first and checks namespace, destination, ownership and relevant source/write/export/read/cancel grants. Export requires its separate capability and owner-allowlisted destination. Row preview requires its independent permission, a positive row cap and policy-allowed projected/redacted columns.
- [ ] Add `tests/agent/test_materialize.py::test_stage_refresh_and_export_require_separate_capabilities`: denied write/export requests produce no write; authorized stage returns an `AgentDatasetReference` with no internal recipe literals; refresh returns a new generation and leaves the old reference unchanged; Parquet export requires separate export permission and an allowed destination.

- [ ] Add `test_preview_is_denied_by_default_and_bounded_redacted_when_granted`: deny rows without explicit preview permission; with permission, reject unapproved columns and nonpositive/over-limit row counts before query, redact configured sensitive columns, and return no other source fields.
- [ ] Add `tests/agent/test_runtime_feedback.py::test_status_events_and_cancel_require_run_destination_ownership`: create two datasets/runs in one namespace; verify a grant for one destination cannot inspect status/events, preview rows, or cancel a run from the other; pages are cursor-bounded and cancellation returns the runtime's actual disposition.
- [ ] Run `uv run pytest tests/agent/test_materialize.py tests/agent/test_runtime_feedback.py -q` and confirm expected initial failures.
- [ ] Implement materialization and refresh as wrappers around existing pinned dataset APIs; use the runtime's stable request key for replay and never publish a second generation on duplicate request.
- [ ] Implement `export_parquet` by calling `Dataset.export_parquet` only after the separate export grant, allowed destination, and budget are checked; return its `ExportManifest`, never file rows or credentials.
- [ ] Implement status/event/cancel wrappers using `get_run`, `read_run_events`, and `cancel_run`; first resolve each run ID to its persisted destination, dataset, owner and sources, then apply operation-specific grants before accessing runtime state. Keep pending/unknown/cancel-requested distinctions and do not treat cancellation request as cancellation completion.
- [ ] Implement `AgentDatasetReference` as a versioned allowlisted projection; do not serialize internal `DatasetManifest` fields or recipe literals by default. Resolve preview to the same authorized reference; read `Dataset.to_arrow_batches(batch_size=min(limit, configured_batch_cap))`, stop at `limit`, close the iterator, and only then return policy-approved projected/redacted columns.
- [ ] Add `examples/dataset_agent_tools.py` as an executable local end-to-end example using an owner-created restricted policy and a typed training or scoring `DatasetRecipe`; show preview then separately authorized preparation, execution, safe manifest projection and refresh. Use sanitized Catalog descriptions; do not print or log source rows, recipe literals or credentials.
- [ ] Run `uv run pytest tests/agent/test_materialize.py tests/agent/test_runtime_feedback.py integration/agent/test_dataset_tool_flow.py -q`; expected: PASS.
- [ ] Commit the end-to-end dataset tool slice: `feat: stage and refresh datasets through agent tools`.


### Task 5: Optional MCP adapter and adversarial eval coverage

**Files:**
- Create: `terrine/agent/mcp.py`
- Create: `tests/agent/test_mcp.py`
- Create: `evals/agent_dataset_tools.yaml`
- Modify: `pyproject.toml`
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Create: `docs/guides/dataset-agent-tools.md`

**Interfaces:**
- Consumes: Tasks 1–4 public Python operations and their versioned JSON Schemas/envelopes.
- Produces: optional `mcp` extra and thin MCP registration that dispatches only the defined `DatasetTools` operations; no alternative compiler, SQL tool or hidden execution route.

- [ ] Add `tests/agent/test_mcp.py::test_mcp_exposes_only_versioned_dataset_operations`: verify optional dependency absence is a coded actionable refusal; when installed, registration names and JSON schemas exactly match the Python contract and each call dispatches to its corresponding method without widening policy.
- [ ] Add eval cases for invalid windows, unknown eligibility, unsupported shapes, unbounded/strict-unknown preview, stale hashes, source/destination escalation, SQL/callback injection, prompt-injected Catalog descriptions, secret exfiltration, repeated request-budget exhaustion, replay/conflict, and cancellation/retry/publication race; assert stable codes/routes and that no unauthorized write occurs.
- [ ] Run `uv run pytest tests/agent/test_mcp.py -q` and confirm the optional-adapter tests fail before implementation (skip only the installed-extra case when the optional extra is absent).
- [ ] Implement the thin optional adapter and dependency extra, register only operations from `DatasetTools`, and ensure MCP serialization preserves immutable contexts by emitting thawed JSON copies.
- [ ] Run the eval fixture against the local restricted DuckDB integration flow; tool-schema checks alone are not acceptance evidence.
- [ ] Run `uv run pytest tests/agent -q` and `uv run pytest integration/agent/test_dataset_tool_flow.py -q`; expected: PASS.
- [ ] Update README/guidance with exact permissions, supported dataset operation list, local execution recipe, unknown scan-cost caveat, refusal routes, privacy boundaries and optional MCP install. Update `CHANGELOG.md` for the public tool surface; make no causal/estimation claims.
- [ ] Commit the MCP/docs/eval slice: `feat: add optional MCP dataset tool adapter`.

## Open Decisions (recommendation)

- **Saved Design storage:** Recommend a trusted, owner-configured design root with safe simple-name paths and atomic replacement, because the current spec codec is safe but no named Design registry exists. Change this only if an owner-provided registry API is adopted before implementation; never let agent input select an arbitrary filesystem path.
- **MCP SDK/API version:** Pin a supported version when Task 5 implementation confirms the chosen protocol SDK is compatible with Terrine's Python floor; keep it an optional extra and test core import with the SDK absent. Do not delay or couple Python tools to MCP.
- **Strict scan budget usability:** Recommend fail-closed when requested scan bytes cannot be verified, with suggested route to use a backend that reports a verified bound or explicitly choose a non-strict budget. This follows current `terrine.compile.cost._scan_policy` semantics; never silently downgrade strict mode.
- **No retained snapshot / no authoritative source generation:** Recommend refuse `prepare_dataset` with stable code and route to configure owner-managed snapshot retention or source-version support; metadata-only preview remains available and never claims replay safety.
- **Broader roadmap capabilities:** This plan implements only the user-authorized Task 12.1 dataset tools (including optional MCP), not the other handoff/master roadmap families. Recommend that the owner review those separate scope additions in the roadmap plan rather than treating this plan or review as approval.
