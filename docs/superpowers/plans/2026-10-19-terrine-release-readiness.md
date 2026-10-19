# Terrine Release Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the existing datasets-only package installable, understandable and independently verifiable without publishing it.

**Architecture:** Retain Model → Design → Frame → Dataset as the everyday API. Execute the exact first-dataset documentation against local DuckDB, derive refusal reference material from module-owned declarations, and validate distributions in isolated environments. Compatibility claims distinguish exercised local execution from unverified remote backends.

**Tech Stack:** Python 3.13/3.14, uv, pytest, Ibis/DuckDB, Hatchling, MkDocs Material, Ruff, ty, Tach, GitHub Actions.

**Spec:** Roadmap Task 13.1 in `docs/superpowers/plans/2026-10-16-terrine-build.md`; binding owner decision `main:docs/superpowers/specs/2026-10-18-terrine-scope-datasets-only.md`; `AGENTS.md`.

## Global Constraints

- Terrine outputs datasets and descriptive summaries; downstream libraries own estimation. No causal claims or effect estimators.
- Requires Python `>=3.13,<3.15`; native use must not require increment or any optional integration.
- Public facades have explicit reviewed `__all__`; supporting constructors require a concrete user workflow. No aliases or compatibility shims.
- Limitations describe inherent source, construction or validity constraints only; absent implementations are compatibility statuses, not inherent limits.
- No public push/tag/publish before owner approval after increment's launch. No version choice or licensing changes.
- Use only the assigned release worktree; no subagents, other worktrees or kata operations.

## Review Focus

- A fresh wheel user without source checkout must execute the exact README first dataset and reopen its pinned Dataset (Task 1/3).
- Importing base Terrine and its focused facades must not import optional integrations, even when they are installed (Task 1/3).
- New module-owned refusal declarations, including inline declarations, must appear in the generated reference with every message variant, template field and recovery category (Task 2).
- Python 3.14 and lowest direct dependency resolution must not silently lose the local dataset path (Task 3).
- An sdist rebuild must retain typed package data, examples, guides and notices, without local credentials or build debris (Task 3).

---

### Task 1: Deliberate public API and executable first dataset

**Files:** Review `terrine/__init__.py`, every `terrine/**/__init__.py` and public module `__all__`; update affected callers; modify `README.md`, `tests/test_optional_dependencies.py`; create `tests/test_documentation.py` and `examples/first_dataset.py`.

**Interfaces:** Consume existing Model/Design/Frame staging and Dataset reopening. Produce one self-contained README Python block using file-backed temporary DuckDB, a native Catalog, explicit coverage, bounded materialization and pinned Dataset reads.

- [ ] Add a regression executing the README block in a temporary directory and asserting known feature/outcome rows, roles and reopened generation parity; run it red before replacing the incomplete quickstart.
- [ ] Audit every facade against concrete documented workflows; remove accidental compiler/proof/runtime exports and migrate all callers to owning modules, retaining focused advanced APIs where earned. Record dispositions in the API guide.
- [ ] Make the exact quickstart runnable both as a tested README block and as an example without duplicating the tutorial implementation. Extend optional-import isolation across the base and focused facades.
- [ ] Run focused doc/import/API tests plus the per-commit gates below; commit this slice.

### Task 2: Complete user documentation

**Files:** Create `mkdocs.yml`, `docs/index.md`, `docs/guides/index.md`, `docs/guides/first-dataset.md`, `docs/guides/public-api.md`, `docs/guides/limitations.md`, `docs/guides/capabilities.md`, `docs/reference/refusals.md`; extend `tests/test_documentation.py`; update `pyproject.toml`, existing guides and `CHANGELOG.md`.

**Interfaces:** Consume module-owned `RefusalSpec` declarations, existing executable examples, integration/recovery tests and benchmark evidence. Produce a deterministic refusal reference with codes, message variants, template interpolation fields (not exhaustive context schemas), source links and linked recovery categories; a private docs generation/check command; complete navigable MkDocs guides. Discover every `RefusalSpec(...)` call through Python AST traversal, including branch/function-local declarations, without importing integrations or executing refusal branches. Preserve multiple declarations of one code. Emitted `TerrineError.context` remains the authoritative structured context; the reference must explicitly distinguish interpolation fields from additional per-request context.

- [ ] Add a failing reference completeness check against every statically discovered declaration, and representative real emitted-error checks for template fields and documented recovery routes. Validate the generator on a small inline/duplicate-code fixture; refuse nonliteral declarations rather than silently omitting them. Recovery categories live in the documentation generator, keyed by code namespace with an explicit unknown-namespace failure, not in a second runtime registry.
- [ ] Add MkDocs Material in an isolated docs dependency group. Connect first-dataset, source/SCD2/coverage, precise temporal semantics, metric × shape × backend capabilities, cost/residency, recovery and benchmark evidence through the guides index. Keep inherent limits separate from implementation support status.
- [ ] Generate the refusal reference from declarations; document regeneration and actionable recovery categories. Consolidate CHANGELOG into one Unreleased section in product terms, removing superseded estimation claims.
- [ ] Run focused documentation tests, strict MkDocs build and per-commit gates; commit this slice.

### Task 3: Compatibility, notices and reproducible distributions

**Files:** Modify `.github/workflows/ci.yml`, `pyproject.toml`, `uv.lock`, `THIRD_PARTY_NOTICES.txt`, `docs/guides/backend-compatibility.md`; create/extend distribution tests under `tests/integration/` and compatibility documentation under `docs/guides/`.

**Interfaces:** Consume the tested README block, built wheel/sdist and project dependency declarations. Produce a reusable clean-wheel smoke command and sdist content/rebuild checks, used by CI on Python 3.13 and 3.14 with locked and `--resolution lowest-direct` environments.

- [ ] Add distribution checks for package data (`py.typed`, simulator YAML), examples/docs/notices and clean-wheel first-dataset execution outside the source tree; exercise missing optional libraries without a sibling checkout.
- [ ] Resolve/test supported floors with uv `--resolution lowest-direct` in isolated environments; raise only demonstrably incompatible dependency floors, documenting measured combinations and optional-extra Python restrictions. Do not weaken requirements to hide errors.
- [ ] Complete direct-dependency and optional-extra third-party license/notice inventory from installed distribution metadata/upstream notices; leave Terrine's license choice to the owner.
- [ ] Add CI for Python/dependency combinations, optional isolation, all top-level examples, docs and distribution checks. Run focused checks plus per-commit gates; commit.

### Task 4: Final evidence and review closure

**Files:** Update this plan's completion record; write untracked `.superpowers/sdd/2026-10-19-terrine-release/report.md` (never stage).

**Interfaces:** Consume all slices and roborev findings. Produce recorded commands, observed outputs, commit IDs, review dispositions and owner actions.

- [ ] Run `roborev-plan-review docs/superpowers/plans/2026-10-19-terrine-release-readiness.md docs/superpowers/plans/2026-10-16-terrine-build.md AGENTS.md` once before implementation; fix valid findings and commit the plan after gates.
- [ ] After commits use `roborev list --repo /Users/kylejcaron/terrine --branch feat/release-readiness --open --json --limit 20`, wait/show each review, fix valid findings with red regressions, comment as `roborev-fix` and close; at most three fix cycles.
- [ ] Run `uv run pytest -q -n 8 tests`, `uv run pytest -q -n 4 integration`, `uv run python -m tests.integration.study_api_smoke`, every `examples/*.py`, `uv build`, sdist check/rebuild and the clean-venv wheel smoke. Record actual output and any credential-gated integration skips without claiming remote execution.

## Per-commit gates

`uv run ruff format terrine tests examples integration`; `uv run ruff check terrine tests examples integration`; `uv run ty check terrine tests`; `uv run tach check`; focused tests for the slice. Never format docs or root paths.

## Owner-reserved open items

- Choose the release version; this work does not change `0.1.0` into a release decision.
- Confirm Terrine's license/open-core boundary and approve any release LICENSE; third-party inventory does not grant a Terrine license.
- Confirm increment's public launch and authorize publishing timing.
- Authorize and perform any public push, tag, GitHub release or PyPI publication. None is part of readiness verification.

## Review and execution record

Native execution in the existing release worktree is required by the assignment. Documentation generation and CI configuration use the TDD configuration/generated-output exceptions; executable code and behavior fixes receive red regressions. No numerical methods are changed; all dataset-value claims are checked by executed examples/tests.

Plan review 9056: changes required. Accepted the obsolete-estimation finding and corrected the master roadmap's decision/coverage claims. Accepted the incomplete-refusal-contract finding: documentation now promises exact declaration/template coverage, explicitly not exhaustive emitted-context schemas, and tests inline declarations, duplicate code variants and representative emitted errors. Declined the additional-scope finding: the owner's binding datasets-only decision explicitly retains panel/trajectory/state/sequence families, simulator worlds, runtime, orchestration and dataset agent tools; this assignment authorizes readiness of already-merged features, not new capabilities. No scope expansion or re-review is required; the requested review ran once.
