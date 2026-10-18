# Sequential Panels Implementation Report

## Task 1 — Sequential, CalendarPanel, EventPanel shape contracts

- Implemented canonical `Initiation`, `PersistentAdoption`, `TrialWeight`, `SequentialSpec`, and `EventPanelSpec`; shape lowering, design JSON round-trips, and direct/constructor treatment conflict refusals.
- Added explicit single-dispatch compilers. EventPanel expands each event into declared relative periods with typed struct anchor IDs, exact elapsed row origins, parent event identity/time, and shared value-column/window compilation.
- Added typed panel provenance and Arrow encoding/decoding while retaining StatePanel metadata decoding and dispatch. Added a pandas period-row reference, spec/compiler/integration tests, executable example, guide, and changelog entry.
- RED evidence: initial spec test failed on missing `EventPanel`; initial compiler test emitted 2 parent rows instead of 6 declared event-period rows. Both were corrected.
- GREEN: `uv run pytest tests/spec/test_panels.py tests/compile/test_panels.py tests/integration/test_sequential_panels.py -q` — 9 passed; after review fixes, focused panel/StatePanel Arrow checks — 15 passed.
- Example: `uv run python examples/sequential_trials.py` — sequential and event panels each materialized and Arrow-round-tripped (6 rows each).
- Gates: `uv run ruff format terrine tests examples`; `uv run ruff check terrine tests examples` passed; `uv run ty check terrine tests` passed; `uv run tach check` passed.
- Feature commit: `97ba320` — `feat: add sequential and event panel shapes`.

### Review 8469 — Fail, findings verified and fixed in `1db152b`; closed

1. Restored StatePanel metadata decoding. Reproduced prior Arrow round-trip failure; regression now passes.
2. Encoded explicit TrialWeight mappings as JSON-compatible dictionaries; normalized safely by max-scaling, rejected nonfinite/zero normalized masses, and added explicit-weight round-trip coverage.
3. Sequential validation now collects declared source property/fact names and dimension property keys without hashing `BoundDimension` records.
4. Direct `Design(shape=SequentialSpec(...), treatment=...)` now uses the same coded conflict refusal as the convenience constructor.
5. Normalized history starts to UTC and added equivalent-offset equality coverage.
6. Focused regressions plus ruff, ty, and tach passed after these changes.

- Review-fix commit: `1db152b` — `fix: preserve panel metadata and history contracts`; review 8473 found canonical-mass drift on explicit-weight JSON round-trips.

### Review 8473 — Fail, finding verified and fixed in `1ca688b`

- Reproduced floating-point mass drift for explicit weights `{a: 1, b: 5, c: 7}`; added a RED regression checking Design equality and `spec_hash`.
- Preserved validated already-normalized masses during decode; focused tests then passed: 3 passed.
- Commit: `1ca688b` — `fix: preserve normalized trial weights on load`.
- Review 8473 closed after fix evidence; review 8483 for `1ca688b` passed and was closed.
- Final fix gates: `uv run ruff format terrine tests examples`; `uv run ruff check terrine tests examples`; `uv run ty check terrine tests`; `uv run tach check` — all passed.

Rulings: none.


## Incomplete roadmap work

Tasks 3–5 remain incomplete: panel AIPW, stacked DiD, diagnostics, and calibration are not implemented.

## Task 2 — Trial histories, weights, and matched t0

- Committed the canonical `MatchedT0Sample` Design/JSON/hash slice early as `8552024` (`feat: serialize matched-t0 sample policy`).
- Sequential selectors now bind to their declared source fact/property, require readiness coverage from `history_start` through the schedule boundary, and classify initiation exactly at each scheduled t0. Persistent adoption remains distinct and runtime history contradictions refuse; later initiation by baseline controls refuses unsupported sustained no-initiation strategies.
- Sequential frames expose trial identity and declared normalized trial weights, identify an explicitly named `target_weight` feature in `Roles`, and retain sampled-control inclusion probability, role information, sample policy, and sampling provenance through Frame recompilation/materialization and Arrow metadata.
- Matched-t0 sampling operates within contemporaneous t0/strata risk sets, keeps every treated unit, samples the requested number of controls without replacement by deterministic seeded identity rank, records exact inclusion probabilities, and refuses insufficient controls. `matched_t0(base, model, sample=...)` lowers the policy onto canonical Design state.
- RED/GREEN regressions cover complete history bounds, exact-t0 classification, contradictory adoption, later initiation refusal, trial-weight columns, target-weight role, sample/hash policy, risk-set probability, insufficiency, row-order invariance, and Arrow/materialization persistence.
- Final focused checks: the focused five-file pytest command passed 39 tests; `ruff check terrine tests examples`, `ty check terrine tests`, and `tach check` passed.
- Review 8486 flagged missing sampling application/constructor preservation (addressed in `cbb5a2d`) and omitted `anchor_range` defaults (fixed with a regression in the following fix commit); review disposition: fixed findings commented and closed.

### Review 8500 remediation

- Fixed matched-t0 controls-only/zero-treated ranking and collision-safe internal counts; reserved generated sequential outputs; fact selectors now use row occurrence; timestamp comparisons, persistent fact-selector refusal, treatment follow-up coverage, per-unit roster observation limits, and eligible-population follow-up checks were corrected.
- Updated integration history declarations and the executable sequential example for the strengthened coverage and no-later-initiation contract.
- Review remediation commits: `6c4c5a3` and `74991ef`; 8500 closed after dispositions recorded. Review 8502 was a pass and already closed.
- Follow-up review 8511 caught incorrect schedule endpoint handling and missing roster-bound test coverage; endpoint semantics were corrected and added exact-boundary, earlier-bound, and eligibility-exclusion materialization regressions.
- Focused verification: `uv run pytest tests/compile/test_panels.py tests/compile/test_matched_t0.py tests/integration/test_sequential_panels.py -q` — 21 passed; ruff, ty, tach, and `uv run python examples/sequential_trials.py` passed.
- Review 8520 follow-up remains pending; its only finding requested regression coverage, now added and verified. Tasks 3–5 remain incomplete.
