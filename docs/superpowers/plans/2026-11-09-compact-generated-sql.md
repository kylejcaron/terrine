# Compact Generated SQL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce generated SQL, query execution and repeated expression-construction overhead while preserving exact timestamp, numerical, dataset and execution contracts.

**Architecture:** Reuse exact timestamp coordinates and declaration-certified native shifts; share window context/membership; narrow per-output finalization branches. Keep Ibis expression generation, dialect ownership and execution-session rebinding. Measure against the existing literal-SQL oracle rather than only against old Terrine.

**Tech Stack:** Python >=3.12; Ibis >=11,<13; DuckDB >=1.5,<2; SQLGlot >=29,<30; PyArrow; pytest. PostgreSQL/BigQuery/Snowflake extras retain their Ibis >=12,<13 floor.

**Spec:** `docs/superpowers/specs/2026-11-09-compact-generated-sql-design.md`

## Scope ledger

Owner request: “can we make a plan to address these things?”, following their
explicit selection of these four areas. No additional roadmap capability is
adopted from historical plans or findings.

| Confirmed planning scope | Owner-selected wording | Tasks |
|---|---|---|
| Timestamp lowering | “Make timestamp lowering compact without weakening correctness” and native comparisons only where representability is proved | 2, 3 |
| Aggregate sharing | “Share work by source and window, not output” | 4 |
| Construction/public execution | “Reduce duplicate expression construction and recompilation without caching stale validation results” | 1, 5 |
| Comparative evidence | “Judge every change against the saved handwritten benchmark” | 1 and every milestone; 6 |

Excluded: new public APIs/flags, persistent query intermediates, new arithmetic
policies, changed source/refusal semantics, global SQL/result caches, broad
compiler rewrites, unrelated recipe work, remote spending, push/merge authority.
No task promises 50× or exactly handwritten runtime.

### Approved bounded continuation

Original Tasks 1–5 are complete; Task 6 remains the final acceptance checkpoint.
At `02acf0b`, README/state-panel formatted SQL ceilings still failed. The owner
replied “sounds good to me” to the recommendation for a bounded optimization pass
targeting those two constructions, not a waiver. Tasks 7 and 8 below precede a
fresh Task 6 verification. They add no other recipe or compiler capability.

| Additional confirmed scope | Approval provenance | Task |
|---|---|---|
| Daily-envelope integer-part reuse | Owner's response to the bounded daily-envelope/state-transition recommendation | 7 |
| Check-preserving state-query compaction | Same response; original refusal/validation domains remain fixed | 8 |

The continuation base is `02acf0b9203786b02ac59502c5147526ffc8cfd6`.
Use the existing integration owner, branch, tracker parent and evidence lineage.
Do not reopen completed tasks or bootstrap a duplicate umbrella.

## Global Constraints

- Keep Model → Design → Frame → Dataset unchanged. No new dependencies, exports,
  caller-authored proof objects or compatibility shims.
- Use Ibis expressions in study code. Engine-specific native operations stay in
  the existing concrete dialect modules; no raw-SQL production shortcut.
- Exact integer/decimal and timestamp domains, native/exact float policies,
  NULL/reason/completeness semantics and per-output refusal precedence remain.
- Unsupported inputs still refuse before source reads where they do today.
  Preserve checks, budgets, full anchor identity, pinned snapshots and provenance.
- `Frame._compiled` is not an execution snapshot. Materialization must rebind and
  compile inside the current `ExecutionContext`; no reuse of validation results.
- Keep the benchmark's literal SQL, fixtures, source schemas, independent oracle,
  typed Arrow comparison and negative controls. Never overwrite prior evidence.
- No wall-clock assertions. Preserve consumer-facing SQL-length budgets; use
  throwaway inspection for expression-sharing mechanics, not source-text tests.
- Tests are derived from contracts, not implementation output. Extend existing
  cases first. Every added regression has a named bug and an observed red check
  against old code or a deliberate one-line break. Never stash user work.
- Workers run only their changed test modules and static gates at coherent
  milestones. The affected integrated selection runs once in CI when published,
  or once on the integration branch when local-only. No full selection per lane.
- CPU-heavy commands use agent-slots; do not disable pytest's shared-core plugin.
  Benchmark runs are serialized, one DuckDB thread, with profilers disabled in
  timed samples.
- No new cloud run/spend or publishing without owner authority. Preserve existing
  warehouse parity selections and report missing live evidence honestly.

## Review Focus

1. **Schedule range proof:** first/second alignment samples do not bound the last
   origin. Task 3 checks true first/last origins and unknown/overridden origins.
2. **Virtual timestamp endpoints:** large shifts, infinities, negative subsecond
   remainders, mixed zones and ns/us input must remain exact. Task 2 preserves the
   existing integer/rank oracles; Task 3 verifies conservative fast-path declines.
3. **Cross-output contamination:** unavailable or count-only requests must not
   activate invalid numerical payloads. Task 4 extends coverage/masking cases.
4. **Dropped finalization dependencies:** extra anchor fields and deferred check
   columns must survive narrower branches. Task 5 adds them to existing cases.
5. **Stale safety state:** source mutation, failed budgets, changed native
   ownership and retained snapshots must still be handled per call. Task 5
   exercises existing runtime regressions and a same-Frame mutation scenario.

## Evidence, file ownership and PR shape

Base: `08bbba69e4432a205aa68c3d5b9c720480271f05`, containing the saved corrected
benchmark; production source baseline `64d7caf4f7925a8bd744b9ceafd4731d1eb67015`.
The baseline manifest is
`benchmarks/results/direct-sql-comparison-20261109-v2.json`.

| Area | Owning files | Ownership |
|---|---|---|
| Comparator diagnostics | `benchmarks/compare_sql.py`, existing `benchmarks/run_overhead.py`, `benchmarks/README.md` | Task 1; frozen before milestone timings |
| Exact timestamp representation | `terrine/backends/timestamps.py`, `terrine/temporal/aggregate.py` | Task 2 |
| Certified native shifts | `terrine/backends/dialect.py`, `terrine/backends/engines/duckdb.py`, `terrine/compile/numeric.py`, `terrine/compile/columns.py`, `terrine/temporal/aggregate.py` | Task 3, after 2 |
| Context/membership reuse | `terrine/temporal/aggregate.py`, `terrine/temporal/daily.py` only where needed for staging | Task 4, after 3 |
| Narrow finalization | `terrine/temporal/aggregate.py`; preserve `daily._batch_projections` contract | Task 5, after 4 |
| Final evidence and shared documentation | new result directories/report, `docs/guides/cost-and-residency.md`, `CHANGELOG.md`, benchmark guide | Task 6 only |

**One integration PR.** Tasks 1 and 2 can use independent isolated worktrees from
the same base; Task 2's measurement waits for Task 1 to land in integration.
Tasks 2–5 share `aggregate.py` and run in one ordered lane. They are not parallel
PRs or a squash-conflicting stack. Read-only test/evidence review can run beside
implementation, but no concurrent heavy benchmarks. Shared guides/changelog are
owned by Task 6; each preceding task updates only relevant existing docstrings.

Recommended execution: subagent-driven, a fresh implementer/reviewer per task,
with one integration owner and explicit task base/worktree/branch. At execution
start choose the actual Orca-managed integration worktree; store its absolute path
and the full-selection regexes in parent `harness.qa`. Do not blindly reuse a
previous planning checkout or another effort's active parent.

**Execution status at the bounded continuation:** Tasks 1–5 below are completed
history, reflected by checked steps; do not dispatch them again. Task 6's first
verification/evidence pass is committed through `02acf0b`, but its acceptance
remains open for the two SQL budgets. Active work is Tasks 7/8, then repeating
the remaining Task 6 gates at their integrated head. Existing task identities:
1=`t5fy`, 2=`8x07`, 3=`k7gb`, 4=`vp3t`, 5=`r1yh`, 6=`5ggg`, parent=`ce4r`.

## Task 1: Preserve the comparator and attribute compiler phases

**Files:** modify `benchmarks/compare_sql.py`, reuse instrumentation in
`benchmarks/run_overhead.py`, update `benchmarks/README.md`.
**Dependencies:** none; can run beside Task 2 in a separate worktree.

**Interfaces:** keep `run(case, out, repeats, units, events_per_unit)` backward
compatible for existing benchmark callers; add keyword-only
`diagnostics: bool = False, profile: bool = False`. CLI gains `--diagnostics` and
`--profile`. Diagnostic records are additive fields/files, not replacement timing
samples. No production module API changes.

- [x] Add one separate instrumented pass that calls the real build and public
  materialization paths. Reuse `run_overhead.compilations()` for SQL translation
  records. Record compile-design calls, expression construction, execution-model
  rebinding, admission, statement rendering/check/execute and stream consumption
  where a genuine boundary exists. Record unknown/unseparated phases as such;
  annotate nested timers instead of summing them.
- [x] Optional cProfile files are separate from timing samples. Instrumentation
  delegates original methods and restores them in `finally`, including failure.
  Keep metadata/admission probes distinguished from captured study statements.
- [x] Retain every existing oracle/control; a failed instrumented run is a failed
  benchmark, not an omitted result. Hash any newly imported benchmark helper in
  environment identity. Freeze the helper revision for all milestone comparisons.
- [x] Smoke README and tiny scaled/wide (10 units, 6/30 events per unit, one
  repeat) with and without diagnostics. Require all schemas/values/quality to
  agree and four checker controls to reject their corruptions. In a throwaway
  failure injection confirm wrappers restore; do not add permanent wiring tests.
- [x] Run `make check TESTS="tests/test_documentation.py"` and
  `uv run --group docs python -m tests.documentation --build`. Record actual
  commands/outcomes, then commit. No application-wide test run for this task.

**Acceptance:** existing benchmark remains reproducible; diagnostics can identify
construction versus translation/execution without changing the timed path.
No claimed performance gain from instrumented numbers.

## Task 2: Prepare exact timestamp comparison keys once

**Files:** modify `terrine/backends/timestamps.py`,
`terrine/temporal/aggregate.py`; extend `tests/backends/test_timestamps.py`,
`tests/temporal/test_range_scaling.py`,
`integration/temporal/test_window_quotient_composition.py` only for missing cases.
Preserve live timestamp matrices in `integration/warehouse_execution/`.
**Dependencies:** none for coding; Task 1 before integrated measurement.

**Interfaces:** implement `_TimestampComparisonKey`,
`_timestamp_comparison_key` and `_compare_timestamp_keys` exactly as the spec.
Add private `_attach_timestamp_comparison_key(table, timestamp, *, rank_name,
coordinate_name) -> tuple[ibis.Table, _TimestampComparisonKey]` in `aggregate.py`.
Extend private `_interval(timestamp, t0, window, *, timestamp_key=None,
t0_key=None)` to consume field-backed keys for nonzero endpoints. Leave public
`compare_timestamps` signature and `__all__` intact.

- [x] Before editing, run LSP references for the exported comparator and inspect
  its consumers. Extend the existing comparison/range cases, not SQL snapshots.
  Use independent Python integer nanosecond/rank comparisons for all five
  operators. Retain NULL, ±infinity, negative subsecond, mixed-zone and ns/us
  boundary cases. Add only missing signed-int64 offset extrema in one batched
  query rather than a large pytest parameter grid.
- [x] Observe the new sensitive case failing under a deliberate one-line faulty
  implementation (for example shifting an infinity rank or dropping signed
  nanos). Existing semantics may already pass on baseline; an import failure is
  not sufficient red evidence. Record the killed defect and restore the break.
- [x] Factor `C = DECIMAL(38,0)(seconds) * 10**9 + nanos` and rank comparison.
  Shift only finite coordinates; preserve SQL three-valued NULL results.
  Project key fields once before the source/points join; after the join rebuild
  key objects from joined field references. Do not reuse pre-join field objects.
- [x] Preserve zero-offset native comparisons and all selection-before-conversion
  boundaries. Keep existing numeric guards and no extra source reads. Use the
  existing projection convention; do not introduce a raw-SQL optimizer.
- [x] Run focused checks:
  `make check TESTS="tests/backends/test_timestamps.py tests/temporal/test_range_scaling.py integration/temporal/test_window_quotient_composition.py"`.
  The integration owner measures all three comparator cases. Inspect actual
  generated SQL to confirm fixed per-source/per-point conversion work rather
  than offset/output-scaled copies; this is diagnostic evidence, not a permanent
  test of private aliases. Commit with per-commit and task-branch reviews.

**Acceptance:** exact results/refusals unchanged for general origins and virtual
endpoints; actual SQL demonstrates prepared-field reuse and records size/time
change versus both saved baseline and handwritten reference.

## Task 3: Add declaration-certified native shifted comparisons

**Files:** modify `terrine/backends/dialect.py`,
`terrine/backends/engines/duckdb.py`, `terrine/compile/numeric.py`,
`terrine/compile/columns.py`, `terrine/temporal/aggregate.py`; extend
`tests/backends/test_timestamps.py`, `tests/compile/test_anchors.py`,
`integration/compile/test_numeric_lowering.py`.
**Dependencies:** Task 2.

**Interfaces:** dialect method `native_shifted_comparison_keys` as specified;
base returns `None`. `_declared_origin_bounds(design: Design, model: Model) ->
tuple[datetime, datetime] | None` in `compile/numeric.py`.
Add explicit `origin_bounds` to `CompileContext`; migrate every constructor found
with LSP. Pass private `_origin_bounds` to `aggregate_windows`; it is suppressed
for relative, overridden and persisted origins. Do not change `fixed_anchors`.

- [x] Extend existing observable timestamp tests for exact SECOND shifts across
  DST under a New York session and both UTC year-1/year-9999 directions. Include
  source ns/mixed-zone and out-of-range shifts that must still compute through
  exact keys; no source conversion to force eligibility.
- [x] Add a small declaration proof regression: Schedule has at least three
  origins and its last is outside a shifted conservative range although first
  two fit; also cover a nonempty monthly clamped schedule and empty schedule.
  Assert the derived extrema/decline and actual consumer results, not an internal
  helper call. A deliberate use of the first-two alignment sample must fail.
  Preserve EventAnchor and supplied/persisted-anchor no-proof behavior in the
  existing numeric lowering integration case.
- [x] Implement bounds from pure declaration arithmetic: FixedDate resolved UTC
  instant; Schedule first and last using count/instant, never enumeration. The
  callsite supplies no proof when `_anchor_table` or persisted anchors replace
  ordinary generation. Relative selectors always use no proof.
- [x] Implement the DuckDB override only: both operands UTC-aware with scale
  None/6, original and shifted bounds within UTC years 1–9999, exact interval
  representability. Catch host bound overflow as a declined optimization. Do not
  add a new refusal. Emit SECOND intervals; stage shifted point fields once per
  unique offset. All missing proofs/dialects use Task 2 keys. Avoid generating
  unused key fields when every endpoint is native.
- [x] Run focused checks:
  `make check TESTS="tests/backends/test_timestamps.py tests/compile/test_anchors.py integration/compile/test_numeric_lowering.py"`.
  On integration, exercise the supported DuckDB floor 1.5.0 and installed 1.5.x
  through the existing version-matrix environment, without changing dependency
  floors. Measure all three comparator cases; README's naive source remains a
  real case, not normalized away. Commit and complete reviews.

**Acceptance:** ordinary known UTC-microsecond origins get compact native
membership; every uncertain case retains exact behavior. No first-two-sample
proof, new data scan, session-zone dependency or timestamp-domain restriction.

## Task 4: Share source/window context and membership

**Files:** modify `terrine/temporal/aggregate.py`; `daily.py` only if required to
preserve the current staging contract. Extend existing cases in
`tests/temporal/test_window_aggregates.py` and
`integration/compile/test_frame_runtime_window_export.py`.
**Dependencies:** Task 3; one writer for these shared files.

**Interfaces:** private `_WindowContextKey` with spec fields; separate reusable
context field references from per-request `RowCheck` records. `_Primitive`,
public `AggregateRequest`, output order and public result schema retain their
contracts. Membership maps use source/window/unique availability references.

- [x] Extend `test_batch_keeps_nonnull_denominators_half_open_bounds_and_anchor_instances`
  and masking/refusal cases where possible. Add one independently missing case:
  same-window sources A/B have different declared coverage; A's values
  `[2, None, 3]` produce count 3, total 5, mean 2.5 and activity True, while B's
  otherwise valid sum stays null with `incomplete_coverage`. Assert duplicate
  anchor instances remain distinct. This catches sharing by window alone.
- [x] Extend selected-invalid/unknown-filter coverage with two aliases of one
  primitive and an earlier unavailable output. Assert the earliest evaluated
  output's refusal context, stable code and failed count. A deliberate shared
  context attributed to the wrong output must fail.
- [x] Intern shared `_window_context` expressions by the semantic key inside one
  call. Keep alignment predicates reusable but instantiate error context per
  request. Deduplicate availability ORs and interval membership without merging
  source/filter/type/counting/arithmetic identities or broadening consumed rows.
- [x] Apply the same deduplication to event and scoped-daily paths. Keep exact
  range union and per-output masking before arithmetic; preserve missing/unknown
  filter reasons, empty/all-null distinction and denominator semantics.
- [x] Run
  `make check TESTS="tests/temporal/test_window_aggregates.py integration/compile/test_frame_runtime_window_export.py"`.
  Integration owner runs the three comparator cases plus the existing bounded
  dense/day benchmark smoke (not a duplicate full scale suite). Record emitted
  SQL and stable schema/check attribution. Commit and complete reviews.

**Acceptance:** equal semantic windows share work; different coverage,
observation, arithmetic and source meanings never leak into one another.

## Task 5: Narrow finalization construction without caching safety state

**Files:** modify `terrine/temporal/aggregate.py`; retain
`daily._batch_projections`' same-base/fresh-name behavior. Extend
`tests/temporal/test_window_aggregates.py`, `tests/compile/test_columns.py`,
`tests/test_frame_runtime.py` and `integration/test_frame.py` only for missing
consumer contracts. No production execution/session cache.
**Dependencies:** Task 4.

**Interfaces:** `_finalization_fields` exactly as the spec. Start each output
branch at `retained.select(*required_names)` using same-name field projections.
Call `_finalize_window` with the original anchors, not the full retained
intermediate as its anchor payload. Merge back through
`_batch_projections(retained, output_branches)`.

- [x] Extend the existing multi-output aggregate cases with an extra anchor
  provenance field and a calendar alignment row-check. Assert all anchor fields,
  support types/reasons, per-output metadata and the alignment refusal survive.
  Extend `test_time_series_columns_are_values_with_named_support` if it already
  owns the relevant public-schema contract; avoid a duplicate schema snapshot.
- [x] Observe red with deliberate omission of a context check or extra anchor
  field from the dependency set. Restore the break, then implement the ordered
  union: all original anchor columns, every requested primitive alias,
  available/unavailable/observed bounds, context row-check columns. No need to
  carry unrelated sibling intermediates or start/end when finalization does not
  read them.
- [x] Keep request-specific numerical flags and names in the forked builder.
  Preserve dependency depth, output order, identity multiplicity and final
  public select. Do not replace staged conversion with one giant inlined CASE.
- [x] Exercise the same built Frame twice around a real source update: second
  ordinary materialization sees the new value; an old retained receipt remains
  pinned. Reuse existing snapshot smoke coverage instead of adding another full
  staging pipeline. Preserve per-call budget failure followed by successful
  materialization and native-function shadowing refusals.
- [x] Run
  `make check TESTS="tests/temporal/test_window_aggregates.py tests/compile/test_columns.py tests/test_frame_runtime.py integration/test_frame.py"`.
  In a separate diagnostic pass compare construction counts/profile and actual
  schemas. Integration owner measures all three unprofiled benchmark cases.
  Report build and full API effects separately; do not skip session recompile,
  revalidation or checks. Commit and complete reviews.

**Acceptance:** the profiled repeated wide branch construction is removed;
observable results and all execution boundaries remain. If any remaining public
API cost dominates, quantify its phase; do not add an unmeasured cache.

## Task 6: Integrated evidence, portability and documentation

**Files:** create fresh milestone/final directories under `benchmarks/results/`
and a dated report under `docs/research/`; update `benchmarks/README.md`,
`docs/guides/cost-and-residency.md`, relevant timestamp/temporal guide sections,
and `CHANGELOG.md`. Do not overwrite the v2 baseline or superseded evidence.
**Dependencies:** completed Tasks 1–5, then continuation Tasks 7 and 8 before
the new final verification/acceptance pass. Task 6 remains existing issue `5ggg`.

**Interfaces:** same comparator result schema/fixtures frozen by Task 1; no
production API change. This task owns shared docs and final integration checks.

- [x] Run the affected integrated selection once:
  `make check TESTS="tests/backends/test_timestamps.py tests/temporal tests/compile tests/test_frame.py tests/test_frame_runtime.py integration/temporal integration/compile integration/test_frame.py integration/backends integration/execution integration/conformance"`.
  Existing generic-policy conformance is also exercised with
  `uv run pytest integration/conformance --generic-dialect`.
- [x] Run the focused live PostgreSQL timestamp and aggregate/conformance
  selection against a scratch server using documented setup/teardown; full
  `make test-postgres` belongs to integration CI, not every lane. Preserve existing
  BigQuery/Snowflake parity selections; execute live cloud checks only through
  an already authorized environment. Report missing credentials/evidence rather
  than fabricating coverage or weakening a gate.
- [x] Run the existing retained refresh/old-generation integration case
  `integration/compile/test_numeric_lowering.py::test_revision_one_replay_refuses_and_empty_change_refresh_recomputes`
  as part of the integrated selection, not an additional duplicate pipeline.
  Exercise consumer SQL-length budgets with
  `uv run pytest integration/test_cost_budget.py -m scale -k 'characters or readme or bigquery'`.
  Existing expected-failure marks are removed only when their actual consumer
  budget is met, never to re-pin internal text.
- [x] Run final unprofiled measurements serially, five repeats each:
  `agent-slots -j 1 -- uv run python -m benchmarks.compare_sql --case readme --repeats 5 --output <new-readme-dir>`;
  repeat for `--case scaled --units 10000 --events-per-unit 30` and
  `--case wide --units 10000 --events-per-unit 30`, each in a new directory.
  Run optional diagnostics/profile separately. No benchmark inside a concurrent
  build/test swarm. Verify source/input hashes and full typed parity every trial.
- [x] Publish a report separating: structural SQL reductions, query median/range
  versus literal and baseline, expression build, SQL compilation and full public
  API. Include emitted SQL, manifests, versions, schemas and capture exclusions.
  Explain remaining dominant costs. Report whether the existing 3× waypoint is
  met, and the measured remaining distance from 1×; do not promise 50× or claim
  cloud/other-recipe parity. Update guides and changelog only with observed facts.
- [x] Run strict documentation build, inspect final diff for scope and complete
  every commit/task/final-branch review. No push or merge without authorization.

## Review, tracking and execution gates

Historical bootstrap: before original implementation, the plan/spec received
native three-reviewer review and the six issues were created under `ce4r`.
That preparation is complete; never bootstrap another copy or reclaim Tasks1–5.
For the approved continuation, resolve the additional plan-review findings,
create only Tasks7/8 under the existing parent, and add both as blockers of
existing Task6. The latter remains open until their integrated verification.

At execution start, record each task's absolute worktree, branch and immutable
base. Use the kata harness watcher, with its full-selection patterns restricted
to the actual integration worktree. Before task close, enumerate its commits via
`git rev-list --first-parent --no-merges BASE..BRANCH` and retrieve native reviews
by full SHA. Every line must be `done P`, or `done F closed=true` with findings
actually fixed/disproved. Include job-ID-first “Review gate” lines in the close.
Run boundary harness checks and one task-branch review before closing; one final
integration branch review after the last task. Queued/missing reviews are not
passes. No broad all-branch fixing or changes to safety settings.

## Plan self-review and handoff

All four owner-selected areas map to concrete tasks. Exact-key and native tiers
have separate interfaces and proofs; Schedule alignment samples are not range
bounds. Context sharing preserves output error identity. Narrow branches carry
all original anchor/check dependencies. Recompilation for a new execution binding
is intentionally retained; the actual duplicate construction is optimized.
Test assertions cover values, types, support and stable errors rather than helper
wiring. There is one shared-code lane and one final shared-doc owner.

Original execution used isolated comparator/timestamp lanes, then ordered shared
aggregate edits and integrated verification. The owner has now approved the
bounded continuation below. Publishing and merging remain unauthorized.
Tasks 7/8 may prepare in isolated lanes; the integration owner serializes their
checks, landing and measurements, then resumes Task 6.

## Task 7: Reuse integer timestamp parts in daily-envelope masking

**Files:** modify `terrine/backends/timestamps.py`,
`terrine/temporal/aggregate.py`; extend existing cases in
`tests/temporal/test_window_aggregates.py` and, only if needed,
`tests/backends/test_timestamps.py`. Preserve the existing generic-provider
admission and ns/us composition tests.
**Dependencies:** original Tasks 1–5; independent source edits from Task 8.

**Interfaces:**

```python
def _compare_timestamp_parts(
    left: tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue],
    right: tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue],
    op: Literal["lt", "le", "gt", "ge", "eq"],
) -> ir.BooleanValue: ...

def _attach_timestamp_parts(
    table: ibis.Table, timestamp: ir.TimestampValue,
    *, names: tuple[str, str, str],
) -> tuple[
    ibis.Table, tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue]
]: ...
```

The comparator lives beside `timestamp_parts`; attachment is private to
`aggregate.py`. Public signatures/exports remain unchanged.

- [x] Use LSP references for `compare_timestamps` before factoring its existing
  integer-only unshifted fallback into the private helper. Native zero-offset
  dispatch and shifted decimal keys retain their current contracts.
- [x] Extend an owning daily execution case with naive us/ns source instants
  just outside/inside `[1969-12-31 00:00, 1970-01-01 00:00)`: selected values
  `[2, 3]` yield count 2, sum 5 and mean 2.5. Include immediately preceding and
  exact-upper-bound invalid payloads plus an unused unit, duplicate anchor
  instances and complete/reason assertions. Derive clocks through independent
  integer nanosecond arithmetic. Observe assertion failure under deliberate
  omission of the staged fractional remainder; restore it.
  Add one truthful declared-bounds us variant under a New York session to cover
  the admitted bound-only normalization; replacing UTC wall-clock conversion
  with a session-sensitive naive cast must fail that variant.
- [x] Project fallback integer triples once for source timestamps and grouped
  envelope endpoints. Use collision-safe names, joined field rebinding and
  existing layered projections. Retain native comparisons where admitted;
  all-native masks omit unused parts. Keep the original active/alignment
  envelope, typed-null exclusion and mask-before-`local_day` order. Retain
  original event fields needed by source filters and contributions.
- [x] Retain/reuse the genuine required unit/day-to-anchor membership relation
  in both source-day scoping and final fanout. Keep full anchor-instance grain
  and independent context availability/membership/check columns; the source-day
  distinct projection alone is not a fanout identity.
- [x] Where native comparison is unavailable, stage a shared guarded nullable
  key whole before field extraction, without hoisting conditional descendants.
  Do not introduce an unsupported struct projection into the portable path.
- [x] Propagate existing trusted `_origin_bounds` into the day route. Permit
  source-naive-us versus bound-UTC-us native comparison only after proving
  every endpoint shift host-representable and active envelope bounds clipped
  inside those finite requested ranges. Probe existing optional
  `Dialect.comparison_keys(source, source)` before constructing a wall clock;
  base `None`, unknown/persisted bounds, overflow, ns and other zones keep exact
  integer fallback. Stage `utc_wall_clock(bound)` on the bound side only, then
  compare admitted equal-zone us keys. No source conversion or new backend API.
  Include a real unsafe extreme/unknown-proof fallback witness: dtype-only UTC
  conversion can fail on valid near-limit TIMESTAMPTZ values.
- [x] Reuse unfiltered `event_count` only when an unfiltered count's complete
  `_MembershipKey` set is a subset of a same-source unfiltered non-count donor.
  It counts rows before value/non-null/numeric logic. Keep the count's own final
  masks/public aliases/checks; source/measure-filtered, unmatched and count-only
  variants retain their original branch. Extend a case with NULL-valued rows
  and extra future donor days: trailing count must exclude future rows and
  include NULL-valued rows. Retain different-coverage, unknown-filter,
  unavailable-output and duplicate-anchor cases. No numeric/provenance borrowing.
- [x] Share a private masked/reduced event-count term only for identical
  `(original event_count field, complete MembershipKey)`. Keep public aliases
  and all request-specific check/flag/context records independent.
- [x] Reuse only the two window-independent observed-from/until support data
  names when source, typed source-span inputs, anchor observation inputs and
  anchor/fixed branch match. Preserve public support schema/metadata and ensure
  the names exist before use, with no cross-source read. Window endpoints and
  availability/completeness/check records remain independent. Use an O(1)
  guarded existing exemplar or local data index, not a general/validation cache.
- [x] Remove only SQL Boolean identity work: FALSE-OR initialization may use
  the first term (empty remains FALSE), and a timestamp-only conditional already
  maps FALSE/NULL to the same typed null without an immediate false coalesce.
  Preserve separately consumed flags and mask-before-calendar.
- [x] If a bucket's complete membership set is exactly every key whose OR
  filters the required relation, its scope mask is TRUE on every retained row.
  Omit that redundant ANY field and use TRUE only after the unchanged unique
  unit/day scope join. When all masks are redundant, use genuine distinct
  unit/day scope; narrower/noncovering/filtered variants keep their masks.
  Predicates, numerical/alignment checks and final per-request masks remain.
- [x] Run focused checks once at the coherent milestone:
  `make check TESTS="tests/backends/test_timestamps.py tests/temporal/test_window_aggregates.py tests/temporal/test_range_scaling.py integration/execution/test_generic_provider.py integration/temporal/test_window_quotient_composition.py"`.
  Smoke actual README public materialization with the frozen comparator.
  The existing failing README budget is the structural red baseline; require
  its unchanged 20,000-character assertion to pass after the implementation.
- [x] Commit with native per-commit/task-branch gates and fresh spec/quality
  review. Integration measures all three frozen comparator cases serially,
  five samples, into fresh continuation directories; inspect actual SQL reuse.

**Acceptance:** README <=20,000 formatted characters with actual typed values,
schema/support and all checker controls unchanged; integer-only portable
unshifted admission, mixed-clock semantics and masking order preserved.

## Task 8: Compact state-query construction without merging checks

**Files:** modify `terrine/compile/shapes/state_panel.py`,
`terrine/backends/dialect.py` and `terrine/backends/engines/duckdb.py`; extend
existing `tests/compile/test_state_panel.py` and
`integration/state_panels/test_state_panel_workflows.py`. Preserve retained
state-panel cases in `integration/warehouse_execution/test_saved_plan_restart.py`.
**Dependencies:** original Tasks 1–5; independent source edits from Task 7.

**Interfaces:**

```python
def _compact_state_schedule(schedule: Schedule, model: Model) -> Schedule | None: ...

def _state_period_relation(
    design: Design, model: Model, metric: str,
) -> tuple[ibis.Table, CompiledStudy, CompiledStudy]: ...
```

The relation helper replaces `_union_state_periods` through LSP-aware migration.
Its two returned studies remain the original scheduled/fixed endpoint studies
for checks, not the optional compact query study.

- [x] Extend existing successor/membership/null/anchor cases for a whole-day
  schedule and a sub-day fallback. Exercise the valid public schedule
  9999-12-29 through 9999-12-30 with one-day periods: its endpoint is representable
  on December 31, but the compact schedule's exclusive end is not. Actual
  consumer results must preserve the original fallback without a new refusal.
- [x] Add the missing validation-domain regression on the eligible exact route:
  one plain sum over `DECIMAL(38, 0)` inputs, two scheduled origins and endpoint.
  Two positive near-maximum inputs per selected window exceed the public output
  type while their coefficients remain representable. Both scheduled windows
  and endpoint overflowing must refuse `terrine.temporal.exact_overflow` with
  scheduled failed count 2; endpoint-only overflow must give failed count 1.
  Assert stable source/measure context, not alias spelling. Deliberately return
  compact-study checks instead of the original studies to observe merged-domain
  red; restore. Reuse tiny deterministic fixtures.
- [x] Add one native-float multi-day cancellation fallback regression. Put
  alternating finite positive/negative near-maximum contributions in the final
  endpoint window such that the original direct sum is zero while grouping
  same-sign contributions by day overflows. Require the original finite low
  classification. Deliberately admit this float route to the compact query to
  observe red, then restore. This catches the actual plan-panel arithmetic hole.
- [x] Add private `_unbounded_coefficients = False` policy metadata to the base
  dialect and `True` only beside DuckDB's BIGNUM coefficient implementation.
  This says nothing about count accumulators. No public registry capability or
  engine-name/type dispatch in the consumer. `bounds_accumulations=False` is
  insufficient: BigQuery's unchecked BIGNUMERIC sums remain bounded.
- [x] Admit the compact relation only for a resolved metric of kind `mean`,
  one `sum` measure, no transforms, original period result `DECIMAL(38, 0)`,
  and active-policy `_unbounded_coefficients=True`. Exact scale-zero coefficient
  regrouping is mathematical addition; count payloads remain bounded by physical
  source rows below `2**63`, safely within HUGEINT. Final checked narrowing is
  the same. Every other arithmetic mode/policy retains the original union.
- [x] Keep original scheduled/fixed compilations intact. Derive a whole-day
  extended schedule only after host-clock proof:
  `old_count = schedule.count(model.day_boundary)`,
  `start_utc + old_count * every == model.day_boundary(old_end_exclusive)`,
  and `extended.count(model.day_boundary) == old_count + 1`.
  Current boundaries are fixed UTC offsets. Decline date/clock overflow,
  unrepresentable exclusive end, non-whole-day spacing or failed proof; no
  enumeration, source scan, new refusal or user input.
- [x] Compile only the proven-equivalent compact output relation through normal
  `compile_columns` with identical metric/window, zero gap and no membership.
  Keep original study/check/anchor-check registration exactly unchanged, since
  they now certify the identical exact quantities and final narrowing.
  Canonicalize support names if needed. Public candidates, persisted anchors,
  both state joins/classifiers, reasons, observations and provenance are
  unchanged. Replace the obsolete helper name through LSP; no compatibility shim.
- [x] Run
  `make check TESTS="tests/compile/test_state_panel.py integration/state_panels/test_state_panel_workflows.py integration/warehouse_execution/test_saved_plan_restart.py::test_state_panel_consumes_frozen_snapshot_and_scheduled_anchors integration/warehouse_execution/test_saved_plan_restart.py::test_state_panel_composition_matches_full_rebuild"`.
  Run the existing state-panel size/value consumer selectors with scale enabled,
  retaining the 30,000-character ceiling. Smoke a real bounded public Frame.
- [x] Commit with native per-commit/task-branch gates and fresh spec/quality
  review. Integration runs the frozen three-case five-sample comparator and
  reports the state-specific SQL win and extra-construction cost separately.
  No implied state runtime gain from SQL length alone.

**Acceptance:** state-panel <=30,000 formatted characters per feature with
handwritten value agreement, all original validation domains/failed counts,
public anchors/support/provenance and execution binding preserved.

## Continuation integration and final acceptance

- One eventual integration PR; no publish/merge authority. Tasks 7/8 use disjoint
  production/test files and may prepare in isolated Orca worktrees. Only one
  heavy check/benchmark lane runs at a time; each child records immutable base,
  worktree, branch and review receipts. Integration owns shared docs/results.
- Keep the comparator/helper frozen at the existing recorded hashes. Preserve
  all old directories/manifests; create new continuation evidence only.
- After both children land, Task 6 runs its affected integrated command once at
  the new head, adding `integration/state_panels`, plus generic conformance,
  the focused local PostgreSQL probes and unchanged SQL-budget selection.
  Exercise changed daily behavior on the supported DuckDB floor with the same
  disposable-overlay method; do not reinstall the main environment or change
  floors. No live cloud run without separate authority.
- Retain exact refusal/session/snapshot behavior, strict documentation build,
  five-sample final comparator evidence and all commit/task/final-branch gates.
  Close Task 6 and the parent only when both previously failed budgets actually
  pass. No new xfails, altered thresholds or unchecked timestamp fast path.

