# Terrine Trajectories, Cohorts, LTV and Survival Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add reproducible cohort trajectories, lifetime-value summaries, first-event and survival analysis, and Arrow exports while preserving explicit observation, censoring, completeness, and provenance semantics.

**Architecture:** Extend the existing lazy Design → Frame compiler through the single shape dispatcher and reuse compiled period outcomes as the source of cohort aggregates; do not create a parallel query path. Keep first-occurrence event aggregation in the temporal layer and Kaplan–Meier/backtest fitting on materialized data, with the estimator consuming no Ibis/compiler modules. Export surrogate-training data and its typed metadata through Arrow without coupling to increment.

**Tech Stack:** Python, existing Ibis/DuckDB reference execution, PyArrow, NumPy/SciPy where required by the existing estimate package, and focused pytest tests. No new mandatory statistical-library dependency is presumed.

**Spec:** Roadmap Task 6.1 in `docs/superpowers/plans/2026-10-16-terrine-build.md`; companion contracts in `docs/superpowers/plans/2026-10-16-terrine-temporal.md`, `docs/superpowers/plans/2026-10-16-terrine-estimation.md`, `docs/superpowers/plans/2026-10-16-terrine-study-api.md`, and `docs/superpowers/plans/2026-10-16-terrine-dataset-workflows.md`; contributor requirements in `AGENTS.md`.

## Global Constraints

- Preserve exact UTC-aware timestamps, half-open windows, declared source completeness, observation bounds, checked numeric certificates, snapshot verification, and null reasons.
- No hidden unit-by-day or anchor-by-event fanout. Explicit bounded period output is allowed; LTV cohorts must aggregate the same compiled LTV frame, not trigger a second query.
- Extend the merged `feat/dataset-engine` Task 6.2 implementation (base commit `aa40e69`): use its `terrine/compile/dispatch.py`, `terrine/compile/shapes/state_panel.py`, and `Design.shape: "cross_section" | StatePanel`; add cases to that single dispatcher, never create a second dispatcher or assume Task 6.2 is unmerged.
- `CohortTable` reports risk-set and observable denominators separately. Censoring, churn, administrative cutoff, membership exit, and measurement observation end are distinct concepts.
- Churn can be an event/outcome; it is not an automatic censoring rule. Do not claim survival-times-value identity without naming and implementing a model/assumptions.
- Unit tests use deterministic local fixtures only: no network or warehouse. Live warehouse checks belong under `integration/warehouse_execution/`.
- Native execution, temporal code, examples and tests must not import increment at runtime. Do not edit increment or add a runtime cross-import.
- Backtests select only pre-cutoff measurement-time entrants and recensor follow-up; they label retrospective effective-time provenance and do not claim historically recorded/known features absent explicit evidence.
- Preserve exact integer/decimal values and existing `checked_float64_v1` rules; missing/unavailable is null with reason, never an imputed zero. No increment edits, new causal estimator claim, or paid resource use.
- Adoption/vendorization of Callaway–Sant'Anna, survival estimand selection beyond the explicit nonparametric KM contract, and licensing/release decisions are owner gates; this plan does not decide them.
- Every task updates its owned guides/example and changelog before commit; no task claims a user-facing API before its composed local smoke succeeds.

## Review Focus

1. **Subsecond time ties and ordering:** distinct nanosecond event/censor/entry instants must remain distinct through export. Test Task 3 `test_km_subsecond_event_censor_and_entry_order_exactly`.
2. **Observed zero differs from unknown:** all-zero observed cohorts remain zero, while unavailable ages/denominators and incomplete values are null. Test Task 2 `test_zero_partial_and_unavailable_cohort_denominators`.
3. **Eligibility and cutoff boundaries:** prevalent events, delayed entry, observation end, exit, and cutoff equality follow the declared interval. Test Task 3 `test_only_first_event_inside_eligible_interval_is_selected`.
4. **Repeated anchors and missing periods:** cumulative value stays isolated by full trajectory identity and propagates incompleteness/reasons/certificates. Test Task 2 `test_cumulative_partitions_repeated_anchors_and_propagates_missing_period`.
5. **Historical refit leakage and execution authority:** retain/re-censor historical entrants, expose only earlier events, and materialize recipe-backed cohorts under the verified snapshot. Test Task 4 `test_backtest_recensors_survivors_and_is_post_cutoff_invariant` and Task 2 `test_cohort_collection_observes_fresh_snapshot_and_budget`.

---

## File and Interface Map

- `terrine/temporal/first.py`: reusable first-eligible-occurrence aggregate with exact timestamp and no-event indicator; consumes normalized temporal event relations.
- `terrine/compile/shapes/ltv.py`: bounded per-unit/cohort-age period outcomes and cumulative value over existing aggregate/prefix APIs.
- `terrine/compile/shapes/cohorts.py`: `CohortTable` recipe and projection over LTV frame, with explicit denominators and grouping semantics.
- `terrine/compile/shapes/time_to_event.py`: eligible event interval, event/exit/admin-cutoff representation, and lazy survival input relation.
- `terrine/compile/dispatch.py`: sole Design-shape dispatcher, already has the merged StatePanel branch; extend it without replacement or duplication.
- `terrine/spec/` and `terrine/data.py`: typed shape/design and shared metadata, exact elapsed-time serialization, administrative cutoff, and provenance.
- `terrine/estimate/survival.py`: Arrow-data KM and earlier-only recensoring backtests; no compiler, Ibis, or `Frame` imports.
- `tests/temporal/test_first_occurrence.py`, `tests/compile/test_ltv.py`, `tests/compile/test_cohorts.py`, `tests/compile/test_time_to_event.py`, `tests/estimate/test_survival.py`, `tests/estimate/test_survival_backtest.py`, `tests/integration/test_trajectories.py`.
- `examples/trajectories_survival.py`, `docs/guides/trajectories-survival.md`, `CHANGELOG.md`.

Use the current `Model`, `Design`, `Frame`, `MaterializedFrame`, `ColumnMeta`, `compile_columns`, `compile_anchors`, temporal prefix/window operations, execution authority, estimator conventions, and independent simulation/reference utilities. The current column compiler derives values independently from `AnchorTable` and joins on null-safe `(unit, anchor_id)`; new shape compilation must preserve this identity discipline and use the one bound execution snapshot.

## Interfaces and Semantic Decisions

The following are the Task 6.1 contracts; names/types are shared across tasks and should be added to the existing serialization/publication paths rather than shadowed by parallel contracts.

```python
@dataclass(frozen=True)
class ElapsedTime:
    seconds: int       # floor-normalized elapsed seconds from this row's Roles.t0
    nanoseconds: int   # 0 <= nanoseconds < 1_000_000_000

@dataclass(frozen=True)
class Trajectory:
    metric: str        # existing additive metric name
    period: str
    periods: int       # finite number of ages, indexed from zero
    cumulative: bool = False

@dataclass(frozen=True)
class CohortSpec:
    origin: str        # first occurrence of this event fact; must match Design.anchor
    trajectory: Trajectory

@dataclass(frozen=True)
class CohortGrouping:
    bucket: str
    bucket_origin: datetime  # aware UTC instant
    statistic: Literal["total", "mean"] = "mean"

@dataclass(frozen=True)
class TimeToEvent:
    event: str
    exit: str | None = None
    entry: str | None = None
    delayed_entry: bool = False

@dataclass(frozen=True)
class CohortRow:
    bucket_start: datetime
    bucket_index: int
    age: int
    value: int | float | Decimal | None
    at_risk: int | None
    observable: int | None
    complete: bool
    reason: str | None

@dataclass(frozen=True)
class SurvivalCurve:
    time: tuple[ElapsedTime, ...]
    at_risk: tuple[int, ...]
    events: tuple[int, ...]
    censored: tuple[int, ...]
    survival: tuple[float, ...]
    variance: tuple[float | None, ...]

@dataclass(frozen=True)
class ArrowSurrogate:
    data: pyarrow.Table
    metadata: Mapping[str, JsonValue]

@dataclass(frozen=True)
class CohortTable:
    parent_frame: Frame
    grouping: CohortGrouping
    value_column: str

def first_occurrence(events: ibis.Table, candidates: ibis.Table, *, unit: str,
                     timestamp: str, lower: str, upper: str,
                     event_id: str | None = None) -> TemporalResult: ...
def compile_ltv(design: Design, model: Model) -> CompiledStudy: ...
def compile_cohorts(study: CompiledStudy, *, grouping: CohortGrouping,
                    value_column: str) -> CompiledStudy: ...
def compile_time_to_event(design: Design, model: Model) -> CompiledStudy: ...
def kaplan_meier(data: MaterializedFrame, *, duration: str, event: str,
                 entry: str | None = None) -> SurvivalCurve: ...
def backtest_survival(data: MaterializedFrame, *, duration: str, event: str,
                      entry: str, prediction_cutoffs: Sequence[datetime],
                      fitter: Callable[[MaterializedFrame], object],
                      knowledge: Literal["measurement_time", "recorded_at"] =
                          "measurement_time") -> tuple[object, ...]: ...
def to_surrogate_arrow(data: MaterializedFrame, *, features: tuple[str, ...],
                       outcomes: tuple[str, ...]) -> ArrowSurrogate: ...
def Frame.cohorts(self, *, grouping: CohortGrouping,
                  value_column: str) -> CohortTable: ...
def CohortTable.materialize(self, *, budget: Budget | None = None) -> MaterializedFrame: ...
def CohortTable.to_pyarrow(self, *, budget: Budget | None = None) -> pyarrow.Table: ...
```

Extend `Design.shape` from `"cross_section" | StatePanel` to `"cross_section" | StatePanel | CohortSpec | TimeToEvent`; preserve the merged `StatePanel` constructor, serialization and existing branch in the single `terrine/compile/dispatch.py` dispatcher. Add optional `Design.administrative_cutoff: datetime | None`, an aware absolute instant included in design hash and provenance. `CohortSpec.origin` must equal the fact of `Design.anchor == EventAnchor(origin, which="first")`; reject fixed-date/schedule/mismatched anchors. This reuses one first-occurrence selection to set `Roles.t0 == cohort_time`. For the trajectory shape, `Design.outcomes` must be empty (the trajectory names its one additive `metric` and generated age windows); reject explicit outcomes rather than double-counting or silently reconciling overlapping windows.

`ElapsedTime` is floor-normalized seconds (including negative durations) plus nanoseconds in `[0, 1_000_000_000)`, relative to the row's absolute `Roles.t0`. Event, entry and censor durations therefore share one exact elapsed axis; an absolute cutoff `c` is compared to a row by exact `c - t0`, without routing timestamps through float or overflowing Int64 nanoseconds. Serialize as tagged integer fields and non-null Arrow struct when present (nullable for unavailable time). KM includes event at zero; at ties, failures and censors use the pre-time risk set and failures are applied before tied censors leave subsequent sets. Delayed entry enters at its exact elapsed instant only when explicitly declared.

The same optional absolute administrative cutoff limits LTV windows, cohort completeness and survival follow-up independently of source `BoundCoverage` and unit observed-from/until bounds. A changed cutoff rebuilds values, completeness, observable denominators and survival risk sets/follow-up under unchanged readiness. For event source coverage `[coverage.start, coverage.end_exclusive)` and unit measurement bounds, define follow-up lower/upper before event selection. Without declared delayed entry, any gap between `t0` and the earliest supported history yields `incomplete_initial_history`, never presumed event-free time. With `delayed_entry=True`, left-truncate at the latest of declared entry, `t0`, source coverage start and unit observed-from; cap at the earliest of administrative cutoff, coverage end, unit observed-until and declared exit. Select event at/after lower and strictly before upper; equality at the lower bound is eligible, equality at any exclusive upper bound is censoring. A known event before the risk-entry lower bound is prevalent and excluded with reason, never replaced by a later event. Churn is a declared event outcome, not an automatic censor. Unit membership exit is not itself a measurement bound; only an independently declared observation bound or `TimeToEvent.exit` affects follow-up.

Trajectory age `i` uses the disjoint half-open interval `[t0 + i*period, t0 + (i+1)*period)`, for zero-based `i < periods`. `CohortGrouping.bucket` is fixed elapsed duration from its explicit UTC origin and emits exact `bucket_start`, `bucket_index`, and age. Cohort denominator semantics are not KM risk sets: `at_risk` is the unique first-entry cohort size (each unit counted once despite duplicated anchor rows), retained across ages including inactive units and membership exits. It is null only where the age is wholly immature beyond the administrative cutoff; `observable` is the count of entrants whose full age interval has known measurement under source and unit bounds, and is null for wholly immature ages. For a started age, known zero observable is integer 0. A group value is published only when every at-risk entrant is observable; mean divides by all cohort entrants, not survivors or only observed members. An observed no-event/all-zero additive period contributes numeric zero; a fully covered nonempty all-null metric period contributes null and propagates null through later cumulative values even if its coverage flag alone is complete. Partial groups and immature ages have null values, not zero; available all-zero groups are 0 with `complete=True`.

`Frame.cohorts(...)` returns a recipe-backed `CohortTable` over the same parent LTV Frame. `compile_cohorts(study: CompiledStudy, ...) -> CompiledStudy` transforms that already compiled relation; it MUST NOT accept a Frame, compile/execute a second query, or import execution authority. Put the public recipe next to Frame in `terrine/frame.py`, add the deterministic transformation in `terrine/compile/shapes/cohorts.py`, and route execution through a shared verified Frame materialization helper. Record a typed `CohortOperationMeta` in `Provenance` with canonical grouping/statistic, value column, parent design hash and derived-operation hash; preserve it through Arrow/JSON round-trip. Arrow export records schema version, design/model hashes, source generations and execution snapshot, exact administrative cutoff and per-column missingness/certificate references. Survival is descriptive: never claim survival-times-value identity absent a separately stated model.

Backtest v1 promises measurement-time recensoring only. It labels inputs `retrospective_effective_time`; effective event time and `FeatureCutoff.knowledge` do not prove what was historically recorded/known. `knowledge="recorded_at"` MUST refuse unless source metadata proves recorded-at availability. Do not implement or imply bitemporal history, and do not claim that late corrections to pre-cutoff measurements were historically available.

---

### Task 1: First-occurrence temporal aggregate

**Files:**
- Create: `terrine/temporal/first.py`
- Test: `tests/temporal/test_first_occurrence.py`

**Interfaces:**
- Consumes: normalized event relation and candidate-window relation with exact lower/upper time points from `compile/anchors.py`; declared occurrence-only fact; optional stable event ID.
- Produces: `first_occurrence(...) -> TemporalResult` (the existing temporal table/columns/deferred-check carrier): one row per candidate `(unit, anchor_id)` with earliest event in `[lower, upper)`, exact timestamp and explicit no-event flag. Duplicate-event-ID checks remain deferred and are registered by the consuming `CompiledStudy` so validation runs inside the same execution snapshot, including for the independent survival event source.

- [ ] **Step 1: Write focused behavior tests.** Add `test_first_occurrence_orders_exact_timestamps_and_tied_ids`, `test_no_occurrence_retains_unit_with_null_timestamp_and_censor_flag`, and `test_first_occurrence_does_not_floor_time`. Use exact timestamp ties with stable IDs, distinct events 1ns, 2ns and 3ns apart, and a no-event roster unit. Assert exact timestamps, stable tie order, candidate retention and explicit no-event flag.
- [ ] **Step 2: Run the focused red check.** Run `uv run pytest tests/temporal/test_first_occurrence.py -q`. Expected: failures identify the absent primitive/behavior.
- [ ] **Step 3: Implement first eligible occurrence.** Accept candidate unit/window rows with exact lower/upper bounds; use temporal first occurrence's existing point-lookup strategy over ordered event states, not an anchor×event join. Return `TemporalResult` carrying duplicate-ID deferred checks; its caller registers those checks in `CompiledStudy` so they run under the pinned execution snapshot. Return null timestamp and no-event flag for no eligible occurrence; callers apply prevalent-event exclusion.
- [ ] **Step 4: Verify and document.** Run the focused test command; expected: exact subsecond, tie and no-event assertions pass. Update guide/changelog.
- [ ] **Step 5: Commit.** `git add terrine/temporal/first.py tests/temporal/test_first_occurrence.py docs/guides/trajectories-survival.md CHANGELOG.md && git commit -m "feat: add first occurrence temporal aggregate"`.

### Task 2: LTV period trajectories and same-frame cohort table

**Depends:** 4.2.

**Files:**
- Create: `terrine/spec/trajectory.py`, `terrine/compile/shapes/ltv.py`, `terrine/compile/shapes/cohorts.py`
- Modify: `terrine/spec/design.py`, existing shape serialization/validation, the existing `terrine/compile/dispatch.py`, `terrine/data.py`, `terrine/frame.py` and reviewed `terrine/__init__.py` exports.
- Tests: `tests/compile/test_ltv.py`, `tests/compile/test_cohorts.py`, `tests/integration/test_trajectories.py`
**Interfaces:**
- Consumes: `Trajectory`, `CohortSpec`, `CohortGrouping`; `compile_anchors(...) -> AnchorTable`; `compile_columns(...) -> CompiledStudy`; `aggregate_window(...) -> TemporalResult`; Task 1 `TemporalResult` and deferred checks; declared readiness, `BoundCoverage` and independent unit observation bounds.
- Produces: `compile_ltv(design: Design, model: Model) -> CompiledStudy` with full `(unit, anchor_id, cohort_time)` identity, age, `period_value`, optional `cumulative_value`, completeness/reasons and cumulative certificates. `Frame.cohorts(self, *, grouping: CohortGrouping, value_column: str) -> CohortTable` retains its parent Frame recipe; `CohortTable.to_pyarrow(self, *, budget: Budget | None = None) -> pyarrow.Table` groups within the verified Frame lifecycle.
**Executable Design example:** the example uses the model built by `examples/trajectories_survival.py`'s local DuckDB fixture; this is the complete design declaration and its expected values are hand-checked in `test_ltv_period_and_cumulative_values_match_hand_sum`.
```python
from datetime import UTC, datetime

from terrine.data import Budget
from terrine.spec import CohortSpec, Design, EventAnchor
from terrine.spec.trajectory import CohortGrouping, Trajectory

# `model` is the local DuckDB Model assembled in examples/trajectories_survival.py.
design = Design(
    unit="user",
    anchor=EventAnchor("first_purchase", which="first"),
    shape=CohortSpec(
        origin="first_purchase",
        trajectory=Trajectory(
            metric="net_revenue", period="7d", periods=3, cumulative=True
        ),
    ),
    administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
)
ltv = design.build(model)
cohort = ltv.cohorts(
    grouping=CohortGrouping(
        bucket="7d",
        bucket_origin=datetime(2025, 1, 1, tzinfo=UTC),
        statistic="mean",
    ),
    value_column="cumulative_value",
)
arrow = cohort.to_pyarrow(
    budget=Budget(max_rows=100, max_output_bytes=1_000_000)
)
```
Fixture: users `u1/u2` enter Jan 1, `u3` enters Jan 15; `u1` has revenue 10 then 5 in its first two ages, `u2` has complete observed zeros, and `u3`'s age 1 starts at the Jan 22 cutoff and is wholly immature. Assert `u1` cumulative ages are 10 then 15; `u2` remains 0; the younger cohort's unavailable later age is null, not 0; a group with any unobservable entrant has null value and reports at-risk/observable counts.

- [ ] **Step 1: Write hand-computed LTV/cohort tests.** Add `test_ltv_period_and_cumulative_values_match_hand_sum`, `test_zero_partial_and_unavailable_cohort_denominators`, `test_cumulative_partitions_repeated_anchors_and_propagates_missing_period`, `test_cohort_grouping_bucket_statistic_and_denominators_match_hand_calculation`, `test_cohort_table_reuses_parent_ltv_relation_and_provenance`, and `test_cohort_collection_observes_fresh_snapshot_and_budget`. Use repeated copies of a first-entry anchor (count one cohort entrant), inactive units with observed zero, an exited member that remains in the cohort denominator, measurement loss, a partial age, and an immature young cohort age. Include a fully covered nonempty all-null middle period and a separate incomplete period; assert cumulative values/certificates propagate null for either missing contribution, while complete all-zero cohorts stay zero. Assert totals/means use all unique cohort entrants, values are null unless all are observable, and immature `at_risk`/`observable` are null, unlike a started age with known zero observable. Prove a different grouping/value recipe has a different operation hash while retaining the parent design hash.
- [ ] **Step 2: Run focused red checks.** Run `uv run pytest tests/compile/test_ltv.py tests/compile/test_cohorts.py -q`.
- [ ] **Step 3: Add serializable types and shared cutoff.** Add immutable `Trajectory(metric, period, periods, cumulative)`, `CohortSpec`, `CohortGrouping`, and optional aware `Design.administrative_cutoff`. Require the cohort origin to match `Design.anchor=EventAnchor(origin, which="first")`; reject incompatible anchors, explicit outcome windows, non-additive metric and invalid finite horizons. Validate positive period/bucket duration and explicit bucket origin. Cutoff is independent of readiness; serialize/hash it and record it in provenance. Add typed `CohortOperationMeta` to `Provenance`/Arrow metadata with canonical grouping, statistic, value column, parent design hash, and derived-operation hash.
- [ ] **Step 4: Compile trajectories through the single dispatcher.** Add `terrine/compile/shapes/ltv.py` and extend the existing `terrine/compile/dispatch.py` branches for `CohortSpec` and `TimeToEvent`; preserve the merged `StatePanel` implementation in `terrine/compile/shapes/state_panel.py` and its existing dispatch branch. Emit only requested disjoint ages. Partition cumulative aggregation by `(unit, first-entry anchor_id, cohort_time)` and order by age. Null a cumulative value/certificate for any null contributing metric, including a fully covered nonempty all-null period; propagate the first relevant reason. Retain complete observed zero as numeric zero. Do not turn incomplete contributions into zero.
- [ ] **Step 5: Build the same-frame cohort recipe.** Put `CohortTable` beside `Frame` in `terrine/frame.py`; `compile_cohorts(study: CompiledStudy, ...) -> CompiledStudy` in `terrine/compile/shapes/cohorts.py` transforms the already compiled LTV relation only. `Frame.cohorts(...)` returns `CohortTable(parent_frame, grouping, value_column)` and uses a shared verified Frame materialization helper to apply the aggregate inside the parent's one execution transaction, preserving deferred checks, eligibility audit, snapshot, provenance and output budgets. Do not accept/compile a second Frame or run an independent source query. Group by fixed elapsed buckets; deduplicate each unit to its selected first entry. `at_risk` is the fixed unique entrant cohort size; it is not a KM risk set and does not shrink at churn/membership exit. `observable` counts entrants with the complete metric interval under source and independent unit observation bounds; if the entire age is beyond cutoff both counts are null, while a started age with no fully observable members has integer `observable=0`. Publish total/mean only for the full at-risk population; mean denominator is cohort entrants, never survivors.
- [ ] **Step 6: Verify/docs/public workflow.** Run `uv run pytest tests/compile/test_ltv.py tests/compile/test_cohorts.py tests/integration/test_trajectories.py -q`. Integration asserts `test_design_cutoff_updates_ltv_and_cohort_with_readiness_fixed` with unchanged readiness, demonstrates `Design.build(model).cohorts(...).to_pyarrow(budget=...)`, and exercises mutation timing: a source commit before the fresh collection transaction is visible in that transaction; a write after its execution view is established is excluded; published inputs remain sealed and invalidated backing/authority refuses. Budgets abort rather than truncate. Update example, guide and changelog.
- [ ] **Step 7: Commit.** `git add terrine/spec terrine/compile/shapes/ltv.py terrine/compile/shapes/cohorts.py terrine/compile/dispatch.py terrine/data.py terrine/frame.py terrine/__init__.py tests/compile/test_ltv.py tests/compile/test_cohorts.py tests/integration/test_trajectories.py examples/trajectories_survival.py docs/guides/trajectories-survival.md CHANGELOG.md && git commit -m "feat: add cohort LTV trajectories"`.

### Task 3: Event/exit states, administrative cutoff and Kaplan–Meier


**Depends:** 4.2 for verified Frame/compiler lifecycle; estimator integration also depends on 4.3's materialized-data/coded-refusal contracts.

**Files:**
- Create: `terrine/compile/shapes/time_to_event.py`, `terrine/estimate/survival.py`
- Modify: the existing `terrine/compile/dispatch.py`, shared metadata/serialization only as required.
- Tests: `tests/compile/test_time_to_event.py`, `tests/estimate/test_survival.py`

**Interfaces:**
- Consumes: Task 1 `first_occurrence`; `TimeToEvent`; existing anchor population and shared `MaterializedFrame`/`ColumnMeta`/`Provenance`; Task 4.3 estimator input conventions for coded refusals and Arrow-only estimation; source `BoundCoverage` and unit observation bounds.
- Produces: `compile_time_to_event(design: Design, model: Model) -> CompiledStudy` with exact duration/event/entry/exit-censor state, independent administrative cutoff, completeness/reason and unit identity. `kaplan_meier(data: MaterializedFrame, *, duration: str, event: str, entry: str | None = None) -> SurvivalCurve` computes product-limit KM with Greenwood variance from materialized rows only.

- [ ] **Step 1: Write hand-calculated interval/KM tests.** Add `test_km_ties_and_time_zero_events_match_hand_calculation`, `test_km_subsecond_event_censor_and_entry_order_exactly`, `test_only_first_event_inside_eligible_interval_is_selected`, `test_churn_event_is_not_relabelled_as_censor`, `test_incomplete_initial_history_requires_declared_delayed_entry`, and `test_cutoff_updates_completeness_and_risk_denominators`. Use exact tied rows plus event/entry/censor timestamps at 1ns, 2ns and 3ns; include prevalent event before t0, event before delayed entry, event at t0, and events at observation-end/exit/cutoff. Assert prevalent exclusion, incomplete-initial-history reason, exclusive-end equality censoring, exact ties/risk sets, and results rebuilt with two cutoffs under unchanged readiness.
- [ ] **Step 2: Run focused red checks.** Run `uv run pytest tests/compile/test_time_to_event.py tests/estimate/test_survival.py -q`.
- [ ] **Step 3: Compile independent event/exit histories.** Require one analysis entry per unit for KM (refuse `EventAnchor(which="each")` or duplicated unit rows rather than treating episodes as independent). For each candidate, apply event-source `BoundCoverage`, unit `_observed_from/_observed_until`, optional declared entry and `Design.administrative_cutoff`. Without declared delayed entry, a history start later than t0 cannot imply survival through the gap: emit the stable incomplete-history refusal/reason and exclude from KM. With explicit delayed entry, left truncate to the latest declared/supported start. Use Task 1's exact first-occurrence lookup; if a known event is before lower bound it is prevalent and excluded rather than replaced by a later event. The exclusive upper bound is the earliest cutoff, coverage end, observed-until and declared exit. Select earliest event `>= lower` and `< upper`. Churn is event only when `TimeToEvent.event` names it; membership exit alone is not censoring.
- [ ] **Step 4: Implement KM on materialized rows only.** Validate unique unit identity, boolean event and normalized elapsed seconds/nanos. Compute entry, event and censor durations on one row-t0 axis; convert each absolute cutoff exactly as `cutoff - Roles.t0`, never compare absolute timestamps to elapsed durations. Compare/tie on exact pairs, include time zero, process tied failures against the pre-time risk set before tied censors leave subsequent risk sets, and admit delayed entry at equality only when declared. Compute product-limit and Greenwood variance; refuse impossible risk sets with stable coded errors. No compiler/Ibis/Frame/increment imports.
- [ ] **Step 5: Verify and document.** Run `uv run pytest tests/compile/test_time_to_event.py tests/estimate/test_survival.py -q`; round-trip fractional-second curve data through Arrow metadata; prove increment-blocked imports. Update guide/changelog.
- [ ] **Step 6: Commit.** `git add terrine/compile/shapes/time_to_event.py terrine/compile/dispatch.py terrine/estimate/survival.py terrine/data.py tests/compile/test_time_to_event.py tests/estimate/test_survival.py docs/guides/trajectories-survival.md CHANGELOG.md && git commit -m "feat: add time to event and Kaplan Meier"`.

### Task 4: Earlier-only backtests and Arrow surrogate export

**Files:**
- Modify: `terrine/estimate/survival.py`, `terrine/data.py` metadata/Arrow serialization, `terrine/frame.py` execution lifecycle only as required.
- Create: `tests/estimate/test_survival_backtest.py`, `tests/integration/test_trajectories.py`, `examples/trajectories_survival.py`, `docs/guides/trajectories-survival.md`
- Modify: `CHANGELOG.md`.

**Interfaces:**
- Consumes: `MaterializedFrame`, typed completeness/reason metadata, exact absolute cutoffs and row `Roles.t0`, source generations/execution snapshot, Task 3 `SurvivalCurve`, and Task 2 recipe-backed `CohortTable`.
- Produces: `backtest_survival(data: MaterializedFrame, *, duration: str, event: str, entry: str, prediction_cutoffs: Sequence[datetime], fitter: Callable[[MaterializedFrame], object], knowledge: Literal["measurement_time", "recorded_at"] = "measurement_time") -> tuple[object, ...]`. For each absolute cutoff `c`, retain rows only when exact `t0 + entry_elapsed < c` (equivalently `entry_elapsed < c - t0`), compare event time to that same row-relative elapsed cutoff, recensor at `min(original_end, c - t0)`, expose event only if strictly before cutoff, and censor equality. `to_surrogate_arrow(...) -> ArrowSurrogate` retains identity, cutoff, exact outcomes, missingness and versioned provenance.

- [ ] **Step 1: Write leakage/recensor and Arrow tests.** Add `test_backtest_recensors_survivors_and_is_post_cutoff_invariant`, `test_backtest_rejects_naive_or_unsorted_cutoffs`, `test_backtest_uses_cutoff_elapsed_from_each_row_t0`, `test_recorded_at_request_refuses_without_recorded_at_source`, and `test_surrogate_arrow_round_trip_preserves_identity_cutoffs_null_reasons_and_provenance`. Include different t0 values, delayed entries, early failures, post-cutoff failures and event-free survivors. Assert each fitter receives only earlier entrants by `entry_elapsed < cutoff - t0`; late failures are recensored at row-specific cutoff elapsed; cutoff equality censors; post-cutoff outcome mutations cannot alter fit inputs; a late correction to a pre-cutoff effective-time feature is labeled `retrospective_effective_time`, not claimed historically known; recorded-at request refuses absent source evidence. Round-trip asserts nanosecond struct precision, identity/cutoff/outcome/missingness, schema version and design/model/source-generation/snapshot/operation provenance.
- [ ] **Step 2: Run focused red checks.** Run `uv run pytest tests/estimate/test_survival_backtest.py -q`.
- [ ] **Step 3: Implement measurement-time recensoring.** For each cutoff, select entrants strictly before it and do not drop a subject merely because eventual observed follow-up crosses it. Rebuild survival inputs with same t0 elapsed axis, censor at cutoff, and mask outcomes occurring at/after cutoff before invoking fitter. Label this retrospective effective-time data. Do not infer recorded-at availability from event/effective time or `FeatureCutoff.knowledge`; `knowledge="recorded_at"` refuses unless the source's explicit recorded-at metadata supports the requested historical feature set. No bitemporal implementation in this task.
- [ ] **Step 4: Implement versioned Arrow export.** Export requested projections plus required unit/time/cutoff and support columns. Use exact `ElapsedTime` struct and canonical metadata with schema version, cutoffs, design/model hashes, generations, execution snapshot, completeness/reason/certificate references, and typed `CohortOperationMeta` where exporting cohort-derived data. Preserve decimal/null/Arrow types. No row payloads in metadata, increment import, or survival×value multiplication.
- [ ] **Step 5: End-to-end local verification.** Run `uv run pytest tests/estimate/test_survival_backtest.py tests/integration/test_trajectories.py -q` and `uv run python examples/trajectories_survival.py`. Example uses local DuckDB only; demonstrates Design→Frame→same-frame cohort Arrow collection under budget, backtest on two absolute cutoffs with different row t0 values, cutoff completeness under unchanged readiness and Arrow metadata. Print hand-checkable cohort/KM/export results. No warehouse/network or paid resources.
- [ ] **Step 6: Update guide/changelog and commit.** Document event/censor, independent admin cutoff, cohort denominator, age grouping, measurement-time backtest limits, exact time serialization and surrogate schema. Explicitly state no survival-times-value identity. `git add terrine/estimate/survival.py terrine/data.py terrine/frame.py tests/estimate/test_survival_backtest.py tests/integration/test_trajectories.py examples/trajectories_survival.py docs/guides/trajectories-survival.md CHANGELOG.md && git commit -m "feat: add trajectory backtests and Arrow export"`.

## Self-Review

- **Spec coverage:** Expanded/cumulative LTV, fully-covered-null versus observed-zero distinctions, same-frame recipe and typed operation metadata, exact subsecond KM, delayed entry/initial history, independent cutoff/provenance, Arrow surrogate export, earlier-only measurement-time recensoring and no unsupported recorded-at claim are assigned and tested. Each of the five Review Focus risks maps to a named test.
- **Step scan:** Each plan task is independently testable and reviewable, names interfaces/files/tests/assertions/verification commands/commit. Cohort collection transforms one LTV relation inside the verified Frame lifecycle; survival consumes materialized Arrow only.
- **Type/snapshot consistency:** `ElapsedTime`, absolute `Roles.t0`, row-relative entry/event/end and cutoff conversion share one exact axis. Cohort denominators are unique entrants, not KM risk sets. A new materialization transaction sees commits before collection; writes after its execution view is pinned are excluded; sealed published inputs do not mutate.
- **Proportion and gates:** Plan specifies decisions and interfaces, not code bodies. Adopting/vendoring Callaway–Sant'Anna and licensing/release remain explicit owner gates; no package or release decision is implied.
- **Spec reconciliation:** Product-limit KM is descriptive and no LTV×survival identity is claimed. Backtest v1 is explicitly measurement-time/retrospective effective-time only; recorded-at availability and stronger historical-known guarantees are not asserted.

