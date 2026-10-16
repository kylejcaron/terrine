# terrine: build handoff

Design and build plan for **terrine**, a sister package to `increment` that compiles declarative study specs into point-in-time-correct analysis datasets ("frames") from increment's semantic layer. Estimators and diagnostics plug in behind those frames, and an agent works by editing specs rather than writing SQL.

The core bet: in observational work the estimator is a commodity and the *design* is where analyses fail (misaligned time zero, covariates measured after treatment, immortal time bias, leakage across windows). Panels, event-anchored windows on a cadence, cross-sections, cohorts, LTV and event sequences are all one grammar with different anchors and output shapes, so temporal correctness is enforced once, in the compiler.

---

## 0. How to use this document

- Read **§2 Decisions** and **§3 Guardrails** before writing code. They are settled; don't relitigate them.
- **§19 Open decisions** need the owner's input. Pick the stated default only if the owner hasn't answered, and say so in your PR.
- API code in this doc is illustrative. Names and signatures can change; the semantics (roles, windows, invariants) cannot.
- Work in phase order (**§18**). Every phase ends with a vertical slice that runs end to end on synthetic data and meets its exit criteria.
- Start by reading increment's existing semantic layer and test suite. Several phases depend on details not captured here (notably how attributes are modeled over time).

---

## 1. Context

- **Owner:** Kyle, solo developer. Builds heavily with AI coding agents; maintainability by agents is a design goal (strong tests, clear module boundaries, small PRs).
- **increment:** open-source Python A/B testing and causal inference package. Repo `github.com/kylejcaron/increment`. Apache-2.0; contributions require a CLA. Docs use MkDocs + Material. Alpha is on PyPI but **not yet announced**. Long-term intent is an open-core commercial product.
- increment already has a **semantic layer** for metric definitions. terrine builds on it.
- **Branching:** terrine, the agent, and observational work live on an experimental branch/worktree and are **not released before increment's public launch**. Exception: the Phase 1 extraction of the semantic layer should land *before* the alpha is announced, because import paths become public API at announcement.
- Target users: product data scientists and analysts; the agent layer targets non-experts.

---

## 2. Decisions (settled)

| Area | Decision |
| --- | --- |
| Package name | `terrine` (import `terrine`) |
| Repo structure | One uv workspace (monorepo) with separate distributions, not separate repos |
| Packages | `increment-semantic` (shared core), `increment` (experiments), `terrine` (observational studies and datasets), `increment-agent` (later). The `increment-semantic` and `increment-agent` names are working names |
| Import names | Distinct top-level imports (`increment_semantic`, `terrine`, `increment_agent`). Not namespace packages, because `increment` is a regular package |
| Extraction timing | Extract the semantic layer before the alpha is announced |
| Query layer | ibis expressions are the public contract. xorq is an optional extra for caching and execution |
| Heavy dependencies | econml, DoubleML, lifelines, NumPyro, distributed trainers are optional extras. Native code covers only basics |
| Control time zero | Sequential trials are the default; matched t0 is supported |
| Window aggregation | Computed from per-unit cumulative (prefix-sum) tables via point lookups, never via anchor × window range joins |
| Data residency | All materialized tables live in the customer's warehouse, in a schema they own, built with their compute. We store only metadata, and that metadata also lives in their warehouse |
| Orchestration | Orchestrator-agnostic core (planner + state table + idempotent tasks); adapters for Airflow, Dagster, dbt are thin |
| Agent | Edits specs, never writes SQL. Causal estimates are gated by diagnostics |

---

## 3. Scope and guardrails

**Goals**

- One grammar covering cross-sections, calendar panels, event-anchored panels, repeated-cadence snapshots, cohorts, LTV, time-to-event and (later) event sequences.
- Leakage-free by construction: no feature measured at or after time zero, enforced by a static validator, not by convention.
- Frames carry column roles (unit, time, treatment, outcome, features, weights, censoring) so estimators need no manual wiring.
- The same frames serve descriptive analytics and causal estimation.
- Specs serialize to YAML for reproducibility and serve as the agent's interface.
- Scales to warehouse-sized data (tens of millions of units, billions of events).

**Hard guardrails (do not cross)**

- **Do not implement synthetic control, augmented synthetic control, SynthDiD, or related estimators**, even where panel data makes them a natural fit. This is an owner IP constraint. Panel estimation stays in the DiD / event-study family.
- **Product-analytics framing only.** Examples, docs and positioning use product questions: feature adoption, onboarding changes, rollouts that couldn't be randomized. No marketing incrementality, media, or channel-attribution examples.
- LTV stays generic (cohort curves, survival × value), not tied to acquisition channels.
- terrine is not a new estimator library. Native estimators cover the basics; everything else sits behind adapters.
- Nothing from this plan ships publicly before increment's launch (Phase 1 extraction excepted).

---

## 4. Package layout

```
workspace/
  packages/
    increment-semantic/   # import increment_semantic  (shared core)
    increment/            # import increment           (experiments)
    terrine/              # import terrine             (studies and frames)
    increment-agent/      # import increment_agent     (later)
```

**Dependencies (arrows = "depends on")**

```
increment        -> increment-semantic
terrine          -> increment-semantic
increment-agent  -> increment-semantic, terrine
increment-semantic -> ibis (+ optional xorq)
```

- `increment` and `terrine` are siblings and do not import each other. The surrogate-index hook (terrine's LTV frames as long-term outcome training data for increment) passes data across an interface still to be designed (§19).
- **import-linter contract:** `increment_semantic` imports nothing from `increment`, `terrine` or `increment_agent`.
- `increment_semantic` must contain **no notion of variants, assignment or exposure**. If those concepts have leaked into metric or entity definitions, move them into `increment` during extraction.
- **Materialization runtime** (state table, planner, task runner, CLI, orchestrator adapters): default home is `increment_semantic`, since it owns the daily and cumulative tables. terrine registers its own layers (anchors, frames) with the planner through a plugin interface. See §19.
- Contents at a glance:
  - `increment-semantic`: entities, events, metrics, attributes, time spine, unit × day and cumulative tables, as-of attribute history, compilation to ibis, materialization runtime.
  - `increment`: A/B tests, CUPED, HTE, switchbacks, variants/assignment/exposure, surrogate index (planned).
  - `terrine`: study specs, primitives, validator, frame shapes, native estimators, diagnostics, adapters (extras).
  - `increment-agent`: MCP server over the semantic core and terrine.

---

## 5. Core model: primitives

Two time axes: **calendar time** (each row's time zero, `t0`) and **relative time** (offsets from `t0`). Every frame is built from a small set of primitives.

### 5.1 Core primitives

1. **Anchor**: a table of `(unit_id, anchor_id, t0)`. Constructors:
    - `FixedDate(date)`: every eligible unit gets the same t0.
    - `EventAnchor(event, which="first" | "each" | "nth")`: t0 = occurrence(s) of an event per unit.
    - `Schedule(every, start, end)`: eligible units × cadence dates (repeated snapshots, sequential trials, backtests).
2. **Window**: a relative interval `[a, b)` in offsets from t0, plus a grain. Lookback, gap, grace and horizon are all windows. A window is a value, not an operator.
3. **Aggregate**: `agg(metric, window)` produces one column per anchor row. Includes time-valued aggregates such as "first occurrence time in window," which give adoption times and durations.
4. **As-of**: `asof(attribute, offset)` returns an attribute's value at `t0 + offset`, read from validity-ranged history.
5. **Expand**: `anchors × periods(k)` produces long format at relative grain, for trajectories.

**Censoring is a property, not a primitive.** Every aggregate carries `observed_until = min(t0 + window_end, data_end, exit_date)`. Cells past it are **null, never zero**.

### 5.2 Sequence primitives (later phase)

The core five aggregate from unit × day tables, which have already discarded event order. Event sequences need three additions:

6. **Sequence**: `seq(events, window, order_by="ts", max_len=n)` returns the ordered events in a window, as an array column per anchor row or long at `(anchor, position)`. Reads the raw event table, not daily rollups, so the window and `max_len` keep it bounded. Ties need a deterministic tie-break (e.g. `event_id`) or results won't reproduce.
7. **Re-anchoring**: windows can be relative to any time-valued column, not only t0. Example: `t_b = first(B, window=(t_a, t_a + 1h])` where `t_a = first(A, ...)`.
8. **Sessionize**: gap-based grouping of events into sessions (e.g. new session after 30 minutes idle), producing a session entity that can itself be anchored.

### 5.3 Everything else is composition

| Concept | Composition |
| --- | --- |
| Eligibility | Filter on aggregates and as-ofs whose windows end at or before `-gap` |
| Treatment | Aggregate over `[0, grace]`: a boolean, or a first-occurrence time |
| Features | Aggregates over lookback windows, plus as-ofs at t0 |
| Outcomes | Aggregates over horizon windows; trajectories = Expand + Aggregate |
| Funnels | Chain of first-occurrence aggregates, each re-anchored on the previous step |
| Transitions / paths | Counts of consecutive event pairs within a window; per unit as sparse features, across units as a transition table for path analysis |
| Backtests | Any spec rerun on past `Schedule` cutoffs, with treatment swapped for a pseudo-treatment date (placebo) or outcomes held out (forecast validation) |

---

## 6. Validation: the temporal type check

Every column carries `(role, window)`. Validation is **static** and needs no data.

| Role | Rule |
| --- | --- |
| feature, eligibility | window end ≤ `-gap` (gap ≥ 0; covers lagged pipelines) |
| attribute (as-of) | offset ≤ `-gap` |
| treatment | window ⊆ `[0, grace]` |
| outcome | window start ≥ 0 |

- **Re-anchored windows** need interval-bound propagation through the chain. Example: if `t_a ∈ [t0 − 28d, t0)` and a window is `(t_a, t_a + 1h]`, its upper bound is `t0 + 1h`, which leaks into a feature. The validator must clip it at t0 or reject it.
- **Outcomes overlapping the grace window** cannot be settled by the type check alone. Strict target trial emulation handles this with clone-censor-weighting. Default is an open decision (§19).
- The validator **rejects** invalid specs. Property tests inject leaks deliberately and assert each is caught (§17).

### 6.1 Control time zero

Event anchors give treated units a natural t0 but leave controls without one, which is the usual source of immortal time bias.

1. **Sequential trials (default):** a `Schedule` anchor makes each cadence date a trial. Units initiating treatment within the grace window are treated; eligible non-initiators are controls. Units appear in many trials, so inference clusters by unit.
2. **Matched t0:** controls get pseudo-t0s drawn from the treated t0 distribution, optionally within strata.

---

## 7. Frame shapes

The shape sets the row grain; the anchor sets t0.

| Shape | Row grain | Built from | Feeds | Example question |
| --- | --- | --- | --- | --- |
| CrossSection | unit (× t0) | `FixedDate` or `EventAnchor` + columns | IPW, AIPW, DML, causal forests | Did enabling autopay change 8-week revenue? |
| Sequential | trial × unit | `Schedule` + columns | Sequential target trial emulation, stacked DiD | Effect of starting a habit, re-measured weekly |
| CalendarPanel | unit × calendar period | Special case of Sequential: `Schedule` at every period, one-period outcome window, treatment = adopted by t0 | Staggered DiD, event study | Effect of a staggered rollout across regions |
| EventPanel | unit × relative period k ∈ [−L, H] | `EventAnchor` + Expand | Event-study plots, pre-trends, trajectories | How does activity evolve around a first support ticket? |
| LTV | unit × age, with censoring | `EventAnchor(first)` + cumulative Expand | Cohort extrapolation, survival × value, surrogate index training | What is a new user worth at 12 months? |
| CohortTable | cohort × age | LTV grouped by `(cohort(t0), k)` | Retention and cumulative value curves | How do monthly signup cohorts retain? |
| TimeToEvent | unit | Anchor + first-occurrence aggregate + censoring | Kaplan–Meier, hazard models | Time to churn or second order |
| EventSequence (later) | anchor × position, or array column | Anchor + `seq` | Path analysis, sequence features | What paths precede churn? |

- CohortTable is always built by aggregating the LTV frame, so the two never disagree.
- CalendarPanel can be exposed as a named convenience, but it compiles to Sequential.

---

## 8. API sketch (illustrative)

### 8.1 High level: a study spec

```python
import terrine as tr

design = tr.Design(
    unit="user",
    anchor=tr.Schedule(every="1w", start="2025-01-06", end="2025-12-29"),
    eligibility=[tr.active_in("28d"), tr.never_treated()],
    treatment=tr.Treatment(event="autopay_enabled", grace="7d"),
    features=tr.Window(metrics=["orders", "sessions", "revenue"], lookback="28d"),
    attributes=["plan_tier", "platform", "region"],
    outcomes=tr.Window(metrics=["orders", "revenue"], horizon="56d"),
    shape="sequential",
)

design.validate()            # static point-in-time checks; raises on leakage
frame = design.build(model)  # lazy ibis expression + roles + window metadata
df = frame.to_polars()
design.to_yaml("autopay_sequential.yaml")
```

```python
ltv = tr.Design(
    unit="user",
    anchor=tr.EventAnchor("first_order"),
    outcomes=tr.Trajectory(metrics=["revenue"], cadence="1w", periods=52, cumulative=True),
    shape="ltv",
)
cohorts = ltv.build(model).to_cohorts(grain="month")
```

### 8.2 Low level: what a spec compiles to

```python
anchors = tr.anchors.schedule(unit="user", every="1w", start="2025-01-06", end="2025-12-29")

frame = anchors.with_columns(
    active=tr.agg("sessions", tr.window(-28, 0, "d")) > 0,                 # eligibility
    orders_28d=tr.agg("orders", tr.window(-28, 0, "d")),                    # feature
    plan_tier=tr.asof("plan_tier", 0),                                      # feature
    treated=tr.agg("autopay_enabled", tr.window(0, 7, "d")) > 0,            # treatment
    revenue_56d=tr.agg("revenue", tr.window(0, 56, "d")),                   # outcome
).where("active").roles(
    features=["orders_28d", "plan_tier"], treatment="treated", outcome="revenue_56d",
)
```

### 8.3 Funnel with re-anchoring (later phase)

```python
t_view = tr.first("product_view", window=tr.after(0, "7d"))
t_cart = tr.first("add_to_cart", window=tr.after(t_view, "1h"))
t_buy  = tr.first("checkout",    window=tr.after(t_cart, "24h"))
```

---

## 9. Semantic layer requirements

terrine needs these from `increment_semantic`. Inspect the current implementation first; gaps here set the size of Phase 2.

- **Entities** (units), **event sources** with timestamps and a stable event id, **metrics**, **attributes**, and a **time spine**.
- **Declared aggregation kind per metric**, because windows roll up from daily tables:

| Kind | How windows are computed |
| --- | --- |
| sum, count (additive) | Cumulative table differences (prefix sums) |
| min, max, last | Roll up from daily values |
| ratio | Numerator/denominator pair, each additive |
| distinct count, percentile | Mergeable sketches (HLL-style, quantile sketches), or the raw-event path |

- **Unit × day metric tables**, kept **sparse** (no rows for inactive days).
- **Per-unit cumulative tables** for additive metrics.
- **As-of attributes**: `valid_from` / `valid_to` history (SCD2) or daily snapshots. If the layer only models current-state dimensions, this is the first gap to close.
- **Per-source readiness**: the date through which each source is complete (`data_end`).
- No variants, assignment or exposure concepts (those belong to `increment`).

---

## 10. Implementation on ibis

- **Anchors first.** Every build compiles the anchor to `(unit_id, anchor_id, t0)`. All shapes join to it, so they share one code path.
- **Window aggregates via prefix sums.** `agg(m, [a, b)) = C(t0 + b) − C(t0 + a)`, where `C` is the per-unit cumulative table. Read `C` with an as-of join on the sparse cumulative table (latest row at or before the date). Where a backend lacks native as-of joins, emulate with last-value window functions.
- **Schedule anchors** can alternatively use rolling window functions over the per-unit daily series, sampled at cadence dates.
- **Spines for trajectories.** Expand = anchors × integer range `[−L, H]`, then left join. Missing days become zero only inside the observed window; past `data_end` they stay null.
- **Never densify** unit × day. Fill zeros only at the end, for cells before `observed_until`.
- **Censoring column** on every row: `observed_until = min(t0 + H, data_end, exit_date)`.
- **Numeric precision:** store money as integers or decimals. Differencing large float cumulative sums loses precision.
- **Frames are lazy.** A frame is an ibis expression plus metadata (roles, per-column windows, spec hash). Materialize to polars, pandas or Arrow on demand; stream Arrow batches for large outputs.
- **Backends:** DuckDB is the reference backend for tests. Warehouse backends get integration tests on as-of joins and window behavior, where ibis support varies.
- **xorq** (optional extra): cache daily, cumulative and anchor tables while iterating on windows.

---

## 11. Scaling to warehouse-sized data

**Rule one: never materialize the anchor × window range join.**

- **Why prefix sums matter:** with 20M users, 52 weekly trials and an 84-day window, a range join touches up to ~87B rows on a dense table. Prefix sums need ~2B point lookups per metric.
- **Stay sparse**, and use mergeable sketches for distinct counts and percentiles so they roll up like sums.
- **Hash-based control sampling.** Sequential trials multiply rows and controls dominate. Sample controls by a hash of `unit_id` so a unit is consistently in or out across trials (preserving clustering structure and reproducibility). Keep all treated units. Carry inverse-probability weights as a frame role that **every estimator and diagnostic must respect**. At 10 controls per treated unit, intervals are only ~5% wider than with all controls (variance ∝ 1 + 1/k).
- **Sampling only touches nuisance-model training. The estimate always uses every row:**
    - Fit nuisance models on a sample, with hash-based cross-fitting folds.
    - Score all rows in Arrow batches or in-warehouse.
    - Final AIPW stage = mean of pseudo-outcomes, computed in SQL. Cluster-robust SEs = a group-by on unit.
    - Balance, overlap histograms and linear adjustments are sums and sums of squares, so they push down to SQL too.
- **When sampled nuisance training isn't good enough:**
    - Stratified sampling: keep every treated unit, rare-outcome unit and small HTE subgroup; sample only the bulk, with weights.
    - Sufficient statistics: linear nuisance models fit exactly from X'X and X'y computed in SQL; logistic models take a few IRLS passes, each one an aggregation.
    - Distributed training as an optional extra: LightGBM or XGBoost on Spark, Dask or Ray.
    - HTE: DR-learner pseudo-outcomes on all rows, then a final-stage model on binned covariates aggregated in SQL, instead of a causal forest on a billion rows.
- **Cost guardrails:** every build supports a dry run that reports estimated rows and bytes before execution, with a configurable budget per call. Mandatory for the agent.
- **Benchmarks:** the synthetic generator must scale; run a benchmark suite on DuckDB and at least one warehouse.

---

## 12. Estimators and diagnostics

Estimators sit behind a thin adapter that reads frame roles (including weights).

| Shape | Native (core install) | Extras |
| --- | --- | --- |
| CrossSection | IPW, AIPW, regression adjustment | econml (LinearDML, CausalForestDML, DRLearner), DoubleML |
| Sequential / CalendarPanel | Pooled per-trial AIPW, stacked DiD, event-study regression, unit-clustered SEs | Callaway–Sant'Anna (vendored or native, §19), econml per trial |
| EventPanel | Event-study coefficients, descriptive trajectories | — |
| LTV / CohortTable | Empirical curves with censoring | NumPyro extrapolation models |
| TimeToEvent | Kaplan–Meier | lifelines |

- Naive TWFE on staggered adoption raises a warning.
- Synthetic control and SynthDiD are out of scope (§3).

**Diagnostics** run with every estimate, attach to the result, and each returns `pass` / `warn` / `fail` against configurable thresholds:

- **Overlap:** propensity distributions by arm, with suggested trimming.
- **Balance:** standardized mean differences before and after weighting.
- **Pre-trends:** event-time coefficients for k < 0 on panel shapes.
- **Placebos:** the same spec on backtest cutoffs, plus negative-control outcomes declared in the spec.
- **Sensitivity:** E-values and Cinelli–Hazlett omitted-variable bounds.
- **Censoring:** share of rows censored per arm.

---

## 13. Incremental materialization

**We never store customer data.** All derived tables (unit × day, cumulative, anchors, frames) live in the customer's warehouse, in a schema they own, built with their compute. The runtime keeps only metadata, also in that schema. A future hosted product is a control plane that never holds customer data.

**Mechanics**

1. **Definition hashes.** Each derived table is keyed by a hash of the metric or spec that built it. A changed definition means a new table and a full backfill; old tables are garbage-collected.
2. **Watermarks.** A state table records materialized intervals per table. Each run computes only missing date partitions and appends them.
3. **Trailing reprocess.** Each run overwrites the last N partitions (N configured per source, e.g. 3–7 days) to absorb late-arriving data. Partition overwrites are idempotent, so reruns are safe.
4. **Cumulative tables extend cheaply.** `C(d) = C(d − 1) + daily(d)`, recomputed forward only from the oldest rewritten day. A restatement older than the trailing window forces an **explicit** rebuild of that metric.
5. **Frames are cached, not incremental.** With prefix sums they're cheap to rebuild. Cache by spec hash plus the watermarks they read. Sequential frames append new trials as cadence dates arrive.
6. **Physical layout:** partition by date, cluster by `unit_id`.

This is the same machinery as dbt incremental models or SQLMesh interval tracking. Build a minimal version in-house rather than taking either as a dependency.

**State tables (sketch)**

```
_increment_state.intervals
  table_name, definition_hash, interval_start, interval_end,
  plan_id, rows, bytes, materialized_at, status

_increment_state.leases
  plan_id, holder, acquired_at, expires_at
```

---

## 14. Orchestration integration

Customers run different systems (dbt, Airflow, Dagster, cron). The runtime owns **what** to compute; the orchestrator only decides **when**.

**Core**

1. **State in their warehouse** (§13). Every runner sees the same truth, even if a company uses more than one orchestrator.
2. **Planner.** Inputs: definitions, state, upstream readiness. Output: missing work as tasks `(table, interval)` with dependencies `daily → cumulative → anchors → frames`. Task granularity:
    - `daily`: one task per (table, date chunk); parallel across both.
    - `cumulative`: one task per table; a single forward pass over the new dates.
    - `anchors`: one task per anchor spec.
    - `frames`: one task per frame spec.
3. **Idempotent tasks.** Every task overwrites its partitions, so retries and duplicate runs are harmless. Ordering constraints (cumulative tables run forward) are enforced by the planner's dependencies.
4. **Leases** in the state table guard against two orchestrators (or a manual CLI run) materializing the same intervals concurrently. Leases expire, so a failed run never blocks the next one.
5. **Upstream readiness**, configured per source: a fixed lag ("day d is complete by 06:00"), a check query on the source, or a signal from the orchestrator when the upstream job finishes.

**Interfaces**

- CLI: `increment plan` (JSON dry run, including estimated bytes) and `increment run`.
- Python API exposing the same plan/run calls.

**Adapters**

| System | Integration |
| --- | --- |
| Cron, CI, anything | CLI |
| Airflow | Task group: plan task + one dynamically mapped task per layer; sensor or Dataset/Asset for upstream readiness |
| Dagster | Daily-partitioned assets, mapping one-to-one onto intervals |
| dbt Core | Run after `dbt build`, reading dbt models as sources |
| dbt Cloud only | Generate dbt incremental models (target the `microbatch` strategy, which is already time-batched with a lookback); a post-hook writes back to the state table so the planner stays the source of truth |

**Build order:** CLI and Python API → Dagster and Airflow adapters → dbt code generation last (most work, since incremental behavior differs by warehouse adapter).

**Open-core line:** planner, CLI and adapters are open source. A hosted scheduler with monitoring and alerting over the same state table is the paid layer.

---

## 15. Airflow reference implementation (mockup)

**Design choices**

- **Airflow is only the trigger.** `catchup=False`; the state table decides what's missing. Backfills and reruns go through the planner, so Airflow's data intervals never fight the state table.
- **One mapped task per layer** keeps the graph the same shape every run: `wait_for_events → plan → daily[n] → cumulative[n] → anchors[n] → frames[n] → finish`.
- **`trigger_rule="none_failed"` on every layer.** Airflow skips a mapped task with nothing to map, and the default `all_success` rule would cascade that skip through every later layer.
- **The plan task enforces the byte budget and takes a lease** before any work is queued.
- `finish` uses `none_failed`, not `all_done`: with `all_done`, a successful leaf can mark a run green even when an earlier task failed. On failure the lease simply expires.
- Module paths below assume the runtime lives in `increment_semantic` (§19).

**What a customer writes**

```python
from datetime import datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from increment_semantic.airflow import materialize

with DAG(
    dag_id="increment_materialize",
    schedule="0 7 * * *",
    # Alternative: schedule on an Airflow Dataset/Asset updated by the ingest DAG.
    start_date=datetime(2026, 1, 1),
    catchup=False,       # the state table owns backfills, not Airflow
    max_active_runs=1,   # one planner at a time (the planner also takes a lease)
    tags=["increment"],
) as dag:
    # Upstream readiness: wait for the customer's own event load to finish.
    events_loaded = ExternalTaskSensor(
        task_id="wait_for_events",
        external_dag_id="ingest_events",
        external_task_id="load_events",
        mode="reschedule",
        timeout=6 * 60 * 60,
    )

    build = materialize(
        project="/opt/airflow/increment/increment.yaml",  # semantic layer + terrine specs
        conn_id="snowflake_analytics",  # any ibis-supported warehouse connection
        ready_through="{{ ds }}",       # source data complete through this date
        max_parallel=8,
        max_bytes_scanned="2TB",        # plan fails before running if over budget
    )

    events_loaded >> build
```

**What the provider does**

```python
from __future__ import annotations

import logging

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException

log = logging.getLogger(__name__)

LAYERS = ("daily", "cumulative", "anchors", "frames")  # terrine registers anchors and frames


def ibis_from_connection(conn_id: str):
    """Build an ibis backend from an Airflow connection.

    Real version: a per-connection-type mapping (Snowflake, BigQuery,
    Databricks, Postgres, DuckDB) that pulls secrets through the hook.
    """
    import ibis
    from airflow.hooks.base import BaseHook

    return ibis.connect(BaseHook.get_connection(conn_id).get_uri())


@task_group(group_id="increment")
def materialize(
    project: str,
    conn_id: str,
    ready_through: str,
    max_parallel: int = 8,
    max_bytes_scanned: str | None = None,
) -> None:
    @task(multiple_outputs=True)
    def plan(ready_through: str) -> dict:
        from increment_semantic.runtime import Planner, parse_bytes

        planner = Planner.from_project(project, con=ibis_from_connection(conn_id))
        p = planner.plan(ready_through=ready_through)  # definitions vs state table

        if max_bytes_scanned and p.estimated_bytes > parse_bytes(max_bytes_scanned):
            raise AirflowFailException(
                f"Plan would scan ~{p.estimated_bytes_human}; budget is {max_bytes_scanned}."
            )

        planner.acquire_lease(p.id, ttl="6h")
        log.info(p.summary())

        out = {layer: [t.to_dict() for t in p.tasks(layer)] for layer in LAYERS}
        out["plan_id"] = p.id
        return out

    @task(
        retries=3,
        max_active_tis_per_dagrun=max_parallel,
        trigger_rule="none_failed",  # an empty layer is skipped, not failed
    )
    def run(spec: dict) -> dict:
        from increment_semantic.runtime import execute

        # Idempotent: overwrites the task's partitions, then records the interval.
        return execute(spec, con=ibis_from_connection(conn_id)).summary()

    @task(trigger_rule="none_failed")
    def finish(plan_id: str) -> None:
        from increment_semantic.runtime import Planner

        planner = Planner.from_project(project, con=ibis_from_connection(conn_id))
        log.info(planner.complete(plan_id).table())  # releases the lease

    planned = plan(ready_through)

    upstream = None
    for layer in LAYERS:
        mapped = run.override(task_id=layer).expand(spec=planned[layer])
        if upstream is not None:
            upstream >> mapped  # each layer waits for every task in the previous one
        upstream = mapped

    upstream >> finish(planned["plan_id"])
```

---

## 16. Agent interface (`increment-agent`, later)

The agent **edits specs and never writes SQL**. The semantic layer limits it to named entities and metrics; the validator limits it to temporally valid designs; dry-run budgets limit its cost.

Exposed as an MCP server over `increment_semantic` and `terrine`:

| Tool | Does |
| --- | --- |
| `list_entities`, `list_metrics`, `describe_metric` | Browse the semantic layer |
| `query_metric` | Descriptive slices and trends |
| `propose_design` | Draft a spec from a question, with assumptions in plain language |
| `validate_design` | Point-in-time and eligibility checks |
| `preview_frame` | Row counts, arm sizes, censored share, estimated bytes, before anything runs |
| `run_diagnostics` | Overlap, balance, pre-trends, placebos |
| `estimate` | Effect with intervals, gated by diagnostics |

**Modes**

- **Descriptive:** metric queries, cohort curves, trajectories. No gate.
- **Causal:** `estimate` returns an effect only when diagnostics pass. On `warn` it attaches the failing checks. On `fail` it returns the diagnostics and a suggested design change instead of an effect.

The failure mode to design against is a confident causal claim from a bad design, handed to a non-expert. The defenses: the diagnostics gate, specs as the only interface, plain-language assumptions, and cost budgets on every call.

---

## 17. Testing strategy

A **synthetic data generator with known ground truth** is the backbone, and it's what makes agent-driven development safe. It simulates units, timestamped events, time-varying confounders, staggered and event-triggered treatment, heterogeneous effects, churn, late-arriving data and a data cutoff, and returns the true effect for each design. It must scale to benchmark sizes.

- **Leakage property tests:** generate random specs and assert no feature draws on data at or after t0 (minus gap). Inject leaks deliberately, including re-anchoring chains, and assert the validator catches each.
- **Reference builders:** every shape has a slow pandas reference; ibis output must match it row for row on DuckDB.
- **Recovery tests:** each estimator × shape pair recovers the planted effect, with interval coverage near nominal across seeds. Include weighted (sampled-control) variants.
- **Censoring tests:** move `data_end` earlier and assert young cohorts go null, not zero.
- **Materialization tests:** running twice yields identical tables and state (idempotency); late data within the trailing window is absorbed; older restatements are detected and require explicit rebuild; a definition change triggers a new table and backfill; concurrent runs respect leases.
- **Adapter tests:** example Airflow and Dagster definitions run against DuckDB fixtures in CI.
- **Backend tests:** a small fixture runs nightly on each supported warehouse.
- **Benchmarks:** prefix-sum window aggregation and end-to-end frame builds at scale on DuckDB and one warehouse.

---

## 18. Build phases

Each phase ends with a vertical slice on synthetic data. Phase 1 lands before increment's alpha is announced; everything else stays on the experimental branch until after launch.

1. **Workspace and extraction** (before announcement)
    - uv workspace with `increment-semantic`, `increment`, `terrine`.
    - Move the semantic layer out; push any variant, assignment or exposure concepts back into `increment`.
    - import-linter contract: the semantic core imports nothing from the other packages.
    - **Exit:** increment's test suite passes against the extracted core.
2. **Temporal foundations** (semantic core)
    - Aggregation kind per metric; sparse unit × day tables; cumulative tables; time spine; as-of attribute history; per-source `data_end`.
    - Window aggregation via prefix sums and as-of lookups, with the emulation path for backends lacking as-of joins.
    - Synthetic data generator v1.
    - **Exit:** window aggregates match a pandas reference on DuckDB row for row; no densification; decimal precision test passes.
3. **Spec, anchors, validator, first slice** (terrine)
    - Spec with YAML round trip; `FixedDate`, `EventAnchor`, `Schedule`; eligibility; roles and per-column windows; static validator.
    - CrossSection shape, native AIPW, overlap and balance diagnostics, dry-run estimates.
    - **Exit:** recovers a planted effect on confounded synthetic data; the validator catches every injected leak.
4. **Panel family**
    - Sequential (with CalendarPanel as a special case), EventPanel, matched t0.
    - Stacked DiD, event-study regression, unit-clustered SEs, pre-trend diagnostics, backtest placebos.
    - **Exit:** recovers staggered effects with heterogeneous timing; the naive TWFE warning fires where expected.
5. **Cohorts, LTV, time-to-event**
    - Trajectory outcomes, censoring column, LTV, CohortTable built from LTV, TimeToEvent, LTV forecast backtests.
    - Surrogate-index hook interface (LTV frames as long-term outcome training data for increment).
    - **Exit:** cohort curves match hand-computed values; young cohorts never show false zeros.
6. **Materialization runtime**
    - State and lease tables, definition hashes, watermarks, trailing reprocess, cumulative extension, frame caching.
    - Planner with layer plugins (terrine registers anchors and frames); CLI (`plan`, `run`) and Python API; byte budgets.
    - **Exit:** all materialization tests in §17 pass.
7. **Scale**
    - Hash-based control sampling with weights end to end; estimation split (sampled nuisance fits, batch scoring, SQL final stage, clustered SEs via group-by); sketches; benchmark suite.
    - **Exit:** sampled and full estimates agree within tolerance on synthetic data; benchmarks run at a scale the owner sets (e.g. ~1B events) within a documented budget.
8. **Orchestrator adapters**
    - Airflow (per §15) and Dagster adapters; dbt Core recipe. dbt Cloud code generation only if the owner confirms it's in scope for v1.
    - **Exit:** adapter tests in §17 pass.
9. **Estimator adapters and sensitivity**
    - econml, DoubleML, lifelines, NumPyro and distributed-training extras; E-values; Cinelli–Hazlett bounds.
    - **Exit:** adapter estimates match native AIPW within tolerance on shared synthetic cases.
10. **Event sequences**
    - `seq`, re-anchoring with interval-bound propagation, `sessionize`; funnels; transitions; EventSequence shape. This is the only path that reads raw events, so bound it carefully.
    - **Exit:** the validator catches re-anchoring leaks; funnel and transition outputs match a pandas reference.
11. **Agent**
    - `increment-agent` MCP server with the §16 tools, descriptive and gated causal modes, budgets.
    - **Exit:** on an eval set of questions, the agent picks valid designs and declines on scenarios built to fail overlap or pre-trends.
12. **Docs and release**
    - One worked example per shape on synthetic or public data (product framing only); MkDocs pages; first tagged release of terrine after increment's launch.

---

## 19. Open decisions (need owner input)

| # | Decision | Default if unanswered |
| --- | --- | --- |
| 1 | Spec object name: `Design`, or something fitting the terrine metaphor (`Recipe`) | `Design` |
| 2 | Where frame builders live: terrine or the semantic core | terrine |
| 3 | Where the materialization runtime lives (state, planner, CLI, orchestrator adapters) | `increment_semantic`, with terrine registering layers as plugins |
| 4 | Spec format: pydantic models with YAML round trip, or plain dataclasses | pydantic (validation plus JSON schema for the agent) |
| 5 | Frame return type | ibis wrapper carrying roles and windows; polars, pandas or Arrow on demand |
| 6 | Callaway–Sant'Anna: vendor an existing implementation or write a native one on the stacked frame | Ask before starting |
| 7 | How the semantic layer currently models attributes over time | Discover in Phase 2 and report |
| 8 | Outcomes overlapping the grace window: start outcomes at grace end, or implement clone-censor-weighting | Start outcomes at grace end; clone-censor-weighting later as an option |
| 9 | Agent packaging (separate package vs extra) and where the open-core line falls | Separate `increment-agent` package |
| 10 | Surrogate-index hook interface between terrine and increment | Design in Phase 5; neither package imports the other |
| 11 | Final names for `increment-semantic` and `increment-agent` | Keep working names |
| 12 | Whether dbt Cloud code generation is in v1 | Out of v1 |