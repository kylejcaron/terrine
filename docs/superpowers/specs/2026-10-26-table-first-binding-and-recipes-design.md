# Table-First Binding and Dataset Constructors

## Status and purpose

This specification defines two public usability layers that compile to Terrine's existing native model and design abstractions: `Model.from_tables(...)` for the common table-backed source case, and `terrine.recipes` constructors for common dataset row shapes. It does not change execution, staging, serialization, refusal, or materialization workflows.

## Scope confirmation

The owner confirmed table-first binding plus one constructor per dataset type, while keeping the low-level API, in the parent public-readiness scope ledger, row 3 (`docs/superpowers/plans/2026-10-26-public-readiness.md`). The family and two-layer scope are therefore fixed.

## API boundaries

There are two supported public layers:

1. **Dataset constructors** in `terrine.recipes` are the documented starting point for common row meanings. They return ordinary `terrine.spec.Design` instances and never query or materialize data.
2. **Low-level `Design` API**—`Design`, anchors (`FixedDate`, `Schedule`, `EventAnchor`), expressions (`agg`, `asof`, `event_time`, and related expression helpers), `shape=`, and `terrine.spec` transforms including `Sequential` and `EventPanel`—is the full-flexibility layer to which constructors compile. It remains public, supported, and unchanged; it is not an alias for `terrine.recipes`.

A caller may inspect a constructor result directly as a `Design`. Documentation must show an equivalent low-level `Design` for each recipe; constructors introduce no wrapper type or representation.

## `Model.from_tables`

### Signature and binding behavior

```python
@dataclass(frozen=True, slots=True)
class Events:
    table: str
    time: str
    keys: Mapping[str, str]  # exactly one unit name -> event-table key column
    values: tuple[str, ...] = ()
    complete: tuple[date | str, date | str] | None = None

Model.from_tables(
    con,
    *,
    units: Mapping[str, UnitSource],
    events: Mapping[str, Events],
    dialect: str | None = None,
    day_boundary: str = "UTC",
    readiness: Mapping[str, Coverage] | None = None,
    event_ids: Mapping[str, str] | None = None,
) -> Model
```

`Events` is an immutable, validated descriptor following the frozen typed-record style of `UnitSource` and `Coverage` in `terrine.data`. `table` is a relation name, `time` is its event-time column, `keys` maps exactly one declared model unit name to that table's corresponding key column, `values` declares numeric event-value columns, and `complete` is an inclusive local-calendar-day `(start, end)` pair in the Model's `day_boundary` timezone. The constructor normalizes `values` to a tuple and detaches/freezes `keys`. Each event descriptor requires `complete`; omission refuses with stable code `terrine.model.missing_event_completeness`. It never infers completeness from observed rows or event maxima. More than one key entry refuses with stable code `terrine.model.multiple_event_units`, with context identifying the event source; use `Model.from_catalog` to define multi-unit source semantics.

`units` is required and remains the authoritative roster, including units with no matching events. It is never derived from event relations. `events` is a mapping keyed by stable source name; its sole key must name a declared unit. `complete` becomes that generated event source's `Coverage` in the readiness mapping; optional `readiness` adds coverage for other catalog sources. Attribute binding is intentionally not part of this API: use `Model.from_catalog` for static/as-of attributes, filters, ratios, SCD2 dimensions, SQL relations, or multi-unit event sources.

The method assembles the equivalent native Catalog as an ordinary mapping, then calls `Model.from_catalog` with that mapping and `units` as a plain mapping, forwarding the remaining binding options. The separate `from_catalog` mapping-input extension is a prerequisite. Binding is metadata-only and does not scan event rows.

### Generated sources, facts, and metrics

For each event source name `source`, the source name itself is the generated event fact, usable as `start_event=` or `event=` in recipes. It represents event presence/count. For each declared value column `v`, generate a value fact named `<source>_<v>`. Generate these metrics:

- `<source>_count`: count of the event fact;
- `<source>_<v>_sum`: sum of value fact `<source>_<v>`;
- `<source>_<v>_mean`: mean of value fact `<source>_<v>`.

There are no `tr.count` or `tr.sum` shorthands. Use existing `agg(metric, window)` expressions to consume metrics. Any name collision among generated source, fact, or metric names refuses before binding with stable code `terrine.model.generated_metric_collision`; context identifies the conflicting name and source names. If an explicit Catalog is allowed to mix with generated definitions, a generated name also colliding with an explicit Catalog name refuses with that same code. Unrepresentable names refuse rather than silently rewrite.

Aggregation semantics are exactly those of equivalent hand-written Catalog metrics: in complete windows, an empty count and sum are `0`; an event mean is null with reason `no_observations`. An incomplete window is null with the existing incompleteness reason, regardless of aggregation. Null values within a nonempty event set follow existing aggregate semantics.

## `terrine.recipes` constructors

Each constructor returns a plain `Design`, never data. All accepted keywords have an observable effect and are forwarded into the corresponding Design/shape. Keywords unsupported by that shape are absent from its signature; Python's normal `TypeError` applies rather than silently dropping them. Parameters below are the complete public signature contracts. Date scheduling uses calendar dates or ISO calendar-date strings, never timestamps.

```python
def time_series(
    unit: str, *, every: str | int, start: date | str, end: date | str,
    features=None, outcomes=None, eligibility=None, treatment=None,
    anchor_range=None, gap="0d",
) -> Design

def trial_panel(
    unit: str, *, every: str | int, start: date | str, end: date | str,
    treatment: Initiation | PersistentAdoption, trial_weight: TrialWeight | None = None,
    anticipation="0d", reference_period="-1d", features=None, outcomes=None,
    eligibility=None, anchor_range=None, gap="0d",
) -> Design

def event_panel(
    unit: str, *, event: str, periods: tuple[int, ...], period: str | int,
    occurrence: Literal["first", "each", "nth"] = "first", n: int | None = None,
    reference_period: int = -1, features=None, outcomes=None, eligibility=None,
    anchor_range=None, gap="0d",
) -> Design

def ltv(
    unit: str, *, start_event: str, metric: str, period: str | int, periods: int,
    features=None, eligibility=None, treatment: Treatment | None = None,
    anchor_range=None, gap="0d", administrative_cutoff: datetime | None = None,
) -> Design

def survival(
    unit: str, *, start_event: str, end_event: str,
    occurrence: Literal["first", "nth"] = "first", n: int | None = None,
    features=None, eligibility=None, treatment: Treatment | None = None,
    anchor_range=None, gap="0d", administrative_cutoff: datetime | None = None,
) -> Design

def life_table(
    unit: str, *, start_event: str, end_event: str, bin: str | int,
    occurrence: Literal["first", "nth"] = "first", n: int | None = None,
    features=None, eligibility=None, treatment: Treatment | None = None,
    anchor_range=None, gap="0d", administrative_cutoff: datetime | None = None,
) -> Design

def state_panel(
    unit: str, *, state_source: StateObservations | BinnedMetricState,
    every: str | int, start: date | str, end: date | str, periods: int,
    features=None, eligibility=None, anchor_range=None, gap="0d",
) -> Design

def event_sequence(
    unit: str, *, event: str, horizon: str | int, max_events: int,
    occurrence: Literal["first", "each", "nth"] = "first", n: int | None = None,
    session_gap: str | int | None = None, features=None, outcomes=None,
    eligibility=None, anchor_range=None, gap="0d",
) -> Design
```

`time_series` builds `Design(unit, Schedule(every, start, end), ...)`; the schedule end is exclusive, and its dates—not a redundant periods count—define which anchors exist. `trial_panel` builds a scheduled base Design using `Schedule(every, start, end)`, then applies `Sequential(base, treatment=..., trial_weight=..., anticipation=..., reference_period=...)`. It passes no unsupported `periods` or `period` arguments to `Sequential`.

`event_panel` builds an `EventAnchor` and applies `EventPanel(base, periods=..., period_seconds=..., reference_period=...)`. It accepts no `treatment`, because `EventPanel` rejects a Design that carries treatment. `ltv` uses the start event as the `CohortSpec.origin` and a cumulative `Trajectory`; it accepts no arbitrary outcomes because the trajectory defines the shape's outcome. LTV accepts a `Treatment` when supplied, as the low-level compiler preserves that role. The origin event contributes to age 0 in the existing half-open `[t0, t0 + period)` window; this preserves the current low-level behavior and does not change the compiler.

`survival` uses `TimeToEvent`; `life_table` uses `TimeToEvent` plus `LifeTable`. These recipes select either the first start event by default or a requested nth start event; they do not accept `each`. They require both start and terminal event facts. Neither shape has a relative duration horizon: follow-up ends according to existing observation coverage or the optional absolute `administrative_cutoff`. They accept `Treatment` as supported by the underlying time-to-event compiler, but no arbitrary `outcomes` mapping. `life_table` is separate because it returns risk-set bins, not merely a display option.

`state_panel` uses `Schedule` and `StatePanel`; it accepts no `outcomes` or `treatment`, which the shape does not support. `event_sequence` uses `EventSequence` and requires its bounded window (`horizon`) and `max_events`. For the event-selected anchors in panel/survival/sequence recipes, occurrence defaults to `first`; `each` is exposed only where the low-level shape accepts it, and `n` is required and positive for `nth`. For `first` or `each`, `n` is omitted. Conditional events remain an expression pattern: use an `EventAnchor` and an expression selector with `relative_to=event_time(...)`; there is no conditional-event constructor.

Shared Design keywords are not a catch-all: each signature above lists only supported parameters. `features`, `eligibility`, `anchor_range`, and `gap` are accepted where the underlying shape preserves/uses them. `outcomes` are accepted only by time-series, trial-panel, event-panel, and event-sequence designs; `treatment` is accepted as a treatment role only by time-series, LTV, survival, and life-table designs (trial-panel takes a required `Initiation`/`PersistentAdoption` treatment definition instead); and `administrative_cutoff` is accepted only by LTV, survival, and life-table designs. Recipe tests and examples assert an observable effect for every accepted keyword.


## Public documentation and policy updates

Update `AGENTS.md` under “Public API and refusals” to state: constructors in `terrine.recipes` are the documented starting point for common dataset types; the low-level `Design` API is their full-flexibility compilation target and remains public and fully supported, not an alias. Update `docs/guides/public-api.md` to list the module and both layers. Add a table-first binding section to `docs/guides/temporal-engine.md` showing `Events`, `Model.from_tables`, roster retention of zero-activity units, explicit completeness, generated names, materialization, and when to choose `Model.from_catalog`; add one runnable `examples/table_first_dataset.py` example and execute it in CI. Constructor guides must be constructor-first and include equivalent low-level `Design` examples under “Under the hood.” Update refusal reference for the new codes and add the public additions to the unreleased CHANGELOG section without changing version `0.1.0.dev0`.

## Acceptance and review focus

- A single integration composition compares rows from `Model.from_tables` with a hand-written native Catalog for count, sum and mean metrics, including zero-activity roster units, complete/incomplete windows, and a nonzero LTV origin amount plus later contribution proving that age 0 includes the origin event.
- A `from_tables`-bound source is used as an event fact by a recipe in a runnable table-first example.
- Every constructor has a runnable example executed by CI; `trial_panel` demonstrates schedule dates.
- Tests pin required row-defining arguments, occurrence restrictions/defaults, explicit completeness, exactly-one-unit key restriction, stable collision/missing-completeness/multi-unit refusal codes and refusal pickle/deepcopy, the empty-window value/reason contract, and unchanged low-level construction/serialization behavior.
- Preserve null versus zero and per-cell null reasons through Arrow, manifests and exports; preserve the version and all repository validation constraints in `AGENTS.md`.
