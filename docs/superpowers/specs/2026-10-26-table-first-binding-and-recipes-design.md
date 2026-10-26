# Table-First Binding and Dataset Constructors

## Status and purpose

This specification defines two public usability layers that compile to Terrine's existing native model and design abstractions: `Model.from_tables(...)` for the common table-backed source case, and `terrine.recipes` constructors for common dataset row shapes. It does not change execution, staging, serialization, refusal, or materialization workflows.

## API boundaries

There are two supported public layers:

1. **Dataset constructors** in `terrine.recipes` are the documented starting point for common row meanings. They return ordinary `terrine.spec.Design` instances and never query or materialize data.
2. **Low-level `Design` API**—`Design`, anchors (`FixedDate`, `Schedule`, `EventAnchor`), expressions (`agg`, `asof`, `event_time`, and related expression helpers), `shape=`, and `terrine.spec` transforms including `Sequential` and `EventPanel`—is the full-flexibility layer to which constructors compile. It remains public, supported, and unchanged; it is not an alias for `terrine.recipes`.

A caller may inspect a constructor result directly as a `Design`. Documentation must show an equivalent low-level `Design` for each recipe (a constructor's returned object/repr need not invent another wrapper or representation).

## `Model.from_tables`

### Signature and binding behavior

```python
@dataclass(frozen=True, slots=True)
class Events:
    table: str
    time: str
    keys: Mapping[str, str]  # unit name -> event-table key column
    values: tuple[str, ...] = ()
    complete: tuple[date | str, date | str] | None = None

Model.from_tables(
    con,
    *,
    units: Mapping[str, UnitSource],
    events: Mapping[str, Events],
    attributes: Mapping[str, UnitSource] | None = None,
    dialect: str | None = None,
    day_boundary: str = "UTC",
    readiness: Mapping[str, Coverage] | None = None,
    event_ids: Mapping[str, str] | None = None,
) -> Model
```

`Events` is an immutable, validated descriptor following the frozen typed-record style of `UnitSource` and `Coverage` in `terrine.data`. `table` is a relation name, `time` is its event-time column, `keys` maps each declared model unit name to that table's corresponding key column, `values` declares numeric event-value columns, and `complete` is an inclusive local-calendar-day `(start, end)` pair in the Model's `day_boundary` timezone. The constructor normalizes `values` to a tuple and detaches/freezes `keys` like other public metadata mappings. Each event descriptor requires `complete`; omission refuses with stable code `terrine.model.missing_event_completeness`. It never infers completeness from observed rows or event maxima.

`units` is required and remains the authoritative roster, including units with no matching events. It is never derived from event relations. `events` is a mapping keyed by stable source name; each descriptor's `keys` must map every named unit used by its event table. `complete` becomes that generated event source's `Coverage` in the readiness mapping; optional `readiness` adds coverage for other catalog sources. `attributes` optionally maps named dimension sources to `UnitSource`; it does not replace the native Catalog for filters, ratios, SCD2 dimensions, or SQL relations. If those features are needed, callers continue to use `Model.from_catalog`.

The method creates the equivalent current native `Catalog` (sources, facts, measures, metrics, plus supplied unit and attribute sources) then calls `Model.from_catalog`; it passes `units` as a plain mapping and otherwise preserves all binding options. It performs metadata binding only and does not scan source rows. `Model.from_catalog` continues to accept its existing catalog input; the separately planned mapping input extension is a prerequisite for this method's mapping handoff, not a reason to accept or expose a second catalog format here.

### Generated metrics

For each event source `source`, generate:

- `<source>_count`: event count;
- `<source>_<value>_sum`: sum of each declared value column;
- `<source>_<value>_mean`: mean of each declared value column.

There are no `tr.count` or `tr.sum` shorthands. Use existing `agg(metric, window)` expressions to consume these metrics. Generated names must be unique across all generated metrics. If an explicit metric catalog is supported alongside generation, a generated name must also be unique against every explicit metric name. Any collision refuses before binding with stable code `terrine.model.generated_metric_collision`; context identifies the conflicting name and sources. If a name cannot be represented under current Catalog naming rules, refusal is coded rather than silently rewriting it.

Aggregation semantics are exactly those of equivalent hand-written Catalog metrics: in complete windows, an empty count and sum are `0`; an event mean is null with reason `no_observations`. An incomplete window is null with the existing incompleteness reason, regardless of aggregation. Null values within a nonempty event set follow existing aggregate semantics.

## `terrine.recipes` constructors

Each function returns a plain `Design`, never data. Shared keywords are accepted with consistent names and forwarded to the matching low-level `Design`: `features`, `outcomes`, `eligibility`, `anchor_range`, `gap`, `treatment`, and `administrative_cutoff` where meaningful. The following row-defining inputs are required. Defaults are only conventions, such as selecting the first event occurrence.

All signatures below are the public contract; expression mappings and predicate mappings use the same types as `Design`.

```python
def time_series(
    unit: str, *, every: str | int, periods: int, start: date | datetime | str,
    end: date | datetime | str, features=None, outcomes=None, eligibility=None,
    anchor_range=None, gap="0d", treatment=None,
) -> Design

def trial_panel(
    unit: str, *, treatment: Initiation | PersistentAdoption, periods: tuple[int, ...],
    period: str | int, features=None, outcomes=None, eligibility=None,
    anchor_range=None, gap="0d",
) -> Design

def event_panel(
    unit: str, *, event: str, periods: tuple[int, ...], period: str | int,
    occurrence: Literal["first", "each", "nth"] = "first", n: int | None = None,
    features=None, outcomes=None, eligibility=None, anchor_range=None,
    gap="0d", treatment=None,
) -> Design

def ltv(
    unit: str, *, start_event: str, metric: str, period: str | int, periods: int,
    features=None, outcomes=None, eligibility=None, anchor_range=None,
    gap="0d", treatment=None,
) -> Design

def survival(
    unit: str, *, start_event: str, end_event: str,
    occurrence: Literal["first", "each", "nth"] = "first", n: int | None = None,
    features=None, outcomes=None, eligibility=None, anchor_range=None,
    gap="0d", administrative_cutoff: datetime | None = None,
) -> Design

def life_table(
    unit: str, *, start_event: str, end_event: str, bin: str | int,
    occurrence: Literal["first", "each", "nth"] = "first", n: int | None = None,
    features=None, outcomes=None, eligibility=None, anchor_range=None,
    gap="0d", administrative_cutoff: datetime | None = None,
) -> Design

def state_panel(
    unit: str, *, state_source: StateObservations | BinnedMetricState,
    every: str | int, periods: int, start: date | datetime | str,
    end: date | datetime | str, features=None, outcomes=None, eligibility=None,
    anchor_range=None, gap="0d", treatment=None,
) -> Design

def event_sequence(
    unit: str, *, event: str, horizon: str | int, max_events: int,
    occurrence: Literal["first", "each", "nth"] = "first", n: int | None = None,
    session_gap: str | int | None = None, features=None, outcomes=None,
    eligibility=None, anchor_range=None, gap="0d",
) -> Design

```

`time_series` and `state_panel` build on a `Schedule`; `trial_panel` creates the base Design then applies `Sequential`; `event_panel` creates the base Design then applies `EventPanel`; `ltv` uses a cohort origin and a cumulative `Trajectory`; `survival` uses `TimeToEvent`; `life_table` uses `TimeToEvent` plus `LifeTable`; `state_panel` uses `StatePanel`; `event_sequence` uses `EventSequence`. The constructors preserve those low-level specifications on the returned Design.

The event-selected anchor in `event_panel`, `survival`, and `event_sequence` is an `EventAnchor`; supported occurrence values are `first`, `each`, and `nth`. For nth selection, `n` is required and positive; for first/each, `n` is omitted. `trial_panel` passes the explicit treatment definition to `Sequential`; it does not also set `Design.treatment`. `ltv` counts the start event only as the cohort origin: its cumulative trajectory begins after the cohort origin and must not count that origin as a repeated outcome. `event_sequence.horizon` is the required bounded `Window` duration and `max_events` is required by the low-level `EventSequence` shape.

`survival` and `life_table` require a starting event and terminal event but have no relative duration horizon in the existing low-level `TimeToEvent` shape. Follow-up ends according to that shape's existing observation-coverage rules or the optional absolute `administrative_cutoff`; the recipes must not invent a per-unit horizon or change low-level behavior. `life_table` is separate from `survival` because it selects risk-set bins (`LifeTable`), not merely a display option. Conditional events remain an expression pattern: use an `EventAnchor` and an expression selector with `relative_to=event_time(...)`; there is no conditional-event constructor.

## Public documentation and policy updates

Update `AGENTS.md` under “Public API and refusals” to state: constructors in `terrine.recipes` are the documented starting point for common dataset types; the low-level `Design` API is their full-flexibility compilation target and remains public and fully supported, not an alias. Update `docs/guides/public-api.md` to list the module and both layers, and add constructor-first guides whose “Under the hood” sections show the equivalent `Design`. Add one runnable example per constructor and execute each in CI. Update the refusal reference for the two new codes and add an unreleased breaking-API CHANGELOG entry if the release policy categorizes these new public interfaces as breaking; do not change package version `0.1.0.dev0`.

## Acceptance and review focus

- A single integration composition compares rows from `Model.from_tables` with a hand-written native Catalog for count, sum and mean metrics, including zero-activity roster units and complete/incomplete windows.
- Every constructor has a runnable example executed by CI.
- Tests pin required row-defining arguments, default first occurrence, explicit completeness, stable collision/missing-completeness refusal codes and refusal pickle/deepcopy, the empty-window value/reason contract, and unchanged low-level construction/serialization behavior.
- Preserve null versus zero and per-cell null reasons through Arrow, manifests and exports; preserve the version and all repository validation constraints in `AGENTS.md`.
