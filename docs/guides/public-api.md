# Public API

The everyday workflow is **Model → Design → Frame → Dataset**. Bind source
meaning and completeness once on a Model, describe rows and columns with a
Design, build a lazy Frame, then collect a bounded result or stage a pinned
Dataset. The [first dataset](first-dataset.md) executes that whole path.

## Two supported construction layers

Start common dataset types with constructors in `terrine.recipes`. Each returns
an ordinary `Design`: it describes rows without querying or materializing data.
Call `design.build(model).materialize(budget=budget)` through the same workflow as any
other Design. These constructors are not aliases or a second representation.

The low-level **Design API** is the full-flexibility layer they compile to:
`Design`, anchors, expressions, `shape=`, and the public transforms in
`terrine.spec`, including `Sequential` and `EventPanel`. It remains supported
and unchanged, not an alias for recipes. Each dataset guide shows the equivalent
low-level construction under **Under the hood**.

| `terrine.recipes` constructor | Row meaning | Guide |
| --- | --- | --- |
| `time_series` | One unit at each scheduled calendar anchor | [Table-first binding](temporal-engine.md#scheduled-time-series) |
| `trial_panel` | One eligible unit at each scheduled trial time zero | [Sequential trials](sequential-panels.md#scheduled-trial-panels) |
| `event_panel` | One selected event and declared relative period | [Event panels](sequential-panels.md#event-panels) |
| `ltv` | One unit/cohort origin and age, with cumulative SUM-metric value | [LTV](trajectories-survival.md#cumulative-ltv) |
| `survival` | One unit/start event with observed event or censor time | [Survival](trajectories-survival.md#survival-records) |
| `life_table` | One declared feature group and elapsed-time bin | [Life tables](trajectories-survival.md#grouped-life-tables) |
| `state_panel` | One unit-period transition from prior to response state | [State panels](state-panels.md) |
| `event_sequence` | One selected event anchor with bounded ordered events | [Event sequences](temporal-engine.md#bounded-event-sequences-and-event-time-origins) |

For `ltv`, the required `metric` must be an untransformed native metric with
exactly one SUM measure, such as a `tr.sum(column)` metric declared on
`Events`; counts, means and ratios are not accepted by this shape.

Every constructor takes `unit` first, positional or keyword, and defaults it to
`None`: the model's only unit, resolved when the design meets a model (see
[unit inference](design-specs.md#unit-inference)). The guides list row-defining
arguments and supported options; unsupported keywords raise Python's `TypeError`
rather than being silently ignored. Shared defaults are `eligibility=None`,
`anchor_range=None`, and `gap="0d"`. Where accepted, `features`, `outcomes`, `treatment`,
and `administrative_cutoff` default to `None`. Event occurrence defaults to
`"first"`; `occurrence="nth"` requires a positive one-based `n`, while `n` is
omitted for `"first"` or `"each"`.

An argument value a constructor cannot accept refuses at construction with a
`TerrineError`, before a model or any row is read. Its context carries `field`,
the parameter name, and `reason`, such as `empty_name`, `wrong_type`,
`not_positive` or `unsupported_value` (then with `choices`). Context fields are
not stable; refusal codes are contractual only when raised by a stable name.
Design specifications, recipes and expressions use `terrine.design.invalid`;
state sources and `StatePanel` use `terrine.state_panel.invalid_state_source`;
`TimeSplit`, `GroupSplit` and `training_set(split=...)` use
`terrine.dataset.invalid_split`; `Budget` uses `terrine.budget.invalid_limit`;
and `Coverage`, `UnitSource`, `Events` and `History` use
`terrine.model.invalid_binding`. Owner runtime, manifest, export, orchestration
and agent-policy inputs refuse the same way under their own family's code (for
example `terrine.dataset.invalid_destination` for a runtime namespace and
`terrine.runtime.invalid_configuration` for orchestration and control settings).
`TerrineError` subclasses `ValueError`.

Where a constructor accepts `features` or `outcomes`, each named value may be an
expression or a Boolean predicate, just as in `Design`. Eligibility remains a
mapping of predicates. Shape-specific restrictions still apply: `life_table`
declares its features through `group_by={name: expr}`, and shapes with derived
outcomes do not accept arbitrary outcome mappings.

## Package-root workflow

Use `import terrine as tr`, never `from terrine import *`. The root keeps
binding, Design, everyday expressions, recipes, results and errors together.
In particular, `tr.sum`, `tr.min` and `tr.max` remain metric constructors without
shadowing Python's builtins in your own namespace.

| Workflow | `terrine` names | Example or guide |
| --- | --- | --- |
| Bind and build | `Model`, `Design`, `Dataset`, `Events`, `History`, `Coverage`, `UnitSource`, `Budget`, `preview` | First dataset; [event tables and catalogs](temporal-engine.md) |
| Metrics on `Events` | `count`, `sum`, `mean`, `min`, `max`, `count_distinct`, `ratio` | [Binding event tables](temporal-engine.md#binding-event-tables) |
| Anchors | `FixedDate`, `EventAnchor`, `AnchorRange`, `Schedule` | [Anchors](candidate-anchors.md) |
| Feature/outcome expressions and eligibility | `agg`, `asof`, `event_time`, `since`, `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `isin`, `between`, `is_null`, `all_of`, `any_of`, `not_` | [Temporal semantics](temporal-engine.md) |
| Common dataset types | `recipes` | Construction layers above |
| Save a design or handle a refusal | `from_yaml`, `to_yaml`, `TerrineError`, `UnverifiedBackendWarning` | [Design specifications](design-specs.md), [refusal reference](../reference/refusals.md) |

Import shape-specific inputs from `terrine.spec`, as with `Initiation`:
`Treatment`, `EventTime`, `Trajectory`, `CohortSpec`, `CohortGrouping`,
`TimeToEvent`, `LifeTable`, `StatePanel`, `StateObservations`,
`BinnedMetricState`, `EventSequence`, `SessionSpec` and `FunnelSpec`. Stable
shape values include `SequentialSpec` and `EventPanelSpec`.
Use the `Expr` and `Predicate` aliases to type expression and eligibility
inputs; expression functions remain the usual construction path.
The specialized `retention` and `quantile` expressions, the provisional
`approx_distinct` and `approx_quantile` expressions, and `DatasetDefinition`
also live there. Import elapsed `Window`, `Duration` and calendar `Months`
values from `terrine.windows`. A Frame is returned by `design.build(model)`;
its type is available from `terrine.frame` when needed.

Bind Models through `tr.Model.from_tables(...)` or `tr.Model.from_catalog(...)`,
not the Model constructor. Create definitions as
`DatasetDefinition(base_design=design, view=None, transforms=())`; their codecs
stamp the wire-format version rather than accepting it as a study input.

`Events` and `History` belong at the top level because `Model.from_tables` is
the starting binding path for simple tables: it combines explicit completeness
with an authoritative `UnitSource` roster, user-named metrics built with the
`count`, `sum`, `mean`, `min`, `max`, `count_distinct` and `ratio`
constructors, labels for filtered counts and lookups, and history tables for
as-of attributes. Each descriptor names exactly one unit key. The immutable
typed descriptors are defined in `terrine.data` and the constructors in
`terrine.metrics`; `Model.to_catalog()` writes the equivalent catalog. Use
`Model.from_catalog` for SQL relations, multi-unit sources or explicit Catalog
names.

`Model.from_tables(units=..., events=..., history=...)` accepts pandas, Polars
and Arrow tables directly when `con` is omitted, querying them in a private
in-process DuckDB. Such a model **materializes only**: staging refuses with
`terrine.dataset.invalid_destination` and
`reason="file_backed_connection_required"`. For a durable Dataset, load tables
into a file-backed DuckDB and pass its connection and table names instead.

Call `design.describe(model)` to inspect the rows, schedule, windows and declared
completeness using metadata only. `Schedule`, `time_series` and `trial_panel`
accept `"1mo"`/`"1y"` calendar steps and include a calendar-date end; relative
windows still use elapsed durations with an exclusive end.
`frame.to_pandas(values_only=True)` and `frame.to_polars(values_only=True)` keep
identity, role and value columns without completeness/reason columns; incomplete
values remain null. `frame.training_set(label=...).to_pandas(values_only=True)`
also drops incomplete-label rows. Leave the flag unset or use Arrow to retain
observation evidence, and do not combine `values_only=True` with
`include_observation_metadata=True`.

All three Model binding constructors accept keyword-only
`allow_unverified: bool = False`. A genuine unregistered Ibis backend refuses
by default with `terrine.backend.capability_unavailable` (`operation="query"`,
`status="unverified"`) before its schema is read, and needs
`allow_unverified=True`; truthy strings are not consent and refuse with
`terrine.model.invalid_binding`. Admission emits `UnverifiedBackendWarning`, a
`UserWarning` subclass exported from `terrine` for warning filters; filtering
it changes no gate and no stamp. The choice applies only to the returned Model.
Registered engine identity failures, raw drivers and name-only connections
cannot opt in. Ibis schema inspection on an unverified backend may end an open
transaction on the caller's connection. Pass a connection with no pending
writes or a dedicated read connection; transaction preservation is not
guaranteed.

```python
import terrine as tr

# catalog.dialect must remain the connection's real SQL dialect.
model = tr.Model.from_catalog(
    catalog, con=con, readiness=readiness, allow_unverified=True,
)
```

This permits read-only attempts, not verified semantics: each statement has an
independent source view, an unavailable primitive gives a coded refusal naming
its operation, and every result's `Provenance` records the actual backend as
`engine_name`, `generic` as `dialect` and `engine_status="unverified"`. It
never enables retained inputs, staging, publication or published Dataset
reopening. See [backend compatibility](backend-compatibility.md#unverified-ibis-backends)
for ownership, consistency, admission witnesses and the portable numerical
contract, and [`examples/unverified_backend.py`](../../examples/unverified_backend.py)
for a runnable opt-in on the Ibis SQLite backend.

Use the expression functions rather than constructing their underlying node
records. Advanced code inspecting a returned node can import its type from
`terrine.spec.expressions`; those records are not additional facade constructors.
`EventTime` remains available for typed nested selectors. `CohortTable`,
`MaterializedFrame`, manifests and provenance are returned metadata/results,
not required inputs for ordinary dataset construction. Their owning modules
remain available for inspection and explicit serialization.

## Focused facades

Every package facade has an explicit export list. This table records its
purpose, the workflow that warrants its exports, and its tier in
[API stability](stability.md): stable names follow Semantic Versioning from 1.0,
provisional ones may change in a minor release with a changelog entry, and
internal modules may change in any release. The `terrine` root is stable; its
CLI is provisional.

| Facade | Tier | Supported use |
| --- | --- | --- |
| `terrine.recipes` | Stable | The eight constructor-first dataset workflows above; returns plain Designs that compile through the existing low-level API. |
| `terrine.metrics` | Stable | Metric constructors used in the everyday modeling workflow. |
| `terrine.spec` | Stable, except provisional names listed below | The full-flexibility Design layer, stable declarations and JSON/YAML serialization. `Sequential`, `EventPanel`, `CalendarPanel`, `CalendarComparisonPanel`, `matched_t0`, `CalendarComparisonSpec`, `approx_distinct` and `approx_quantile` are provisional. See [sequential panels](sequential-panels.md) and [ML staging](ml-staging.md). |
| `terrine.execution` | Provisional | `publish_native_inputs` and its `PublishedInputs` result for [retained input publication](materialization-runtime.md#owned-inputs-for-repeated-builds). Verified bindings/relation authority are derived, not facade inputs. |
| `terrine.runtime` | Provisional | Owner-side planning, saved plans, execution/scan authorization protocols, snapshot retention, exact-generation reads and run status/events/cancellation. See [runtime](materialization-runtime.md). Persistent state rows and layer translation stay in their owning modules. |
| `terrine.dataset` | Stable facade; `Dataset` provisional | `GroupSplit` and `TimeSplit` policies supplied to `Frame.training_set(split=...)`; split strategy is an everyday training workflow choice. |
| `terrine.frame` | Stable facade; `Frame.stage` and `Frame.expr` provisional | Frame construction, conversion and training workflows. |
| `terrine.model` | Stable | `Catalog` and `Readiness` types used with `Model.from_catalog`; `Model` remains the binding entry point. |
| `terrine.data` | Stable input declarations and metric types; result metadata provisional | Bind inputs with `Coverage`, `UnitSource`, `Events`, `History` and `Budget`; `EventMetric` and `Ratio` describe stable metric declarations. Returned `*Meta` records, `CoverageSummary`, `EligibilitySummary` and `MaterializedFrame` remain inspectable results, not required inputs. Use the stable constructors from `terrine.metrics` to declare metrics. |
| `terrine.reasons` | Stable vocabulary | Missing-value reason vocabulary used in output columns. |
| `terrine.errors` | Stable names; refusal codes stable only when raised by a stable name | Coded errors and warnings. |
| `terrine.windows` | Stable except provisional `duration` | `Window`, `Duration` and `Months` are elapsed-window values; `duration` is provisional. |
| `terrine.export` | Provisional | Shared preflight and Arrow/lifelines/diff-diff hand-offs, with their typed conversion results. See [library hand-offs](library-handoffs.md). |
| `terrine.agent` | Provisional | `AgentPolicy` and `DatasetTools` for the separately authorized [dataset tools](dataset-agent-tools.md). |
| `terrine.simulate` | Provisional | Seeded raw-source worlds and bounded scale batches, with independent expected-row oracles. See [synthetic worlds](synthetic-worlds.md). |
| `terrine.adapters`, `terrine.orchestration` | Provisional | Empty facades; select an explicit optional adapter module. Importing the namespace never selects an integration. |
| `terrine.compile`, `terrine.compile.shapes` | Internal | Empty internal facades. Select shapes through Design and build through `Design.build`, not compiler state constructors. |
| `terrine.backends`, `terrine.semantic`, `terrine.temporal` | Internal | Empty namespaces. Native source configuration uses `Model.from_catalog` with a YAML path, a validated `Catalog`, or a plain mapping; backend/temporal machinery is not an everyday facade. |


Dataset split policies are available from `terrine.dataset`: import
`GroupSplit` or `TimeSplit` when selecting a leakage-safe training view with
`Frame.training_set(split=...)`. These are ordinary workflow inputs; compiler,
runtime-state, and orchestration records remain in their focused modules.

Metadata records are inspectable without becoming user-authored compiler or
runtime state. Optional dependency import isolation covers the root and all
package facades, including resolution of their exported symbols.

## Pre-release API review

Removed accidental root exports: `ExportManifest`, `CohortTable`,
`ApproxMetricDistinct`, `ApproxMetricQuantile`, `MetricQuantile` and
`RetentionMetric`. Use the dataset/Frame result or expression function instead.
Removed expression implementation records from `terrine.spec`, compiler
primitives from compiler facades, proof/authority types from
`terrine.execution`, and persistent state/layer translation records from
`terrine.runtime`. No compatibility aliases remain. Specialized runtime,
compiler and expression tests import their owning modules directly.
