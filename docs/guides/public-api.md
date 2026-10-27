# Public API

The everyday workflow is **Model → Design → Frame → Dataset**. Bind source
meaning and completeness once on a Model, describe rows and columns with a
Design, build a lazy Frame, then collect a bounded result or stage a pinned
Dataset. The [first dataset](first-dataset.md) executes that whole path.

## Two supported construction layers

Start common dataset types with constructors in `terrine.recipes`. Each returns
an ordinary `Design`: it describes rows without querying or materializing data.
Call `design.build(model).materialize(budget)` through the same workflow as any
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
exactly one SUM measure. Use `<source>_<value>_sum` from `Model.from_tables`;
the generated count and mean metrics are not accepted by this shape.

All constructors require `unit`. The guides list row-defining arguments and
supported options; unsupported keywords raise Python's `TypeError` rather than
being silently ignored. Shared defaults are `features=None`, `eligibility=None`,
`anchor_range=None`, and `gap="0d"`. Where accepted, `outcomes`, `treatment`,
and `administrative_cutoff` default to `None`. Event occurrence defaults to
`"first"`; `occurrence="nth"` requires a positive one-based `n`, while `n` is
omitted for `"first"` or `"each"`.

Where a constructor accepts `features` or `outcomes`, each named value may be an
expression or a Boolean predicate, just as in `Design`. Eligibility remains a
mapping of predicates. Shape-specific restrictions still apply: life-table
features must exactly match `group_by`, and shapes with derived outcomes do not
accept arbitrary outcome mappings.

## Top-level constructors

| Workflow | `terrine` names | Example or guide |
| --- | --- | --- |
| Bind and build | `Model`, `Design`, `Frame`, `Dataset`, `Events`, `Coverage`, `UnitSource`, `Budget`, `preview` | First dataset; [event tables and catalogs](temporal-engine.md) |
| Exact anchors and windows | `FixedDate`, `EventAnchor`, `AnchorRange`, `Schedule`, `Window` | [Anchors](candidate-anchors.md) |
| Named feature/outcome expressions | `agg`, `asof`, `event_time`, `EventTime`, `retention`, `quantile`, `approx_distinct`, `approx_quantile`, `SketchRequest` | [Temporal semantics](temporal-engine.md) |
| Eligibility and descriptive treatment | `eq`, `gt`, `is_null`, `all_of`, `Treatment` | [Design specifications](design-specs.md) |
| Bounded trajectories and cohorts | `Trajectory`, `CohortSpec`, `CohortGrouping`, `TimeToEvent`, `LifeTable` | [Trajectories and risk sets](trajectories-survival.md) |
| State pairs | `StatePanel`, `StateObservations`, `BinnedMetricState` | [State panels](state-panels.md) |
| Ordered event datasets | `EventSequence`, `SessionSpec`, `FunnelSpec` | [Sequences](temporal-engine.md#bounded-event-sequences-and-event-time-origins) |
| Save a description or handle a refusal | `DatasetDefinition`, `from_yaml`, `to_yaml`, `TerrineError` | [ML staging](ml-staging.md), [refusal reference](../reference/refusals.md) |

`Events` belongs at the top level because `Model.from_tables` is the starting
binding path for simple event tables: it combines explicit event completeness
with an authoritative `UnitSource` roster. Each descriptor names exactly one
unit. Its immutable typed descriptor is defined in `terrine.data`. Use
`Model.from_catalog` for static/as-of attributes, filters, ratios, SCD2 dimension
histories, SQL relations, multi-unit sources, or explicit Catalog names.

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
from terrine import Model, UnverifiedBackendWarning

# catalog.dialect must remain the connection's real SQL dialect.
model = Model.from_catalog(
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
purpose and the workflow that warrants its exports.

| Facade | Supported use |
| --- | --- |
| `terrine.recipes` | The eight constructor-first dataset workflows above; returns plain Designs that compile through the existing low-level API. |
| `terrine.spec` | The full-flexibility Design layer: public `Sequential` and `EventPanel` transforms, calendar panels, initiation/adoption declarations, matched-time sampling, trial weights, winsorization, definition storage and JSON/YAML serialization. See [sequential panels](sequential-panels.md) and [ML staging](ml-staging.md). Expression implementation records stay in their owning module. |
| `terrine.execution` | `publish_native_inputs` and its `PublishedInputs` result for [retained input publication](materialization-runtime.md#owned-inputs-for-repeated-builds). Verified bindings/relation authority are derived, not facade inputs. |
| `terrine.runtime` | Owner-side planning, saved plans, execution/scan authorization protocols, snapshot retention, exact-generation reads and run status/events/cancellation. See [runtime](materialization-runtime.md). Persistent state rows and layer translation stay in their owning modules. |
| `terrine.dataset` | `GroupSplit` and `TimeSplit` policies supplied to `Frame.training_set(split=...)`; split strategy is an everyday training workflow choice. |
| `terrine.export` | Shared preflight and Arrow/lifelines/diff-diff hand-offs, with their typed conversion results. See [library hand-offs](library-handoffs.md). |
| `terrine.agent` | `AgentPolicy` and `DatasetTools` for the separately authorized [dataset tools](dataset-agent-tools.md). |
| `terrine.simulate` | Seeded raw-source worlds and bounded scale batches, with independent expected-row oracles. See [synthetic worlds](synthetic-worlds.md). |
| `terrine.adapters`, `terrine.orchestration` | Empty facades; select an explicit optional adapter module. Importing the namespace never selects an integration. |
| `terrine.compile`, `terrine.compile.shapes` | Empty internal facades. Select shapes through Design and build through `Design.build`, not compiler state constructors. |
| `terrine.backends`, `terrine.semantic`, `terrine.temporal` | Empty namespaces. Native source configuration uses `Model.from_catalog` with a YAML path, a validated `Catalog`, or a plain mapping; backend/temporal machinery is not an everyday facade. |


Dataset split policies are available from `terrine.dataset`: import
`GroupSplit` or `TimeSplit` when selecting a leakage-safe training view with
`Frame.training_set(split=...)`. These are ordinary workflow inputs; compiler,
runtime-state, and orchestration records remain in their focused modules.

The focused public modules `terrine.data`, `terrine.errors`, `terrine.windows`,
`terrine.model` and `terrine.frame` retain their explicit typed contracts:
budgets/coverage and returned metadata, coded refusals, elapsed windows, Model,
and Frame/cohort results respectively. Data metadata is inspectable without
becoming user-authored compiler or runtime state. Optional dependency import
isolation covers the root and all package facades, including resolution of
their exported symbols.

## Pre-release API review

Removed accidental root exports: `ExportManifest`, `CohortTable`,
`ApproxMetricDistinct`, `ApproxMetricQuantile`, `MetricQuantile` and
`RetentionMetric`. Use the dataset/Frame result or expression function instead.
Removed expression implementation records from `terrine.spec`, compiler
primitives from compiler facades, proof/authority types from
`terrine.execution`, and persistent state/layer translation records from
`terrine.runtime`. No compatibility aliases remain. Specialized runtime,
compiler and expression tests import their owning modules directly.
