# Public API

The everyday workflow is **Model → Design → Frame → Dataset**. Bind source
meaning and completeness once on a Model, describe rows and columns with a
Design, build a lazy Frame, then collect a bounded result or stage a pinned
Dataset. The [first dataset](first-dataset.md) executes that whole path.

## Top-level constructors

| Workflow | `terrine` names | Example or guide |
| --- | --- | --- |
| Bind and build | `Model`, `Design`, `Frame`, `Dataset`, `Coverage`, `UnitSource`, `Budget`, `preview` | First dataset; [catalogs](temporal-engine.md) |
| Exact anchors and windows | `FixedDate`, `EventAnchor`, `AnchorRange`, `Schedule`, `Window` | [Anchors](candidate-anchors.md) |
| Named feature/outcome expressions | `agg`, `asof`, `event_time`, `EventTime`, `retention`, `quantile`, `approx_distinct`, `approx_quantile`, `SketchRequest` | [Temporal semantics](temporal-engine.md) |
| Eligibility and descriptive treatment | `eq`, `gt`, `is_null`, `all_of`, `Treatment` | [Design specifications](design-specs.md) |
| Bounded trajectories and cohorts | `Trajectory`, `CohortSpec`, `CohortGrouping`, `TimeToEvent`, `LifeTable` | [Trajectories and risk sets](trajectories-survival.md) |
| State pairs | `StatePanel`, `StateObservations`, `BinnedMetricState` | [State panels](state-panels.md) |
| Ordered event datasets | `EventSequence`, `SessionSpec`, `FunnelSpec` | [Sequences](temporal-engine.md#bounded-event-sequences-and-event-time-origins) |
| Save a description or handle a refusal | `DatasetDefinition`, `from_yaml`, `to_yaml`, `TerrineError` | [ML staging](ml-staging.md), [refusal reference](../reference/refusals.md) |

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
| `terrine.spec` | Pure dataset descriptions: the top-level design constructors plus sequential/calendar/event panels, initiation/adoption declarations, matched-time sampling, trial weights, winsorization, definition storage and JSON/YAML serialization. See [sequential panels](sequential-panels.md) and [ML staging](ml-staging.md). Expression implementation records stay in their owning module. |
| `terrine.execution` | `publish_native_inputs` and its `PublishedInputs` result for [retained input publication](../../README.md#owned-inputs-for-repeated-builds). Verified bindings/relation authority are derived, not facade inputs. |
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
