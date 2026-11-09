# Durable materialization runtime

The materialization runtime persists plan/run state and publishes immutable output generations in the customer's own execution environment. It does not infer whether source rows changed: source revisions, change feeds, checksums, or explicit operator restatements must provide that evidence. A readiness watermark says which period is declared complete; it is not a change detector. This durable materialization runtime is currently DuckDB-only and provisional; its API may change in a minor release.

The runtime accepts an immutable `RefreshPlan` and delegates physical source
reads, execution and publication to an owner-configured `Executor`. Request
keys are durable and scoped to an explicit `DestinationRef`; a `RunReceipt`
pins the published generation for later reads. The durable DuckDB owner
serializes publication and cancellation. Separate processes must use owner IPC
while that owner holds the database; cancellation is a durable request, not
proof that backend work stopped.

## Build and plan from a Model and Design

The runnable [`examples/materialization_runtime.py`](../../examples/materialization_runtime.py) is the end-to-end workflow. It builds a real `Model` and immutable `Design`, captures a retained input snapshot, serializes and saves a `SavedPlan` bound to a destination, runs it through the owner resolver and snapshot authority, then queries the exact returned generation:

```console
uv run python examples/materialization_runtime.py
```

The example publishes per-user one-day event totals and an effective-dated tier from a local DuckDB source, then verifies each exact-receipt result against a fresh `design.build(model).materialize(budget=budget)` of the same Model/Design and source state. It then inserts an event and corrects the effective-dated tier, captures a new retained snapshot, saves a new plan against the first generation, and queries that second receipt. The totals change from `3, 4` to `8, 4`, and the tier changes from `gold` to `silver`; the first receipt remains bound to its original generation. Saved plans use the production `DesignExecutor`; no application-specific aggregation executor is involved in this example.

In the example Catalog, the `tier` property references the `history` dimension, whose `starts` and `ends` columns define validity intervals in `user_tier`. The Design requests `"tier": asof("tier", before="0s")`, so the attribute is taken strictly before the anchor from the effective-dated event history. Updating those dimension rows requires a new retained snapshot just as inserting an event does. The example reopens the first receipt after publishing the correction and checks that its original rows are unchanged.

For lower-level planning, `dependency_graph(model, design, partitions=...)` and `plan_refresh(...)` are metadata-only. A manually supplied `snapshot_id` is only an identity label; it does not freeze source rows. Persist and execute a saved plan tied to `SnapshotAuthority.capture` when source inputs must be retained and reopened across partitions, retries, or process restarts. Saved plans contain no live connection, relation handle, or credentials.

`SavedPlan` lives in `terrine.runtime.saved_plan` and is also exported from
`terrine.runtime`. Its serialized reconstruction binds `dataset_definition_json`
and `plan_hash`; `restore_dataset_definition()` returns the saved dataset
construction. The canonical runtime `PlanDefinition` is available as
`RefreshPlan.definition`, including the fingerprint used for scan authorization.
Owner preparation accepts `PlanRequest.dataset_definition` and persists the
complete saved plan as `PlanInvocation.saved_plan_json`.

New saved plans use envelope version **2** and runtime plan-definition version
**3**, with execution revision **1** in the execution fingerprint. Declaration
identity is unchanged: a plan's `definition_hash` still equals its saved
Design's hash. Historical version-1 saved plans and version-2 runtime plans
verify their original checksums and remain available for inspection, snapshot
pin release, and authorized status, event and cancellation operations. They
cannot execute or return an execution replay: rebuild a plan from the dataset
definition after `terrine.runtime.saved_plan_mismatch`.

Missing, unknown or superseded current envelope fields also refuse, even with
a valid checksum. Superseded owner request fields refuse with
`terrine.runtime.plan_request_invalid` instead of silently dropping the
training/scoring view. Manifest envelopes retain their own
`terrine.dataset.invalid_manifest` refusal; [definition persistence](ml-staging.md#reopening-a-pinned-dataset)
describes which bare definitions remain readable.

A destination reference identifies `(database, schema, dataset)` for request-key isolation. It is not a provisioning command: arrange the customer-owned database and schema through the application before starting the owner. The owner uses its configured connection and namespace; do not put a connection string or secret in a plan or request key.

The owner namespace records the version of its runtime metadata tables. A namespace without terrine's runtime metadata tables is initialized at version 1, with every table and its version row created in one transaction; other tables in the namespace, including unrelated `runtime_*` ones, are ignored and left untouched. A namespace written by an earlier pre-release (terrine's runtime tables without a recorded version) refuses with `terrine.runtime.schema_unsupported` (`reason="unversioned"`), and one recorded by a newer release refuses with `reason="newer"`; snapshot capture, staging, execution, run status and events, reopening a dataset or generation and agent reference resolution all refuse before writing anything, including retained snapshot files. Pre-1.0 namespaces are rebuilt rather than migrated: stage again into a new namespace or owner database. A later schema version will migrate through the same owner, under its exclusive lease, after every non-terminal run has drained or been recovered.

`SnapshotAuthority.capture` is scoped to the resolved `Design`: it captures only required fact/property sources, their required dimensions and integrity inputs, and that design unit's roster. It keeps the full Catalog/model identity bound in the receipt without copying unrelated source rows. For example, a label source unused by the plan is not captured.

DuckDB capture uses backend-native `COPY` into retained storage rather than transferring source rows through the Ibis client. Output row/byte limits do not limit this input capture, and unknown source scan work cannot satisfy a strict scan budget. Capture acceptance increases input cardinality while prohibiting client row-fetch APIs and bounding steady-state Python allocations independently of the retained row count; backend-native memory and one-time initialization are not presented as measured client buffers.

## Runtime shape support

Runtime composition enters through the same `compile_design` dispatcher as
ordinary Frames. Its supported logical-input matrix is:

| Shape | Retained logical inputs |
| --- | --- |
| `cross_section` | Verified anchors and the retained source snapshot |
| `SequentialSpec` | Verified scheduled anchors and snapshot inputs; treatment, sampling and timing metadata retained |
| `EventPanelSpec` | Verified parent event anchors expanded into declared periods, with snapshot-backed aggregation at translated evaluation times |
| `StatePanel` | Verified scheduled output anchors and snapshot inputs for features, binned state periods/endpoints and baseline features; internal endpoint and baseline anchors keep their own domains |
| Calendar comparisons, event sequences, cohorts, survival and other shapes | Refuse |

Unsupported substitutions refuse before source resolution with
`terrine.compile.runtime_shape_unsupported`, including direct `compile_design`
calls. The coded refusal carries the shape and the route
`ordinary Frame materialization or a full rebuild`. Additive and nonadditive
windows use the ordinary compiler against the retained snapshot. Retained
execution does not produce daily/prefix tasks, substitute cumulative prefix
relations, or transport exact prefix state through Arrow. The ordinary compiler
can still build inline prefix expressions; it is not a persisted intermediate
ABI. Nested event selectors keep their existing dependency invalidation.

Approximate quantile sidecars remain available only through ordinary Frame
materialization. Dataset staging, owner planning and saved plans refuse them
with `terrine.runtime.sketch_sidecar_unavailable` before source validation or
snapshot capture; use exact quantiles for retained runtime outputs.

For retained-anchor staging, pass the resolved design, model, logical anchor relations,
and budget through `terrine.runtime.materialize.compose_runtime_frame`, with
`admit=context.admit` from the task's execution context. This entry point applies
the same shape-budget, integrity and unknown-eligibility gates as
`Frame.materialize` before staging, and admits every relation before reading it.
The built-in executor also admits each anchor relation before reading it.
Composition runs the complete check schedule once; the output statement
carries the eligibility summary, decoded before its first batch is written.

The built-in executor transfers anchor and final outputs in bounded Arrow batches
to owner-retained IPC artifacts. Task state stores checksum-verified artifact
references rather than full Arrow payloads. Final output budgets are enforced
while writing batches. Publication streams these artifacts into immutable
generation tables and retains their logical Arrow schema, including duration
ticks and Frame metadata. Readers and schema-marker wrappers close their
underlying streams. `DuckDBBackend.read_batches` reopens an exact partition
with that schema and closes its cursor when the reader closes.

Every newly executed partition, including typed-empty and plain Arrow output,
carries a separate `terrine.execution` schema marker:
`{"version":1,"execution_revision":1}`. The publisher takes this revision from
the validated executing plan, not the installed code or incoming data. Existing
markers and any Frame provenance must agree; historical Frame metadata cannot
be silently relabeled. Ordinary Frames also record `Provenance.execution_revision`;
legacy provenance leaves that optional field absent when reserialized.

`generation_execution_revision(dataset, generation)` reads the exact published
generation's complete partition-schema evidence without reading source rows.
All markers absent means historical/unknown revision 0. Missing schema rows,
mixed absence, malformed markers or unequal revisions cannot authorize reuse.
Only a verified current-revision base permits a reduced or no-change plan;
unknown or older bases retain the full graph even for a partial ChangeSet.
Reuse is rechecked under the dataset lease before callbacks and transactionally
before recording its receipt. A custom no-op returning an arbitrary generation
is not verified reuse. Request-key replay and post-commit recovery check the
exact committed generation, including when a newer generation is current.
The marker uses the existing partition schema storage: runtime namespaces
remain at schema version **1**, with no schema migration or new runtime columns.

## Cost evidence and scan budgets

`plan_refresh(..., cost_provider=owner_provider)` can attach owner-supplied
metadata estimates without executing source rows. The provider implements
`CostProvider` and declares `BackendCapabilities.scan_estimate=True`; its
`estimate_task` preserves task identity and returns `task.with_estimate(...)`.
Rows and output bytes are independent forecasts with an explicit basis.
`estimated_scan_bytes` is the sum of the phase estimates only when every phase
is known; an unknown phase makes the aggregate `None`.

`ScanEvidence` binds the definition hash and snapshot ID to separate
`ScanPhase` entries for **capture, validation, execution, and retry**. Each entry
names its basis and is `unknown`, `forecast`, `verified_upper_bound`, or
`backend_enforced_cap`. Retry bytes cover *all* additional allowed attempts,
including failed work and snapshot reopening, not just one retry. Plan scan
work sums every task and phase, regardless of concurrency. Output limits and
table sizes are not physical scan guarantees.

A strict `Budget.max_scan_bytes` admits only if every phase has a sufficient
verified bound or enforceable cap and a finite `max_attempts` allowance.
Unknown or forecast-only evidence refuses with `terrine.budget.scan_unknown`;
an excessive bound refuses with `terrine.budget.scan_exceeded`. Non-strict
unknown costs remain explicit warnings, not an implicit scan limit.

Before any task source callback, the executor must implement
`ScanAuthorizingExecutor.authorize_scan(plan)` and return
`ScanAuthorization(plan.definition.fingerprint)` only after verifying the saved
evidence against its actual retained inputs and read/cap implementation.
A fingerprint verifies the plan definition, not the truth of a backend claim. Missing
authorization refuses with `terrine.budget.scan_authority_unavailable`.
Ordinary Model/Design callers do not supply numeric proofs; capable owner
adapters derive and enforce them. Declared attempt limits are persisted before
dispatch and survive failed runs/restarts. Exhaustion refuses before another
read with `terrine.budget.scan_attempts_exhausted`; a new run requires a new
budget allowance.

Native DuckDB query scan bounds/caps remain unavailable. Its estimates stay
`None`, including snapshot capture; strict scan budgets still refuse before
source execution. Apply the same budget to capture as to execution: a later
plan cannot authorize already-unbudgeted source capture. Retaining input rows
or limiting output does not turn their size into a scan bound.

Runtime plan schema **3** fingerprints execution revision, verified reuse-base
identity/evidence, cost evidence, forecast fields, attempt limits, budget,
publication preconditions and any saved Model/Design reconstruction binding.
Version 2 remains decodable for administration, not executable; version 1
cannot supply the required cost evidence. Use
`plan.bind_base_manifest(generation)` to bind publication preconditions.
That method alone does not supply evidence authorizing computational reuse.

`get_run(...).cost` separately reports observed completed-task artifact rows
and bytes, intermediate artifact bytes, and durable attempt counts. When
available, its nested `capture` entry reports retained input artifact accounting;
it does not replace the completed-task observations. These are not total
allocation, repeated-query work, or scan measurements. Unavailable scan
observations remain `None` with a reason; legacy missing artifact observations
remain unavailable. Observations never rewrite saved forecasts.

The remaining task graph has no retained daily or prefix outputs. Observed
intermediate bytes account for actual retained anchors and other remaining
task artifacts; fewer intermediates do not turn unknown source scans into
zero-cost or verified scan bounds.

Published datasets are sorted on read; runtime byte accounting describes the stored artifact, not the re-sorted table.

## Save, run, and follow runs

An owner process keeps the database connection open, configures its executor, and serves a private local IPC endpoint:

```python
from pathlib import Path
from terrine.runtime.control import ControlEndpoint, ControlServer
from terrine.runtime.state import RuntimeStore

store = RuntimeStore(owner_connection, namespace=namespace)
server = ControlServer(
    store,
    namespace=namespace,
    endpoint=ControlEndpoint(Path("/run/terrine/analytics.sock")),
    executor=executor,
)
server.start()
```

The socket is owner-local and is removed when the server closes. Set `TERRINE_RUNTIME_SOCKET` and `TERRINE_RUNTIME_NAMESPACE` for the CLI. Canonicalize a saved plan through the owner, then submit that file:

```console
terrine plan orders.plan.json > orders.canonical-plan.json
terrine run orders.canonical-plan.json \
  --request-key orders:source-revision-42 \
  --destination-database analytics.duckdb \
  --destination-schema published \
  --destination-dataset orders
terrine status RUN_ID
terrine events RUN_ID --limit 100
terrine cancel RUN_ID
```

`plan` verifies the serialized plan version/fingerprint at the owner and emits its canonical serialization. `run` sends the plan to the owner; it never opens the owner's DuckDB file. A request key is optional for a one-off run. If present, it must be paired with all destination fields. The owner binds that key to the canonical plan fingerprint and destination: replaying the same pair returns the existing run, while changing its plan is refused. The same key may name a different destination without aliasing the prior run. Use a new key after an intentional source correction or definition change.

For saved plans, the durable request fingerprint includes the complete reconstruction definition: the owner Model reference and its Catalog, roster, readiness and event-ID bindings, Design, destination, retained snapshot, and expected base manifest. Changing any binding is a different request even after the original run has finished; terminal replay verifies that identity without requiring the resolver or snapshot artifact to remain available.

Status, event pages, and cancellation are projections of durable owner state. Event cursors are exclusive and pages bounded. Endpoint availability is tied to the owner process: after a crash, wait until a recovery owner can reacquire the DuckDB database and reconcile before querying again. A cancellation request is durable but does not prove that an active backend query or its billing stopped. A stale heartbeat means liveness is unknown, not that a run is safely terminal.

Executed runs expose durable task progress (`completed_tasks`, `running_tasks`, `total_tasks`), an execution/publication or terminal phase, the captured source readiness bounds, and a stable failure code. Capture accounting records the retained artifact's byte size, root count, and snapshot identity; `scan_bytes` remains unknown rather than being reported as zero. These projections and event pages read runtime metadata only, never customer source tables. Legacy records without capture or task accounting retain unknown values.

Status freshness describes whether the persisted attempt lease is current or stale, not proof that a backend query is alive or stopped. After an interrupted owner loses its lease, a recovery owner records `terrine.runtime.owner_interrupted` and can resume the same saved request from completed task outputs. Consumers can redeliver event pages safely by deduplicating on `(run_id, sequence)` and advancing only the acknowledged exclusive cursor.

Committed publication recovery verifies the matching saved plan from an owner
invocation, dataset request or fingerprint-bound direct execution record. Direct
execution persists that identity atomically with its fenced running transition in
the existing metadata tables. Recovery checks execution revision and any no-change
reuse-base restriction. If that identity cannot be verified, the interrupted
run becomes terminal with `terrine.runtime.saved_plan_mismatch`; recovery never
infers a revision from installed code or leaves an unverified commit running.

Run-state transitions distinguish recoverable failure from terminal results. A `failed` run may retry only after acquiring a newer lease fencing token; an expired writer cannot resume or publish. `succeeded` and `cancelled` runs are terminal and never regress.

### Optional telemetry

Telemetry is off unless the owner application creates an adapter and supplies
its own OpenTelemetry tracer and meter. The adapter needs no OpenTelemetry
package dependency of its own and never configures an exporter or endpoint:

```python
from terrine.adapters.opentelemetry import OpenTelemetryAdapter

telemetry = OpenTelemetryAdapter(tracer, meter)
try:
    for event in page.events:
        telemetry.observe_event(event)
finally:
    telemetry.close()
```

Only allowlisted lifecycle states are projected. Attributes contain the state
only—not run IDs, dataset names, request keys, SQL, credentials, prompts, or
backend error text. A bounded nonblocking queue drops signals when full;
exporter errors and stalls cannot change run results or block the owner.

## Query the exact published generation

For direct programmatic execution, `terrine.runtime.run_plan` takes the immutable
plan, owner state `con`, `namespace`, `request_key`, `destination` and `executor`,
and returns a `RunReceipt`. The saved-plan example above supplies the retained
input and reconstruction bindings for the Model/Design workflow.

Use the returned `RunReceipt`, not the current dataset manifest, when reading results. The API checks the authoritative run receipt and opens only its pinned generation and recorded result partitions:

```python
from terrine.runtime import open_published_generation

published = open_published_generation(receipt, con=owner_connection, namespace=namespace)
result = published.execute()
```

The receipt remains bound to its generation after a later run advances the current manifest. Do not guess internal table names or query the manifest as a substitute for the receipt.

Use `open_published_frame(receipt, con=owner_connection, namespace=namespace)` for a materialized Frame with its compiled column metadata and original Arrow schema, including `StatePanel` duration columns. A Design has one combined published frame per partition even when its expressions depend on several sources. Named Design mappings use canonical name order, so saved plans and ordinary materialization retain the same column metadata.

The lazy `open_published_generation` relation restores duration columns as Ibis intervals with their recorded units, so expressions such as `published.t0 + published.gap` retain timestamp-arithmetic semantics. DuckDB/Ibis cannot express nanosecond intervals: only that lazy-read variant is refused with `terrine.runtime.lazy_duration_unsupported`. Use `open_published_frame` (or the backend's `read_partition`) to retain exact nanosecond durations in Arrow instead.

## Refresh after a declared correction

When an operator declares a correction, capture or identify the corrected source snapshot, provide change evidence or a conservative restatement range, and construct a new plan against that snapshot. Reuse the same `Model`/`Design` identities only if their definitions and schemas are unchanged; otherwise create a new definition fingerprint. Submit the correction with a distinct request key. The runtime does not treat a newer readiness watermark or a changed “last updated” timestamp as proof that earlier rows did not change. Without authoritative change support, choose a conservative rebuild rather than omitting potentially affected outputs.

Effective-dated attribute corrections and roster exits or re-entries are source changes too: they can change output rows without changing fact-table totals. Nested `event_time(..., relative_to=...)` selectors can depend on corrections outside an aggregate's window around `t0`; refresh their dependent outputs rather than limiting change evidence to that aggregate window. The runtime correction matrix compares complete published rows and column metadata with ordinary Frame materialization, including these cases and a two-period `StatePanel`.

That comparison also covers stable `EventAnchor(..., "each")` identities after removal, input schema widening, incomplete-to-complete readiness, panel baseline attributes, and corrected absorbing terminal histories. Every corrected saved request is replayed without re-executing its tasks. Published provenance names the exact retained input generations. A fresh process can reconstruct and execute a previously saved Model/Design plan even after live facts change. If any retained input artifact changes after partial task completion, execution refuses the artifact and publishes no generation; live fact, dimension, and roster mutations do not change a valid retained artifact.

An authoritative empty revision change set against a verified current base produces
a no-change refresh. `Dataset.refresh` checks the original runtime owner and
rechecks that exact base and revision under a lease, returning the identical
manifest without source capture or recomputation-only scan-budget preflight.
The built-in executor records no-change runs against their expected current
publication without creating an empty generation: exact result selection, column
metadata, and the publication's original retained input binding remain unchanged.

## Consistency guarantees and limits

- Plans and runtime records separate definition identity from source-snapshot identity. Results are associated with the plan/run and published generation; retries use the persisted run/task state and fences.
- The DuckDB backend stages immutable generation tables and switches the authoritative manifest in one DuckDB transaction. Its manifest publication is atomic: a reader observes the prior committed generation or the complete new one, not a mixture. The owner serializes overlapping writes and rejects stale fencing tokens.
- `DuckDBBackend` does **not** itself capture a source snapshot or discover source changes (`snapshot=False`, no declared change-evidence mechanism, and unknown scan estimates). Snapshot identity and source consistency are the responsibility of the configured executor/input authority. A label such as `snapshot_id` alone does not freeze upstream tables.
- Other backends must not be assumed to have atomic publication. Their publication capability is unknown until a backend-specific protocol and crash tests prove it; use only a backend that explicitly advertises the guarantee you require.
- Readiness/watermarks describe declared completeness, never change detection. Unknown scan costs remain unknown and cannot satisfy a strict scan budget. A requested cancellation does not guarantee physical query termination.
- The runtime does not collect or send customer rows, raw SQL, request keys, credentials, or raw backend errors to a hosted service. Status/events remain local authoritative state; telemetry, when configured by an application, is only a best-effort projection.

These are materialization orchestration and publication guarantees, not a promise that every study design or source backend has a built-in incremental compiler. The executor owns that translation and must retain the same-snapshot equivalence required by its workload.

## Owned inputs for repeated builds

Ordinary native builds use one private transaction without taking over a caller
transaction. For a stable, runtime-owned copy of the complete input frontier,
use `publish_native_inputs`. Given an open Model connection and Design:

```python
from terrine.execution import publish_native_inputs

with publish_native_inputs(model.catalog, con=con, units=model.units) as stored:
    binding = model.catalog.verify_immutable_inputs(
        stored.con, units=model.units, relations=stored.relations
    )
    pinned = tr.Model.from_catalog(
        model.catalog,
        con=stored.con,
        units=model.units,
        readiness=model.readiness.sources,
        event_ids=model.event_ids,
        input_binding=binding,
    )
    result = design.build(pinned).materialize(budget=budget)
```

Keep the publication open for new builds. An execution already holding a lease
can finish after owner closure; later builds refuse. The managed artifact is
read-only and cleaned up after the last lease. A stored SQL root substitutes its
whole result, not a fresh query against mutable upstream tables.

Ordinary DuckDB builds require a dedicated native connection with no active
caller transaction or unresolved TEMP inputs. Explicit publication can capture
admitted owner-local TEMP roots. For ambiguous partial names, declare fully
qualified source SQL such as `SELECT * FROM "analytics"."main"."events"`.
Owner-staged named tables are the route for unproven functions or data-dependent
schema inference. Explicit single-root file-reader capture is available through
publication; arbitrary side-effecting queries are not admitted.

A Model pins its admitted relation definitions and schemas. Rebind after a
schema or view-definition change; ordinary row updates are captured at safe
execution.

Concurrent exports from one Model coordinate shared-origin metadata and setup;
their private snapshot queries remain independent. Do not use the origin
connection externally during setup or capture. SQL retains the caller's timezone,
while exported anchor timestamps preserve their instants in UTC Arrow columns.

These are local execution identities, not portable snapshot/replay certificates.
Ordinary source generations and snapshot IDs remain unknown; managed copies have
issued generations, while replay equivalence is still `unverified`.
