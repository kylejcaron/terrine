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

Use `OwnerPlanner.plan(request, con=owner_connection, namespace=namespace)` to
prepare a built-in run. The owner captures and pins the inputs and returns
`invocation.saved_plan_json` and `invocation.request_key`. Pass both to
`run_serialized_plan` with the same owner configuration. The runnable
[`examples/materialization_runtime.py`](../../examples/materialization_runtime.py)
shows the whole workflow:

```console
uv run python examples/materialization_runtime.py
```

The example publishes per-user one-day event totals and an effective-dated tier from a local DuckDB source, then verifies each exact-receipt result against a fresh `design.build(model).materialize(budget=budget)` of the same Model/Design and source state. It then inserts an event and corrects the effective-dated tier, captures a new retained snapshot, saves a new plan against the first generation, and queries that second receipt. The totals change from `3, 4` to `8, 4`, and the tier changes from `gold` to `silver`; the first receipt remains bound to its original generation. Saved plans use the production `DesignExecutor`; no application-specific aggregation executor is involved in this example.

In the example Catalog, the `tier` property references the `history` dimension, whose `starts` and `ends` columns define validity intervals in `user_tier`. The Design requests `"tier": asof("tier", before="0s")`, so the attribute is taken strictly before the anchor from the effective-dated event history. Updating those dimension rows requires a new retained snapshot just as inserting an event does. The example reopens the first receipt after publishing the correction and checks that its original rows are unchanged.

If you captured inputs yourself with `SnapshotAuthority.capture`, pass the
receipt as `snapshot=` to `OwnerPlanner.plan`. It verifies and pins that
snapshot instead of recapturing. On retry, supply the same request and
receipt. Saved plans contain no live connection, relation handle or credentials.

**Migrating a built-in planner:** replace `plan_refresh(..., execution_mode="design")`
and manual `SavedPlan` assembly with `OwnerPlanner.plan(...)`.

Finish queued or failed legacy built-in requests (runtime plan version 3) on
the version that saved them before upgrading. If you cannot drain a request,
keep its old owner state and replan intentionally from the saved Design or
dataset definition. The new request does not resume the previous schedule or
promise the same input image. You can still reopen a completed publication
by its pinned generation.

**Custom execution:** `dependency_graph(...)` describes logical dependencies;
`plan_refresh(...)` turns them into one task per selected node without reading
data. Use this path with your own callbacks. A manually supplied `snapshot_id`
in that plan is a label, not a frozen copy of source rows. A custom owner
service can keep automatic capture by configuring
`OwnerPlanner(execution_mode="custom", ...)`; its executor receives the recorded
tasks unchanged.

`SavedPlan` lives in `terrine.runtime.saved_plan` and is also exported from
`terrine.runtime`. Its serialized reconstruction binds `dataset_definition_json`
and `plan_hash`; `restore_dataset_definition()` returns the saved dataset
construction. The canonical runtime `PlanDefinition` is available as
`RefreshPlan.definition`, including the fingerprint used for scan authorization.
Owner preparation accepts `PlanRequest.dataset_definition` and persists the
complete saved plan as `PlanInvocation.saved_plan_json`.

Saved plans use envelope version **2** and execution revision **2**. The built-in
schedule is fingerprinted in runtime plan version **4**; logical callback plans
keep version **3** and their original fingerprints. A nonempty version-3 plan
requires its custom executor, or you can replan it for built-in execution after
`terrine.runtime.saved_plan_mismatch` (`reason="physical_jobs_required"`).
Current-revision terminal replay and zero-job reuse keep their original generation fences.
Version-1 saved plans, version-2 runtime plans and obsolete execution revisions
remain checksum-verifiable for inspection, snapshot pin release, and authorized
status, event and cancellation operations. Replan them before execution or replay.

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

For built-in execution, each attempt copies and verifies the retained input once
when it can detach a private read-only DuckDB image. Its jobs use separate
readers and temporary relations against that image. If detaching isn't supported,
each job verifies the retained file on open, as custom callbacks do. A new
attempt verifies again, including on process restart. A zero-job reuse opens
no input image. Neither artifact sizes nor task counts are physical scan bounds.

## Runtime shape support

The retained runtime uses the same `compile_design` dispatcher as ordinary
Frames, but only these shapes can substitute persisted logical inputs:

| Shape | Retained logical inputs |
| --- | --- |
| `cross_section` | Verified anchors and the retained source snapshot |
| `SequentialSpec` | Verified scheduled anchors and snapshot inputs; treatment, sampling and timing metadata retained |
| `EventPanelSpec` | Verified parent event anchors expanded into declared periods, with snapshot-backed aggregation at translated evaluation times |
| `StatePanel` | Verified scheduled output anchors and snapshot inputs for features, binned state periods/endpoints and baseline features; internal endpoint and baseline anchors keep their own domains |
| Calendar comparisons, event sequences, cohorts, survival and other shapes | Refuse |

An `EventAnchor` must have a zero offset for reuse, regardless of shape.
Shifted anchors and unsupported shapes refuse before source resolution with
`terrine.compile.runtime_shape_unsupported`, including direct `compile_design`
calls with persisted anchors. When both restrictions apply, the refusal
reports `shape="EventAnchor.offset"` first. The route is
`ordinary Frame materialization or a full rebuild`. This matrix does not
restrict direct Frames without persisted inputs or guarantee that a given
feature, backend or owner policy allows staging.

Additive windows use the same dependency-ready local batch collector as ordinary Frames, with raw
contributions or declaration-proven aligned noncumulative day payloads inside
the retained snapshot. Supplied/generated anchor clocks do not inherit an
outer schedule's alignment proof. Nonadditive and selector algorithms remain
separate. Neither path builds lifetime numeric prefixes; retained execution
produces no daily/prefix tasks or numeric intermediate Arrow ABI.
Nested event selectors keep their existing dependency invalidation.

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
`{"version":1,"execution_revision":2}`. The publisher takes this revision from
the validated executing plan, not the installed code or incoming data. Existing
markers and any Frame provenance must agree; historical Frame metadata cannot
be silently relabeled. Ordinary Frames also record `Provenance.execution_revision`;
legacy provenance leaves that optional field absent when reserialized.

`generation_execution_revision(dataset, generation)` reads the exact published
generation's complete partition-schema evidence without reading source rows.
All markers absent means historical/unknown revision 0. Missing schema rows,
mixed absence, malformed markers or unequal revisions cannot authorize reuse.
An empty operator `ChangeSet` says the inputs did not change relative to the
specified base. With `Dataset.refresh` or an explicitly base-bound
`PlanRequest`, the owner checks the current generation, destination and
scope, the full saved definition (including its view and transforms), the
Model declarations, and the retained snapshot receipt. Only a matching
current-revision base can be reused without capturing or reading source rows.
Missing change evidence, a readiness watermark alone, or a legacy revision
label does not establish that the inputs are unchanged.
The base must also have its recorded Dataset recipe. A scheduler that only
runs `OwnerPlanner.plan` and `run_serialized_plan` does not record that recipe
just by publishing a generation; it follows the ordinary capture path.

Reuse is rechecked under the dataset lease and again before recording its
receipt. If the base moved, prepare a new refresh from the current generation.
An already completed request still resolves its exact committed generation,
even after a later publication. Historical reads keep the original values and
provenance; refreshing does not relabel or mutate the old generation.
The marker uses the existing partition schema storage: runtime namespaces
remain at schema version **1**, and DatasetManifest envelopes remain version
**1**, with no schema migration or new runtime columns.

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

Native DuckDB query scan bounds/caps remain unavailable. Strict source-scan
budgets refuse before capture when refresh needs to read inputs. A verified
no-work refresh has no source-scan phases and can reuse its generation under
a strict scan cap; checking the published output's integrity is separate.
Retaining input rows or limiting output does not turn their size into a scan
bound.

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

Status freshness describes the persisted attempt lease, not whether a backend query is alive. When a lease expires without a committed publication or cancellation request, recovery records `terrine.runtime.owner_interrupted`; the owner can resume the same saved request from completed task outputs. Consumers can redeliver event pages safely by deduplicating on `(run_id, sequence)` and advancing only the acknowledged exclusive cursor.

Committed publication recovery verifies the matching saved plan from an owner
invocation, dataset request or fingerprint-bound direct execution record. Direct
execution persists that identity atomically with its fenced running transition in
the existing metadata tables. Recovery checks execution revision and any no-change
reuse-base restriction. A verified publication wins even if someone requested
cancellation: the run ends `succeeded`. If the publication cannot be verified,
the run ends `failed` with `terrine.runtime.saved_plan_mismatch` unless a direct
cancellation request was already recorded. In that case it ends `cancelled`
without an error code, just as it does when cancellation precedes recovery and
there is no publication. A `cancelled` status does not verify or undo a commit.

Recovery releases active-run snapshot pins only after the fenced terminal
transition. Failed runs retain retry inputs unless their exact checksummed
plan identity proves that the execution format or revision is no longer
admissible. Missing identity or invalid publication evidence alone does not
authorize release. Snapshot collection also reconciles obsolete failed-run
pins left by earlier owners; saved-plan and current-publication references
still protect the inputs until separately released or superseded.
Only runs still owning active snapshot references need cleanup identity checks;
each recovery or collection invocation resolves its required fingerprints in a
single stored-plan pass, without retaining an identity cache between invocations.
Active references recognize exact historical bare run IDs, including JSON-text
IDs, and two-item fenced lists with a nonempty run ID and a positive integer
fence. If both conventions name existing runs, every owner must be terminal
and nonretryable before cleanup can release the reference. Opaque, malformed
or unrecognized references remain protected unless their exact bare owner is
established; interpreting JSON alone never proves run ownership.

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

That comparison also covers stable `EventAnchor(..., "each")` identities after removal, input schema widening, incomplete-to-complete readiness, panel baseline attributes, and corrected absorbing terminal histories. Every corrected saved request is replayed without re-executing its tasks. Published provenance names the exact retained input generations. A fresh process can reconstruct and execute a previously saved Model/Design plan even after live facts change. Changes to live facts, dimensions, or rosters do not alter a valid retained artifact.

If the retained artifact changes after the built-in executor has detached a
verified input image, the current attempt can still publish from that image.
A later attempt that opens the changed artifact refuses it. Custom executors
and built-in runs that fall back to per-open verification check the artifact
on each open and refuse if it has changed.

To skip a rebuild, pass `ChangeSet((), "operator")` to `Dataset.refresh` on a
pinned dataset, or use it in a `PlanRequest` with `bind_base_manifest=True` and
`expected_base_manifest` set to the current generation. The owner reuses the
generation only if the committed definition and retained input receipt match
and the execution revision is current. `ChangeSet((), "revision")` alone
requires a rebuild; with a strict source-scan cap, it instead refuses with
`terrine.budget.scan_unknown` before capture.

On reuse, `Dataset.refresh` returns the identical manifest. The built-in
executor records a run against the existing generation, preserving its result
partitions, column metadata, and original retained input binding.

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
