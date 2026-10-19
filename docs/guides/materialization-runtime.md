# Durable materialization runtime

The materialization runtime persists plan/run state and publishes immutable output generations in the customer's own execution environment. It does not infer whether source rows changed: source revisions, change feeds, checksums, or explicit operator restatements must provide that evidence. A readiness watermark says which period is declared complete; it is not a change detector.

## Build and plan from a Model and Design

The runnable [`examples/materialization_runtime.py`](../../examples/materialization_runtime.py) is the end-to-end workflow. It builds a real `Model` and immutable `Design`, captures a retained input snapshot, serializes and saves a `SavedPlanRecipe` bound to a destination, runs it through the owner resolver and snapshot authority, then queries the exact returned generation:

```console
uv run python examples/materialization_runtime.py
```

The example publishes per-user one-day event totals and an effective-dated tier from a local DuckDB source, then verifies each exact-receipt result against a fresh ordinary `compile_columns` rebuild of the same Model/Design and source state. It then inserts an event and corrects the effective-dated tier, captures a new retained snapshot, saves a new recipe against the first generation, and queries that second receipt. The totals change from `3, 4` to `8, 4`, and the tier changes from `gold` to `silver`; the first receipt remains bound to its original generation. Saved Design recipes use the production `DesignExecutor`; no application-specific aggregation executor is involved in this example.

In the example Catalog, the `tier` property references the `history` dimension, whose `starts` and `ends` columns define validity intervals in `user_tier`. The Design requests `"tier": asof("tier", before="0s")`, so the attribute is taken strictly before the anchor from the effective-dated event history. Updating those dimension rows requires a new retained snapshot just as inserting an event does. The example reopens the first receipt after publishing the correction and checks that its original rows are unchanged.

For lower-level planning, `dependency_graph(model, design, partitions=...)` and `plan_refresh(...)` are metadata-only. A manually supplied `snapshot_id` is only an identity label; it does not freeze source rows. Persist and execute a saved recipe tied to `SnapshotAuthority.capture` when source inputs must be retained and reopened across partitions, retries, or process restarts. Saved recipes contain no live connection, relation handle, or credentials.

A destination reference identifies `(database, schema, dataset)` for request-key isolation. It is not a provisioning command: arrange the customer-owned database and schema through the application before starting the owner. The owner uses its configured connection and namespace; do not put a connection string or secret in a plan or request key.

`SnapshotAuthority.capture` is scoped to the resolved `Design`: it captures only required fact/property sources, their required dimensions and integrity inputs, and that design unit's roster. It keeps the full Catalog/model identity bound in the receipt without copying unrelated source rows. For example, a label source unused by the plan is not captured.

DuckDB capture uses backend-native `COPY` into retained storage rather than transferring source rows through the Ibis client. Output row/byte limits do not limit this input capture, and unknown source scan work cannot satisfy a strict scan budget. Capture acceptance increases input cardinality while prohibiting client row-fetch APIs and bounding steady-state Python allocations independently of the retained row count; backend-native memory and one-time initialization are not presented as measured client buffers.

## Runtime shape support

Runtime composition enters through the same `compile_design` dispatcher as
ordinary Frames. Its supported logical-input matrix is:

| Shape | Persisted anchor/prefix substitution |
| --- | --- |
| `cross_section` | Verified anchor and additive prefix layers |
| `SequentialSpec` | Verified scheduled anchors and additive prefixes; treatment, sampling and timing metadata retained |
| `EventPanelSpec` | Verified parent event anchors expanded into the declared periods; additive prefixes reused at translated evaluation times |
| `StatePanel` | Verified scheduled output anchors and additive prefixes for features, binned state periods/endpoints, and baseline features; internal endpoint and baseline anchors keep their own domains |
| Calendar comparisons, event sequences, cohorts, survival and other shapes | Refuse |

Unsupported substitutions refuse before source resolution with
`terrine.compile.runtime_shape_unsupported`, including direct `compile_design`
calls. The coded refusal carries the shape and the route
`ordinary Frame materialization or a full rebuild`. Nonadditive windows use raw
events with their compiler-derived sparse range bounds, never an additive
prefix layer. Nested event selectors reuse the existing selector-dependency
invalidation contract, and re-anchored additive metrics require event-resolution
prefixes rather than assuming calendar alignment.

For persisted-layer staging, pass the resolved design, model, logical relations, and budget through `terrine.runtime.materialize.compose_runtime_frame`. This runtime entry point applies the same execution-owned shape-budget, integrity, and unknown-eligibility gates as `Frame.materialize` before returning the compiled relation for staging.

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
`ScanAuthorization(plan.recipe.fingerprint)` only after verifying the saved
evidence against its actual retained inputs and read/cap implementation.
A fingerprint verifies the recipe, not the truth of a backend claim. Missing
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

Runtime plan schema **2** fingerprints cost evidence, forecast fields, attempt
limits, budget, publication preconditions, and the reconstruction identity when
bound to a saved Model/Design recipe. Schema 1 is explicitly rejected; replan
older recipes rather than silently upgrading absent evidence. Use
`plan.bind_base_manifest(generation)` when binding publication preconditions
without a saved Model/Design recipe.

`get_run(...).cost` separately reports observed completed-task artifact rows
and bytes, intermediate artifact bytes, and durable attempt counts. When
available, its nested `capture` entry reports retained input artifact accounting;
it does not replace the completed-task observations. These are not total
allocation, repeated-query work, or scan measurements. Unavailable scan
observations remain `None` with a reason; legacy missing artifact observations
remain unavailable. Observations never rewrite saved forecasts.

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

For saved recipes, the durable request fingerprint includes the complete reconstruction recipe: the owner Model reference and its Catalog, roster, readiness and event-ID bindings, Design, destination, retained snapshot, and expected base manifest. Changing any binding is a different request even after the original run has finished; terminal replay verifies that identity without requiring the resolver or snapshot artifact to remain available.

Status, event pages, and cancellation are projections of durable owner state. Event cursors are exclusive and pages bounded. Endpoint availability is tied to the owner process: after a crash, wait until a recovery owner can reacquire the DuckDB database and reconcile before querying again. A cancellation request is durable but does not prove that an active backend query or its billing stopped. A stale heartbeat means liveness is unknown, not that a run is safely terminal.

Executed runs expose durable task progress (`completed_tasks`, `running_tasks`, `total_tasks`), an execution/publication or terminal phase, the captured source readiness bounds, and a stable failure code. Capture accounting records the retained artifact's byte size, root count, and snapshot identity; `scan_bytes` remains unknown rather than being reported as zero. These projections and event pages read runtime metadata only, never customer source tables. Legacy records without capture or task accounting retain unknown values.

Status freshness describes whether the persisted attempt lease is current or stale, not proof that a backend query is alive or stopped. After an interrupted owner loses its lease, a recovery owner records `terrine.runtime.owner_interrupted` and can resume the same saved request from completed task outputs. Consumers can redeliver event pages safely by deduplicating on `(run_id, sequence)` and advancing only the acknowledged exclusive cursor.

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

Use the returned `RunReceipt`, not the current dataset manifest, when reading results. The API checks the authoritative run receipt and opens only its pinned generation and recorded result partitions:

```python
from terrine.runtime import open_published_generation

published = open_published_generation(receipt, con=owner_connection, namespace=namespace)
result = published.execute()
```

The receipt remains bound to its generation after a later run advances the current manifest. Do not guess internal table names or query the manifest as a substitute for the receipt.

Use `open_published_frame(receipt, con=owner_connection, namespace=namespace)` for a materialized Frame with its compiled column metadata and original Arrow schema, including `StatePanel` duration columns. A Design has one combined published frame per partition even when its expressions depend on several sources. Named Design mappings use canonical name order, so saved recipes and ordinary materialization retain the same column metadata.

The lazy `open_published_generation` relation restores duration columns as Ibis intervals with their recorded units, so expressions such as `published.t0 + published.gap` retain timestamp-arithmetic semantics. DuckDB/Ibis cannot express nanosecond intervals: only that lazy-read variant is refused with `terrine.runtime.lazy_duration_unsupported`. Use `open_published_frame` (or the backend's `read_partition`) to retain exact nanosecond durations in Arrow instead.

## Refresh after a declared correction

When an operator declares a correction, capture or identify the corrected source snapshot, provide change evidence or a conservative restatement range, and construct a new plan against that snapshot. Reuse the same `Model`/`Design` identities only if their definitions and schemas are unchanged; otherwise create a new definition fingerprint. Submit the correction with a distinct request key. The runtime does not treat a newer readiness watermark or a changed “last updated” timestamp as proof that earlier rows did not change. Without authoritative change support, choose a conservative rebuild rather than omitting potentially affected outputs.

Effective-dated attribute corrections and roster exits or re-entries are source changes too: they can change output rows without changing fact-table totals. Nested `event_time(..., relative_to=...)` selectors can depend on corrections outside an aggregate's window around `t0`; refresh their dependent outputs rather than limiting change evidence to that aggregate window. The runtime correction matrix compares complete published rows and column metadata with ordinary Frame materialization, including these cases and a two-period `StatePanel`.

That comparison also covers stable `EventAnchor(..., "each")` identities after removal, input schema widening, incomplete-to-complete readiness, panel baseline attributes, and corrected absorbing terminal histories. Every corrected saved request is replayed without re-executing its tasks. Published provenance names the exact retained input generations, and retained prefix outputs are checked when one aggregate window changes while another remains unaffected. A fresh process can reconstruct and execute a previously saved Model/Design recipe even after live facts change. If any retained input artifact changes after partial task completion, execution refuses the artifact and publishes no generation; live fact, dimension, and roster mutations do not change a valid retained artifact.

An authoritative empty revision change set produces a no-change refresh. The built-in executor records that run against its expected current publication without creating an empty generation: exact result selection, column metadata, and the publication's original retained input binding remain unchanged.

## Consistency guarantees and limits

- Plans and runtime records separate definition identity from source-snapshot identity. Results are associated with the plan/run and published generation; retries use the persisted run/task state and fences.
- The DuckDB backend stages immutable generation tables and switches the authoritative manifest in one DuckDB transaction. Its manifest publication is atomic: a reader observes the prior committed generation or the complete new one, not a mixture. The owner serializes overlapping writes and rejects stale fencing tokens.
- `DuckDBBackend` does **not** itself capture a source snapshot or discover source changes (`snapshot=False`, no declared change-evidence mechanism, and unknown scan estimates). Snapshot identity and source consistency are the responsibility of the configured executor/input authority. A label such as `snapshot_id` alone does not freeze upstream tables.
- Other backends must not be assumed to have atomic publication. Their publication capability is unknown until a backend-specific protocol and crash tests prove it; use only a backend that explicitly advertises the guarantee you require.
- Readiness/watermarks describe declared completeness, never change detection. Unknown scan costs remain unknown and cannot satisfy a strict scan budget. A requested cancellation does not guarantee physical query termination.
- The runtime does not collect or send customer rows, raw SQL, request keys, credentials, or raw backend errors to a hosted service. Status/events remain local authoritative state; telemetry, when configured by an application, is only a best-effort projection.

These are materialization orchestration and publication guarantees, not a promise that every study design or source backend has a built-in incremental compiler. The executor owns that translation and must retain the same-snapshot equivalence required by its workload.
