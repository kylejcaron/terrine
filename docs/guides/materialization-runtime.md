# Durable materialization runtime

The materialization runtime persists plan/run state and publishes immutable output generations in the customer's own execution environment. It does not infer whether source rows changed: source revisions, change feeds, checksums, or explicit operator restatements must provide that evidence. A readiness watermark says which period is declared complete; it is not a change detector.

## Build and plan from a Model and Design

Construct the ordinary `Model` and immutable `Design` as usual. The runtime dependency graph uses the model's declared sources and the design hash; choose bounded partition names appropriate for the workload. Supply a snapshot identity that represents the exact source snapshot the owner executor will read:

```python
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef

namespace = "analytics_owner"
destination = DestinationRef("analytics.duckdb", "published", "orders")
graph = dependency_graph(model, design, partitions=("2026-06", "2026-07"))
plan = plan_refresh(
    namespace=namespace,
    dataset=destination.dataset,
    definition_hash=design.spec_hash,
    snapshot_id="orders-source-revision-42",
    graph=graph,
)
saved_plan = plan.serialize()
```

Persist `saved_plan` with the owner's job/configuration records before submitting execution. It is canonical, versioned JSON and contains no live connection, relation handle, or credentials. Keep the Model/Catalog, Design, source revision, and destination bound together in the owner application. The runtime graph and planner are metadata-only: they do not compile or execute source queries. An owner-provided `Executor` must implement bounded task reads against the named snapshot and produce the task outputs; its publisher writes those outputs through the backend. See [`examples/materialization_runtime.py`](../../examples/materialization_runtime.py) for a complete local DuckDB executor and Python API example.

A destination reference identifies `(database, schema, dataset)` for request-key isolation. It is not a provisioning command: arrange the customer-owned database and schema through the application before starting the owner. The owner uses its configured connection and namespace; do not put a connection string or secret in a plan or request key.

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

Status, event pages, and cancellation are projections of durable owner state. Event cursors are exclusive and pages bounded. Endpoint availability is tied to the owner process: after a crash, wait until a recovery owner can reacquire the DuckDB database and reconcile before querying again. A cancellation request is durable but does not prove that an active backend query or its billing stopped. A stale heartbeat means liveness is unknown, not that a run is safely terminal.

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

## Refresh after a declared correction

When an operator declares a correction, capture or identify the corrected source snapshot, provide change evidence or a conservative restatement range, and construct a new plan against that snapshot. Reuse the same `Model`/`Design` identities only if their definitions and schemas are unchanged; otherwise create a new definition fingerprint. Submit the correction with a distinct request key. The runtime does not treat a newer readiness watermark or a changed “last updated” timestamp as proof that earlier rows did not change. Without authoritative change support, choose a conservative rebuild rather than omitting potentially affected outputs.

## Consistency guarantees and limits

- Plans and runtime records separate definition identity from source-snapshot identity. Results are associated with the plan/run and published generation; retries use the persisted run/task state and fences.
- The DuckDB backend stages immutable generation tables and switches the authoritative manifest in one DuckDB transaction. Its manifest publication is atomic: a reader observes the prior committed generation or the complete new one, not a mixture. The owner serializes overlapping writes and rejects stale fencing tokens.
- `DuckDBBackend` does **not** itself capture a source snapshot or discover source changes (`snapshot=False`, no declared change-evidence mechanism, and unknown scan estimates). Snapshot identity and source consistency are the responsibility of the configured executor/input authority. A label such as `snapshot_id` alone does not freeze upstream tables.
- Other backends must not be assumed to have atomic publication. Their publication capability is unknown until a backend-specific protocol and crash tests prove it; use only a backend that explicitly advertises the guarantee you require.
- Readiness/watermarks describe declared completeness, never change detection. Unknown scan costs remain unknown and cannot satisfy a strict scan budget. A requested cancellation does not guarantee physical query termination.
- The runtime does not collect or send customer rows, raw SQL, request keys, credentials, or raw backend errors to a hosted service. Status/events remain local authoritative state; telemetry, when configured by an application, is only a best-effort projection.

These are materialization orchestration and publication guarantees, not a promise that every study design or source backend has a built-in incremental compiler. The executor owns that translation and must retain the same-snapshot equivalence required by its workload.
