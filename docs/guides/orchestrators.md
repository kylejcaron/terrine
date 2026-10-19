# Orchestrator integrations

Airflow, Dagster, and dbt Core are optional triggers for Terrine's owner-planned
materialization runtime. They do not construct task graphs, acquire leases,
write runtime state, or open the owner's DuckDB file. The owner resolves each
request, captures the source dependency closure, performs shared preflight,
persists a canonical plan and request key, and owns execution and publication.

See the [materialization runtime guide](materialization-runtime.md) for owner
setup, IPC limits, source consistency, and cancellation behavior. Local examples
are [`airflow_materialization.py`](../../examples/orchestrators/airflow_materialization.py),
[`dagster_materialization.py`](../../examples/orchestrators/dagster_materialization.py),
and [`dbt_core_after_build.py`](../../examples/orchestrators/dbt_core_after_build.py).

## Optional installs and owner endpoint

Install only the integration that runs in the scheduler or caller process:

```sh
uv sync --extra duckdb --extra airflow
uv sync --extra duckdb --extra dagster
uv sync --extra duckdb --extra dbt
```

Airflow, Dagster, dbt Core, and dbt-duckdb are optional extras; importing the
Terrine base package does not load or require them. Run the runtime owner beside
its database and configure `ControlEndpoint` plus the owner namespace for each
trigger. The owner owns backend credentials and connections. Do not pass an
Airflow connection URI to Ibis or give scheduler workers direct access to the
owner's database.


Each owner process must install the fresh-planning service; constructing a
`ControlServer` without `plan_planner` cannot accept orchestration `PlanRequest`s.
The application's model resolver returns the referenced `Model`. The owner
persists a `SavedPlanRecipe` in the invocation and runs that exact recipe through
the built-in `DesignExecutor` by default, reopening retained inputs for compiler
layers and fenced publication. No application executor is required.

```python
from pathlib import Path

from terrine.runtime.control import ControlEndpoint, ControlServer
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planning import OwnerPlanner
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore

namespace = "analytics_owner"
endpoint = ControlEndpoint(Path("/run/terrine/owner.sock"))
store = RuntimeStore(owner_connection, namespace=namespace)
store.initialize()
snapshot_authority = SnapshotAuthority(
    RuntimeOwner(Path("analytics.duckdb"), Path("/var/lib/terrine/snapshots"))
)
planner = OwnerPlanner(resolver=model_resolver, snapshot_authority=snapshot_authority)
server = ControlServer(
    store,
    namespace=namespace,
    endpoint=endpoint,
    plan_planner=planner,
    resolver=model_resolver,
    snapshot_authority=snapshot_authority,
)
server.start()
```

`owner_connection` and `model_resolver` are configured by the application that
owns the model/catalog and backend. Keep the connection open only in the owner
process. Restart the owner with the same namespace and snapshot root so its
persisted invocation and retained recipes remain readable. Applications with
custom execution requirements may supply an executor implementing
`run_snapshot_task` and fenced publication instead of the built-in executor.
A `PlanRequest` contains a stable invocation ID, owner-resolvable
`ModelReference`, `Design`, destination, and optional changes, budget, and output
scope. The owner resolves the current source revision and expected base manifest,
runs shared static preflight, captures only the plan's source dependency closure,
and stores the canonical invocation before returning. Repeating the same
invocation ID and request replays the same plan, key, destination, and run;
changing the request under that identity is refused. A new logical build uses a
new ID and resolves a fresh source snapshot. Keep row data, credentials, raw SQL,
connection strings, and backend error text out of scheduler payloads.

## Retry, status, and cancellation

Transport retries and execution retries use the exact same owner invocation and
request key. A failed runtime run may resume under a newer runtime fence with
the same run ID and completed task outputs reused. Do not mint a new identity
for an attempt or imitate the runtime fence in scheduler state. Succeeded and
cancelled runs are terminal. Only authoritative `succeeded` status means the
trigger succeeded; an empty task layer is not evidence that downstream work or
publication can be skipped.

Cancellation is a durable request, not proof that backend work or billing has
stopped. A status read can be stale or unknown, and monitoring failures must not
turn a run into success. The owner serves status and cancellation independently
of the blocking run request.

## Airflow and Dagster

The Airflow TaskGroup submits a whole plan through owner IPC and stores the
returned invocation in XCom. Keep DAG catch-up and whole-owner concurrency
bounded; derive the invocation ID from the logical DAG run, never the retry
number. Airflow worker pools may limit whole-resource concurrency but do not
replace owner leases.

The Dagster asset maps each date partition to a date-scoped design and distinct
output destination. A retry/re-execution of the same root run and partition
reuses its invocation and key. The asset applies the configured Dagster
`dagster/concurrency_key` to the whole owner resource set. Local
`DagsterInstance.ephemeral()` uses a synchronous in-memory run coordinator, so
it cannot demonstrate queued-run concurrency; use a configured deployment run
queue to enforce that scheduling limit.

## dbt Core after-build and local DuckDB handoff

The after-build function does not manage processes. For a dbt project sharing
the local DuckDB file with the runtime owner, use this lifecycle:

1. Stop the owner completely and close its DuckDB connection.
2. Run `dbt build` against the shared file.
3. Restart the owner. Let startup recovery reconcile interrupted work before
   submitting a plan.
4. Read the dbt manifest and bind only explicitly reviewed model node IDs to
   Terrine catalog sources. `reviewed_dbt_relation_names` rejects missing,
   non-model, and identity-less manifest entries; reviewed identities should be
   checked into the caller's reviewed source configuration, not inferred from
   arbitrary manifest content.
5. Build a `PlanRequest` from the reviewed model and design. Set
   `invocation_id` to the caller-supplied stable `build_id`, then call
   `run_after_dbt_build` with the owner endpoint and namespace.

For example, a dbt caller can use the recipe after `dbt build` and owner restart:

```python
receipt = run_after_dbt_build(
    build_id=build_id,  # generated once for this logical dbt build
    plan_request_provider=plan_request_for_reviewed_manifest,
    endpoint=ControlEndpoint(Path("/run/terrine/owner.sock")),
    namespace="analytics_owner",
)
```

`plan_request_for_reviewed_manifest(build_id)` must return a `PlanRequest` whose
invocation ID equals that build ID. The caller creates the ID before the build
and preserves it across response-loss retries or a fresh client process. A
failed run can be retried with the same ID after the owner is available; the
runtime advances its fence and reuses completed outputs. A new dbt build uses a
new ID. The helper submits the owner plan, runs the exact persisted recipe with
its destination-scoped key, and checks authoritative status. It does not create
generated dbt models, write state from a post-hook, or manage owner processes.

## Planning boundary and guarantees

The owner routes requests through the same supported-shape and budget preflight
as direct runtime use. Unsupported static shapes or strict budgets are refused
before source capture; row-dependent eligibility is evaluated in the pinned
snapshot and refusal precedes output staging/publication. The runtime's selector
invalidation and source-snapshot rules remain authoritative. Orchestrator
success does not expand those guarantees or claim capabilities for a live
warehouse that has not been tested.
