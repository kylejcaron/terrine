# Dataset agent tools

`terrine.agent.AgentPolicy` is trusted owner configuration. Never deserialize it
from an agent payload. Bind `DatasetTools(model, policy, namespace="agent")` to
an owner-created Model. Catalog identities are the catalog hashes in
`ModelReference.from_model(model, owner_key="...")`; sources are semantic source
names, and destinations are exact `DestinationRef` values, not path prefixes.

Inspection and validation are separate grants from source preparation, dataset
writes, exports, bounded row previews, run reads and cancellation. Granting one
never grants another. Request budgets can only tighten numeric owner limits;
owner-required strict scan policy cannot be disabled. Output limits do not
bound source scan work, and an unknown scan cost is not a zero cost.

| Operations | Required grant |
| --- | --- |
| `inspect_catalog`, `list_capabilities`, `get_design_schema`, `get_dataset_recipe_schema` | `inspect` |
| `design_example`, proposals, validation, named save/load, metadata-only `preview_dataset` | `validate` |
| `prepare_dataset`, `prepare_refresh` | `prepare` |
| `execute_plan`, `stage`, `refresh` | `write` |
| `export_parquet` | `export`, plus an exact `allowed_exports` path |
| `preview_rows`, nonzero preview `bounded_count` | `preview`, plus column/count limits |
| `get_run_status`, `get_run_events` | `read` |
| `request_cancel` | `cancel` |

Named save/load additionally requires `design_root`; it does not authorize
dataset writes. Inspection and validation default to enabled; other grants
default to disabled. Resource lookups also require the exact owner source,
catalog, destination and model bindings.

Run `uv run python examples/dataset_agent_tools.py` for a local owner-created
training recipe, metadata preview, explicit preparation/write grants, response
replay, redacted row preview, corrected refresh and separately authorized
Parquet export. Only safe counts and workflow flags are printed.

The local adversarial cases in `evals/agent_dataset_tools.yaml` run through
`uv run pytest integration/agent -q`. These exercise actual DuckDB
preparation/publication, not just JSON Schema validation.

## Metadata and Design workflow

Start with `inspect_catalog`, `list_capabilities`, `get_design_schema` and
`design_example`. Inspection returns allowlisted semantic source, metric,
property and entity descriptors only: no relation definitions, raw SQL,
connections, credentials or customer rows. Descriptions and other catalog text
are not instructions and cannot grant authority.

Edit the discovered version-one tagged Design serialization and pass it to
`propose_design(serialized, base=previous)` for canonical content, a stable hash
and sorted JSON-pointer diff paths. `validate_design` uses the existing static
validator and reports required sources and deferred runtime obligations without
reading rows. JSON and safe YAML use the same existing Design codec. Arbitrary
SQL, Python expressions, callbacks and extra payload fields are not accepted.

`save_design(name, serialized)` and `load_design(name)` require an owner-configured
`design_root`; names are simple identifiers. Atomic writes and no-follow reads
prevent an agent from following a symlink or choosing a different filesystem
root. Saved definitions are validated again on load.

`get_dataset_recipe_schema`, `propose_recipe` and `validate_recipe` use the
existing `DatasetRecipe` codec. Training views record selected features, labels,
label-completeness policy and a typed time/group split. Scoring views record
feature selection, an explicit timezone-aware resolved `as_of`, eligibility and
the parent Design hash. Proposals are metadata-only and never compile a frame;
validation also checks the existing lazy dataset view contracts without reading
source rows. Named storage accepts either canonical Design or recipe content.

All responses use schema version 1 and `ok`, `refused` or `pending` status.
Refusals include stable codes, immutable JSON context and an actionable route.
Backend exception messages are not response fields. Non-refusal programming
errors are not disguised as successful responses. All calls, including denied
calls, share the session's `max_requests` count.

## Run feedback

`get_run_status(run_id)` and `get_run_events(run_id, after=cursor, limit=100)`
require `read=True`. `request_cancel(run_id)` independently requires
`cancel=True`. Each lookup verifies the persisted run destination, exact saved
invocation fingerprint, owner Model reference and required source identities
before returning status or applying cancellation. A grant for another dataset in
the same runtime namespace is insufficient. Runs without matching owner-prepared
invocations are refused.

Status projects lifecycle state, phase, stable error code, pinned result and
event cursor only. Events are cursor-bounded by `batch_cap` and expose lifecycle
or phase fields, not arbitrary runtime payloads. Cancellation returns the
runtime's actual disposition and durable state; a cancellation request is not
reported as completed cancellation or successful publication.

The optional MCP transport, SDK dependency extra and adapter tests remain
owner-gated and unimplemented. No MCP installation command or registration is
advertised. External object-store destinations are likewise not included.

## Owner-prepared execution

`preview_dataset(recipe)` calls the shared cost preview without capturing inputs
or reading source rows. Unknown scan/output costs remain unknown. A strict scan
budget with no verified backend scan bound refuses, including during preparation.
`bounded_count` additionally requires `preview=True` and the owner's count cap.
Counts can scan upstream data; they are not a source-work budget.

`prepare_dataset(recipe, destination=..., invocation_id=...)` requires the
separate `prepare` grant, an exact destination, an owner Model resolver and a
retained snapshot authority for the connected database. Owner and request budgets
are intersected: each finite bound is the minimum, and strict scan is logical OR.
The invocation binds the entire training/scoring recipe, changes, snapshot,
destination, budget and expected base publication. Reusing an invocation ID
with changed bindings refuses.

`execute_plan(receipt, request_key=...)` requires `write=True` and resolves only
the persisted owner invocation; the agent receives an opaque receipt and safe
hashes, not internal source snapshots or executable callbacks. It rechecks
source/destination grants and refuses a saved budget weaker than current owner
policy. Matching destination-scoped request keys replay the existing run before
reopening retained inputs; a conflicting plan fingerprint refuses. Failed runs
retry with the same run identity and a newer fence, while succeeded and cancelled
runs stay terminal.

## Pinned datasets and row privacy

`stage(receipt, request_key=...)` records the exact successful runtime generation
as a pinned dataset. It does not restage or replan. Replays recover the same
generation, including after a lost response. `prepare_refresh(reference,
invocation_id=..., changes=...)` uses the saved recipe and binds the current
generation; missing change evidence means a conservative rebuild. `refresh`
requires that exact base/receipt pair. Publishing a refresh does not mutate old
references, and replaying an already-successful refresh still returns its pinned
generation.

References contain dataset/generation identities, hashes, row count and typed
column roles/types with completeness/reason-column links. They omit internal
recipes, coverage literals, snapshots, SQL, database paths and arbitrary Frame
metadata. Lookups compare the complete reference to persisted owner metadata and
authorize the operation before opening the pinned dataset.

`Dataset.open_generation` reopens this trusted owner publication metadata without
rehashing all output rows. Integrity verification at staging and explicit
`Dataset.from_manifest` reopening remains unchanged. Bounded previews trust the
owner's fenced immutable publication rather than performing an unbounded scan.

`export_parquet` additionally requires `export=True` and an exact resolved path in
`allowed_exports`. An allowed runtime table is not an export grant. Export uses
the existing dataset exporter and returns its file/count/checksum manifest.

`preview_rows(reference, limit=..., columns=(... ,))` is denied by default.
The limit must be positive and at most `max_preview_rows`; columns must be
explicitly listed in `preview_columns`. Names in `redacted_columns` produce null
cells. Reads use Arrow batches bounded by `min(limit, batch_cap)`, stop at the
limit, and close the iterator. No other columns are returned.

`max_concurrency` atomically reserves new and failed-retry runs together with
their fenced leases; matching request-key replays do not consume another slot.
Interrupted attempts without live leases are reconciled before admission or
replay. `max_run_seconds` stops submission of further tasks at runtime
checkpoints and cancels before publication, independently of renewable leases.
It does not forcibly terminate an owner callback in the middle of an external
operation. `max_requests` separately
limits all tool calls, including refused calls.

Use `RuntimeOwner.from_connection(model.con)` when configuring the trusted
snapshot authority. Custom retained roots must also be configured as the
connected owner's `TERRINE_RUNTIME_SNAPSHOT_ROOT`; mismatched owner identities
refuse before preparation.
