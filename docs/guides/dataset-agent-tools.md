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

The optional MCP transport and external object-store destinations require
separate owner authorization and are not included in this Python interface.

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
