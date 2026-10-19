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

All responses use schema version 1 and `ok`, `refused` or `pending` status.
Refusals include stable codes, immutable JSON context and an actionable route.
Backend exception messages are not response fields. Non-refusal programming
errors are not disguised as successful responses. All calls, including denied
calls, share the session's `max_requests` count.

The optional MCP transport and external object-store destinations require
separate owner authorization and are not included in this Python interface.
