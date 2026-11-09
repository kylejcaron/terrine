# Refusal codes

This reference is generated from every module-owned `RefusalSpec` declaration,
including inline declarations. A refusal code is stable only when raised by a
stable name; the Tier column identifies its code family. Message wording is not
an API. Interpolation fields below are required by the message template, **not**
an exhaustive context schema. Inspect the immutable `TerrineError.context` for
additional request-specific fields, exact reasons and available routes.
Multiple message variants for the same code are retained. Do not retry a
refusal by silently widening a budget, changing an input or dropping rows.

Regenerate with `uv run python -m tests.documentation`; completeness is checked
by `uv run pytest -q tests/test_documentation.py -k refusal`.

## Handling a refusal

Catch `terrine.TerrineError`, dispatch on `.code`, and report `.context` only
inside the owner's trust boundary: backend errors can describe source names.
A missing extra names its installation route. Unknown coverage requires an
authoritative declaration, not the latest event timestamp. Unknown strict scan
costs require independently verified bounds or an explicit owner policy choice.
See the recovery category linked with each code before changing the request.

## agent

Use owner-approved resources, grants and fresh receipts.

[Recovery guide](../guides/dataset-agent-tools.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.agent.invalid_request` | Provisional | <code>Tool request input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/agent/policy.py](../../terrine/agent/policy.py) |
| `terrine.agent.invalid_request` | Provisional | <code>Use a valid typed Design payload.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.agent.model_mismatch` | Provisional | <code>Owner model configuration has changed.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.agent.owner_configuration` | Provisional | <code>Owner execution configuration is unavailable.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.agent.owner_configuration` | Provisional | <code>Owner policy input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/agent/policy.py](../../terrine/agent/policy.py) |
| `terrine.agent.owner_configuration` | Provisional | <code>Owner tool configuration {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.agent.permission_denied` | Provisional | <code>Owner policy denies this operation.</code> | None | [terrine/agent/policy.py](../../terrine/agent/policy.py) |
| `terrine.agent.receipt_unavailable` | Provisional | <code>Use an unchanged owner-prepared receipt.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.agent.request_limit` | Provisional | <code>Owner tool request limit has been reached.</code> | None | [terrine/agent/policy.py](../../terrine/agent/policy.py) |
| `terrine.agent.run_unavailable` | Provisional | <code>Run has no matching owner-prepared invocation.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.agent.scope_denied` | Provisional | <code>Requested resources are outside owner policy.</code> | None | [terrine/agent/policy.py](../../terrine/agent/policy.py) |
| `terrine.agent.storage_denied` | Provisional | <code>Named Design storage is unavailable.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |

## backend

Use a registered verified engine, or explicitly opt in to unverified read-only operations.

[Recovery guide](../guides/backend-compatibility.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.backend.capability_unavailable` | Stable | <code>Backend {backend!r} cannot provide {operation!r}: {status}. {route}</code> | `backend`, `operation`, `route`, `status` | [terrine/backends/capabilities.py](../../terrine/backends/capabilities.py) |
| `terrine.backend.capability_unavailable` | Stable | <code>The detected database server is not verified for PostgreSQL execution. {route}</code> | `route` | [terrine/backends/engines/postgres.py](../../terrine/backends/engines/postgres.py) |
| `terrine.backend.nonfinite_decimal` | Stable | <code>Exact decimal column {column!r} contains {value}, which has no exact decimal value. Filter or replace non-finite numeric values in the relation SQL.</code> | `column`, `value` | [terrine/execution/_postgres.py](../../terrine/execution/_postgres.py) |

## budget

Reduce output or obtain a separately reviewed owner budget.

[Recovery guide](../guides/cost-and-residency.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.budget.invalid_limit` | Stable | <code>Budget {field} is invalid: {reason}; limits are nonnegative integers or None.</code> | `field`, `reason` | [terrine/data.py](../../terrine/data.py) |
| `terrine.budget.output_bytes_exceeded` | Stable | <code>Output exceeds the configured byte limit {limit}.</code> | `limit` | [terrine/data.py](../../terrine/data.py) |
| `terrine.budget.rows_exceeded` | Stable | <code>Output exceeds the configured row limit {limit}.</code> | `limit` | [terrine/compile/shapes/life_table.py](../../terrine/compile/shapes/life_table.py) |
| `terrine.budget.rows_exceeded` | Stable | <code>Output exceeds the configured row limit {limit}.</code> | `limit` | [terrine/data.py](../../terrine/data.py) |
| `terrine.budget.scan_attempts_exhausted` | Stable | <code>Task scan allowance exhausted after {limit} attempts; create a newly budgeted run before reading sources again.</code> | `limit` | [terrine/runtime/cost.py](../../terrine/runtime/cost.py) |
| `terrine.budget.scan_authority_unavailable` | Stable | <code>Strict scan execution requires owner authorization of the saved all-phase bounds or caps; configure a capable owner executor or explicitly choose non-strict scan policy.</code> | None | [terrine/runtime/cost.py](../../terrine/runtime/cost.py) |
| `terrine.budget.scan_exceeded` | Stable | <code>The verified scan bound exceeds the configured limit {limit}.</code> | `limit` | [terrine/compile/cost.py](../../terrine/compile/cost.py) |
| `terrine.budget.scan_unknown` | Stable | <code>A strict scan-byte budget requires a verified bound or enforceable cap for every source-read phase. Configure a capable owner or explicitly choose non-strict policy after reviewing capture, validation, execution and retries.</code> | None | [terrine/compile/cost.py](../../terrine/compile/cost.py) |
| `terrine.budget.scan_unknown` | Stable | <code>A strict scan-byte budget requires a verified scan bound; this backend has none. Explicitly choose non-strict scan policy after reviewing upstream work.</code> | None | [terrine/compile/materialize.py](../../terrine/compile/materialize.py) |
| `terrine.budget.scan_unknown` | Stable | <code>A strict scan-byte budget requires a verified scan bound; this backend has none. Explicitly choose non-strict scan policy after reviewing upstream work.</code> | None | [terrine/execution/_publish.py](../../terrine/execution/_publish.py) |
| `terrine.budget.scan_unknown` | Stable | <code>A strict scan-byte budget requires a verified scan bound; this backend has none. Explicitly choose non-strict scan policy after reviewing upstream work.</code> | None | [terrine/runtime/snapshot.py](../../terrine/runtime/snapshot.py) |

## catalog

Correct the named Catalog field or source declaration.

[Recovery guide](../guides/temporal-engine.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.catalog.duplicate_yaml_key` | Stable | <code>The model YAML repeats key {key!r}. Keep one declaration for each key.</code> | `key` | [terrine/semantic/io.py](../../terrine/semantic/io.py) |
| `terrine.catalog.invalid_definition` | Stable | <code>Model document input {field!r} is invalid ({reason}). Use a model YAML document with the supported schema version and declared names.</code> | `field`, `reason` | [terrine/semantic/io.py](../../terrine/semantic/io.py) |
| `terrine.catalog.invalid_definition` | Stable | <code>The model document is invalid. Correct the listed inputs:<br>{diagnostics}</code> | `diagnostics` | [terrine/semantic/io.py](../../terrine/semantic/io.py) |
| `terrine.catalog.invalid_yaml` | Stable | <code>The model YAML cannot be read safely ({reason}). Correct the YAML and use plain values without custom tags.</code> | `reason` | [terrine/semantic/io.py](../../terrine/semantic/io.py) |

## cohort

Check grouping and observable cohort denominators.

[Recovery guide](../guides/trajectories-survival.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.cohort.invalid_operation` | Stable | <code>Cohort aggregation cannot be compiled ({reason}).</code> | `reason` | [terrine/compile/shapes/cohorts.py](../../terrine/compile/shapes/cohorts.py) |
| `terrine.cohort.invalid_operation` | Stable | <code>Cohort input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/frame.py](../../terrine/frame.py) |
| `terrine.cohort.invalid_operation` | Stable | <code>Cohort input {field} must be one of {choices}.</code> | `choices`, `field` | [terrine/frame.py](../../terrine/frame.py) |

## compile

Correct the named Design or timestamp constraint.

[Recovery guide](../guides/design-validation.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.compile.anchor_integrity` | Stable | <code>Event {event!r} has tied timestamps, so its occurrence order is ambiguous. Set event_ids={{{source!r}: &quot;&lt;id column&gt;&quot;}} on Model.from_tables or Model.from_catalog.</code> | `event`, `source` | [terrine/compile/anchors.py](../../terrine/compile/anchors.py) |
| `terrine.compile.anchor_integrity` | Stable | <code>Unit and t0 rows failed check {check_key!r}. Correct missing or ambiguous unit keys, event IDs and event times before building the design.</code> | `check_key` | [terrine/compile/anchors.py](../../terrine/compile/anchors.py) |
| `terrine.compile.anchor_timestamp_precision` | Stable | <code>Time column {column!r} contains values finer than microseconds that cannot be represented exactly as UTC. Convert those values to exact microsecond times.</code> | `column` | [terrine/compile/anchors.py](../../terrine/compile/anchors.py) |
| `terrine.compile.anchor_utc_conversion_unsupported` | Stable | <code>Event times without a timezone cannot be converted exactly to UTC on {backend!r}. Convert the times to timezone-aware UTC before adding the table to the model.</code> | `backend` | [terrine/compile/anchors.py](../../terrine/compile/anchors.py) |
| `terrine.compile.contradictory_persistent_adoption_history` | Stable | <code>Treatment history changes after a unit has adopted, but adoption must be permanent. Correct the history or use a design that allows treatment changes.</code> | None | [terrine/compile/shapes/sequential.py](../../terrine/compile/shapes/sequential.py) |
| `terrine.compile.incompatible_unit_keys` | Stable | <code>Temporal lookup unit key types {left_type} and {right_type} are incompatible. {route}</code> | `left_type`, `right_type`, `route` | [terrine/backends/joins.py](../../terrine/backends/joins.py) |
| `terrine.compile.insufficient_matched_t0_controls` | Stable | <code>Matched-t0 stratum at {t0!r} has {controls} eligible controls but requires {required}.</code> | `controls`, `required`, `t0` | [terrine/compile/shapes/matched_t0.py](../../terrine/compile/shapes/matched_t0.py) |
| `terrine.compile.invalid_anchor` | Stable | <code>The design cannot choose t0 ({reason}). Use a fixed time, an event time or a supported schedule.</code> | `reason` | [terrine/compile/anchors.py](../../terrine/compile/anchors.py) |
| `terrine.compile.later_control_initiation` | Stable | <code>A control unit starts treatment during a follow-up that requires no treatment. Use a design whose control rule matches that treatment history.</code> | None | [terrine/compile/shapes/sequential.py](../../terrine/compile/shapes/sequential.py) |
| `terrine.compile.persisted_anchor_invalid` | Stable | <code>The saved unit and t0 rows do not match this design. Rebuild the design instead of reusing these saved rows.</code> | None | [terrine/compile/materialize.py](../../terrine/compile/materialize.py) |
| `terrine.compile.runtime_shape_unsupported` | Stable | <code>Design shape {shape!r} cannot reuse saved unit and t0 rows. Build the frame directly or rebuild the full dataset.</code> | `shape` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.compile.unsupported_calendar_comparison` | Stable | <code>Calendar comparison compilation is unsupported: {reason}.</code> | `reason` | [terrine/compile/shapes/calendar_comparison.py](../../terrine/compile/shapes/calendar_comparison.py) |

## convert

Install the conversion extra; retain Arrow metadata.

[Recovery guide](../guides/common-stack-interop.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.convert.missing_dependency` | Stable | <code>Conversion requires the optional {extra!r} dependency; install terrine[{extra}].</code> | `extra` | [terrine/data.py](../../terrine/data.py) |
| `terrine.convert.missing_dependency` | Stable | <code>Conversion requires the optional {extra!r} dependency; install terrine[{extra}].</code> | `extra` | [terrine/export/diff_diff.py](../../terrine/export/diff_diff.py) |
| `terrine.convert.missing_dependency` | Stable | <code>Conversion requires the optional {extra!r} dependency; install terrine[{extra}].</code> | `extra` | [terrine/export/lifelines.py](../../terrine/export/lifelines.py) |
| `terrine.convert.missing_dependency` | Stable | <code>In-memory tables run on the optional {extra!r} dependency; {route}, or bind the tables by name on a connection.</code> | `extra`, `route` | [terrine/model.py](../../terrine/model.py) |

## dataset

Use pinned manifests, valid definitions and owned destinations.

[Recovery guide](../guides/ml-staging.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.dataset.export_conflict` | Stable | <code>Export destination conflicts with an existing export or partial export.</code> | None | [terrine/dataset/export.py](../../terrine/dataset/export.py) |
| `terrine.dataset.invalid_batch_size` | Stable | <code>Arrow batch size must be a positive integer.</code> | None | [terrine/dataset/reference.py](../../terrine/dataset/reference.py) |
| `terrine.dataset.invalid_definition` | Stable | <code>Dataset definition input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/spec/dataset.py](../../terrine/spec/dataset.py) |
| `terrine.dataset.invalid_definition` | Stable | <code>Invalid dataset definition: {reason}</code> | `reason` | [terrine/spec/dataset.py](../../terrine/spec/dataset.py) |
| `terrine.dataset.invalid_destination` | Stable | <code>Dataset destination is invalid: {reason}.</code> | `reason` | [terrine/dataset/reference.py](../../terrine/dataset/reference.py) |
| `terrine.dataset.invalid_destination` | Stable | <code>Destination {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/runtime/saved_plan.py](../../terrine/runtime/saved_plan.py) |
| `terrine.dataset.invalid_destination` | Stable | <code>Runtime {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/orchestration/airflow.py](../../terrine/orchestration/airflow.py) |
| `terrine.dataset.invalid_destination` | Stable | <code>Runtime {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/orchestration/dagster.py](../../terrine/orchestration/dagster.py) |
| `terrine.dataset.invalid_destination` | Stable | <code>Runtime {field} is invalid: {reason}; use a simple SQL identifier.</code> | `field`, `reason` | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.dataset.invalid_destination` | Stable | <code>Runtime {field} is invalid: {reason}; use a simple SQL identifier.</code> | `field`, `reason` | [terrine/runtime/leases.py](../../terrine/runtime/leases.py) |
| `terrine.dataset.invalid_destination` | Stable | <code>Staging, refreshing and reopening a dataset need a file-backed DuckDB connection; an in-memory model materializes only. Bind the same tables with {route}.</code> | `route` | [terrine/dataset/reference.py](../../terrine/dataset/reference.py) |
| `terrine.dataset.invalid_export` | Stable | <code>Invalid Parquet export: {reason}.</code> | `reason` | [terrine/dataset/export.py](../../terrine/dataset/export.py) |
| `terrine.dataset.invalid_generation` | Stable | <code>Dataset manifest does not identify a verified committed runtime generation.</code> | None | [terrine/dataset/reference.py](../../terrine/dataset/reference.py) |
| `terrine.dataset.invalid_label_support` | Stable | <code>Selected label lacks completeness and reason metadata: {label}</code> | `label` | [terrine/frame.py](../../terrine/frame.py) |
| `terrine.dataset.invalid_manifest` | Stable | <code>Dataset manifest input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/dataset/manifest.py](../../terrine/dataset/manifest.py) |
| `terrine.dataset.invalid_manifest` | Stable | <code>Dataset manifest input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/dataset/reference.py](../../terrine/dataset/reference.py) |
| `terrine.dataset.invalid_manifest` | Stable | <code>Invalid dataset manifest: {reason}. Stage its dataset definition to create a new manifest.</code> | `reason` | [terrine/dataset/manifest.py](../../terrine/dataset/manifest.py) |
| `terrine.dataset.invalid_selection` | Stable | <code>Training {field} selection is invalid: {reason}.</code> | `field`, `reason` | [terrine/frame.py](../../terrine/frame.py) |
| `terrine.dataset.invalid_selection` | Stable | <code>Unknown training feature selection: {names}</code> | `names` | [terrine/frame.py](../../terrine/frame.py) |
| `terrine.dataset.invalid_selection` | Stable | <code>Unknown training label selection: {names}</code> | `names` | [terrine/frame.py](../../terrine/frame.py) |
| `terrine.dataset.invalid_split` | Stable | <code>Split input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/dataset/splits.py](../../terrine/dataset/splits.py) |
| `terrine.dataset.invalid_split` | Stable | <code>Split input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/frame.py](../../terrine/frame.py) |
| `terrine.dataset.invalid_view` | Stable | <code>Dataset view flags {conflicts!r} conflict ({reason}); choose one view.</code> | `conflicts`, `reason` | [terrine/data.py](../../terrine/data.py) |
| `terrine.dataset.invalid_view` | Stable | <code>Invalid dataset view: {reason}.</code> | `reason` | [terrine/errors.py](../../terrine/errors.py) |
| `terrine.dataset.stale_generation` | Stable | <code>Refresh requires the pinned generation to remain the current publication.</code> | None | [terrine/dataset/reference.py](../../terrine/dataset/reference.py) |
| `terrine.dataset.unsupported_platform` | Stable | <code>Parquet export is unavailable on this operating system; export on macOS or Linux.</code> | None | [terrine/dataset/export.py](../../terrine/dataset/export.py) |
| `terrine.dataset.unsupported_view` | Stable | <code>Unsupported dataset view: {reason}.</code> | `reason` | [terrine/spec/dataset.py](../../terrine/spec/dataset.py) |

## design

Repair temporal/history contracts; do not zero-fill unknowns.

[Recovery guide](../guides/design-validation.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.design.ambiguous_dimension` | Stable | <code>Label {name!r} belongs to more than one history. Give each history label its own name.</code> | `name` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.conflicting_treatment` | Stable | <code>Sequential treatment conflicts with Design.treatment.</code> | None | [terrine/spec/panels.py](../../terrine/spec/panels.py) |
| `terrine.design.gap_violation` | Stable | <code>The outcome {name!r} window starts at {start_offset!r}, before t0. Start the outcome window at or after {limit_offset!r} relative to t0.</code> | `limit_offset`, `name`, `start_offset` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.gap_violation` | Stable | <code>The {role} {name!r} cutoff {end_offset!r} is after its allowed time {limit_offset!r} relative to t0. Set before={limit_offset!r} or an earlier time.</code> | `end_offset`, `limit_offset`, `name`, `role` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.gap_violation` | Stable | <code>The {role} {name!r} uses retention from {start_offset!r} to {end_offset!r} relative to t0. Put retention in outcomes rather than features or eligibility.</code> | `end_offset`, `name`, `role`, `start_offset` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.gap_violation` | Stable | <code>The {role} {name!r} window ends at {end_offset!r}, after its allowed end {limit_offset!r} relative to t0. End the window at or before {limit_offset!r} relative to t0.</code> | `end_offset`, `limit_offset`, `name`, `role` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.incompatible_unit` | Stable | <code>Unit {unit!r} is not declared in the model. Use a declared unit or add it to Model.from_tables(units=...).</code> | `unit` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.incompatible_unit` | Stable | <code>{name!r} does not provide values for unit {unit!r}. Use a unit shared by the design and the requested event, label, history or metric.</code> | `name`, `unit` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.incomplete_treatment_history` | Stable | <code>Event history {source!r} does not cover all treatment time required by the design, starting at {boundary!r}. Supply complete history for the required time, or narrow the schedule and follow-up.</code> | `boundary`, `source` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.incomplete_unit_treatment_history` | Stable | <code>The time observed for some units does not cover the required calendar treatment history. Use a schedule within the observed time, or supply the missing unit history.</code> | None | [terrine/compile/shapes/calendar_comparison.py](../../terrine/compile/shapes/calendar_comparison.py) |
| `terrine.design.incomplete_unit_treatment_history` | Stable | <code>The time observed for some units does not cover the scheduled treatment history. Use a schedule within the observed time, or supply the missing unit history.</code> | None | [terrine/compile/shapes/sequential.py](../../terrine/compile/shapes/sequential.py) |
| `terrine.design.invalid` | Stable | <code>Design document is invalid: {reason}.</code> | `reason` | [terrine/spec/io.py](../../terrine/spec/io.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/recipes.py](../../terrine/recipes.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/spec/anchors.py](../../terrine/spec/anchors.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/spec/design.py](../../terrine/spec/design.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/spec/expressions.py](../../terrine/spec/expressions.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/spec/io.py](../../terrine/spec/io.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/spec/panels.py](../../terrine/spec/panels.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/spec/trajectory.py](../../terrine/spec/trajectory.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/windows.py](../../terrine/windows.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} must be one of {choices}.</code> | `choices`, `field` | [terrine/recipes.py](../../terrine/recipes.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} must be one of {choices}.</code> | `choices`, `field` | [terrine/spec/anchors.py](../../terrine/spec/anchors.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} must be one of {choices}.</code> | `choices`, `field` | [terrine/spec/expressions.py](../../terrine/spec/expressions.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} must be one of {choices}.</code> | `choices`, `field` | [terrine/spec/panels.py](../../terrine/spec/panels.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field} must be one of {choices}.</code> | `choices`, `field` | [terrine/spec/trajectory.py](../../terrine/spec/trajectory.py) |
| `terrine.design.invalid` | Stable | <code>Design input {field}[{name!r}] is invalid ({reason}, got {got}; expected {expected}).</code> | `expected`, `field`, `got`, `name`, `reason` | [terrine/spec/design.py](../../terrine/spec/design.py) |
| `terrine.design.invalid` | Stable | <code>Elapsed event selectors cannot be used here ({reason}). {route}</code> | `reason`, `route` | [terrine/spec/expressions.py](../../terrine/spec/expressions.py) |
| `terrine.design.invalid` | Stable | <code>End day {end!r} is before start day {start!r}. The end is the last included day; choose an end on or after the start.</code> | `end`, `start` | [terrine/spec/anchors.py](../../terrine/spec/anchors.py) |
| `terrine.design.invalid` | Stable | <code>End day {end!r} leaves no following day to include the whole end day. Choose an earlier end day.</code> | `end` | [terrine/spec/anchors.py](../../terrine/spec/anchors.py) |
| `terrine.design.invalid` | Stable | <code>End time {end!r} is not after start time {start!r}. Choose a later end time; the end time itself is excluded.</code> | `end`, `start` | [terrine/spec/anchors.py](../../terrine/spec/anchors.py) |
| `terrine.design.invalid` | Stable | <code>The design cannot use this input ({reason}). Check its unit, event, label, metric and schedule declarations against the model.</code> | `reason` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.invalid_calendar_comparison` | Stable | <code>Calendar comparison specification is invalid: {reason}.</code> | `reason` | [terrine/spec/panels.py](../../terrine/spec/panels.py) |
| `terrine.design.invalid_calendar_comparison` | Stable | <code>Calendar comparisons need equal elapsed periods, but every={every!r} uses calendar months. Use every={route!r} for fixed-width periods.</code> | `every`, `route` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.invalid_matched_t0_sample` | Stable | <code>Matched-t0 sample policy is invalid: {reason}.</code> | `reason` | [terrine/spec/panels.py](../../terrine/spec/panels.py) |
| `terrine.design.invalid_treatment_history` | Stable | <code>Treatment history input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/spec/panels.py](../../terrine/spec/panels.py) |
| `terrine.design.invalid_trial_weight` | Stable | <code>Trial weights do not name every scheduled trial exactly once. Provide one weight for each trial in the schedule.</code> | None | [terrine/compile/shapes/sequential.py](../../terrine/compile/shapes/sequential.py) |
| `terrine.design.invalid_window` | Stable | <code>Metric {name!r} for output {output!r} has window start {start!r} and end {end!r} seconds relative to t0. Set window=(earlier_time, later_time).</code> | `end`, `name`, `output`, `start` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.metric_window_conflict` | Stable | <code>Metric {metric!r} requires a different window length from the design. Match the design window to the metric&#x27;s declared window_days.</code> | `metric` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.missing_reference` | Stable | <code>{kind} {name!r} is not declared in the model; available names are {available!r}. Did you mean {suggestions!r}?</code> | `available`, `kind`, `name`, `suggestions` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.missing_reference` | Stable | <code>{kind} {name!r} is not declared in the model; available names are {available!r}. Use one of these names or add the missing {kind} to the model.</code> | `available`, `kind`, `name` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |

## diff_diff

Use a supported persistent-adoption panel.

[Recovery guide](../guides/library-handoffs.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.diff_diff.invalid_panel` | Provisional | <code>Cannot encode this adoption panel ({reason}); {route}.</code> | `reason`, `route` | [terrine/export/diff_diff.py](../../terrine/export/diff_diff.py) |

## execution

Use owned native sessions or retained inputs.

[Recovery guide](../guides/backend-compatibility.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.execution.arrow_timestamp_range` | Provisional | <code>A nanosecond timestamp output lies outside Arrow&#x27;s range (1677-09-21 to 2262-04-11): {detail}. Declare the column with microsecond precision, such as TIMESTAMP_TZ(6), or cast it in a SQL relation.</code> | `detail` | [terrine/execution/_snowflake.py](../../terrine/execution/_snowflake.py) |
| `terrine.execution.invalid_input_binding` | Provisional | <code>The event and history tables cannot be verified for this model ({reason}). Reopen the model on the declared tables and a supported connection.</code> | `reason` | [terrine/execution/inputs.py](../../terrine/execution/inputs.py) |
| `terrine.execution.snapshot_unavailable` | Provisional | <code>A coherent native execution snapshot is unavailable: {reason}.</code> | `reason` | [terrine/backends/capabilities.py](../../terrine/backends/capabilities.py) |

## export

Satisfy shared preflight and the library shape contract.

[Recovery guide](../guides/library-handoffs.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.export.cluster_required` | Provisional | <code>Repeated rows per independent unit require a valid cluster column; declare the unit cluster.</code> | None | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.grace_baseline_treatment` | Provisional | <code>Grace-defined treatment cannot be exported as a baseline treatment; use a baseline design.</code> | None | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.incomplete_outcome` | Provisional | <code>Outcome windows are incomplete; provide complete data or explicitly drop incomplete rows.</code> | None | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.invalid_input` | Provisional | <code>Export input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.invalid_input` | Provisional | <code>Export input {field} must be one of {choices}.</code> | `choices`, `field` | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.invalid_surrogate` | Provisional | <code>Surrogate export input is invalid ({reason}).</code> | `reason` | [terrine/export/surrogate.py](../../terrine/export/surrogate.py) |
| `terrine.export.repeated_units_unsupported` | Provisional | <code>{consumer} cannot handle repeated units; use one row per unit or a cluster-aware library.</code> | `consumer` | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.snapshot_required` | Provisional | <code>Warehouse export needs its own DuckDB snapshot transaction; use a DuckDB relation outside an existing transaction and fit inside the context.</code> | None | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.unknown_eligibility` | Provisional | <code>Eligibility is unknown for some candidate rows; resolve eligibility before export.</code> | None | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.unsupported_weights` | Provisional | <code>{consumer} does not apply declared weights; use a downstream method supporting those weights.</code> | `consumer` | [terrine/export/preflight.py](../../terrine/export/preflight.py) |

## filter

Use typed, known filters and complete dimension histories.

[Recovery guide](../guides/temporal-engine.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.filter.unknown` | Stable | <code>Metric filters are unknown for {failed_count} requested rows. Provide the missing label history and resolve null or non-finite label values and filter values before exporting.</code> | `failed_count` | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |

## increment

Use the supported extra or a native Catalog.

[Recovery guide](../guides/increment-compatibility.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.increment.ambiguous_property` | Provisional | <code>Increment label {property!r} is ambiguous in event table {source!r}. Qualify the label with its event table name.</code> | `property`, `source` | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |
| `terrine.increment.duplicate_yaml_key` | Provisional | <code>The increment YAML at {path!r} repeats key {key!r}. Keep one declaration for each key.</code> | `key`, `path` | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |
| `terrine.increment.invalid_definitions` | Provisional | <code>Increment rejected the model definitions with code {upstream_code!r}. Correct the definitions according to that code before loading the model.</code> | `upstream_code` | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |
| `terrine.increment.missing_extra` | Provisional | <code>The increment compatibility extra is required; install terrine[increment].</code> | None | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |
| `terrine.increment.property_name_collision` | Provisional | <code>Increment label {name!r} conflicts with another name in the model. Give each label its own name before loading the model.</code> | `name` | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |
| `terrine.increment.unsupported_definition` | Provisional | <code>The model cannot represent increment declaration {declaration!r}. Declare supported units, events, labels and metrics directly in the model.</code> | `declaration` | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |

## lifelines

Use per-unit records to retain delayed-entry history.

[Recovery guide](../guides/library-handoffs.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.lifelines.delayed_entry` | Provisional | <code>Risk counts cannot recover delayed entry times; use {route}.</code> | `route` | [terrine/export/lifelines.py](../../terrine/export/lifelines.py) |
| `terrine.lifelines.invalid_input` | Provisional | <code>Invalid lifelines input ({reason}); supply complete, finite time-to-event records.</code> | `reason` | [terrine/export/lifelines.py](../../terrine/export/lifelines.py) |
| `terrine.lifelines.unsupported_shape` | Provisional | <code>This survival input cannot support the requested conversion ({reason}); use {route}.</code> | `reason`, `route` | [terrine/export/lifelines.py](../../terrine/export/lifelines.py) |

## metadata

Use consistent coverage, roles and representable metadata.

[Recovery guide](../guides/temporal-engine.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.metadata.invalid` | Stable | <code>Materialized frame metadata is invalid ({reason}); use a lossless Terrine Arrow or JSON representation.</code> | `reason` | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.invalid_eligibility_summary` | Stable | <code>Eligibility audit did not return one aggregate count record</code> | None | [terrine/execution/gates.py](../../terrine/execution/gates.py) |
| `terrine.metadata.invalid_eligibility_summary` | Stable | <code>Eligibility counts must be nonnegative and reconcile</code> | None | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.invalid_engine_stamp` | Stable | <code>Engine stamp is {reason} (present fields {fields!r}); re-materialize the Design with the current Terrine version.</code> | `fields`, `reason` | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.missing_columns` | Stable | <code>Arrow table is missing required columns {columns!r}; use lossless Arrow with all observation columns.</code> | `columns` | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.unrepresentable_coverage` | Stable | <code>Complete days {start!r} through {end!r} cannot be represented in the model&#x27;s local time. Choose days farther from the calendar limits.</code> | `end`, `start` | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.unrepresentable_coverage` | Stable | <code>Complete end day {end!r} leaves no following day to include the whole end day. Choose an earlier last complete day.</code> | `end` | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.unrepresentable_coverage` | Stable | <code>Complete times {start!r} through {end_exclusive!r} cannot be represented in UTC. Choose times farther from the calendar limits.</code> | `end_exclusive`, `start` | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.unsupported_version` | Stable | <code>Arrow metadata version {version!r} is unsupported; re-materialize the Design with the current Terrine version.</code> | `version` | [terrine/data.py](../../terrine/data.py) |

## metric

Use a supported aggregation, measure, precision and shape.

[Recovery guide](../guides/temporal-engine.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.metric.approx_backend_unavailable` | Stable | <code>Approximate distinct is unavailable for backend {dialect!r}.</code> | `dialect` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.approx_distinct_requires_single_measure` | Stable | <code>Metric {metric!r} combines values that cannot define one distinct count. Use a metric over a single event value instead of a ratio.</code> | `metric` | [terrine/compile/columns.py](../../terrine/compile/columns.py) |
| `terrine.metric.approx_eligibility_unavailable` | Stable | <code>Approximate quantiles cannot determine predicate values; use exact quantile or output the approximate value without a predicate.</code> | None | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.approx_shape_unavailable` | Stable | <code>Approximate quantile supports cross_section only; use exact quantile or cross_section.</code> | None | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.exact_precision_unavailable` | Stable | <code>Exact {operation} requires output precision unavailable for source type {value_type!r}; use an exactly representable type/probability or a wider supported numeric backend.</code> | `operation`, `value_type` | [terrine/temporal/nonadditive.py](../../terrine/temporal/nonadditive.py) |
| `terrine.metric.exact_quantile_backend_unsupported` | Stable | <code>Exact type-7 quantiles are unavailable for backend {dialect!r}; use a verified supported backend.</code> | `dialect` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.nonadditive_kind_unsupported` | Stable | <code>Metric aggregation {aggregation!r} cannot be used with metric kind {kind!r}. Use a metric over a single event value instead of a ratio.</code> | `aggregation`, `kind` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.nonadditive_type_unsupported` | Stable | <code>Exact {operation} cannot use column {column!r} of type {value_type!r}. Use a value type supported for this exact operation.</code> | `column`, `operation`, `value_type` | [terrine/temporal/nonadditive.py](../../terrine/temporal/nonadditive.py) |
| `terrine.metric.nonadditive_type_unsupported` | Stable | <code>Exact {operation} cannot use event values of type {value_type!r}. Use a metric over integer or Decimal event values; floating-point values are not supported for exact quantiles.</code> | `operation`, `value_type` | [terrine/temporal/nonadditive.py](../../terrine/temporal/nonadditive.py) |
| `terrine.metric.nonadditive_type_unsupported` | Stable | <code>The {operation} metric has no event value column ({value_type!r}). Declare the metric over an event value column.</code> | `operation`, `value_type` | [terrine/temporal/nonadditive.py](../../terrine/temporal/nonadditive.py) |
| `terrine.metric.quantile_requires_single_measure` | Stable | <code>Metric {metric!r} combines values that cannot define one approximate quantile. Use a metric over a single event value instead of a ratio.</code> | `metric` | [terrine/compile/columns.py](../../terrine/compile/columns.py) |
| `terrine.metric.quantile_requires_single_measure` | Stable | <code>Metric {metric!r} combines values that cannot define one exact quantile. Use a metric over a single event value instead of a ratio.</code> | `metric` | [terrine/compile/columns.py](../../terrine/compile/columns.py) |
| `terrine.metric.quantile_requires_single_measure` | Stable | <code>Metric {metric!r} combines values that cannot define one exact quantile. Use a metric over a single event value instead of a ratio.</code> | `metric` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.retention_identity_unavailable` | Stable | <code>Retention event {event!r} has no occurrence identity; bind its column to an occurrence ID or declare Model.event_ids for source {source!r}.</code> | `event`, `source` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.sketch_unavailable` | Stable | <code>Sketch {algorithm!r} version {version!r} is unavailable or cannot meet its declared error bound; install terrine[sketches] or use the exact form.</code> | `algorithm`, `version` | [terrine/temporal/sketches.py](../../terrine/temporal/sketches.py) |
| `terrine.metric.sketch_unavailable` | Stable | <code>Sketch {algorithm!r} version {version!r} is unavailable; install terrine[sketches] or use the exact form.</code> | `algorithm`, `version` | [terrine/spec/expressions.py](../../terrine/spec/expressions.py) |
| `terrine.metric.unaligned_calendar_average` | Stable | <code>A per-day mean needs a window of whole local days with both window bounds at local midnight. Choose matching times, or use an event sum or per-event mean for other window bounds.</code> | None | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |
| `terrine.metric.unsupported_aggregation` | Stable | <code>Metric aggregation {aggregation!r} cannot be calculated over a time window. Use count, sum, a per-event mean or a per-day mean.</code> | `aggregation` | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |
| `terrine.metric.unsupported_temporal_kind` | Stable | <code>Metric kind {kind!r} cannot be calculated over a time window. Use a mean, count, sum or ratio metric over the window.</code> | `kind` | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |
| `terrine.metric.unsupported_transform` | Stable | <code>Metric transform {transform!r} cannot be applied over a time window. Use the metric without this transform.</code> | `transform` | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |

## model

Correct source, roster and coverage bindings; rebind the Model.

[Recovery guide](../guides/temporal-engine.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.model.ambiguous_unit` | Stable | <code>{owner} does not say which unit its rows belong to and the model declares {choices}; pass {parameter}= with one of them.</code> | `choices`, `owner`, `parameter` | [terrine/model.py](../../terrine/model.py) |
| `terrine.model.ambiguous_unit` | Stable | <code>{owner} does not say which unit its rows belong to and the model declares {choices}; pass {parameter}= with one of them.</code> | `choices`, `owner`, `parameter` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.model.invalid_binding` | Stable | <code>Column {column!r} of {source!r} has type {dtype}, which cannot be a label or history value; supported types are {supported}.</code> | `column`, `dtype`, `source`, `supported` | [terrine/model.py](../../terrine/model.py) |
| `terrine.model.invalid_binding` | Stable | <code>History {table!r} needs exactly one time axis: valid_from, optionally with valid_to, or changed_at alone.</code> | `table` | [terrine/data.py](../../terrine/data.py) |
| `terrine.model.invalid_binding` | Stable | <code>In-memory tables are registered under their mapping keys, which DuckDB matches without regard to ASCII case, and {name!r} is used by {owners}; give the unit or the event table its own name.</code> | `name`, `owners` | [terrine/model.py](../../terrine/model.py) |
| `terrine.model.invalid_binding` | Stable | <code>Metric argument {field!r} is unusable: name a column or metric, pass per=&#x27;event&#x27; or per=&#x27;day&#x27; and exact=True or exact=False, and give where= as a mapping keyed by label.</code> | `field` | [terrine/metrics.py](../../terrine/metrics.py) |
| `terrine.model.invalid_binding` | Stable | <code>Model input {field!r} is invalid ({reason}). Provide the declared complete time range for each event and history table.</code> | `field`, `reason` | [terrine/temporal/readiness.py](../../terrine/temporal/readiness.py) |
| `terrine.model.invalid_binding` | Stable | <code>Model input {field!r} is invalid ({reason}). Use a declared name, the expected input type and an ordered complete time range.</code> | `field`, `reason` | [terrine/data.py](../../terrine/data.py) |
| `terrine.model.invalid_binding` | Stable | <code>The model cannot use this input ({reason}). Check the unit, event and history declarations passed to the model.</code> | `reason` | [terrine/model.py](../../terrine/model.py) |
| `terrine.model.invalid_binding` | Stable | <code>where= key {key!r} is not a label name or &lt;label&gt;__&lt;op&gt; with a usable value; the operators are ne, in, not_in, gt, gte, lt, lte and between, comparisons take one scalar, in and not_in a non-empty list, and between a (low, high) pair.</code> | `key` | [terrine/metrics.py](../../terrine/metrics.py) |
| `terrine.model.invalid_binding` | Stable | <code>{field} must be a table name or an in-memory pyarrow, pandas or polars table, not {type}.</code> | `field`, `type` | [terrine/data.py](../../terrine/data.py) |
| `terrine.model.invalid_binding` | Stable | <code>{inputs} are in-memory tables but a connection was given; Terrine never uploads data, so load them into the warehouse yourself or omit con to build on a private in-process DuckDB.</code> | `inputs` | [terrine/model.py](../../terrine/model.py) |
| `terrine.model.invalid_binding` | Stable | <code>{kind} name {name!r} contains the reserved &#x27;__&#x27; separator; rename the {kind}.</code> | `kind`, `name` | [terrine/data.py](../../terrine/data.py) |
| `terrine.model.invalid_binding` | Stable | <code>{kind} name {name!r} is declared by {owners}; names are shared across a model and must be unique.</code> | `kind`, `name`, `owners` | [terrine/model.py](../../terrine/model.py) |
| `terrine.model.invalid_binding` | Stable | <code>{tables} are table names but no connection was given; pass the connection that holds them, or pass the tables themselves.</code> | `tables` | [terrine/model.py](../../terrine/model.py) |
| `terrine.model.missing_event_completeness` | Stable | <code>{source!r} has no complete=(first_day, last_day). Declare the inclusive calendar days that its event or history table is fully loaded for, not the first and last event times you happen to have.</code> | `source` | [terrine/model.py](../../terrine/model.py) |

## readiness

Declare authoritative completeness for selected sources.

[Recovery guide](../guides/temporal-engine.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.readiness.missing_source` | Stable | <code>Event or history table {source!r} has no declared complete time range. Pass complete=(first_day, last_day) with the inclusive days fully loaded for that table.</code> | `source` | [terrine/temporal/readiness.py](../../terrine/temporal/readiness.py) |

## runtime

Inspect authoritative run state. Replay unchanged requests; replan changed inputs.

[Recovery guide](../guides/materialization-runtime.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.runtime.concurrency_limit` | Provisional | <code>Owner concurrent run limit has been reached.</code> | None | [terrine/runtime/execute.py](../../terrine/runtime/execute.py) |
| `terrine.runtime.control_message_too_large` | Provisional | <code>Owner control message exceeds the configured limit.</code> | None | [terrine/runtime/control.py](../../terrine/runtime/control.py) |
| `terrine.runtime.control_unavailable` | Provisional | <code>Owner control request was refused.</code> | None | [terrine/runtime/control.py](../../terrine/runtime/control.py) |
| `terrine.runtime.cost_estimate_mismatch` | Provisional | <code>Cost evidence does not match the task, definition or snapshot; replan with the owner&#x27;s metadata provider.</code> | None | [terrine/runtime/cost.py](../../terrine/runtime/cost.py) |
| `terrine.runtime.design_partition_unsupported` | Provisional | <code>The built-in Design executor supports one output partition; use a single-partition plan or a custom executor with per-partition bounds.</code> | None | [terrine/runtime/api.py](../../terrine/runtime/api.py) |
| `terrine.runtime.execution_failed` | Provisional | <code>Runtime execution failed outside a coded Terrine refusal.</code> | None | [terrine/runtime/execute.py](../../terrine/runtime/execute.py) |
| `terrine.runtime.execution_failed` | Provisional | <code>Terrine runtime run {run_id} finished as {state}.</code> | `run_id`, `state` | [terrine/orchestration/airflow.py](../../terrine/orchestration/airflow.py) |
| `terrine.runtime.execution_failed` | Provisional | <code>Terrine runtime run {run_id} finished as {state}.</code> | `run_id`, `state` | [terrine/orchestration/dagster.py](../../terrine/orchestration/dagster.py) |
| `terrine.runtime.executor_unavailable` | Provisional | <code>No owner execution callbacks are configured for this runtime.</code> | None | [terrine/runtime/api.py](../../terrine/runtime/api.py) |
| `terrine.runtime.invalid_configuration` | Provisional | <code>Owner runtime configuration {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/adapters/opentelemetry.py](../../terrine/adapters/opentelemetry.py) |
| `terrine.runtime.invalid_configuration` | Provisional | <code>Owner runtime configuration {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/orchestration/airflow.py](../../terrine/orchestration/airflow.py) |
| `terrine.runtime.invalid_configuration` | Provisional | <code>Owner runtime configuration {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/orchestration/dagster.py](../../terrine/orchestration/dagster.py) |
| `terrine.runtime.invalid_configuration` | Provisional | <code>Owner runtime configuration {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/runtime/control.py](../../terrine/runtime/control.py) |
| `terrine.runtime.invalid_configuration` | Provisional | <code>Owner runtime configuration {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/runtime/snapshot.py](../../terrine/runtime/snapshot.py) |
| `terrine.runtime.lazy_duration_unsupported` | Provisional | <code>DuckDB lazy reads cannot represent duration unit {unit!r} in column {column!r}; use {route}.</code> | `column`, `route`, `unit` | [terrine/runtime/backends/duckdb.py](../../terrine/runtime/backends/duckdb.py) |
| `terrine.runtime.owner_interrupted` | Provisional | <code>The runtime owner stopped before completing or publishing this run.</code> | None | [terrine/runtime/status.py](../../terrine/runtime/status.py) |
| `terrine.runtime.plan_invocation_conflict` | Provisional | <code>Invocation ID is already bound to a different planning request.</code> | None | [terrine/runtime/planning.py](../../terrine/runtime/planning.py) |
| `terrine.runtime.plan_request_invalid` | Provisional | <code>Owner plan request input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/dataset/reference.py](../../terrine/dataset/reference.py) |
| `terrine.runtime.plan_request_invalid` | Provisional | <code>Owner plan request input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/orchestration/airflow.py](../../terrine/orchestration/airflow.py) |
| `terrine.runtime.plan_request_invalid` | Provisional | <code>Owner plan request input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/orchestration/dagster.py](../../terrine/orchestration/dagster.py) |
| `terrine.runtime.plan_request_invalid` | Provisional | <code>Owner plan request input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/runtime/api.py](../../terrine/runtime/api.py) |
| `terrine.runtime.plan_request_invalid` | Provisional | <code>Owner plan request is invalid ({reason}).</code> | `reason` | [terrine/runtime/planning.py](../../terrine/runtime/planning.py) |
| `terrine.runtime.request_key_conflict` | Provisional | <code>Request key is already bound to a different plan or destination.</code> | None | [terrine/runtime/execute.py](../../terrine/runtime/execute.py) |
| `terrine.runtime.request_key_conflict` | Provisional | <code>Request key is already bound to a different plan.</code> | None | [terrine/runtime/api.py](../../terrine/runtime/api.py) |
| `terrine.runtime.run_unavailable` | Provisional | <code>Run status is unavailable for the requested run.</code> | None | [terrine/runtime/status.py](../../terrine/runtime/status.py) |
| `terrine.runtime.saved_plan_mismatch` | Provisional | <code>Execution identity failed verification ({reason}); rebuild the plan and refresh.</code> | `reason` | [terrine/runtime/backends/base.py](../../terrine/runtime/backends/base.py) |
| `terrine.runtime.saved_plan_mismatch` | Provisional | <code>Saved plan input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/runtime/saved_plan.py](../../terrine/runtime/saved_plan.py) |
| `terrine.runtime.saved_plan_mismatch` | Provisional | <code>Saved plan reconstruction failed verification (retained snapshot Model declaration differs).</code> | None | [terrine/runtime/api.py](../../terrine/runtime/api.py) |
| `terrine.runtime.saved_plan_mismatch` | Provisional | <code>Saved plan reconstruction failed verification ({reason}).</code> | `reason` | [terrine/runtime/planner.py](../../terrine/runtime/planner.py) |
| `terrine.runtime.saved_plan_mismatch` | Provisional | <code>Saved plan reconstruction failed verification ({reason}).</code> | `reason` | [terrine/runtime/saved_plan.py](../../terrine/runtime/saved_plan.py) |
| `terrine.runtime.schema_unsupported` | Provisional | <code>Runtime namespace {namespace!r} holds {reason} runtime metadata that this release (schema version {supported_version}) cannot read; stage into a new namespace or owner database, or open it with the release that wrote it.</code> | `namespace`, `reason`, `supported_version` | [terrine/runtime/schema.py](../../terrine/runtime/schema.py) |
| `terrine.runtime.sketch_sidecar_unavailable` | Provisional | <code>Approximate quantile sidecars are unavailable in runtime planning and composition; use ordinary Frame materialization.</code> | None | [terrine/runtime/materialize.py](../../terrine/runtime/materialize.py) |
| `terrine.runtime.snapshot_invalid` | Provisional | <code>Runtime snapshot receipt is invalid: {reason}.</code> | `reason` | [terrine/runtime/snapshot.py](../../terrine/runtime/snapshot.py) |
| `terrine.runtime.unsupported_platform` | Provisional | <code>Owner control uses a local Unix socket; run the owner on macOS or Linux.</code> | None | [terrine/runtime/control.py](../../terrine/runtime/control.py) |

## semantic

Correct native semantics or use an admitted staged table.

[Recovery guide](../guides/temporal-engine.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.semantic.ambiguous_column` | Stable | <code>Event table column {column!r} conflicts with a name Terrine needs in its output. Rename the column before adding the table to the model.</code> | `column` | [terrine/semantic/bind.py](../../terrine/semantic/bind.py) |
| `terrine.semantic.decimal_scale_required` | Stable | <code>Decimal column {column!r} has no fixed precision and scale up to 38 digits (declared {declared}). Cast it to an explicit numeric(p, s) with p &lt;= 38 in the relation SQL.</code> | `column`, `declared` | [terrine/semantic/bind.py](../../terrine/semantic/bind.py) |
| `terrine.semantic.dialect_mismatch` | Stable | <code>The model uses SQL dialect {declared!r}, but the connection uses {execution!r}. Set the model dialect to match the connection and use SQL for that dialect.</code> | `declared`, `execution` | [terrine/semantic/sql.py](../../terrine/semantic/sql.py) |
| `terrine.semantic.missing_reference` | Stable | <code>The model does not declare {reference!r}. Add the missing declaration or use a name already declared in the model.</code> | `reference` | [terrine/semantic/bind.py](../../terrine/semantic/bind.py) |
| `terrine.semantic.missing_reference` | Stable | <code>{owner!r} uses {kind} {name!r}, which is not declared; available names are {available!r}. Did you mean {suggestions!r}?</code> | `available`, `kind`, `name`, `owner`, `suggestions` | [terrine/model.py](../../terrine/model.py) |
| `terrine.semantic.missing_reference` | Stable | <code>{owner!r} uses {kind} {name!r}, which is not declared; available names are {available!r}. Use one of these names or add the missing {kind} to the model.</code> | `available`, `kind`, `name`, `owner` | [terrine/model.py](../../terrine/model.py) |
| `terrine.semantic.schema_mismatch` | Stable | <code>Filter value {value!r} for label {label!r} has the wrong type for {label_type}; use a value matching the declared label type.</code> | `label`, `label_type`, `value` | [terrine/semantic/bind.py](../../terrine/semantic/bind.py) |
| `terrine.semantic.schema_mismatch` | Stable | <code>The {field!r} column {column!r} for unit {unit!r} has type {source_type}, but the unit roster key has type {unit_type}. Use matching key types in the source and roster.</code> | `column`, `field`, `source_type`, `unit`, `unit_type` | [terrine/semantic/bind.py](../../terrine/semantic/bind.py) |
| `terrine.semantic.schema_mismatch` | Stable | <code>The {field!r} column {column!r} is missing or has the wrong type. Choose an existing column with the required type, or correct the table before adding it to the model.</code> | `column`, `field` | [terrine/semantic/bind.py](../../terrine/semantic/bind.py) |

## sequence

Declare stable event IDs and bounded sequence sizes.

[Recovery guide](../guides/temporal-engine.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.sequence.budget_required` | Stable | <code>Event-sequence materialization requires positive max_rows and max_output_bytes budgets.</code> | None | [terrine/execution/gates.py](../../terrine/execution/gates.py) |
| `terrine.sequence.incomplete_time_bounds` | Stable | <code>Event sequence completeness bounds are invalid.</code> | None | [terrine/temporal/sequence.py](../../terrine/temporal/sequence.py) |
| `terrine.sequence.invalid_spec` | Stable | <code>Invalid event sequence design: {reason}.</code> | `reason` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.sequence.invalid_spec` | Stable | <code>Invalid event sequence request: {reason}.</code> | `reason` | [terrine/spec/sequence.py](../../terrine/spec/sequence.py) |
| `terrine.sequence.max_events_exceeded` | Stable | <code>Sequence contains more than the configured max_events value {limit}.</code> | `limit` | [terrine/temporal/sequence.py](../../terrine/temporal/sequence.py) |
| `terrine.sequence.reanchor_leak` | Stable | <code>The {role} {name!r} event-relative window can end at {end_offset!r}, after its allowed time {limit_offset!r} relative to t0. Keep the selected event and the whole window at or before that time.</code> | `end_offset`, `limit_offset`, `name`, `role` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.sequence.unsupported_shape` | Stable | <code>The design cannot calculate this event-time request. Use a declared event and label, and a supported occurrence and time window.</code> | None | [terrine/spec/sequence.py](../../terrine/spec/sequence.py) |

## simulate

Use representable, consistent raw-source configurations.

[Recovery guide](../guides/synthetic-worlds.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.simulate.invalid_config` | Provisional | <code>Simulation input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/simulate/scale.py](../../terrine/simulate/scale.py) |
| `terrine.simulate.invalid_seed` | Provisional | <code>Use an integer seed in [0, 2**64).</code> | None | [terrine/simulate/generate.py](../../terrine/simulate/generate.py) |
| `terrine.simulate.invalid_seed` | Provisional | <code>Use an integer seed in [0, 2**64).</code> | None | [terrine/simulate/scale.py](../../terrine/simulate/scale.py) |
| `terrine.simulate.nonfinite` | Provisional | <code>Generated {quantity} is nonfinite; reduce coefficient or noise magnitudes.</code> | `quantity` | [terrine/simulate/generate.py](../../terrine/simulate/generate.py) |

## spec

Use typed constructors, supported fields and unique output names.

[Recovery guide](../guides/design-specs.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.spec.output_name_conflict` | Stable | <code>Design output name {name!r} conflicts with {other!r}. Give the feature or outcome a different name.</code> | `name`, `other` | [terrine/compile/columns.py](../../terrine/compile/columns.py) |
| `terrine.spec.reserved_output_name` | Stable | <code>Design output name {name!r} is reserved for observation details. Give the feature or outcome a different name.</code> | `name` | [terrine/spec/design.py](../../terrine/spec/design.py) |

## sql

Use one admitted read-only query or an owner-staged table.

[Recovery guide](../guides/temporal-engine.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.sql.invalid_read_query` | Stable | <code>SQL relation is not an admitted read-only query: {reason}.</code> | `reason` | [terrine/semantic/sql.py](../../terrine/semantic/sql.py) |

## state_panel

Use ordered, bounded observations and explicit missingness.

[Recovery guide](../guides/state-panels.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.state_panel.budget_required` | Stable | <code>State-panel materialization requires positive max_rows and max_output_bytes budgets.</code> | None | [terrine/execution/gates.py](../../terrine/execution/gates.py) |
| `terrine.state_panel.invalid_observations` | Stable | <code>State observations are invalid: {reason}.</code> | `reason` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |
| `terrine.state_panel.invalid_schedule` | Stable | <code>Schedule days {start!r} through {end!r}, inclusive, do not contain whole {every}-second periods. Adjust the end day or every= so all periods fit exactly.</code> | `end`, `every`, `start` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |
| `terrine.state_panel.invalid_schedule` | Stable | <code>State panels need equal elapsed periods, but every={every!r} uses calendar months. Use every={route!r} for fixed-width periods.</code> | `every`, `route` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |
| `terrine.state_panel.invalid_schedule` | Stable | <code>The schedule has {schedule_periods} periods, more than the {periods} allowed. Shorten the schedule or set periods= to include the full schedule.</code> | `periods`, `schedule_periods` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |
| `terrine.state_panel.invalid_schedule` | Stable | <code>The state panel needs a finite schedule ({reason}). Choose start and end days and an elapsed every= duration.</code> | `reason` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |
| `terrine.state_panel.invalid_state_source` | Stable | <code>State source input {field} is invalid: {reason}.</code> | `field`, `reason` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |
| `terrine.state_panel.invalid_state_source` | Stable | <code>State source is invalid: {reason}.</code> | `reason` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |
| `terrine.state_panel.unsupported_combination` | Stable | <code>State panel combination is unsupported: {reason}.</code> | `reason` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |

## study

Repair the named Design contract before collecting rows.

[Recovery guide](../guides/design-validation.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.study.eligibility_unknown` | Stable | <code>Eligibility is unresolved for {unknown} of {candidate} candidates; complete the required history before exporting rows.</code> | `candidate`, `unknown` | [terrine/execution/gates.py](../../terrine/execution/gates.py) |

## survival

Declare per-unit event/censor and observation bounds.

[Recovery guide](../guides/trajectories-survival.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.survival.invalid_compilation` | Stable | <code>Time-to-event compilation failed ({reason}).</code> | `reason` | [terrine/compile/shapes/time_to_event.py](../../terrine/compile/shapes/time_to_event.py) |

## temporal

Check exact endpoints, coverage and arithmetic validity.

[Recovery guide](../guides/temporal-engine.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.temporal.alias_collision` | Stable | <code>The table of unit and cutoff times already has columns {columns!r}, which are reserved for history lookup results. Rename or remove those columns before looking up history.</code> | `columns` | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.duplicate_event_id` | Stable | <code>Event identifiers must be non-null and unique within each unit.</code> | None | [terrine/temporal/first.py](../../terrine/temporal/first.py) |
| `terrine.temporal.duplicate_history` | Stable | <code>History repeats a start time for the same unit. Keep one history value per unit and start time.</code> | None | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.exact_overflow` | Stable | <code>The requested exact result cannot be represented by the native output type. Use a supported exact representation or a semantically sufficient window.</code> | None | [terrine/backends/numeric.py](../../terrine/backends/numeric.py) |
| `terrine.temporal.exact_unsupported` | Stable | <code>Backend {backend!r} lacks exact sparse BIGNUM state (minimum DuckDB {minimum}). Use DuckDB 1.5 or later.</code> | `backend`, `minimum` | [terrine/temporal/daily.py](../../terrine/temporal/daily.py) |
| `terrine.temporal.first_occurrence_invalid_relation` | Stable | <code>First-occurrence input relation is invalid ({reason}).</code> | `reason` | [terrine/temporal/first.py](../../terrine/temporal/first.py) |
| `terrine.temporal.first_occurrence_unsupported_backend` | Stable | <code>First-occurrence point lookup requires the DuckDB, PostgreSQL, BigQuery or Snowflake backend; got {backend!r}.</code> | `backend` | [terrine/temporal/first.py](../../terrine/temporal/first.py) |
| `terrine.temporal.float64_precision` | Stable | <code>Float64 result unavailable ({reason}). Use finite inputs with magnitude below 10**29 whose quantized result fits binary64, or declared integer/decimal values.</code> | `reason` | [terrine/backends/numeric.py](../../terrine/backends/numeric.py) |
| `terrine.temporal.malformed_history` | Stable | <code>History has missing unit keys or start times, or an end time at or before its start. Fill the missing keys and times, and make each end later than its start.</code> | None | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.missing_join_key` | Stable | <code>History {dimension!r} has no matching unit key on its event table. Use the same unit key for the history and its events.</code> | `dimension` | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.overlapping_history` | Stable | <code>History has overlapping time ranges for the same unit. Make the ranges non-overlapping so each unit has at most one value at any time.</code> | None | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.range_bound_unavailable` | Stable | <code>A bounded range strategy is unavailable for {expression}: {reason}. The candidate multiplier limit is {limit}; {route}.</code> | `expression`, `limit`, `reason`, `route` | [terrine/temporal/ranges.py](../../terrine/temporal/ranges.py) |
| `terrine.temporal.retention_invalid` | Stable | <code>The retention request cannot be represented ({reason}).</code> | `reason` | [terrine/temporal/retention.py](../../terrine/temporal/retention.py) |
| `terrine.temporal.static_key_multiplicity` | Stable | <code>A label declared static has more than one row for the same unit. Keep one row per unit, or declare time-varying values as history.</code> | None | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.timestamp_order_unsupported` | Stable | <code>Event times of type {dtype!r} cannot be ordered without losing precision. Convert the times losslessly to a supported microsecond or nanosecond time type before adding the table to the model.</code> | `dtype` | [terrine/backends/timestamps.py](../../terrine/backends/timestamps.py) |
| `terrine.temporal.unaligned_day_resolution` | Stable | <code>Per-day windows need both time bounds at local midnight. Choose matching t0 and window bounds, or use event-time windows for other times.</code> | None | [terrine/temporal/aggregate.py](../../terrine/temporal/aggregate.py) |
| `terrine.temporal.unsupported_event_resolution` | Stable | <code>Exact event-time windows are unavailable for {dialect!r}. Use local-midnight t0 and window bounds, or use DuckDB.</code> | `dialect` | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |

## trajectory

Use bounded periods and consistent observation.

[Recovery guide](../guides/trajectories-survival.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.trajectory.invalid_compilation` | Stable | <code>Trajectory compilation failed ({reason}).</code> | `reason` | [terrine/compile/shapes/ltv.py](../../terrine/compile/shapes/ltv.py) |

## transform

Fit on training rows; reuse unchanged frozen scoring limits.

[Recovery guide](../guides/ml-staging.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.transform.empty_training_population` | Stable | <code>Exact winsorization for {column!r} requires non-null training values. {route}</code> | `column`, `route` | [terrine/dataset/transforms.py](../../terrine/dataset/transforms.py) |
| `terrine.transform.invalid_winsorization` | Stable | <code>Invalid winsorization: {reason}. {route}</code> | `reason`, `route` | [terrine/spec/transforms.py](../../terrine/spec/transforms.py) |
| `terrine.transform.training_split_required` | Stable | <code>Fitting winsorization requires a declared training split. {route}</code> | `route` | [terrine/dataset/transforms.py](../../terrine/dataset/transforms.py) |
| `terrine.transform.winsorization_expr_unsupported` | Stable | <code>Post-aggregation winsorization requires guarded Arrow output. {route}</code> | `route` | [terrine/frame.py](../../terrine/frame.py) |
| `terrine.transform.winsorization_identity_mismatch` | Stable | <code>Frozen winsorization for {column!r} does not match this view. {route}</code> | `column`, `route` | [terrine/dataset/transforms.py](../../terrine/dataset/transforms.py) |
| `terrine.transform.winsorization_precision_unavailable` | Stable | <code>Winsorization output for {column!r} needs unavailable exact Decimal precision. {route}</code> | `column`, `route` | [terrine/dataset/transforms.py](../../terrine/dataset/transforms.py) |
| `terrine.transform.winsorization_type_unsupported` | Stable | <code>Exact winsorization does not support {value_type!r} for {column!r}. {route}</code> | `column`, `route`, `value_type` | [terrine/dataset/transforms.py](../../terrine/dataset/transforms.py) |

## window

Use ordered integral elapsed endpoints with s/min/h/d/w units.

[Recovery guide](../guides/design-specs.md)

| Code | Tier | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- | --- |
| `terrine.window.invalid_duration` | Stable | <code>{field}={value!r} is not a calendar step: use a whole number of months such as Months(3), or text such as &quot;3mo&quot; or &quot;1y&quot;.</code> | `field`, `value` | [terrine/windows.py](../../terrine/windows.py) |
| `terrine.window.invalid_duration` | Stable | <code>{field}={value!r} is not an elapsed duration: use a signed integer with a unit such as &quot;-30s&quot;, &quot;-30min&quot;, or &quot;4w&quot;; bare 0 is allowed. Calendar months (&quot;1mo&quot;, &quot;1y&quot;) are accepted only by a Schedule&#x27;s every.</code> | `field`, `value` | [terrine/windows.py](../../terrine/windows.py) |
| `terrine.window.invalid_window` | Stable | <code>Window start {start_offset!r} is not before end {end_offset!r}. Use window=(earlier_time, later_time); the start is included and the end is excluded.</code> | `end_offset`, `start_offset` | [terrine/windows.py](../../terrine/windows.py) |
