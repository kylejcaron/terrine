# Refusal codes

This reference is generated from every module-owned `RefusalSpec` declaration,
including inline declarations. Codes are stable; message wording is not an API.
Interpolation fields below are required by the message template, **not** an
exhaustive context schema. Inspect the immutable `TerrineError.context` for
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

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.agent.invalid_request` | <code>Use a valid typed Design payload.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.agent.model_mismatch` | <code>Owner model configuration has changed.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.agent.owner_configuration` | <code>Owner execution configuration is unavailable.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.agent.permission_denied` | <code>Owner policy denies this operation.</code> | None | [terrine/agent/policy.py](../../terrine/agent/policy.py) |
| `terrine.agent.receipt_unavailable` | <code>Use an unchanged owner-prepared receipt.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.agent.request_limit` | <code>Owner tool request limit has been reached.</code> | None | [terrine/agent/policy.py](../../terrine/agent/policy.py) |
| `terrine.agent.run_unavailable` | <code>Run has no matching owner-prepared invocation.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |
| `terrine.agent.scope_denied` | <code>Requested resources are outside owner policy.</code> | None | [terrine/agent/policy.py](../../terrine/agent/policy.py) |
| `terrine.agent.storage_denied` | <code>Named Design storage is unavailable.</code> | None | [terrine/agent/tools.py](../../terrine/agent/tools.py) |

## backend

Use verified local DuckDB operation capabilities.

[Recovery guide](../guides/backend-compatibility.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.backend.capability_unavailable` | <code>Backend {backend!r} cannot provide {operation!r}: {status}. {route}</code> | `backend`, `operation`, `route`, `status` | [terrine/backends/capabilities.py](../../terrine/backends/capabilities.py) |
| `terrine.backend.capability_unavailable` | <code>The detected database server is not verified for PostgreSQL execution. {route}</code> | `route` | [terrine/execution/_postgres.py](../../terrine/execution/_postgres.py) |
| `terrine.backend.nonfinite_decimal` | <code>Exact decimal column {column!r} contains {value}, which has no exact decimal value. Filter or replace non-finite numeric values in the relation SQL.</code> | `column`, `value` | [terrine/execution/_postgres.py](../../terrine/execution/_postgres.py) |

## budget

Reduce output or obtain a separately reviewed owner budget.

[Recovery guide](../guides/cost-and-residency.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.budget.output_bytes_exceeded` | <code>Output exceeds the configured byte limit {limit}.</code> | `limit` | [terrine/data.py](../../terrine/data.py) |
| `terrine.budget.rows_exceeded` | <code>Output exceeds the configured row limit {limit}.</code> | `limit` | [terrine/compile/shapes/life_table.py](../../terrine/compile/shapes/life_table.py) |
| `terrine.budget.rows_exceeded` | <code>Output exceeds the configured row limit {limit}.</code> | `limit` | [terrine/data.py](../../terrine/data.py) |
| `terrine.budget.scan_attempts_exhausted` | <code>Task scan allowance exhausted after {limit} attempts; create a newly budgeted run before reading sources again.</code> | `limit` | [terrine/runtime/cost.py](../../terrine/runtime/cost.py) |
| `terrine.budget.scan_authority_unavailable` | <code>Strict scan execution requires owner authorization of the saved all-phase bounds or caps; configure a capable owner executor or explicitly choose non-strict scan policy.</code> | None | [terrine/runtime/cost.py](../../terrine/runtime/cost.py) |
| `terrine.budget.scan_exceeded` | <code>The verified scan bound exceeds the configured limit {limit}.</code> | `limit` | [terrine/compile/cost.py](../../terrine/compile/cost.py) |
| `terrine.budget.scan_unknown` | <code>A strict scan-byte budget requires a verified bound or enforceable cap for every source-read phase. Configure a capable owner or explicitly choose non-strict policy after reviewing capture, validation, execution and retries.</code> | None | [terrine/compile/cost.py](../../terrine/compile/cost.py) |
| `terrine.budget.scan_unknown` | <code>A strict scan-byte budget requires a verified scan bound; this backend has none. Explicitly choose non-strict scan policy after reviewing upstream work.</code> | None | [terrine/compile/materialize.py](../../terrine/compile/materialize.py) |
| `terrine.budget.scan_unknown` | <code>A strict scan-byte budget requires a verified scan bound; this backend has none. Explicitly choose non-strict scan policy after reviewing upstream work.</code> | None | [terrine/execution/_publish.py](../../terrine/execution/_publish.py) |
| `terrine.budget.scan_unknown` | <code>A strict scan-byte budget requires a verified scan bound; this backend has none. Explicitly choose non-strict scan policy after reviewing upstream work.</code> | None | [terrine/runtime/snapshot.py](../../terrine/runtime/snapshot.py) |

## catalog

Correct the named Catalog field or source declaration.

[Recovery guide](../guides/temporal-engine.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.catalog.duplicate_yaml_key` | <code>Catalog YAML repeats key {key!r}.</code> | `key` | [terrine/semantic/io.py](../../terrine/semantic/io.py) |
| `terrine.catalog.invalid_definition` | <code>Catalog definition is invalid:<br>{diagnostics}</code> | `diagnostics` | [terrine/semantic/io.py](../../terrine/semantic/io.py) |
| `terrine.catalog.invalid_yaml` | <code>Catalog YAML is not safe valid YAML: {reason}.</code> | `reason` | [terrine/semantic/io.py](../../terrine/semantic/io.py) |

## cohort

Check grouping and observable cohort denominators.

[Recovery guide](../guides/trajectories-survival.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.cohort.invalid_operation` | <code>Cohort aggregation cannot be compiled ({reason}).</code> | `reason` | [terrine/compile/shapes/cohorts.py](../../terrine/compile/shapes/cohorts.py) |

## compile

Correct the named Design or timestamp constraint.

[Recovery guide](../guides/design-validation.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.compile.anchor_integrity` | <code>Candidate anchor integrity check failed ({check_key}).</code> | `check_key` | [terrine/compile/anchors.py](../../terrine/compile/anchors.py) |
| `terrine.compile.anchor_timestamp_precision` | <code>Anchor timestamp column {column!r} contains submicrosecond values that cannot be represented exactly as UTC; normalize those values to microseconds.</code> | `column` | [terrine/compile/anchors.py](../../terrine/compile/anchors.py) |
| `terrine.compile.anchor_utc_conversion_unsupported` | <code>Exact conversion of naive timestamps to UTC is unsupported for backend {backend!r}.</code> | `backend` | [terrine/compile/anchors.py](../../terrine/compile/anchors.py) |
| `terrine.compile.contradictory_persistent_adoption_history` | <code>Persistent adoption history contradicts absorbing state.</code> | None | [terrine/compile/shapes/sequential.py](../../terrine/compile/shapes/sequential.py) |
| `terrine.compile.insufficient_matched_t0_controls` | <code>Matched-t0 stratum at {t0!r} has {controls} eligible controls but requires {required}.</code> | `controls`, `required`, `t0` | [terrine/compile/shapes/matched_t0.py](../../terrine/compile/shapes/matched_t0.py) |
| `terrine.compile.invalid_anchor` | <code>Anchor compilation is invalid: {reason}.</code> | `reason` | [terrine/compile/anchors.py](../../terrine/compile/anchors.py) |
| `terrine.compile.later_control_initiation` | <code>Control initiates during the sustained no-initiation follow-up.</code> | None | [terrine/compile/shapes/sequential.py](../../terrine/compile/shapes/sequential.py) |
| `terrine.compile.persisted_anchor_invalid` | <code>Persisted anchor layer identity does not match compiler requirements.</code> | None | [terrine/compile/materialize.py](../../terrine/compile/materialize.py) |
| `terrine.compile.runtime_shape_unsupported` | <code>Persisted-layer runtime does not support shape {shape!r}; use ordinary Frame materialization or a full rebuild.</code> | `shape` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.compile.unsupported_calendar_comparison` | <code>Calendar comparison compilation is unsupported: {reason}.</code> | `reason` | [terrine/compile/shapes/calendar_comparison.py](../../terrine/compile/shapes/calendar_comparison.py) |

## convert

Install the conversion extra; retain Arrow metadata.

[Recovery guide](../guides/common-stack-interop.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.convert.missing_dependency` | <code>Conversion requires the optional {extra!r} dependency; install terrine[{extra}].</code> | `extra` | [terrine/export/diff_diff.py](../../terrine/export/diff_diff.py) |
| `terrine.convert.missing_dependency` | <code>Conversion requires the optional {extra!r} dependency; install terrine[{extra}].</code> | `extra` | [terrine/export/lifelines.py](../../terrine/export/lifelines.py) |
| `terrine.convert.missing_dependency` | <code>Conversion requires the optional {extra!r} dependency; install terrine[{extra}].</code> | `extra` | [terrine/frame.py](../../terrine/frame.py) |

## dataset

Use pinned manifests, valid definitions and owned destinations.

[Recovery guide](../guides/ml-staging.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.dataset.export_conflict` | <code>Export destination conflicts with an existing export or partial export.</code> | None | [terrine/dataset/export.py](../../terrine/dataset/export.py) |
| `terrine.dataset.invalid_batch_size` | <code>Arrow batch size must be a positive integer.</code> | None | [terrine/dataset/reference.py](../../terrine/dataset/reference.py) |
| `terrine.dataset.invalid_definition` | <code>Invalid dataset definition: {reason}</code> | `reason` | [terrine/spec/dataset.py](../../terrine/spec/dataset.py) |
| `terrine.dataset.invalid_destination` | <code>Dataset destination is invalid: {reason}.</code> | `reason` | [terrine/dataset/reference.py](../../terrine/dataset/reference.py) |
| `terrine.dataset.invalid_export` | <code>Invalid Parquet export: {reason}.</code> | `reason` | [terrine/dataset/export.py](../../terrine/dataset/export.py) |
| `terrine.dataset.invalid_generation` | <code>Dataset manifest does not identify a verified committed runtime generation.</code> | None | [terrine/dataset/reference.py](../../terrine/dataset/reference.py) |
| `terrine.dataset.invalid_label_support` | <code>Selected label lacks completeness and reason metadata: {label}</code> | `label` | [terrine/frame.py](../../terrine/frame.py) |
| `terrine.dataset.invalid_manifest` | <code>Invalid dataset manifest: {reason}. Stage its dataset definition to create a new manifest.</code> | `reason` | [terrine/dataset/manifest.py](../../terrine/dataset/manifest.py) |
| `terrine.dataset.invalid_selection` | <code>Unknown training feature selection: {names}</code> | `names` | [terrine/frame.py](../../terrine/frame.py) |
| `terrine.dataset.invalid_selection` | <code>Unknown training label selection: {names}</code> | `names` | [terrine/frame.py](../../terrine/frame.py) |
| `terrine.dataset.invalid_view` | <code>Invalid dataset view: {reason}.</code> | `reason` | [terrine/spec/dataset.py](../../terrine/spec/dataset.py) |
| `terrine.dataset.stale_generation` | <code>Refresh requires the pinned generation to remain the current publication.</code> | None | [terrine/dataset/reference.py](../../terrine/dataset/reference.py) |
| `terrine.dataset.unsupported_view` | <code>Unsupported dataset view: {reason}.</code> | `reason` | [terrine/spec/dataset.py](../../terrine/spec/dataset.py) |

## design

Repair temporal/history contracts; do not zero-fill unknowns.

[Recovery guide](../guides/design-validation.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.design.ambiguous_dimension` | <code>Property {name!r} resolves to multiple dimensions.</code> | `name` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.conflicting_treatment` | <code>Sequential treatment conflicts with Design.treatment.</code> | None | [terrine/spec/panels.py](../../terrine/spec/panels.py) |
| `terrine.design.gap_violation` | <code>{role} window or cutoff violates gap {gap}.</code> | `gap`, `role` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.incompatible_unit` | <code>Analysis unit {unit!r} is incompatible with {name!r}.</code> | `name`, `unit` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.incomplete_treatment_history` | <code>Treatment history for source {source!r} does not cover {boundary!r}.</code> | `boundary`, `source` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.incomplete_unit_treatment_history` | <code>Roster observation bounds do not cover a scheduled treatment history.</code> | None | [terrine/compile/shapes/sequential.py](../../terrine/compile/shapes/sequential.py) |
| `terrine.design.incomplete_unit_treatment_history` | <code>Roster observation bounds do not cover the required calendar treatment history.</code> | None | [terrine/compile/shapes/calendar_comparison.py](../../terrine/compile/shapes/calendar_comparison.py) |
| `terrine.design.invalid` | <code>Design validation failed: {reason}.</code> | `reason` | [terrine/recipes.py](../../terrine/recipes.py) |
| `terrine.design.invalid` | <code>Design validation failed: {reason}.</code> | `reason` | [terrine/spec/anchors.py](../../terrine/spec/anchors.py) |
| `terrine.design.invalid` | <code>Design validation failed: {reason}.</code> | `reason` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.invalid_calendar_comparison` | <code>Calendar comparison specification is invalid: {reason}.</code> | `reason` | [terrine/spec/panels.py](../../terrine/spec/panels.py) |
| `terrine.design.invalid_matched_t0_sample` | <code>Matched-t0 sample policy is invalid: {reason}.</code> | `reason` | [terrine/spec/panels.py](../../terrine/spec/panels.py) |
| `terrine.design.invalid_treatment_history` | <code>Treatment history is invalid: {reason}.</code> | `reason` | [terrine/spec/panels.py](../../terrine/spec/panels.py) |
| `terrine.design.invalid_trial_weight` | <code>Trial weights must name every scheduled trial exactly once.</code> | None | [terrine/compile/shapes/sequential.py](../../terrine/compile/shapes/sequential.py) |
| `terrine.design.invalid_window` | <code>Window for {name!r} must have start &lt; end.</code> | `name` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.metric_window_conflict` | <code>Metric {metric!r} declares a conflicting window_days value.</code> | `metric` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.design.missing_reference` | <code>Design references unknown {kind} {name!r}.</code> | `kind`, `name` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |

## diff_diff

Use a supported persistent-adoption panel.

[Recovery guide](../guides/library-handoffs.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.diff_diff.invalid_panel` | <code>Cannot encode this adoption panel ({reason}); {route}.</code> | `reason`, `route` | [terrine/export/diff_diff.py](../../terrine/export/diff_diff.py) |

## execution

Use owned native sessions or retained inputs.

[Recovery guide](../guides/backend-compatibility.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.execution.arrow_timestamp_range` | <code>A nanosecond timestamp output lies outside Arrow&#x27;s range (1677-09-21 to 2262-04-11): {detail}. Declare the column with microsecond precision, such as TIMESTAMP_TZ(6), or cast it in a SQL relation.</code> | `detail` | [terrine/execution/_snowflake.py](../../terrine/execution/_snowflake.py) |
| `terrine.execution.invalid_input_binding` | <code>Execution inputs are not an exact verified binding: {reason}.</code> | `reason` | [terrine/execution/inputs.py](../../terrine/execution/inputs.py) |
| `terrine.execution.snapshot_unavailable` | <code>A coherent native execution snapshot is unavailable: {reason}.</code> | `reason` | [terrine/execution/inputs.py](../../terrine/execution/inputs.py) |

## export

Satisfy shared preflight and the library shape contract.

[Recovery guide](../guides/library-handoffs.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.export.cluster_required` | <code>Repeated rows per independent unit require a valid cluster column; declare the unit cluster.</code> | None | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.grace_baseline_treatment` | <code>Grace-defined treatment cannot be exported as a baseline treatment; use a baseline design.</code> | None | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.incomplete_outcome` | <code>Outcome windows are incomplete; provide complete data or explicitly drop incomplete rows.</code> | None | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.invalid_surrogate` | <code>Surrogate export input is invalid ({reason}).</code> | `reason` | [terrine/export/surrogate.py](../../terrine/export/surrogate.py) |
| `terrine.export.repeated_units_unsupported` | <code>{consumer} cannot handle repeated units; use one row per unit or a cluster-aware library.</code> | `consumer` | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.snapshot_required` | <code>Warehouse export needs its own DuckDB snapshot transaction; use a DuckDB relation outside an existing transaction and fit inside the context.</code> | None | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.unknown_eligibility` | <code>Eligibility is unknown for some candidate rows; resolve eligibility before export.</code> | None | [terrine/export/preflight.py](../../terrine/export/preflight.py) |
| `terrine.export.unsupported_weights` | <code>{consumer} does not apply declared weights; use a downstream method supporting those weights.</code> | `consumer` | [terrine/export/preflight.py](../../terrine/export/preflight.py) |

## filter

Use typed, known filters and complete dimension histories.

[Recovery guide](../guides/temporal-engine.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.filter.unknown` | <code>Required metric filters are unknown for {failed_count} requested rows. Resolve source/dimension history and filter-null semantics before exporting.</code> | `failed_count` | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |

## increment

Use the supported extra or a native Catalog.

[Recovery guide](../guides/increment-compatibility.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.increment.ambiguous_property` | <code>increment filter property {property!r} is ambiguous in source {source!r}.</code> | `property`, `source` | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |
| `terrine.increment.duplicate_yaml_key` | <code>increment YAML repeats mapping key {key!r} in {path!r}.</code> | `key`, `path` | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |
| `terrine.increment.invalid_definitions` | <code>increment definitions were rejected with upstream code {upstream_code!r}.</code> | `upstream_code` | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |
| `terrine.increment.missing_extra` | <code>The increment compatibility extra is required; install terrine[increment].</code> | None | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |
| `terrine.increment.property_name_collision` | <code>increment property names cannot be qualified without a Catalog name collision: {name!r}.</code> | `name` | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |
| `terrine.increment.unsupported_definition` | <code>increment declaration {declaration!r} has semantics not represented by the native Catalog.</code> | `declaration` | [terrine/adapters/increment.py](../../terrine/adapters/increment.py) |

## lifelines

Use per-unit records to retain delayed-entry history.

[Recovery guide](../guides/library-handoffs.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.lifelines.delayed_entry` | <code>Risk counts cannot recover delayed entry times; use {route}.</code> | `route` | [terrine/export/lifelines.py](../../terrine/export/lifelines.py) |
| `terrine.lifelines.invalid_input` | <code>Invalid lifelines input ({reason}); supply complete, finite time-to-event records.</code> | `reason` | [terrine/export/lifelines.py](../../terrine/export/lifelines.py) |
| `terrine.lifelines.unsupported_shape` | <code>This survival input cannot support the requested conversion ({reason}); use {route}.</code> | `reason`, `route` | [terrine/export/lifelines.py](../../terrine/export/lifelines.py) |

## metadata

Use consistent coverage, roles and representable metadata.

[Recovery guide](../guides/temporal-engine.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.metadata.invalid_eligibility_summary` | <code>Eligibility audit did not return one aggregate count record</code> | None | [terrine/execution/gates.py](../../terrine/execution/gates.py) |
| `terrine.metadata.invalid_eligibility_summary` | <code>Eligibility counts must be nonnegative and reconcile</code> | None | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.missing_columns` | <code>Arrow table is missing required columns {columns!r}; use lossless Arrow with all observation columns.</code> | `columns` | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.unrepresentable_coverage` | <code>Coverage boundary exceeds datetime&#x27;s range; use interior calendar dates</code> | None | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.unrepresentable_coverage` | <code>Coverage exceeds UTC datetime&#x27;s range; use interior instants</code> | None | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.unrepresentable_coverage` | <code>Coverage requires a following calendar day; use through before date.max</code> | None | [terrine/data.py](../../terrine/data.py) |
| `terrine.metadata.unsupported_version` | <code>Arrow metadata version {version!r} is unsupported; re-materialize the Design with the current Terrine version.</code> | `version` | [terrine/data.py](../../terrine/data.py) |

## metric

Use a supported aggregation, measure, precision and shape.

[Recovery guide](../guides/temporal-engine.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.metric.approx_backend_unavailable` | <code>Approximate distinct is unavailable for backend {dialect!r}.</code> | `dialect` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.approx_distinct_requires_single_measure` | <code>Approximate distinct requires metric {metric!r} to have exactly one measure.</code> | `metric` | [terrine/compile/columns.py](../../terrine/compile/columns.py) |
| `terrine.metric.approx_eligibility_unavailable` | <code>Approximate quantiles cannot determine predicate values; use exact quantile or output the approximate value without a predicate.</code> | None | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.approx_shape_unavailable` | <code>Approximate quantile supports cross_section only; use exact quantile or cross_section.</code> | None | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.exact_precision_unavailable` | <code>Exact {operation} requires output precision unavailable for source type {value_type!r}; use an exactly representable type/probability or a wider supported numeric backend.</code> | `operation`, `value_type` | [terrine/temporal/nonadditive.py](../../terrine/temporal/nonadditive.py) |
| `terrine.metric.exact_quantile_backend_unsupported` | <code>Exact type-7 quantiles are unavailable for backend {dialect!r}; use a verified supported backend.</code> | `dialect` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.nonadditive_kind_unsupported` | <code>Nonadditive aggregation {aggregation!r} is unsupported for metric kind {kind!r}.</code> | `aggregation`, `kind` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.nonadditive_type_unsupported` | <code>Exact {operation} does not support source type {value_type!r}; use an ordered numeric fact.</code> | `operation`, `value_type` | [terrine/temporal/nonadditive.py](../../terrine/temporal/nonadditive.py) |
| `terrine.metric.quantile_requires_single_measure` | <code>Approximate quantile requires metric {metric!r} to have exactly one measure.</code> | `metric` | [terrine/compile/columns.py](../../terrine/compile/columns.py) |
| `terrine.metric.quantile_requires_single_measure` | <code>Exact quantile requires metric {metric!r} to have exactly one measure.</code> | `metric` | [terrine/compile/columns.py](../../terrine/compile/columns.py) |
| `terrine.metric.quantile_requires_single_measure` | <code>Exact quantile requires metric {metric!r} to have exactly one measure.</code> | `metric` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.retention_identity_unavailable` | <code>Retention fact {fact!r} has no occurrence identity; bind its column to an occurrence ID or declare Model.event_ids for source {source!r}.</code> | `fact`, `source` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.metric.sketch_unavailable` | <code>Sketch {algorithm!r} version {version!r} is unavailable or cannot meet its declared error bound; install terrine[sketches] or use the exact form.</code> | `algorithm`, `version` | [terrine/temporal/sketches.py](../../terrine/temporal/sketches.py) |
| `terrine.metric.sketch_unavailable` | <code>Sketch {algorithm!r} version {version!r} is unavailable; install terrine[sketches] or use the exact form.</code> | `algorithm`, `version` | [terrine/spec/expressions.py](../../terrine/spec/expressions.py) |
| `terrine.metric.unaligned_calendar_average` | <code>avg_calendar_day requires a positive whole number of local days with both endpoints at local midnight; use sum or avg_event for arbitrary instants, or choose genuinely aligned anchors and endpoints.</code> | None | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |
| `terrine.metric.unsupported_aggregation` | <code>Aggregation {aggregation!r} is unsupported by the temporal engine.</code> | `aggregation` | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |
| `terrine.metric.unsupported_temporal_kind` | <code>Metric kind {kind!r} is not supported by the temporal engine.</code> | `kind` | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |
| `terrine.metric.unsupported_transform` | <code>Metric transform {transform!r} is declared but cannot be compiled by the temporal engine.</code> | `transform` | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |

## model

Correct source, roster and coverage bindings; rebind the Model.

[Recovery guide](../guides/temporal-engine.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.model.generated_metric_collision` | <code>Generated name {name!r} conflicts for event sources {sources}; rename the source or value declaration, or use Model.from_catalog for explicit names.</code> | `name`, `sources` | [terrine/model.py](../../terrine/model.py) |
| `terrine.model.invalid_binding` | <code>Model binding input is invalid: {reason}.</code> | `reason` | [terrine/model.py](../../terrine/model.py) |
| `terrine.model.missing_event_completeness` | <code>Event source {source!r} requires complete=(start, end); declare authoritative inclusive local calendar days, not observed event maxima.</code> | `source` | [terrine/model.py](../../terrine/model.py) |
| `terrine.model.multiple_event_units` | <code>Event source {source!r} declares multiple units {units}; use Model.from_catalog for multi-unit source semantics.</code> | `source`, `units` | [terrine/model.py](../../terrine/model.py) |

## readiness

Declare authoritative completeness for selected sources.

[Recovery guide](../guides/temporal-engine.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.readiness.missing_source` | <code>No completeness coverage was declared for fact source {source!r}.</code> | `source` | [terrine/temporal/readiness.py](../../terrine/temporal/readiness.py) |

## runtime

Inspect authoritative run state. Replay unchanged requests; replan changed inputs.

[Recovery guide](../guides/materialization-runtime.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.runtime.concurrency_limit` | <code>Owner concurrent run limit has been reached.</code> | None | [terrine/runtime/execute.py](../../terrine/runtime/execute.py) |
| `terrine.runtime.control_message_too_large` | <code>Owner control message exceeds the configured limit.</code> | None | [terrine/runtime/control.py](../../terrine/runtime/control.py) |
| `terrine.runtime.control_unavailable` | <code>Owner control request was refused.</code> | None | [terrine/runtime/control.py](../../terrine/runtime/control.py) |
| `terrine.runtime.cost_estimate_mismatch` | <code>Cost evidence does not match the task, definition or snapshot; replan with the owner&#x27;s metadata provider.</code> | None | [terrine/runtime/cost.py](../../terrine/runtime/cost.py) |
| `terrine.runtime.design_partition_unsupported` | <code>The built-in Design executor supports one output partition; use a single-partition plan or a custom executor with per-partition bounds.</code> | None | [terrine/runtime/api.py](../../terrine/runtime/api.py) |
| `terrine.runtime.execution_failed` | <code>Runtime execution failed outside a coded Terrine refusal.</code> | None | [terrine/runtime/execute.py](../../terrine/runtime/execute.py) |
| `terrine.runtime.executor_unavailable` | <code>No owner execution callbacks are configured for this runtime.</code> | None | [terrine/runtime/api.py](../../terrine/runtime/api.py) |
| `terrine.runtime.lazy_duration_unsupported` | <code>DuckDB lazy reads cannot represent duration unit {unit!r} in column {column!r}; use {route}.</code> | `column`, `route`, `unit` | [terrine/runtime/backends/duckdb.py](../../terrine/runtime/backends/duckdb.py) |
| `terrine.runtime.owner_interrupted` | <code>The runtime owner stopped before completing or publishing this run.</code> | None | [terrine/runtime/status.py](../../terrine/runtime/status.py) |
| `terrine.runtime.plan_invocation_conflict` | <code>Invocation ID is already bound to a different planning request.</code> | None | [terrine/runtime/planning.py](../../terrine/runtime/planning.py) |
| `terrine.runtime.plan_request_invalid` | <code>Owner plan request is invalid ({reason}).</code> | `reason` | [terrine/runtime/planning.py](../../terrine/runtime/planning.py) |
| `terrine.runtime.request_key_conflict` | <code>Request key is already bound to a different plan or destination.</code> | None | [terrine/runtime/execute.py](../../terrine/runtime/execute.py) |
| `terrine.runtime.request_key_conflict` | <code>Request key is already bound to a different plan.</code> | None | [terrine/runtime/api.py](../../terrine/runtime/api.py) |
| `terrine.runtime.run_unavailable` | <code>Run status is unavailable for the requested run.</code> | None | [terrine/runtime/status.py](../../terrine/runtime/status.py) |
| `terrine.runtime.saved_plan_mismatch` | <code>Saved plan reconstruction failed verification (retained snapshot Model declaration differs).</code> | None | [terrine/runtime/api.py](../../terrine/runtime/api.py) |
| `terrine.runtime.saved_plan_mismatch` | <code>Saved plan reconstruction failed verification ({reason}).</code> | `reason` | [terrine/runtime/saved_plan.py](../../terrine/runtime/saved_plan.py) |
| `terrine.runtime.sketch_sidecar_unavailable` | <code>Approximate quantile sidecars are unavailable in runtime planning and composition; use ordinary Frame materialization.</code> | None | [terrine/runtime/materialize.py](../../terrine/runtime/materialize.py) |
| `terrine.runtime.snapshot_invalid` | <code>Runtime snapshot receipt is invalid: {reason}.</code> | `reason` | [terrine/runtime/snapshot.py](../../terrine/runtime/snapshot.py) |

## semantic

Correct native semantics or use an admitted staged table.

[Recovery guide](../guides/temporal-engine.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.semantic.ambiguous_column` | <code>Normalized event aliases conflict with declared physical column {column!r}.</code> | `column` | [terrine/semantic/bind.py](../../terrine/semantic/bind.py) |
| `terrine.semantic.decimal_scale_required` | <code>Decimal column {column!r} has no fixed precision and scale up to 38 digits (declared {declared}). Cast it to an explicit numeric(p, s) with p &lt;= 38 in the relation SQL.</code> | `column`, `declared` | [terrine/semantic/bind.py](../../terrine/semantic/bind.py) |
| `terrine.semantic.dialect_mismatch` | <code>Catalog dialect {declared!r} does not match connection dialect {execution!r}.</code> | `declared`, `execution` | [terrine/semantic/sql.py](../../terrine/semantic/sql.py) |
| `terrine.semantic.missing_reference` | <code>Catalog reference {reference!r} is not declared.</code> | `reference` | [terrine/semantic/bind.py](../../terrine/semantic/bind.py) |
| `terrine.semantic.schema_mismatch` | <code>Declared {field} column {column!r} is absent or has an incompatible type.</code> | `column`, `field` | [terrine/semantic/bind.py](../../terrine/semantic/bind.py) |

## sequence

Declare stable event IDs and bounded sequence sizes.

[Recovery guide](../guides/temporal-engine.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.sequence.budget_required` | <code>Event-sequence materialization requires positive max_rows and max_output_bytes budgets.</code> | None | [terrine/execution/gates.py](../../terrine/execution/gates.py) |
| `terrine.sequence.incomplete_time_bounds` | <code>Event sequence completeness bounds are invalid.</code> | None | [terrine/temporal/sequence.py](../../terrine/temporal/sequence.py) |
| `terrine.sequence.invalid_spec` | <code>Invalid event sequence design: {reason}.</code> | `reason` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.sequence.invalid_spec` | <code>Invalid event sequence request: {reason}.</code> | `reason` | [terrine/spec/sequence.py](../../terrine/spec/sequence.py) |
| `terrine.sequence.max_events_exceeded` | <code>Sequence contains more than the configured max_events value {limit}.</code> | `limit` | [terrine/temporal/sequence.py](../../terrine/temporal/sequence.py) |
| `terrine.sequence.reanchor_leak` | <code>{role} expression support may cross the required gap {gap}.</code> | `gap`, `role` | [terrine/spec/validate.py](../../terrine/spec/validate.py) |
| `terrine.sequence.unsupported_shape` | <code>Event-time expression is unsupported by the selected compiler.</code> | None | [terrine/spec/sequence.py](../../terrine/spec/sequence.py) |

## simulate

Use representable, consistent raw-source configurations.

[Recovery guide](../guides/synthetic-worlds.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.simulate.invalid_seed` | <code>Use an integer seed in [0, 2**64).</code> | None | [terrine/simulate/generate.py](../../terrine/simulate/generate.py) |
| `terrine.simulate.nonfinite` | <code>Generated {quantity} is nonfinite; reduce coefficient or noise magnitudes.</code> | `quantity` | [terrine/simulate/generate.py](../../terrine/simulate/generate.py) |

## spec

Use typed constructors, supported fields and unique output names.

[Recovery guide](../guides/design-specs.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.spec.output_name_conflict` | <code>Study output name {name!r} conflicts with {other!r}.</code> | `name`, `other` | [terrine/compile/columns.py](../../terrine/compile/columns.py) |
| `terrine.spec.reserved_output_name` | <code>Study output name {name!r} is reserved.</code> | `name` | [terrine/spec/design.py](../../terrine/spec/design.py) |

## sql

Use one admitted read-only query or an owner-staged table.

[Recovery guide](../guides/temporal-engine.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.sql.invalid_read_query` | <code>SQL relation is not an admitted read-only query: {reason}.</code> | `reason` | [terrine/semantic/sql.py](../../terrine/semantic/sql.py) |

## state_panel

Use ordered, bounded observations and explicit missingness.

[Recovery guide](../guides/state-panels.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.state_panel.budget_required` | <code>State-panel materialization requires positive max_rows and max_output_bytes budgets.</code> | None | [terrine/execution/gates.py](../../terrine/execution/gates.py) |
| `terrine.state_panel.invalid_observations` | <code>State observations are invalid: {reason}.</code> | `reason` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |
| `terrine.state_panel.invalid_schedule` | <code>State panel schedule is invalid: {reason}.</code> | `reason` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |
| `terrine.state_panel.invalid_state_source` | <code>State source is invalid: {reason}.</code> | `reason` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |
| `terrine.state_panel.unsupported_combination` | <code>State panel combination is unsupported: {reason}.</code> | `reason` | [terrine/spec/state_panel.py](../../terrine/spec/state_panel.py) |

## study

Repair the named Design contract before collecting rows.

[Recovery guide](../guides/design-validation.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.study.eligibility_unknown` | <code>Eligibility is unresolved for {unknown} of {candidate} candidates; complete the required history before exporting rows.</code> | `candidate`, `unknown` | [terrine/execution/gates.py](../../terrine/execution/gates.py) |

## survival

Declare per-unit event/censor and observation bounds.

[Recovery guide](../guides/trajectories-survival.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.survival.invalid_compilation` | <code>Time-to-event compilation failed ({reason}).</code> | `reason` | [terrine/compile/shapes/time_to_event.py](../../terrine/compile/shapes/time_to_event.py) |

## temporal

Check exact endpoints, coverage and arithmetic validity.

[Recovery guide](../guides/temporal-engine.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.temporal.alias_collision` | <code>Temporal output alias conflicts with an existing point column.</code> | None | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.duplicate_event_id` | <code>Event identifiers must be non-null and unique within each unit.</code> | None | [terrine/temporal/first.py](../../terrine/temporal/first.py) |
| `terrine.temporal.duplicate_history` | <code>Dimension history contains duplicate entity/start keys.</code> | None | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.exact_overflow` | <code>The requested exact result cannot be represented by the native output type. Use a supported exact representation or a semantically sufficient window.</code> | None | [terrine/backends/numeric.py](../../terrine/backends/numeric.py) |
| `terrine.temporal.exact_unsupported` | <code>Backend {backend!r} lacks exact sparse BIGNUM state. Use the supported DuckDB backend.</code> | `backend` | [terrine/temporal/prefix.py](../../terrine/temporal/prefix.py) |
| `terrine.temporal.first_occurrence_invalid_relation` | <code>First-occurrence input relation is invalid ({reason}).</code> | `reason` | [terrine/temporal/first.py](../../terrine/temporal/first.py) |
| `terrine.temporal.first_occurrence_unsupported_backend` | <code>First-occurrence point lookup requires the DuckDB, PostgreSQL, BigQuery or Snowflake backend; got {backend!r}.</code> | `backend` | [terrine/temporal/first.py](../../terrine/temporal/first.py) |
| `terrine.temporal.float64_precision` | <code>Float64 result unavailable ({failure}). Use finite inputs whose result fits binary64, or correctly declared integer/decimal facts.</code> | `failure` | [terrine/temporal/prefix.py](../../terrine/temporal/prefix.py) |
| `terrine.temporal.malformed_history` | <code>Dimension history contains null keys, null starts, or malformed validity intervals.</code> | None | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.missing_join_key` | <code>Dimension {dimension!r} cannot resolve its declared event-side entity key.</code> | `dimension` | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.overlapping_history` | <code>Dimension history contains overlapping validity intervals.</code> | None | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.range_bound_unavailable` | <code>A bounded range strategy is unavailable for {expression}: {reason}. The candidate multiplier limit is {limit}; {route}.</code> | `expression`, `limit`, `reason`, `route` | [terrine/temporal/ranges.py](../../terrine/temporal/ranges.py) |
| `terrine.temporal.retention_invalid` | <code>The retention request cannot be represented ({reason}).</code> | `reason` | [terrine/temporal/retention.py](../../terrine/temporal/retention.py) |
| `terrine.temporal.static_key_multiplicity` | <code>Static dimension has multiple rows for an entity key.</code> | None | [terrine/temporal/attributes.py](../../terrine/temporal/attributes.py) |
| `terrine.temporal.timestamp_order_unsupported` | <code>Timestamp ordering cannot preserve {dtype}. Normalize to a losslessly representable native microsecond or nanosecond timestamp before binding.</code> | `dtype` | [terrine/backends/timestamps.py](../../terrine/backends/timestamps.py) |
| `terrine.temporal.unaligned_day_resolution` | <code>Day prefixes require both endpoints at local midnight; use event prefixes for arbitrary instants.</code> | None | [terrine/temporal/aggregate.py](../../terrine/temporal/aggregate.py) |
| `terrine.temporal.unsupported_event_resolution` | <code>Event-time resolution is unsupported for dialect {dialect!r}; use aligned anchors or a supported DuckDB source.</code> | `dialect` | [terrine/semantic/rules.py](../../terrine/semantic/rules.py) |

## trajectory

Use bounded periods and consistent observation.

[Recovery guide](../guides/trajectories-survival.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.trajectory.invalid_compilation` | <code>Trajectory compilation failed ({reason}).</code> | `reason` | [terrine/compile/shapes/ltv.py](../../terrine/compile/shapes/ltv.py) |

## transform

Fit on training rows; reuse unchanged frozen scoring limits.

[Recovery guide](../guides/ml-staging.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.transform.empty_training_population` | <code>Exact winsorization for {column!r} requires non-null training values. {route}</code> | `column`, `route` | [terrine/dataset/transforms.py](../../terrine/dataset/transforms.py) |
| `terrine.transform.invalid_winsorization` | <code>Invalid winsorization: {reason}. {route}</code> | `reason`, `route` | [terrine/spec/transforms.py](../../terrine/spec/transforms.py) |
| `terrine.transform.training_split_required` | <code>Fitting winsorization requires a declared training split. {route}</code> | `route` | [terrine/dataset/transforms.py](../../terrine/dataset/transforms.py) |
| `terrine.transform.winsorization_expr_unsupported` | <code>Post-aggregation winsorization requires guarded Arrow output. {route}</code> | `route` | [terrine/frame.py](../../terrine/frame.py) |
| `terrine.transform.winsorization_identity_mismatch` | <code>Frozen winsorization for {column!r} does not match this view. {route}</code> | `column`, `route` | [terrine/dataset/transforms.py](../../terrine/dataset/transforms.py) |
| `terrine.transform.winsorization_precision_unavailable` | <code>Winsorization output for {column!r} needs unavailable exact Decimal precision. {route}</code> | `column`, `route` | [terrine/dataset/transforms.py](../../terrine/dataset/transforms.py) |
| `terrine.transform.winsorization_type_unsupported` | <code>Exact winsorization does not support {value_type!r} for {column!r}. {route}</code> | `column`, `route`, `value_type` | [terrine/dataset/transforms.py](../../terrine/dataset/transforms.py) |

## window

Use ordered integral elapsed endpoints with s/m/h/d/w units.

[Recovery guide](../guides/design-specs.md)

| Code | Message template | Interpolation fields | Declaration |
| --- | --- | --- | --- |
| `terrine.window.invalid_duration` | <code>Expected integral elapsed seconds or a signed s/m/h/d/w duration.</code> | None | [terrine/windows.py](../../terrine/windows.py) |
| `terrine.window.invalid_window` | <code>Window start must precede its exclusive end.</code> | None | [terrine/windows.py](../../terrine/windows.py) |
