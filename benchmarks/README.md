# Dataset-scale compiler benchmarks

Run the deterministic local DuckDB workload explicitly, with the optional
`sketches` dependency installed:

```sh
uv run python -m benchmarks.run_duckdb --config benchmarks/workloads/small.json
# Optional: preserve a new machine-readable record without overwriting old evidence.
uv run python -m benchmarks.run_duckdb --config benchmarks/workloads/small.json --output /path/to/new-run.json
uv run python -m benchmarks.report /path/to/new-run.json
```

This executes real Model/Design Frames for exact min/max/distinct, exact Type-7
quantiles, additive prefixes, HLL distinct and t-digest quantiles. Cases include
fixed, sparse scheduled, Sequential and EventPanel windows. The t-digest case
uses the supported ordinary cross-section Frame path; it is not silently
substituted into unsupported runtime composition.

The runtime case executes a complete compiler/shared-gate task from an
owner-retained snapshot, publishes an exact generation, applies declared source
updates/deletions, captures a new snapshot and verifies the new generation
against the independent oracle. Re-reading the old receipt verifies its
immutability. It is explicitly an owner single-task **full rebuild**, not a
claim of incremental prefix reuse or a second compiler.

## Inputs, references and bounds

Generator version 2 uses typed keyed unit/event positions. Batch size and unit
partition traversal do not change identities or observations. Source batches,
roster intervals, independently declared scheduled anchors, and corrections
are separate streams. Physical IDs are unique, occurrence IDs may repeat,
timestamps tie, values/labels may be null, and sparse days are deliberate.
Version 1 is rejected; it is not silently reinterpreted.

The independent metric reference retains bounded integer histograms per unit
and source day rather than a whole event world. Its exact quantile uses rank
`(n-1)/4` and rational interpolation; generator tests also compare it with pandas.
Neither generator nor reference imports the production compiler/runtime.

Every range plan probes `requests <= 2 * anchors` and
`candidates <= max_candidates_per_event * input_events`. The probe uses the raw
source bucket join, an upper relation for operations that filter values.

DuckDB physical-plan text comes from `EXPLAIN`, not `EXPLAIN ANALYZE`.
Cardinality assertions use separately executed count queries; optimizer row
estimates in the plan text are not reported as actual operator row counts.
Bounds are cardinality assertions from inputs, never elapsed-time thresholds.
They do not establish physical scan bytes, query memory or warehouse cost.

Records distinguish:

- Input events, Arrow input bytes, maximum retained source-batch rows, declared
  scheduled anchors, per-case anchors/emitted rows and output bytes.
- Canonical source/output content digests, physical plans/cardinalities where
  available, exact-oracle agreement and separate empirical sketch errors.
- Traced Python allocation peak and process-lifetime peak RSS. Native allocations
  are not covered by tracemalloc; process RSS is not an isolated per-case delta.
- Instrumented elapsed time, applicable row caps, runtime estimate evidence and
  separately observed completed-artifact costs. Instrumentation is included in
  elapsed time; no fixed overhead percentage is claimed.
- Unknown native DuckDB scan bytes/cost as `null`, not zero. Strict unknown scan
  admission is explicitly checked for capture and planning.

`oracle_match` concerns exact values, support and null metadata. Sketch errors
are observed empirical errors with their algorithm/version/error metadata;
HLL/t-digest are not advertised as exact or given an invented hard tolerance.
There is no billion-event or universal scale claim from local calibration.

## Explicit test tiers

```sh
# Fast deterministic generator/reference and no-connection owner-gate checks.
uv run pytest -q tests/simulate/test_scale_generator.py tests/integration/test_live_scale_gate.py
# Instrumented compiler benchmarks are excluded from ordinary pytest by slow.
uv run pytest -q -m slow tests/integration/test_duckdb_scale.py
# Local DuckDB integration lies outside ordinary testpaths; this is NOT live.
uv run pytest -q integration/warehouse_execution/test_duckdb_scale.py
```

## Live warehouse: owner approval required

No live provider, account, selected workload, spend limit or owner adapter is
configured or authorized here. The live command has **not** been run. Missing
approval is unverified, not a successful backend result.

`benchmarks.run_live.LiveApproval` requires explicit `approved`, backend,
account/workspace references, a version-2 workload, selected cases, a scan-byte
ceiling, finite monetary ceiling/currency and an owner `module:function`
factory. Credentials stay in the owner's environment/config references, never
in approval/report records. The factory returns `LiveSession` for a real remote
account and a new isolated disposable workspace. It must:

1. Open connection metadata only until the exact all-phase plan is authorized.
2. Supply `CostProvider` evidence and implement `authorize_workload`, enforcing
   both scan and monetary ceilings across upload, capture, validation, queries,
   streaming reads, cancellation probes and cleanup. Optimistic forecasts,
   billing alerts and output caps are insufficient. Unknown bounds refuse
   before source work. There are no automatic retries.
3. Provide an Ibis backend/input adapter supported by the requested compiler
   operations. Existing DuckDB timestamp/sketch capabilities must not be
   assumed on PostgreSQL, Snowflake, BigQuery or another warehouse. Unsupported
   capabilities remain coded refusals or failed/unverified evidence, never a
   local fallback. Selecting/validating that adapter is part of the owner gate.
4. Restrict workspace cleanup to resources created by this invocation, retain
   available numeric scan/cost observations, and exercise physical cancellation
   only when the backend advertises it. Unsupported cancellation stays
   `unverified_capability`.

Only after that gate is satisfied, the owner may run:

```sh
uv run python -m benchmarks.run_live --approval /owner/approval.json --output /owner/new-live-report.json
TERRINE_LIVE_APPROVAL=/owner/approval.json TERRINE_LIVE_REPORT=/owner/new-live-report.json \
  uv run pytest -q -m slow integration/warehouse_execution/test_live_warehouse_scale.py
```

Reports contain aggregates/digests, not customer rows, credentials, raw SQL or
raw backend errors. A failed/refused live invocation exits unsuccessfully;
missing measurements remain unavailable. A local DuckDB result must not be
relabelled as evidence for an unexercised remote warehouse.
