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

## Fixed compiler overhead

`benchmarks/run_overhead.py` times the ordinary public path, `Design.build`
followed by `Frame.materialize`, on tiny deterministic DuckDB workloads: count,
exact decimal sum, certified float average and ratio over one row, a few rows,
and 10,000/100,000 events, requesting one or four columns. Materialization
includes declared snapshot validation and the output stream. Expected values
are computed independently with integer, `Decimal` and `Fraction` arithmetic
over the generated events; `oracle_match` reports equality for exact outputs
and certificate enclosure for checked floats.

```sh
uv run python -m benchmarks.run_overhead --repeats 5 --output /tmp/terrine-overhead.json
# Optional separate profiled pass; it is never a timing sample.
uv run python -m benchmarks.run_overhead --repeats 5 --profile /tmp/terrine-overhead.pstats
# Select cases by name and label the record for before/after comparison.
uv run python -m benchmarks.run_overhead --case one_row_float_average --label before
```

Each case records the environment (one DuckDB thread by default), workload,
setup time, the first sample, every warm sample, warm medians and spread for
the build and materialization phases, emitted rows, output Arrow bytes and
diagnostics from one separate instrumented run: the number of SQL
compilations, repeated identical expressions, and time per validation,
eligibility and output query. Import time is measured in a fresh interpreter.
Raw SQL is not stored and source scan bytes are not measured. Timings are local
observations for comparing two commits on one machine, not CI thresholds.

## Explicit test tiers

```sh
# Fast deterministic generator/reference and no-connection owner-gate checks.
uv run pytest -q tests/simulate/test_scale_generator.py tests/integration/test_live_scale_gate.py tests/integration/test_benchmark_failure_reporting.py
# Instrumented compiler benchmarks and the retained DuckDB oracle check run in
# the scheduled scale tier; this is NOT live.
make test-scale SCALE_TESTS="integration/warehouse_execution/test_duckdb_compiler_scale.py integration/warehouse_execution/test_duckdb_scale.py"
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
  make test-scale SCALE_TESTS=integration/warehouse_execution/test_live_warehouse_scale.py
```

Reports contain aggregates/digests, not customer rows, credentials, raw SQL or
raw backend errors. A failed/refused live invocation exits unsuccessfully;
missing measurements remain unavailable. A local DuckDB result must not be
relabelled as evidence for an unexercised remote warehouse.

After any authorized attempt, the runner observes available scan/spend metadata
after workspace cleanup and before closing the owner session, even on query,
oracle, cancellation or cleanup failure. Over-cap observations are retained as
failed evidence. Observation/close failures are explicit and do not erase the
primary failure. Programmatic failures raise a coded `TerrineError` with a
sanitized immutable report in `context["report"]`; the CLI and live test save
that report before reporting failure. Aggregation preserves failed/refused runs
and unavailable fields instead of treating them as successful or zero-cost.
