# Changelog

## Unreleased

### Dataset construction

- Build typed, point-in-time analytical and ML datasets with a standalone native
  Model → Design → Frame → Dataset workflow and a complete executable tutorial.
- Describe sources, entities, dimensions, facts and metrics in a strict native
  Catalog; bind existing tables or admitted read-only SQL without requiring a
  semantic adapter or sibling checkout.
- Preserve exact fixed, scheduled and event anchors, stable event identities,
  roster membership and independent observation bounds. First/nth events are
  selected before cohort range filtering.
- Construct named half-open features/outcomes, strict as-of attributes,
  three-valued eligibility and descriptive treatment/cluster/weight roles.
- Preserve source-specific completeness, numeric nulls and exact unavailable
  reasons; distinguish fully observed zero activity from missing follow-up.
- Preserve evaluated null reasons on event-relative results after a selector
  resolves, independently of completeness.
- Build sequential/calendar/event panels, bounded state pairs, LTV trajectories,
  cohort summaries, per-unit time-to-event records and risk-set counts/plain
  rates. Retain row identity, temporal support and typed shape provenance.
- Build deterministic bounded event sequences, sessions, funnels, transitions
  and event-relative selectors with explicit tie, missing-predecessor and
  temporal-leakage rules.
- Support exact retention occurrence bands and deterministic last-event values,
  exact min/max/distinct/type-7 quantiles, explicit native approximate distinct,
  and optional cross-sectional DataSketches quantile sidecars.

### Reproducibility and ML

- Stage immutable DuckDB generations with bounded Arrow transfers and pinned
  reads. Preserve logical types, column roles, coverage, completeness, precision
  metadata and construction recipes through manifests and Arrow serialization.
- Reopen and refresh saved training/scoring recipes in a fresh process. Explicit
  source corrections publish a new generation without mutating existing readers.
- Select training features/labels with exact incomplete-label exclusions and
  leakage-aware group/time splits, purging, embargo and unassigned-row reasons.
- Resolve scoring instants once, select an explicit eligible roster and omit
  unused labels, treatment and label-source reads.
- Apply fixed or train-only fitted post-aggregation winsorization, retain exact
  frozen limits and require matching feature/recipe/snapshot identities on reuse.
- Export resumable checksum-verified local Parquet shards with semantic metadata,
  exact decimal/timestamp/null values and typed empty outputs.
- Preserve selected event-relative timing bounds, trial identity and sampling
  weights in surrogate Arrow projections and their portable metadata.

### Execution and integrations

- Materialize Frames directly from PostgreSQL (`terrine[postgres]`, Ibis 12+):
  each run reads one consistent `REPEATABLE READ READ ONLY` snapshot on a private
  connection. Counts, exact integer/decimal sums, as-of attributes and fixed,
  scheduled, event-anchored, time-to-event, cohort-trajectory and state-panel
  designs match DuckDB. Averages, ratios and float windows are computed exactly
  in `NUMERIC` and rounded once, matching DuckDB within a tight tolerance
  relative to each window's absolute sum (`numeric_policy=None`). Dataset staging
  refuses with a route.
- Materialize Frames directly from BigQuery (`terrine[bigquery]`): each run reads
  one snapshot transaction in a private session on a new client, never the
  caller's. The PostgreSQL coverage and tolerance contract carry over, with exact
  state in `BIGNUMERIC` (sums and float facts beyond its range refuse with a
  code). Views calling dataset-qualified routines refuse. Live BigQuery probes run
  weekly and on dispatch (`make test-bigquery`).
- Refuse decimal sources without a declared precision and scale
  (`terrine.semantic.decimal_scale_required`) and non-finite PostgreSQL numerics
  (`terrine.backend.nonfinite_decimal`).
- Compile Designs entirely from Ibis expressions: building a Frame sends no SQL,
  and a shadowed native function in a compiled query refuses
  (`terrine.execution.snapshot_unavailable`) when the Frame runs, before any
  user function executes.
- Execute in owned private transaction sessions or retained immutable inputs;
  keep caller connections open and preserve exact UTC output instants under
  non-UTC SQL sessions.
- Reuse read-only input schemas within each execution and native relation
  metadata within each admission/capture operation, while revalidating live
  bindings before cached relations are returned.
- Share namespace, relation and function metadata across nested read operations;
  discard those facts after session changes and before a later operation.
- Preserve live input-binding validation when retained snapshots reopen an
  execution, including reads through already-resolved relations.
- Execute admitted SQL directly for Arrow tables and streams rather than
  compiling each expression again after validation.
- Save destination-bound plans and track authoritative run status, bounded event
  pages, request-key replay, cancellation requests, renewable fences and atomic
  generation publication through Python, CLI and owner-local IPC.
- Keep scan estimates, owner authorization and observed completed-artifact costs
  distinct. Unknown strict scan bounds refuse before source work; output limits
  abort rather than returning truncated success.
- Provide optional Airflow/Dagster triggers and a local dbt Core after-build
  recipe over the same owner planning, execution and publication contracts.
- Preserve Arrow metadata through optional pandas/Polars conversions. Add
  preflight-backed DoubleML, DoWhy and same-connection duckreg recipes, and
  lifelines/diff-diff dataset conversions with real integration examples.
- Keep increment as an optional semantic-definition adapter with explicit
  compatibility and shared-dataset evidence, never a native dependency.
- Offer separately authorized dataset agent discovery, Design/recipe edits,
  preparation, staging/refresh, pinned run reads, redacted previews and exports.
  Bound requests and budgets without exposing raw SQL or granting permissions
  through inspection. Telemetry is an optional owner-configured projection.

### Correctness and support

- Preserve integer/decimal scale and checked Float64 arithmetic; reject
  unrepresentable temporal/numeric results instead of narrowing or overflowing.
- Retain snapshot-scoped integrity, exact boundary, source-identity and treatment
  history checks across ordinary, retained and downstream export paths.
- Capture event-anchor and recursive event-selector sources even when the
  analysis unit has no separate roster.
- Build time-to-event designs that declare both an entry and an exit fact;
  they previously failed with a column name collision.
- Read retained run-event pages and their cursor-gap boundary from one consistent
  snapshot so concurrent retention cannot silently skip events.
- Create the complete run-tracking schema when the runtime owner starts, so a
  request-key lookup or status read that overlaps the namespace's first run is no
  longer refused by a DuckDB catalog conflict.
- Share export refusals for incomplete outcomes, unknown eligibility, missing
  repeated-unit clusters and grace-defined baseline treatment. DoWhy additionally
  refuses repeated units and declared weights rather than forwarding ignored
  arguments.
- Keep implementation records out of ordinary public facades and isolate all
  optional backend, dataframe, orchestration and modeling imports.
- Document capability/compatibility evidence, inherent information limits,
  recovery routes and data residency. Supply independent expected-row worlds,
  bounded local scale measurements and an owner-gated live warehouse tier.
- Terrine delivers datasets and descriptive summaries, not native effect
  estimators, inferential diagnostics, survival curves or causal guarantees.

### License

- Terrine is licensed under the Apache License 2.0 (`LICENSE`, `NOTICE`).
  Contributions require signing the Contributor License Agreement
  (`.github/CLA.md`); see `CONTRIBUTING.md`.
