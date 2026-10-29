# Guides

## Start here

- [Your first dataset](first-dataset.md): DataFrames → Model → Design → Frame;
  materialize in memory, or bind a file-backed DuckDB to stage a Dataset.
- [Public API](public-api.md): `Events`, `History`, user-named metric constructors,
  `Model.to_catalog`, `Design.describe`, values-only exports and advanced modules.
- [Common feature patterns](common-feature-patterns.md): rolling windows, conditional
  events, eligibility, as-of attributes, recency and Boolean labels.
- [Dataset capabilities](capabilities.md): metric × shape × backend evidence.
- [Compatibility](backend-compatibility.md): operation-level support and versions.
- [Refusal codes](../reference/refusals.md): generated declarations and recovery routes.
- [Inherent limits](limitations.md): information and validity limits, not missing features.

## Describe sources and time

- [Native catalogs and temporal engine](temporal-engine.md): source relations,
  coverage, precision, exact windows, nonadditive metrics, retention and sequences.
- [Temporal attributes](temporal-attributes.md): SCD2/changelog meaning, strict
  cutoffs, overlapping histories and recorded-at versus effective-time knowledge.
- [Design specifications](design-specs.md): typed expressions and canonical JSON/YAML.
- [Design validation](design-validation.md): metadata-only checks and runtime obligations.
- [Candidate anchors](candidate-anchors.md): fixed/scheduled/event populations,
  exact t0, event identity and independent observation bounds.

## Select a dataset shape

- [Sequential and event panels](sequential-panels.md).
- [State panels](state-panels.md).
- [Trajectories, cohorts and risk sets](trajectories-survival.md).
- [Synthetic worlds and independent expected rows](synthetic-worlds.md).

## Materialize, retain and share

- [Cost and data residency](cost-and-residency.md): scans, output limits,
  local/client artifacts, privacy and measured recovery/benchmark evidence.
- [Materialization runtime](materialization-runtime.md): owner configuration,
  saved plans, CLI/IPC, exact generations, replay, cancellation, corrections and
  [owned inputs for repeated builds](materialization-runtime.md#owned-inputs-for-repeated-builds).
- [ML staging](ml-staging.md): training/scoring, leakage-aware splits,
  frozen preprocessing, persisted definitions and Parquet export.
- [Common-stack interoperability](common-stack-interop.md): Arrow/pandas/Polars.
- [Library hand-offs](library-handoffs.md): shared preflight, a catalog-backed
  grid of trial panels for DoubleML and other downstream workflows.
- [Orchestrators](orchestrators.md): optional Airflow, Dagster and dbt Core triggers.
- [Dataset agent tools](dataset-agent-tools.md): permissions, budgets and redaction.

## Optional semantic definitions

- [Semantic-layer interoperability](semantic-layer-interop.md).
- [increment compatibility](increment-compatibility.md).
- [Shared dataset contract](shared-dataset-contract.md).
