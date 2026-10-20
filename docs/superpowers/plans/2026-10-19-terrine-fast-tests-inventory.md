# Test-speed inventory

Inventory of all 230 measured test cases (2,956.22 worker-seconds from the supplied duration file). Categories follow the repository's smallest-boundary rule: preserve unique composition-dependent E2E behavior; move expensive E2E work to `integration/` (warehouse-independent runtime checks can instead carry `@pytest.mark.slow`); replace narrow pipeline assertions with unit/direct-relation checks; delete only redundant behavior with an already-named integration equivalent or forwarding-only checks. Times below are **current measured worker-seconds**; fast-tier removal estimates account for integration tests already tiered, the named duplicate removals, and minimal fast representatives.

| class | cases | current worker-s | estimated removed from fast tier | recommendation |
|---|---:|---:|---:|---|
| KEEP-E2E | 52 | 1499.20 | ~1,495 | Move unique composition-dependent tests to `integration/`; retain integration-file cases in place, not duplicated. |
| REWRITE | 161 | 998.54 | ~799 | Use a direct small Arrow table / in-memory DuckDB relation at the owning boundary (estimate assumes ~80% execution-time reduction). |
| SLIM | 7 | 68.05 | ~2 | Keep 2–3 fast representatives; full case matrix in `@pytest.mark.slow`. |
| DELETE | 10 | 390.43 | ~390 | Remove only named duplicates of existing integration coverage. |
| **Total** | **230** | **2956.22** | **~2,687** | Estimates are projected, not measured. |

The `DELETE` rows cite their existing integration equivalent per-test below. The E2E recommendation does not mean dropping unique behavior: retain each named assertion in an explicit tier. Two subprocess import checks are retained because they exercise actual dependency isolation/behavior, not source text.

## Shared fixture-builder candidates

- `tests/compile/test_columns.py::column_model` — imported as a fixture across compile and runtime-equivalence/layer tests; builds a sizable DuckDB model. Cache immutable catalog/schema setup session-wide, but expose a per-test connection/fresh relation factory (do not share mutable connection state).
- `tests/dataset/test_staging.py::_durable_frame` — shared by staging/refresh/runtime-staging paths; provide a session fixture for the immutable tiny source Arrow tables/catalog, retain fresh per-test target DBs and dataset state.
- `tests/dataset/test_export.py::_staged_dataset` — repeated staging/export setup; share only immutable source data/recipe construction, not destination files or manifests.
- `tests/agent/conftest.py::tools` and `::owner_tools` — repeated policy/model setup in agent materialization and authorization tests; factor immutable catalog/model spec construction, retain isolated owner DB and policy state per test.
- `tests/test_common_stack_interop.py::common_frame` — parametrized two-row/empty persisted-frame setup reused by Arrow, dataframe and Parquet checks; share source Arrow fixtures, do not share mutable DuckDB files.
- `tests/dataset/test_winsorization_recipe.py::model` / `::training` — rebuild the same tiny model/design for fit/refusal tests; share fixture construction while preserving per-test DB mutation isolation.

## Collision and wait suspects

- **Fixed cross-worker socket collision:** `tests/runtime/test_control_recovery.py:39` and `:52` both use `/tmp/terrine-recovery-test.sock` without PID/UUID. Parallel test processes can collide; use `tmp_path` (or unique PID+UUID path). The sibling recovery-copy socket at `:177` already includes PID.
- Other control tests use PID-scoped sockets: `tests/runtime/test_control_ipc.py:23,53,82,109`, `tests/runtime/test_cli.py:262`, `tests/runtime/test_cancel_publication.py:101`; `tests/runtime/test_planning.py:85` has PID+UUID. No fixed-path collision seen in those hits.
- Synchronization/wait candidates: `tests/dataset/test_runtime_staging.py:82-105` explicitly exercises lease renewal while snapshot work blocks; `tests/runtime/test_execute.py::test_lease_heartbeat_keeps_long_task_alive` (1.14s) and `::test_cancellation_serializes_with_heartbeat_persistence` (1.13s); `tests/runtime/test_cli.py:250-295` waits for long owner execution/cancellation; `tests/runtime/test_cancel_publication.py:63-117`, `tests/runtime/test_run_events.py:123-177`, `tests/runtime/test_run_status.py:163-169`, `tests/test_frame_runtime.py:74-99` use bounded events/barriers rather than sleeps; `tests/dataset/test_recipe.py:56` has a two-party `Barrier.wait()` without timeout. Keep these synchronized, but consider reducing their HANG_GUARD timeouts only if they are actual delay sources (normally they are guards).
- Production heartbeat reached by execution tests: `terrine/runtime/execute.py:392-415` waits at `min(lease_ttl / 3, 1.0)` and renews the lease; the task scheduler waits on futures at `:504`. `LeaseStore.renew` is `terrine/runtime/leases.py:112` (the stage renewal path is covered by the staging test above). The publication lock itself is reached by `tests/runtime/test_cancel_publication.py:66-84` to test cancellation serialization. No polling/back-off loop was identified in the inspected test paths; the observed duration is dominated by repeated DuckDB/model/dataset setup, not sleeps.

## Per-test inventory

### `tests/test_common_stack_interop.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_dataset_batches_and_parquet_preserve_typed_metadata[typed_rows]` | 232.80 | DELETE | DELETE; equivalent: `integration/warehouse_execution/test_backend_capabilities.py::test_duckdb_typed_dataset_arrow_parquet_and_generation_parity` | Redundant with already-tiered integration coverage; retain the integration assertion, not this duplicate. |
| `test_dataset_batches_and_parquet_preserve_typed_metadata[empty]` | 228.64 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[typed_rows-polars]` | 20.36 | REWRITE | terrine.data.MaterializedFrame / Arrow conversion methods; tiny typed or empty pa.Table | Arrow fidelity/conversion metadata is a direct `MaterializedFrame` contract; construct a tiny typed/empty Arrow table. |
| `test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[typed_rows-pandas]` | 18.18 | REWRITE | terrine.data.MaterializedFrame / Arrow conversion methods; tiny typed or empty pa.Table | Arrow fidelity/conversion metadata is a direct `MaterializedFrame` contract; construct a tiny typed/empty Arrow table. |
| `test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[empty-polars]` | 15.03 | REWRITE | terrine.data.MaterializedFrame / Arrow conversion methods; tiny typed or empty pa.Table | Arrow fidelity/conversion metadata is a direct `MaterializedFrame` contract; construct a tiny typed/empty Arrow table. |
| `test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow[empty-pandas]` | 11.94 | REWRITE | terrine.data.MaterializedFrame / Arrow conversion methods; tiny typed or empty pa.Table | Arrow fidelity/conversion metadata is a direct `MaterializedFrame` contract; construct a tiny typed/empty Arrow table. |
| `test_local_arrow_preserves_exact_temporal_values_and_semantic_metadata[typed_rows]` | 6.75 | REWRITE | terrine.data.MaterializedFrame / Arrow conversion methods; tiny typed or empty pa.Table | Arrow fidelity/conversion metadata is a direct `MaterializedFrame` contract; construct a tiny typed/empty Arrow table. |
| `test_local_arrow_preserves_exact_temporal_values_and_semantic_metadata[empty]` | 6.71 | REWRITE | terrine.data.MaterializedFrame / Arrow conversion methods; tiny typed or empty pa.Table | Arrow fidelity/conversion metadata is a direct `MaterializedFrame` contract; construct a tiny typed/empty Arrow table. |
### `tests/runtime/test_layers.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_saved_panel_publication_preserves_ordinary_provenance[sequential]` | 169.24 | KEEP-E2E | integration/warehouse_execution/test_saved_plan_restart.py (add unique persisted-layer provenance case) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_saved_panel_publication_preserves_ordinary_provenance[event_panel]` | 7.95 | KEEP-E2E | integration/warehouse_execution/test_saved_plan_restart.py (add unique persisted-layer provenance case) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_state_panel_composition_matches_full_rebuild` | 2.23 | KEEP-E2E | integration/warehouse_execution/test_saved_plan_restart.py (add unique persisted-layer provenance case) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
### `tests/dataset/test_export.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_export_syncs_shards_and_directories_before_checkpoint_and_publication` | 163.45 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_export_does_not_checkpoint_a_shard_when_fsync_fails` | 133.27 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_empty_training_export_preserves_complete_arrow_metadata[partition_by0]` | 29.47 | REWRITE | terrine.dataset.export.export_parquet; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert Arrow schema metadata via `export_parquet` on an empty pa.Table; partition layouts need not replay full model construction. |
| `test_empty_training_export_preserves_complete_arrow_metadata[partition_by1]` | 29.11 | REWRITE | terrine.dataset.export.export_parquet; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert Arrow schema metadata via `export_parquet` on an empty pa.Table; partition layouts need not replay full model construction. |
| `test_export_retry_resyncs_parent_after_interrupted_publication` | 26.98 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_parquet_export_preserves_pinned_arrow_schema_and_refuses_overwrite` | 15.16 | REWRITE | terrine.dataset.export.export_parquet; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Test schema compatibility/overwrite refusal against a tiny staged Arrow table at the export boundary. |
| `test_export_discards_unfinished_shard_and_rewrites_on_retry` | 11.39 | DELETE | DELETE; equivalent: `integration/warehouse_execution/test_ml_staging_duckdb.py::test_export_recovers_durable_shard_and_discards_interrupted_shard` | Redundant with already-tiered integration coverage; retain the integration assertion, not this duplicate. |
### `tests/dataset/test_refresh.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_refresh_nochange_reuses_only_same_definition_and_rejects_stale_base` | 148.27 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_refresh_reconstructs_recipe_without_mutating_pinned_reader` | 39.84 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_scoring_refresh_preserves_as_of_and_never_reads_unused_labels` | 35.31 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
### `tests/dataset/test_runtime_staging.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_saved_stage_replays_exact_generation_and_refuses_changed_view` | 135.71 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_staged_batches_preserve_frame_metadata_and_verify_recipe_ownership` | 52.72 | REWRITE | terrine.runtime.api.run_serialized_plan / Arrow recipe ownership; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Metadata/recipe ownership is a narrow serialization-boundary assertion; direct Arrow metadata and recipe identity suffice. |
| `test_stage_renews_lease_during_snapshot_tasks` | 24.77 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
### `tests/test_documentation.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_readme_builds_and_reopens_first_dataset` | 134.51 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_examples.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
### `tests/agent/test_materialize.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_typed_dataset_views_survive_preparation_and_staging[training]` | 47.99 | KEEP-E2E | integration/agent/test_dataset_tool_runtime.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_typed_dataset_views_survive_preparation_and_staging[scoring]` | 7.68 | KEEP-E2E | integration/agent/test_dataset_tool_runtime.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_stage_refresh_and_export_require_separate_capabilities` | 3.37 | KEEP-E2E | integration/agent/test_dataset_tool_runtime.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_partitioned_export_projects_no_partition_values[True]` | 2.89 | KEEP-E2E | integration/agent/test_dataset_tool_runtime.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_refresh_preparation_replays_before_live_inputs_or_current_base_checks[True]` | 2.52 | KEEP-E2E | integration/agent/test_dataset_tool_runtime.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_refresh_preparation_replays_before_live_inputs_or_current_base_checks[False]` | 2.01 | KEEP-E2E | integration/agent/test_dataset_tool_runtime.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_refresh_preserves_owner_approved_destination_alias` | 1.70 | KEEP-E2E | integration/agent/test_dataset_tool_runtime.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_runtime_and_concurrency_limits_are_distinct` | 1.60 | REWRITE | terrine.agent.tools.DatasetTools; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert runtime and concurrency limits are distinct directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_partitioned_export_projects_no_partition_values[False]` | 1.49 | KEEP-E2E | integration/agent/test_dataset_tool_runtime.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_row_preview_enforces_owner_budget_without_partial_output[rows-terrine.budget.rows_exceeded]` | 1.46 | REWRITE | terrine.agent.tools.DatasetTools; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert row preview enforces owner budget without partial output directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_row_preview_enforces_owner_budget_without_partial_output[bytes-terrine.budget.output_bytes_exceeded]` | 1.42 | REWRITE | terrine.agent.tools.DatasetTools; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert row preview enforces owner budget without partial output directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_preview_reads_only_bounded_batches_without_rehashing_output` | 1.31 | REWRITE | terrine.agent.tools.DatasetTools; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert preview reads only bounded batches without rehashing output directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_row_preview_byte_budget_counts_exact_projected_arrow_buffers` | 1.24 | REWRITE | terrine.agent.tools.DatasetTools; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert row preview byte budget counts exact projected arrow buffers directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_row_preview_enforces_owner_budget_without_partial_output[scan-terrine.budget.scan_unknown]` | 1.22 | REWRITE | terrine.agent.tools.DatasetTools; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert row preview enforces owner budget without partial output directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_preview_is_denied_by_default_and_bounded_redacted_when_granted` | 1.18 | REWRITE | terrine.agent.tools.DatasetTools; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert preview is denied by default and bounded redacted when granted directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/dataset/test_winsorization_recipe.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_percentile_threshold_fits_training_rows_only_and_freezes_recipe` | 42.04 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert percentile threshold fits training rows only and freezes recipe directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_validation_and_test_rows_cannot_change_fit_or_population_identity` | 35.81 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert validation and test rows cannot change fit or population identity directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_independently_fitted_features_compose_and_reopen` | 29.17 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert independently fitted features compose and reopen directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_frozen_limits_refuse_rebound_selector_dependencies[dimension-True-aggregate]` | 14.16 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert frozen limits refuse rebound selector dependencies directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_fitted_transform_refuses_different_feature_definition` | 13.56 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert fitted transform refuses different feature definition directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_fitted_limits_are_exact_under_small_decimal_context` | 10.92 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert fitted limits are exact under small decimal context directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_frozen_limits_refuse_rebound_selector_dependencies[fact-True-aggregate]` | 10.41 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert frozen limits refuse rebound selector dependencies directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_fitting_requires_declared_training_split_and_exact_numeric_feature` | 9.13 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert fitting requires declared training split and exact numeric feature directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_frozen_limits_refuse_rebound_selector_dependencies[property-True-attribute]` | 9.04 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert frozen limits refuse rebound selector dependencies directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_frozen_limits_refuse_rebound_selector_dependencies[source-False-aggregate]` | 8.85 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert frozen limits refuse rebound selector dependencies directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_fitted_transform_refuses_same_typed_rebound_metric[approximate_distinct]` | 4.17 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert fitted transform refuses same typed rebound metric directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_fitted_transform_refuses_same_typed_rebound_metric[quantile]` | 3.98 | REWRITE | terrine.dataset.transforms (fit/apply winsorization); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert fitted transform refuses same typed rebound metric directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/runtime/test_incremental_equivalence.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_same_snapshot_incremental_matches_clean_after_correction[delete]` | 31.70 | DELETE | DELETE; equivalent scenario `delete`: `integration/warehouse_execution/test_incremental_equivalence.py::test_saved_runtime_correction_matches_clean_rebuild_of_new_generation` | Redundant with already-tiered integration coverage; retain the integration assertion, not this duplicate. |
| `test_same_snapshot_incremental_matches_clean_after_correction[late_arrival]` | 25.65 | DELETE | DELETE; equivalent scenario `late_arrival`: `integration/warehouse_execution/test_incremental_equivalence.py::test_saved_runtime_correction_matches_clean_rebuild_of_new_generation` | Redundant with already-tiered integration coverage; retain the integration assertion, not this duplicate. |
| `test_same_snapshot_incremental_matches_clean_after_correction[definition_change]` | 25.43 | SLIM | runtime/test_incremental_equivalence.py; 2–3 fast representatives, remaining cases @pytest.mark.slow | Keep representative fast cases; retain complete scenario matrix in the slow tier. |
| `test_same_snapshot_incremental_matches_clean_after_correction[roster_exit]` | 22.46 | DELETE | DELETE; equivalent scenario `roster_exit`: `integration/warehouse_execution/test_incremental_equivalence.py::test_saved_runtime_correction_matches_clean_rebuild_of_new_generation` | Redundant with already-tiered integration coverage; retain the integration assertion, not this duplicate. |
| `test_same_snapshot_incremental_matches_clean_after_correction[update]` | 20.47 | DELETE | DELETE; equivalent scenario `update`: `integration/warehouse_execution/test_incremental_equivalence.py::test_saved_runtime_correction_matches_clean_rebuild_of_new_generation` | Redundant with already-tiered integration coverage; retain the integration assertion, not this duplicate. |
| `test_same_snapshot_incremental_matches_clean_after_correction[dimension_correction]` | 18.93 | SLIM | runtime/test_incremental_equivalence.py; 2–3 fast representatives, remaining cases @pytest.mark.slow | Keep representative fast cases; retain complete scenario matrix in the slow tier. |
| `test_same_snapshot_incremental_matches_clean_after_correction[readiness_advance]` | 18.92 | DELETE | DELETE; equivalent scenario `readiness_advance`: `integration/warehouse_execution/test_incremental_equivalence.py::test_saved_runtime_correction_matches_clean_rebuild_of_new_generation` | Redundant with already-tiered integration coverage; retain the integration assertion, not this duplicate. |
| `test_same_snapshot_incremental_matches_clean_after_correction[readiness_retraction]` | 17.42 | SLIM | runtime/test_incremental_equivalence.py; 2–3 fast representatives, remaining cases @pytest.mark.slow | Keep representative fast cases; retain complete scenario matrix in the slow tier. |
| `test_same_snapshot_incremental_matches_clean_after_correction[insert]` | 17.17 | DELETE | DELETE; equivalent scenario `insert`: `integration/warehouse_execution/test_incremental_equivalence.py::test_saved_runtime_correction_matches_clean_rebuild_of_new_generation` | Redundant with already-tiered integration coverage; retain the integration assertion, not this duplicate. |
| `test_ratio_sources_change_independently_match_clean_compilation[numerator]` | 9.23 | KEEP-E2E | integration/warehouse_execution/test_incremental_equivalence.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_ratio_sources_change_independently_match_clean_compilation[denominator]` | 9.08 | KEEP-E2E | integration/warehouse_execution/test_incremental_equivalence.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_event_anchor_identity_corrections_match_clean_compilation[first-insert]` | 5.83 | KEEP-E2E | integration/warehouse_execution/test_incremental_equivalence.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_same_snapshot_incremental_compilation_matches_clean_values_and_eligibility` | 5.69 | KEEP-E2E | integration/warehouse_execution/test_incremental_equivalence.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_event_anchor_identity_corrections_match_clean_compilation[nth-delete]` | 5.61 | KEEP-E2E | integration/warehouse_execution/test_incremental_equivalence.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_historical_1e16_insert_has_identical_float64_refusal` | 2.21 | KEEP-E2E | integration/warehouse_execution/test_incremental_equivalence.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_nested_selector_correction_outside_t0_window_matches_clean_frame` | 1.77 | KEEP-E2E | integration/warehouse_execution/test_incremental_equivalence.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
### `tests/dataset/test_scoring_snapshot.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_scoring_today_is_resolved_once_at_catalog_day_boundary` | 20.04 | REWRITE | terrine.runtime.snapshot scoring-view preparation; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Resolve the catalog-day boundary directly with controlled clock/catalog data; no scoring build required. |
| `test_owner_prepares_scoring_view_before_preflight_without_label_inputs` | 1.21 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
### `tests/dataset/test_split_view.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_group_split_view_matches_typed_reference_and_reconstruction` | 18.71 | REWRITE | terrine.dataset.splits; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert group split view matches typed reference and reconstruction directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_training_split_assigns_support_without_reassigning_overlap` | 16.34 | REWRITE | terrine.dataset.splits; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert training split assigns support without reassigning overlap directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/dataset/test_training_view.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_training_view_hashes_selected_feature_definition_and_parent_design` | 17.22 | REWRITE | terrine.dataset.training; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert training view hashes selected feature definition and parent design directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_incomplete_labels_drop_rows_and_keep_null_reasons` | 11.93 | REWRITE | terrine.dataset.training; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert incomplete labels drop rows and keep null reasons directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_training_projection_preserves_baseline_treatment_role` | 7.15 | REWRITE | terrine.dataset.training; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert training projection preserves baseline treatment role directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_state_panel_training_view_hashes_baseline_and_prior_state` | 6.65 | REWRITE | terrine.dataset.training; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert state panel training view hashes baseline and prior state directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_feature_definition_hash_tracks_source_timestamp_semantics` | 3.73 | REWRITE | terrine.dataset.training; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert feature definition hash tracks source timestamp semantics directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/adapters/test_dagster.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_dagster_historical_partitions_publish_distinct_owner_outputs` | 16.90 | KEEP-E2E | integration/warehouse_execution/test_orchestrator_adapters.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_dagster_same_root_failed_retry_reuses_fence_scoped_outputs` | 2.34 | DELETE | DELETE; equivalent: `integration/warehouse_execution/test_orchestrator_adapters.py::test_airflow_and_dagster_share_owner_state_and_failed_run_fence` | Redundant with already-tiered integration coverage; retain the integration assertion, not this duplicate. |
### `tests/compile/test_cohorts.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_cohort_collection_observes_fresh_snapshot_and_budget` | 16.43 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert cohort collection observes fresh snapshot and budget directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_cohort_mean_preserves_fractional_decimal_and_float_values` | 12.28 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert cohort mean preserves fractional decimal and float values directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_zero_partial_and_unavailable_cohort_denominators` | 9.67 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert zero partial and unavailable cohort denominators directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_cohort_grouping_bucket_statistic_and_denominators_match_hand_calculation` | 8.65 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert cohort grouping bucket statistic and denominators match hand calculation directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_large_zero_float_cohort_mean_remains_available` | 8.09 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert large zero float cohort mean remains available directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_float_cohort_mean_avoids_overflow_and_retains_certificate` | 6.86 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert float cohort mean avoids overflow and retains certificate directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_float_mean_certificate_covers_many_member_rounding` | 5.90 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert float mean certificate covers many member rounding directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_cohort_summary_surrogate_export_retains_age_and_denominators` | 5.21 | KEEP-E2E | integration/warehouse_execution/test_backend_capabilities.py (parent LTV relation/provenance or surrogate export composition) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_float_cohort_total_refuses_nonfinite_sum_and_certificate` | 5.17 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert float cohort total refuses nonfinite sum and certificate directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_cohort_float_total_enforces_final_tolerance` | 4.85 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert cohort float total enforces final tolerance directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_high_scale_decimal_cohort_mean_does_not_overflow_intermediate_sum` | 4.23 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert high scale decimal cohort mean does not overflow intermediate sum directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_high_scale_decimal_cohort_total_accumulates_cancellation_exactly` | 4.00 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert high scale decimal cohort total accumulates cancellation exactly directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_exact_integer_values_are_summed_before_cohort_mean_conversion` | 3.77 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert exact integer values are summed before cohort mean conversion directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_bucket_index_normalizes_timestamp_remainders_across_unix_epoch` | 3.54 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert bucket index normalizes timestamp remainders across unix epoch directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_cohort_table_reuses_parent_ltv_relation_and_provenance` | 3.45 | KEEP-E2E | integration/warehouse_execution/test_backend_capabilities.py (parent LTV relation/provenance or surrogate export composition) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_repeated_first_entry_events_count_one_cohort_member` | 3.36 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert repeated first entry events count one cohort member directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_bucket_index_respects_subsecond_origin_boundaries` | 3.12 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert bucket index respects subsecond origin boundaries directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_high_scale_decimal_cohort_total_refuses_unrepresentable_result` | 2.56 | REWRITE | terrine.compile.shapes.cohorts; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert high scale decimal cohort total refuses unrepresentable result directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/execution/test_backend_provider.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_compiler_and_execution_imports_do_not_depend_on_runtime` | 15.72 | REWRITE | terrine.execution.backend provider boundary; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert compiler and execution imports do not depend on runtime directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/runtime/test_api.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_saved_recipe_execution_reopens_retained_snapshot_for_each_task` | 15.50 | KEEP-E2E | integration/api.py (composed public/runtime path) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_custom_saved_recipe_executor_is_not_limited_by_builtin_shape_preflight` | 1.06 | REWRITE | terrine.runtime.api.run_serialized_plan; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert custom saved recipe executor is not limited by builtin shape preflight directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/dataset/test_stage_normalization.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_state_panel_stage_matches_ordinary_normalization_and_support` | 14.83 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_stage_never_collects_full_output_through_snapshot_execute` | 6.59 | REWRITE | terrine.dataset.reference.stage_frame; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert snapshot execution collection boundary with a tiny controlled executor, not full normalization parity. |
### `tests/compile/test_ltv.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_cutoff_nulls_period_error_certificate_with_its_value` | 14.14 | REWRITE | terrine.compile.shapes.ltv; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert cutoff nulls period error certificate with its value directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_cumulative_float_results_enforce_final_tolerance` | 11.34 | REWRITE | terrine.compile.shapes.ltv; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert cumulative float results enforce final tolerance directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_float_feature_certificates_survive_exact_trajectory_compilation` | 6.87 | REWRITE | terrine.compile.shapes.ltv; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert float feature certificates survive exact trajectory compilation directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_ltv_period_and_cumulative_values_match_hand_sum` | 6.53 | REWRITE | terrine.compile.shapes.ltv; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert ltv period and cumulative values match hand sum directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_trajectory_preserves_event_feature_timing_through_arrow_round_trip` | 5.26 | KEEP-E2E | integration/warehouse_execution/test_backend_capabilities.py (trajectory compilation + Arrow round-trip) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_scale_zero_decimal_cumulative_retains_wide_values` | 2.84 | REWRITE | terrine.compile.shapes.ltv; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert scale zero decimal cumulative retains wide values directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_cumulative_partitions_repeated_anchors_and_propagates_missing_period` | 1.08 | REWRITE | terrine.compile.shapes.ltv; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert cumulative partitions repeated anchors and propagates missing period directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/dataset/test_staging.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_frame_stage_publishes_and_reads_its_pinned_generation` | 13.73 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_reopen_preserves_version_one_manifest_without_frame_metadata` | 9.76 | REWRITE | terrine.dataset.reference.stage_frame / manifest reopen; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Test manifest/recipe/batch refusal directly with a minimal manifest and tiny relation; no full stage pipeline needed. |
| `test_invalid_arrow_batch_size_refuses_before_read` | 9.10 | REWRITE | terrine.dataset.reference.stage_frame / manifest reopen; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Test manifest/recipe/batch refusal directly with a minimal manifest and tiny relation; no full stage pipeline needed. |
| `test_reopen_accepts_resolved_owner_scoped_recipe_reference` | 8.38 | REWRITE | terrine.dataset.reference.stage_frame / manifest reopen; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Test manifest/recipe/batch refusal directly with a minimal manifest and tiny relation; no full stage pipeline needed. |
| `test_manifest_reopen_rejects_tampered_generation_row_count` | 8.10 | REWRITE | terrine.dataset.reference.stage_frame / manifest reopen; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Test manifest/recipe/batch refusal directly with a minimal manifest and tiny relation; no full stage pipeline needed. |
| `test_roster_only_stage_uses_executor_default_durable_outputs` | 1.23 | KEEP-E2E | integration/warehouse_execution/test_ml_staging_duckdb.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
### `tests/compile/test_winsorization.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_winsorization_uses_frozen_explicit_bounds_at_declared_stage` | 12.98 | REWRITE | terrine.dataset.transforms.bind_winsorization / apply_winsorization; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert winsorization uses frozen explicit bounds at declared stage directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/test_frame_runtime.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_shared_model_concurrent_exports_keep_independent_results` | 11.32 | KEEP-E2E | integration/warehouse_execution/test_backend_capabilities.py (frame concurrency case) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_unresolved_eligibility_refuses_export_with_only_aggregate_counts` | 1.60 | REWRITE | terrine.frame.Frame export/refusal boundary; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert unresolved eligibility refuses export with only aggregate counts directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/runtime/test_cli.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_cli_status_events_and_cancel_use_owner_ipc` | 11.04 | REWRITE | terrine.runtime.cli owner IPC and cancellation; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert cli status events and cancel use owner ipc directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_cli_run_waits_for_long_owner_execution_receipt` | 4.50 | KEEP-E2E | integration/cli.py (composed public/runtime path) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_cli_plan_refuses_unknown_scan_under_strict_budget` | 1.37 | REWRITE | terrine.runtime.cli owner IPC and cancellation; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert cli plan refuses unknown scan under strict budget directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/test_optional_dependencies.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_backend_metadata_imports_without_optional_drivers_or_dataframes` | 10.97 | REWRITE | terrine backend capability/import boundary; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert backend metadata imports without optional drivers or dataframes directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/compile/test_retention.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_retention_composes_with_bounded_repeated_shapes[schedule]` | 8.29 | REWRITE | terrine.compile.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert retention composes with bounded repeated shapes directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_retention_composes_with_bounded_repeated_shapes[event_panel]` | 7.89 | REWRITE | terrine.compile.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert retention composes with bounded repeated shapes directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_named_retention_roundtrip_frame_matches_independent_occurrences[2d]` | 7.41 | REWRITE | terrine.compile.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert named retention roundtrip frame matches independent occurrences directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_named_retention_roundtrip_frame_matches_independent_occurrences[open_ended]` | 6.99 | REWRITE | terrine.compile.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert named retention roundtrip frame matches independent occurrences directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_calendar_open_retention_history_includes_anticipation[6-False]` | 3.15 | REWRITE | terrine.compile.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert calendar open retention history includes anticipation directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_calendar_open_retention_history_includes_anticipation[5-True]` | 2.63 | REWRITE | terrine.compile.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert calendar open retention history includes anticipation directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/adapters/test_airflow.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_airflow_operator_runs_and_retries_persisted_owner_invocation` | 7.53 | DELETE | DELETE; equivalent: `integration/warehouse_execution/test_orchestrator_adapters.py::test_airflow_and_dagster_share_owner_state_and_failed_run_fence` | Redundant with already-tiered integration coverage; retain the integration assertion, not this duplicate. |
### `tests/compile/test_persisted_layers.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[1d-totals0-fresh_totals0-observed]` | 6.80 | KEEP-E2E | integration/warehouse_execution/test_saved_plan_restart.py (compiler/runtime stored-layer/frame parity) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[12h-totals1-fresh_totals1-observed]` | 6.09 | KEEP-E2E | integration/warehouse_execution/test_saved_plan_restart.py (compiler/runtime stored-layer/frame parity) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[1d-totals0-fresh_totals0-binned]` | 5.96 | KEEP-E2E | integration/warehouse_execution/test_saved_plan_restart.py (compiler/runtime stored-layer/frame parity) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[12h-totals1-fresh_totals1-binned]` | 5.49 | KEEP-E2E | integration/warehouse_execution/test_saved_plan_restart.py (compiler/runtime stored-layer/frame parity) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_persisted_shape_matrix_preserves_panel_rows_and_metadata[sequential]` | 4.01 | KEEP-E2E | integration/warehouse_execution/test_saved_plan_restart.py (compiler/runtime stored-layer/frame parity) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_layer_definition_identity_binds_catalog_and_design` | 3.79 | REWRITE | terrine.compile.layers / terrine.runtime.materialize; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert layer definition identity binds catalog and design directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_runtime_and_frame_share_unknown_eligibility_refusal` | 3.65 | KEEP-E2E | integration/warehouse_execution/test_saved_plan_restart.py (compiler/runtime stored-layer/frame parity) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_same_fact_measures_keep_filter_and_aggregation_identity` | 2.29 | REWRITE | terrine.compile.layers / terrine.runtime.materialize; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert same fact measures keep filter and aggregation identity directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_persisted_daily_prefix_anchor_reuse_matches_ordinary_frame_compiler` | 2.08 | KEEP-E2E | integration/warehouse_execution/test_saved_plan_restart.py (compiler/runtime stored-layer/frame parity) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_persisted_shape_matrix_preserves_panel_rows_and_metadata[event_panel]` | 2.06 | KEEP-E2E | integration/warehouse_execution/test_saved_plan_restart.py (compiler/runtime stored-layer/frame parity) | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
### `tests/compile/test_panels.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_sequential_anticipation_extends_control_risk_horizon` | 6.40 | REWRITE | terrine.compile.shapes.sequential / event_panel; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert sequential anticipation extends control risk horizon directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_later_initiation_at_trial_specific_exclusive_horizon_is_not_refused` | 4.05 | REWRITE | terrine.compile.shapes.sequential / event_panel; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert later initiation at trial specific exclusive horizon is not refused directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_sequential_roster_history_ignores_ineligible_anchor_rows` | 4.04 | REWRITE | terrine.compile.shapes.sequential / event_panel; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert sequential roster history ignores ineligible anchor rows directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_sequential_roster_history_uses_exclusive_trial_horizon[2025-01-10 00:00:00+00-False]` | 3.91 | REWRITE | terrine.compile.shapes.sequential / event_panel; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert sequential roster history uses exclusive trial horizon directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_sequential_later_initiation_uses_reanchored_outcome_support` | 3.80 | REWRITE | terrine.compile.shapes.sequential / event_panel; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert sequential later initiation uses reanchored outcome support directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_sequential_roster_history_uses_exclusive_trial_horizon[2025-01-09 23:59:59+00-True]` | 1.80 | REWRITE | terrine.compile.shapes.sequential / event_panel; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert sequential roster history uses exclusive trial horizon directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/compile/test_time_to_event.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_wide_exit_source_timestamp_is_not_cast_to_nanoseconds` | 5.82 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert wide exit source timestamp is not cast to nanoseconds directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_wide_exit_coverage_start_is_intersected_before_narrowing` | 5.53 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert wide exit coverage start is intersected before narrowing directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_nanosecond_wide_outcome_and_exit_coverages_do_not_narrow_out_of_range` | 5.33 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert nanosecond wide outcome and exit coverages do not narrow out of range directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_nanosecond_exit_before_outcome_coverage_is_not_ignored` | 5.33 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert nanosecond exit before outcome coverage is not ignored directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_wide_entry_source_timestamp_is_not_cast_to_nanoseconds` | 3.62 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert wide entry source timestamp is not cast to nanoseconds directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_nanosecond_event_source_accepts_utc_timestamptz_entry_source` | 3.51 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert nanosecond event source accepts utc timestamptz entry source directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_wide_entry_coverage_start_is_intersected_before_narrowing` | 2.94 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert wide entry coverage start is intersected before narrowing directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_nanosecond_delayed_entry_keeps_entry_before_outcome_coverage` | 2.55 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert nanosecond delayed entry keeps entry before outcome coverage directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_nanosecond_events_survive_time_to_event_compilation_in_any_session_timezone` | 2.23 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert nanosecond events survive time to event compilation in any session timezone directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_wide_roster_observation_end_is_intersected_before_narrowing` | 1.96 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert wide roster observation end is intersected before narrowing directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_life_table_matches_independent_pandas_risk_set_oracle` | 1.77 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert life table matches independent pandas risk set oracle directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_failure_before_non_delayed_entry_is_not_discarded` | 1.73 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert failure before non delayed entry is not discarded directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_survival_preserves_event_feature_timing_through_arrow_round_trip` | 1.60 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert survival preserves event feature timing through arrow round trip directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_event_before_declared_delayed_entry_is_prevalent` | 1.57 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert event before declared delayed entry is prevalent directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_administrative_cutoff_changes_followup_without_changing_readiness` | 1.51 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert administrative cutoff changes followup without changing readiness directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_non_delayed_entry_advance_without_prior_event_is_incomplete` | 1.32 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert non delayed entry advance without prior event is incomplete directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_nanosecond_event_after_python_microsecond_clamp_is_retained` | 1.23 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert nanosecond event after python microsecond clamp is retained directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_nanosecond_events_support_coverage_outside_nanosecond_range` | 1.07 | REWRITE | terrine.compile.shapes.time_to_event (life-table rows: shapes.life_table.aggregate_life_table); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert nanosecond events support coverage outside nanosecond range directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/agent/test_runtime_feedback.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_status_events_and_cancel_require_run_destination_ownership` | 5.53 | REWRITE | terrine.agent.tools / terrine.runtime.status; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert status events and cancel require run destination ownership directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_dataset_preview_resolves_persisted_destination_before_reading` | 1.58 | REWRITE | terrine.agent.tools / terrine.runtime.status; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert dataset preview resolves persisted destination before reading directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/temporal/test_window_aggregates.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_all_null_ratio_numerator_is_not_a_zero_denominator` | 5.37 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert all null ratio numerator is not a zero denominator directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_float_window_certifies_ordinary_history_but_refuses_cancellation` | 3.55 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert float window certifies ordinary history but refuses cancellation directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_exact_instants_half_open_boundaries_and_named_results[union_window]` | 3.33 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert exact instants half open boundaries and named results directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_complete_empty_and_all_null_remain_distinct[avg_calendar_day-mean-expected4-reasons4]` | 3.26 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert complete empty and all null remain distinct directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_complete_empty_and_all_null_remain_distinct[avg_event-mean-expected3-reasons3]` | 2.97 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert complete empty and all null remain distinct directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_complete_empty_and_all_null_remain_distinct[sum-mean-expected0-reasons0]` | 2.25 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert complete empty and all null remain distinct directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_historical_invalid_float_state_is_not_erased_by_a_later_group` | 2.13 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert historical invalid float state is not erased by a later group directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_unknown_filter_history_cancels_and_unavailable_suppresses_refusal` | 2.07 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert unknown filter history cancels and unavailable suppresses refusal directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_exact_instants_half_open_boundaries_and_named_results[native]` | 1.92 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert exact instants half open boundaries and named results directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_integer_sum_widens_without_losing_units_above_binary64_precision` | 1.78 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert integer sum widens without losing units above binary64 precision directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_count_and_conversion_ignore_unused_float_certificates` | 1.51 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert count and conversion ignore unused float certificates directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_exact_wide_history_retracts_and_retains_decimal_scale` | 1.33 | REWRITE | terrine.temporal.aggregate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert exact wide history retracts and retains decimal scale directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/runtime/test_control_ipc.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_control_returns_full_bounded_event_page` | 5.33 | REWRITE | terrine.runtime.control; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert control returns full bounded event page directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/export/test_surrogate.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_surrogate_round_trip_retains_sampled_sequential_roles` | 5.26 | REWRITE | terrine.export.surrogate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert surrogate round trip retains sampled sequential roles directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_surrogate_round_trip_retains_selected_event_relative_timing` | 3.19 | REWRITE | terrine.export.surrogate; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert surrogate round trip retains selected event relative timing directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/compile/test_state_panel.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_binned_state_periods_ignore_covariate_gap` | 4.74 | REWRITE | terrine.compile.shapes.state_panel; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert binned state periods ignore covariate gap directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/runtime/test_backend_capabilities.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_dataset_arrow_gate_precedes_read_and_export_destination_work` | 4.29 | REWRITE | terrine.backends.capabilities; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert dataset arrow gate precedes read and export destination work directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_unknown_snapshot_refuses_only_retained_execution_before_source_work` | 2.50 | REWRITE | terrine.backends.capabilities; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert unknown snapshot refuses only retained execution before source work directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_dataset_export_uses_arrow_without_native_unload_and_checks_scan_policy` | 2.18 | REWRITE | terrine.backends.capabilities; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert dataset export uses arrow without native unload and checks scan policy directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_dataset_reopen_refuses_unverified_backend_before_native_attributes` | 1.44 | REWRITE | terrine.backends.capabilities; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert dataset reopen refuses unverified backend before native attributes directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/compat/test_semantic_interop.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_native_catalog_executes_a_frame_without_increment` | 3.60 | REWRITE | terrine.compat semantic interop boundary; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert native catalog executes a frame without increment directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/simulate/test_independence.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_generator_and_reference_are_independent_of_compiler_and_warehouse` | 3.48 | KEEP-E2E | integration/independence.py (composed public/runtime path) | Retain process-isolated generator/reference parity and forbidden-import behavior; move this cross-package check out of fast tier. |
### `tests/runtime/test_snapshot.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_rosterless_event_snapshot_executes_all_required_sources[relative_metric]` | 3.37 | REWRITE | terrine.runtime.snapshot; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert rosterless event snapshot executes all required sources directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_snapshot_captures_only_sources_in_design_dependency_closure` | 2.06 | REWRITE | terrine.runtime.snapshot; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert snapshot captures only sources in design dependency closure directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_rosterless_event_snapshot_executes_all_required_sources[metric]` | 1.72 | REWRITE | terrine.runtime.snapshot; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert rosterless event snapshot executes all required sources directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/temporal/test_retention.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_bounded_retention_matches_exact_path_and_independent_pandas[True]` | 3.12 | REWRITE | terrine.temporal.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert bounded retention matches exact path and independent pandas directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_bounded_retention_preserves_submicrosecond_anchor_lower_bound[True]` | 2.79 | REWRITE | terrine.temporal.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert bounded retention preserves submicrosecond anchor lower bound directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_bounded_retention_preserves_submicrosecond_anchor_lower_bound[False]` | 2.61 | REWRITE | terrine.temporal.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert bounded retention preserves submicrosecond anchor lower bound directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_bounded_retention_matches_exact_path_and_independent_pandas[False]` | 2.57 | REWRITE | terrine.temporal.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert bounded retention matches exact path and independent pandas directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_open_ended_retention_requires_complete_source_cutoff` | 1.54 | REWRITE | terrine.temporal.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert open ended retention requires complete source cutoff directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_finite_retention_preserves_nanosecond_end_and_followup[-1-999999999-True]` | 1.16 | REWRITE | terrine.temporal.retention; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert finite retention preserves nanosecond end and followup directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/semantic/test_rules_import.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_rules_import_without_query_or_temporal_packages` | 2.92 | REWRITE | terrine.semantic.rules.calendar_average_alignment; tiny Catalog + Window in isolated import subprocess; minimal direct input: tiny Catalog + Window | Narrow no-execution-dependency import contract plus one direct alignment result; avoid broad semantic setup. |
### `tests/compile/test_columns.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_false_conjunct_excludes_unknown_without_hiding_candidate_audit` | 2.74 | REWRITE | terrine.compile.columns.compile_columns; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert false conjunct excludes unknown without hiding candidate audit directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_exact_attribute_predicates_do_not_narrow_large_integer_or_decimal` | 1.48 | REWRITE | terrine.compile.columns.compile_columns; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert exact attribute predicates do not narrow large integer or decimal directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_event_time_selector_metadata_distinguishes_known_absence_and_missing_origin` | 1.27 | REWRITE | terrine.compile.columns.compile_columns; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert event time selector metadata distinguishes known absence and missing origin directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_reobserved_null_is_known_and_duplicate_inline_observations_dedupe` | 1.21 | REWRITE | terrine.compile.columns.compile_columns; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert reobserved null is known and duplicate inline observations dedupe directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/parity/test_increment_frames.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_exposure_date_bucket_is_not_an_instant_equality_boundary` | 2.64 | REWRITE | terrine.adapters.increment / frame parity; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert exposure date bucket is not an instant equality boundary directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/temporal/test_sparse_state.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_float_prefix_certificates_enclose_exact_stored_leaves[values0]` | 2.21 | SLIM | temporal/test_sparse_state.py; 2–3 fast representatives, remaining cases @pytest.mark.slow | Keep representative fast cases; retain complete scenario matrix in the slow tier. |
| `test_float_prefix_certificates_enclose_exact_stored_leaves[values1]` | 1.66 | SLIM | temporal/test_sparse_state.py; 2–3 fast representatives, remaining cases @pytest.mark.slow | Keep representative fast cases; retain complete scenario matrix in the slow tier. |
| `test_float_prefix_certificates_enclose_exact_stored_leaves[values2]` | 1.25 | SLIM | temporal/test_sparse_state.py; 2–3 fast representatives, remaining cases @pytest.mark.slow | Keep representative fast cases; retain complete scenario matrix in the slow tier. |
| `test_float_prefix_certificates_enclose_exact_stored_leaves[values3]` | 1.15 | SLIM | temporal/test_sparse_state.py; 2–3 fast representatives, remaining cases @pytest.mark.slow | Keep representative fast cases; retain complete scenario matrix in the slow tier. |
### `tests/agent/test_plan_authorization.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_receipt_binds_dataset_recipe_change_evidence_and_base_generation` | 2.13 | KEEP-E2E | integration/agent/test_dataset_tool_runtime.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
| `test_execute_replays_before_source_freshness_and_resolves_exact_invocation` | 1.07 | KEEP-E2E | integration/agent/test_dataset_tool_runtime.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
### `tests/integration/test_calendar_panel_quantile.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_calendar_comparison_compiles_quantile_outcome_horizon` | 2.10 | KEEP-E2E | tests/integration/test_calendar_panel_quantile.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
### `tests/runtime/test_planning.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_owner_planner_persists_same_snapshot_and_manifest_by_invocation` | 2.09 | REWRITE | terrine.runtime.planning; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert owner planner persists same snapshot and manifest by invocation directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_roster_only_owner_plan_runs_without_fact_source_work` | 1.38 | REWRITE | terrine.runtime.planning; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert roster only owner plan runs without fact source work directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_owner_planning_refuses_before_staging_on_pinned_inputs[False]` | 1.36 | REWRITE | terrine.runtime.planning; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert owner planning refuses before staging on pinned inputs directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_plan_request_uses_owner_ipc_from_process_without_database_access` | 1.32 | REWRITE | terrine.runtime.planning; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert plan request uses owner ipc from process without database access directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_owner_plan_rolls_back_snapshot_pin_when_invocation_persistence_fails` | 1.04 | REWRITE | terrine.runtime.planning; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert owner plan rolls back snapshot pin when invocation persistence fails directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/compile/test_nonadditive.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_approximate_quantile_frame_matches_type7_tolerance_and_is_reproducible` | 2.04 | REWRITE | terrine.temporal.nonadditive.exact_quantile / compile columns; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert approximate quantile frame matches type7 tolerance and is reproducible directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_approximate_distinct_executes_in_duckdb_and_marks_column_metadata` | 1.60 | REWRITE | terrine.temporal.nonadditive.exact_quantile / compile columns; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert approximate distinct executes in duckdb and marks column metadata directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_min_max_and_distinct_windows_preserve_empty_and_null_rows` | 1.09 | REWRITE | terrine.temporal.nonadditive.exact_quantile / compile columns; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert min max and distinct windows preserve empty and null rows directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_quantile_frame_matches_independent_numpy_type7_and_null_reasons` | 1.07 | REWRITE | terrine.temporal.nonadditive.exact_quantile / compile columns; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert quantile frame matches independent numpy type7 and null reasons directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/integration/test_sequential_panels.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_all_shapes_build_materialize_and_arrow_round_trip` | 1.90 | KEEP-E2E | tests/integration/test_sequential_panels.py | Retain only composition that spans the named consumer/runtime path; move from fast tier. |
### `tests/compile/test_matched_t0.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_matched_t0_samples_controls_without_replacement_and_records_probability` | 1.88 | REWRITE | terrine.compile.shapes.matched_t0; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert matched t0 samples controls without replacement and records probability directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_matched_t0_convenience_lowers_policy_onto_design` | 1.33 | REWRITE | terrine.compile.shapes.matched_t0; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert matched t0 convenience lowers policy onto design directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_matched_t0_refuses_insufficient_controls_during_materialization` | 1.16 | REWRITE | terrine.compile.shapes.matched_t0; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert matched t0 refuses insufficient controls during materialization directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/compile/test_range_scaling.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_shape_outputs_preserve_range_bound_metadata[observed_state]` | 1.45 | REWRITE | terrine.compile.ranges / terrine.temporal.ranges; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert shape outputs preserve range bound metadata directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_shape_outputs_preserve_range_bound_metadata[binned_state]` | 1.34 | REWRITE | terrine.compile.ranges / terrine.temporal.ranges; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert shape outputs preserve range bound metadata directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/compile/test_event_sequence.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_sequence_compiler_excludes_known_false_eligibility_anchors` | 1.32 | REWRITE | terrine.compile.shapes.event_sequence; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert sequence compiler excludes known false eligibility anchors directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/runtime/test_imports.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_runtime_metadata_imports_do_not_require_duckdb_backend` | 1.31 | REWRITE | terrine.runtime import boundary (isolated import); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert runtime metadata imports do not require duckdb backend directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/reference/test_event_sequences.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_overlapping_event_anchors_match_independent_pandas_oracle` | 1.23 | REWRITE | tests.reference.event_sequences (independent reference oracle); minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert overlapping event anchors match independent pandas oracle directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/runtime/test_execute.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_lease_heartbeat_keeps_long_task_alive` | 1.14 | REWRITE | terrine.runtime.execute.execute_plan; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert lease heartbeat keeps long task alive directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
| `test_cancellation_serializes_with_heartbeat_persistence` | 1.13 | REWRITE | terrine.runtime.execute.execute_plan; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert cancellation serializes with heartbeat persistence directly at its owning boundary, without rebuilding the full model/dataset pipeline. |
### `tests/temporal/test_sketches.py`

| Test | seconds | class | target / owning function | rationale |
|---|---:|---|---|---|
| `test_tdigest_partition_merge_is_canonical_and_fresh_process_reproducible` | 1.07 | REWRITE | terrine.temporal.sketches; minimal direct input: tiny pa.Table / in-memory DuckDB relation | Assert tdigest partition merge is canonical and fresh process reproducible directly at its owning boundary, without rebuilding the full model/dataset pipeline. |