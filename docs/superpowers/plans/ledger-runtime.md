# Runtime, execution, adapters and agent test coverage

## Scope and decisions

Base: `feat/fast-tests` (`35de8c3`); branch: `fast-tests/runtime`.
All original assertion citations below refer to that base, before moves.
The 63 inventoried rows below cover the assigned area. No production change is retained.
Disposition totals for the inventoried rows: DELETE 0, MOVE 42, REWRITE 8, SLIM 0,
KEEP 13. One additional un-inventoried parameter (`expression=none`) moves with
the snapshot execution group, for 43 actual cases removed from the fast tier.

Ruling I: reject the six incremental DELETE proposals after side-by-side comparison.
Original `tests/runtime/test_incremental_equivalence.py:222-251` compares both audit
and eligible tables plus column metadata under float64, and checks corrected values.
The proposed twin `integration/warehouse_execution/test_incremental_equivalence.py:268-272,326-327,432-476`
compares published retained rows with an exact-integer clean rebuild, explicitly pins
`numeric_policy == "exact"`, and does not compare the float64 audit/eligibility paths.
Move, rather than delete, all nine correction scenarios. The three proposed SLIM
scenarios also require that full composition and exceeded 2 seconds in the loaded
baseline; retaining them as fast representatives cannot meet the stated boundary.
The parent agreed to MOVE. Cost if wrong: integration retains extra useful parity work.

Ruling A: reject both adapter DELETE proposals after side-by-side comparison.
Airflow original `tests/adapters/test_airflow.py:200-211` checks the operator's snapshot
identity and successful lookup; its twin at
`integration/warehouse_execution/test_orchestrator_adapters.py:395-412,438-452` does
not assert the snapshot identity. Dagster original
`tests/adapters/test_dagster.py:352-380` checks same-root invocation/request identity,
advanced fences and reuse without duplicate task execution; the same twin does not
exercise a same-root Dagster failure/retry at all. Move both originals unchanged.
Cost if wrong: modest integration duplication rather than silent assertion loss.

Ruling B: retain already-direct tiny import guards, lease/cancellation synchronization,
capability refusals and owner-planning checks instead of rewriting them to weaker
helper return assertions. The inventory is a proposal, not a required outcome.
The source-closure execution, invocation persistence, unavailable retained snapshot,
no-rehash preview and agent runtime-limit cases remain real composition checks in
integration rather than acquiring test-only library seams.

Ruling C: the parent's existing-directory requirement supersedes the inventory's
non-discoverable top-level `integration/api.py` and `integration/cli.py` names.
Their carriers are `integration/warehouse_execution/test_runtime_api.py` and
`integration/warehouse_execution/test_runtime_cli.py`.

## Measurement and failure evidence

Timing samples are **loaded**: siblings were running concurrently. The parent will
run the uncontended integrated-suite acceptance check. No wall-clock assertion was added.

- Initial prescribed `uv run pytest -q -p no:xdist tests/runtime tests/execution tests/adapters tests/agent --durations=0 --durations-min=2.0`
  failed at option parsing: project addopts still contains `-n 8` when xdist is disabled.
- Baseline command: `uv run pytest -o "addopts=-ra --strict-markers -m 'not slow'" -q -p no:xdist tests/runtime tests/execution tests/adapters tests/agent --durations=0 --durations-min=2.0`.
  Output: `397 passed, 1047 warnings in 440.20s`; command wall `444.90s` (artifact://9763).
- Intermediate same-command output: `359 passed, 345 warnings in 46.34s`;
  command wall `48.82s` (artifact://9768). Three loaded calls remained above 2s
  (source execution 2.70s, invocation persistence 2.32s, retained capability 2.12s);
  these compositions were subsequently moved too.
- Final same-command output: `354 passed, 283 warnings in 36.13s`; command wall
  `39.95s` (artifact://9781). All 1,062 test phases were below 2.0s in this
  loaded single-worker run. Do not interpret the before/after ratio as an
  uncontended performance estimate: the concurrent host workload varied.
- Static gates: `uv run ruff format terrine tests examples integration`,
  `uv run ruff check terrine tests examples integration`, `uv run ty check terrine tests`,
  `uv run tach check` all passed in the final area command (artifact://9781).
  An earlier gate caught a missing moved `replace` import and an obsolete
  `_design` import; both were corrected before that green run.
- Final collection command: `uv run pytest --collect-only -q -n 0 integration/warehouse_execution/test_runtime_api.py integration/warehouse_execution/test_runtime_cli.py integration/warehouse_execution/test_saved_plan_restart.py integration/warehouse_execution/test_incremental_equivalence.py integration/warehouse_execution/test_orchestrator_adapters.py integration/agent/test_dataset_tool_runtime.py`.
  Output: `74 tests collected in 0.72s`, including every moved case (artifact://9780).
- Moved-tier command: `uv run pytest -q -n 4 integration/warehouse_execution/test_runtime_api.py integration/warehouse_execution/test_runtime_cli.py integration/warehouse_execution/test_saved_plan_restart.py integration/warehouse_execution/test_incremental_equivalence.py integration/warehouse_execution/test_orchestrator_adapters.py integration/agent/test_dataset_tool_runtime.py`.
  Output: `74 passed, 2754 warnings in 148.63s`; command wall `149.55s`
  (artifact://9793). All optional adapter tests ran; none was skipped.
- Failure-sensitivity command: `uv run pytest -q -n 0 tests/agent/test_materialize.py tests/runtime/test_cli.py::test_cli_status_events_and_cancel_use_owner_ipc tests/runtime/test_cli.py::test_cli_plan_refuses_unknown_scan_under_strict_budget tests/runtime/test_control_ipc.py::test_control_returns_full_bounded_event_page --tb=short`.
  Temporarily replaced preview's policy budget with an unconstrained Budget, disabled
  redaction, dispatched cancel to get_run, returned 0 for CLI refusals, and capped
  control event requests at 100. Output: `8 failed, 10 warnings in 2.97s` (artifact://9765).
  Failures were respectively unredacted `(1, 2)` values, missing scan/row/byte refusals,
  missing 7-byte refusal, missing cancellation disposition, wrong exit status and
  `100 != 1000` events. All temporary production edits were reverted exactly.
- Arrow byte boundary: for two non-null int32 values without a validity bitmap,
  projected value storage is `2 * 4 = 8` bytes, exactly; no floating-point error model.
  The replacement uses literal 8/7-byte budgets and asserts success/refusal while
  leaving a large unapproved string column unprojected.

## CI selectors

- No CI selector changes required: `.github/workflows/ci.yml:20,24` already
  installs all extras and runs `uv run pytest -q -n 4 integration`. The dispatched
  warehouse job also selects `integration/warehouse_execution` at line 77.
- Airflow, Dagster and dbt cases remain in
  `integration/warehouse_execution/test_orchestrator_adapters.py`. Its former
  module-wide dbt skip is now scoped to the dbt test, so a missing dbt install
  does not hide independent Airflow/Dagster cases.
- No slow selector change: the former proposed SLIM matrix is retained in integration,
  not duplicated into a new slow matrix.

## Coverage rows

| test id | disposition | carrier test id | evidence |
|---|---|---|---|
| `tests/runtime/test_layers.py::test_saved_panel_publication_preserves_ordinary_provenance[sequential]` | MOVE | `integration/warehouse_execution/test_saved_plan_restart.py::test_saved_panel_publication_preserves_ordinary_provenance[sequential]` | Assertions preserved at the composition boundary. |
| `tests/runtime/test_layers.py::test_saved_panel_publication_preserves_ordinary_provenance[event_panel]` | MOVE | `integration/warehouse_execution/test_saved_plan_restart.py::test_saved_panel_publication_preserves_ordinary_provenance[event_panel]` | Assertions preserved at the composition boundary. |
| `tests/runtime/test_layers.py::test_state_panel_composition_matches_full_rebuild` | MOVE | `integration/warehouse_execution/test_saved_plan_restart.py::test_state_panel_composition_matches_full_rebuild` | Assertions preserved at the composition boundary. |
| `tests/agent/test_materialize.py::test_typed_dataset_views_survive_preparation_and_staging[training]` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_typed_dataset_views_survive_preparation_and_staging[training]` | Snapshot/receipt/publication composition retained; a stub would erase the persisted-reader or runtime state contract. |
| `tests/agent/test_materialize.py::test_typed_dataset_views_survive_preparation_and_staging[scoring]` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_typed_dataset_views_survive_preparation_and_staging[scoring]` | Snapshot/receipt/publication composition retained; a stub would erase the persisted-reader or runtime state contract. |
| `tests/agent/test_materialize.py::test_stage_refresh_and_export_require_separate_capabilities` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_stage_refresh_and_export_require_separate_capabilities` | Snapshot/receipt/publication composition retained; a stub would erase the persisted-reader or runtime state contract. |
| `tests/agent/test_materialize.py::test_partitioned_export_projects_no_partition_values[True]` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_partitioned_export_projects_no_partition_values[True]` | Snapshot/receipt/publication composition retained; a stub would erase the persisted-reader or runtime state contract. |
| `tests/agent/test_materialize.py::test_refresh_preparation_replays_before_live_inputs_or_current_base_checks[True]` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_refresh_preparation_replays_before_live_inputs_or_current_base_checks[True]` | Snapshot/receipt/publication composition retained; a stub would erase the persisted-reader or runtime state contract. |
| `tests/agent/test_materialize.py::test_refresh_preparation_replays_before_live_inputs_or_current_base_checks[False]` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_refresh_preparation_replays_before_live_inputs_or_current_base_checks[False]` | Snapshot/receipt/publication composition retained; a stub would erase the persisted-reader or runtime state contract. |
| `tests/agent/test_materialize.py::test_refresh_preserves_owner_approved_destination_alias` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_refresh_preserves_owner_approved_destination_alias` | Snapshot/receipt/publication composition retained; a stub would erase the persisted-reader or runtime state contract. |
| `tests/agent/test_materialize.py::test_runtime_and_concurrency_limits_are_distinct` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_runtime_and_concurrency_limits_are_distinct` | Snapshot/receipt/publication composition retained; a stub would erase the persisted-reader or runtime state contract. |
| `tests/agent/test_materialize.py::test_partitioned_export_projects_no_partition_values[False]` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_partitioned_export_projects_no_partition_values[False]` | Snapshot/receipt/publication composition retained; a stub would erase the persisted-reader or runtime state contract. |
| `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[rows-terrine.budget.rows_exceeded]` | REWRITE | `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[rows-terrine.budget.rows_exceeded]` | Pinned two-row Arrow reader; same values, refusal codes, closure, redaction and budget boundary. Red: disable policy budget/redaction; replacement fails (artifact://9765). |
| `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[bytes-terrine.budget.output_bytes_exceeded]` | REWRITE | `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[bytes-terrine.budget.output_bytes_exceeded]` | Pinned two-row Arrow reader; same values, refusal codes, closure, redaction and budget boundary. Red: disable policy budget/redaction; replacement fails (artifact://9765). |
| `tests/agent/test_materialize.py::test_preview_reads_only_bounded_batches_without_rehashing_output` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_preview_reads_only_bounded_batches_without_rehashing_output` | Snapshot/receipt/publication composition retained; a stub would erase the persisted-reader or runtime state contract. |
| `tests/agent/test_materialize.py::test_row_preview_byte_budget_counts_exact_projected_arrow_buffers` | REWRITE | `tests/agent/test_materialize.py::test_row_preview_byte_budget_counts_exact_projected_arrow_buffers` | Pinned two-row Arrow reader; same values, refusal codes, closure, redaction and budget boundary. Red: disable policy budget/redaction; replacement fails (artifact://9765). |
| `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[scan-terrine.budget.scan_unknown]` | REWRITE | `tests/agent/test_materialize.py::test_row_preview_enforces_owner_budget_without_partial_output[scan-terrine.budget.scan_unknown]` | Pinned two-row Arrow reader; same values, refusal codes, closure, redaction and budget boundary. Red: disable policy budget/redaction; replacement fails (artifact://9765). |
| `tests/agent/test_materialize.py::test_preview_is_denied_by_default_and_bounded_redacted_when_granted` | REWRITE | `tests/agent/test_materialize.py::test_preview_is_denied_by_default_and_bounded_redacted_when_granted` | Pinned two-row Arrow reader; same values, refusal codes, closure, redaction and budget boundary. Red: disable policy budget/redaction; replacement fails (artifact://9765). |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[delete]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[delete]` | Ruling I: original float64 audit/eligibility parity is not covered by the integer-only twin; retain unchanged in integration. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[late_arrival]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[late_arrival]` | Ruling I: original float64 audit/eligibility parity is not covered by the integer-only twin; retain unchanged in integration. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[definition_change]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[definition_change]` | Ruling I: original float64 audit/eligibility parity is not covered by the integer-only twin; retain unchanged in integration. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[roster_exit]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[roster_exit]` | Ruling I: original float64 audit/eligibility parity is not covered by the integer-only twin; retain unchanged in integration. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[update]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[update]` | Ruling I: original float64 audit/eligibility parity is not covered by the integer-only twin; retain unchanged in integration. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[dimension_correction]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[dimension_correction]` | Ruling I: original float64 audit/eligibility parity is not covered by the integer-only twin; retain unchanged in integration. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[readiness_advance]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[readiness_advance]` | Ruling I: original float64 audit/eligibility parity is not covered by the integer-only twin; retain unchanged in integration. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[readiness_retraction]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[readiness_retraction]` | Ruling I: original float64 audit/eligibility parity is not covered by the integer-only twin; retain unchanged in integration. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[insert]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_same_snapshot_incremental_matches_clean_after_correction[insert]` | Ruling I: original float64 audit/eligibility parity is not covered by the integer-only twin; retain unchanged in integration. |
| `tests/runtime/test_incremental_equivalence.py::test_ratio_sources_change_independently_match_clean_compilation[numerator]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_ratio_sources_change_independently_match_clean_compilation[numerator]` | Assertions preserved at the composition boundary. |
| `tests/runtime/test_incremental_equivalence.py::test_ratio_sources_change_independently_match_clean_compilation[denominator]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_ratio_sources_change_independently_match_clean_compilation[denominator]` | Assertions preserved at the composition boundary. |
| `tests/runtime/test_incremental_equivalence.py::test_event_anchor_identity_corrections_match_clean_compilation[first-insert]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_event_anchor_identity_corrections_match_clean_compilation[first-insert]` | Assertions preserved at the composition boundary. |
| `tests/runtime/test_incremental_equivalence.py::test_same_snapshot_incremental_compilation_matches_clean_values_and_eligibility` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_same_snapshot_incremental_compilation_matches_clean_values_and_eligibility` | Assertions preserved at the composition boundary. |
| `tests/runtime/test_incremental_equivalence.py::test_event_anchor_identity_corrections_match_clean_compilation[nth-delete]` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_event_anchor_identity_corrections_match_clean_compilation[nth-delete]` | Assertions preserved at the composition boundary. |
| `tests/runtime/test_incremental_equivalence.py::test_historical_1e16_insert_has_identical_float64_refusal` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_historical_1e16_insert_has_identical_float64_refusal` | Assertions preserved at the composition boundary. |
| `tests/runtime/test_incremental_equivalence.py::test_nested_selector_correction_outside_t0_window_matches_clean_frame` | MOVE | `integration/warehouse_execution/test_incremental_equivalence.py::test_nested_selector_correction_outside_t0_window_matches_clean_frame` | Assertions preserved at the composition boundary. |
| `tests/adapters/test_dagster.py::test_dagster_historical_partitions_publish_distinct_owner_outputs` | MOVE | `integration/warehouse_execution/test_orchestrator_adapters.py::test_dagster_historical_partitions_publish_distinct_owner_outputs` | Ruling A: original snapshot or same-root retry assertions are not all in the cited twin; retain unchanged in integration. |
| `tests/adapters/test_dagster.py::test_dagster_same_root_failed_retry_reuses_fence_scoped_outputs` | MOVE | `integration/warehouse_execution/test_orchestrator_adapters.py::test_dagster_same_root_failed_retry_reuses_fence_scoped_outputs` | Ruling A: original snapshot or same-root retry assertions are not all in the cited twin; retain unchanged in integration. |
| `tests/execution/test_backend_provider.py::test_compiler_and_execution_imports_do_not_depend_on_runtime` | KEEP | `tests/execution/test_backend_provider.py::test_compiler_and_execution_imports_do_not_depend_on_runtime` | Already one fresh-process import guard: process isolation is the behavior, not incidental setup. |
| `tests/runtime/test_api.py::test_saved_recipe_execution_reopens_retained_snapshot_for_each_task` | MOVE | `integration/warehouse_execution/test_runtime_api.py::test_saved_recipe_execution_reopens_retained_snapshot_for_each_task` | Retained snapshots, failed retry, replay, published rows and provenance remain composed. |
| `tests/runtime/test_api.py::test_custom_saved_recipe_executor_is_not_limited_by_builtin_shape_preflight` | KEEP | `tests/runtime/test_api.py::test_custom_saved_recipe_executor_is_not_limited_by_builtin_shape_preflight` | Existing tiny owner-boundary test already exercises the owning operation; no narrower substitute preserves its public outcome. |
| `tests/runtime/test_cli.py::test_cli_status_events_and_cancel_use_owner_ipc` | REWRITE | `tests/runtime/test_cli.py::test_cli_status_events_and_cancel_use_owner_ipc` | Real CLI main/parser and owner IPC, without repeated interpreter startup; all original output assertions retained. Red: wrong cancel dispatch / refusal exit status (artifact://9765). |
| `tests/runtime/test_cli.py::test_cli_run_waits_for_long_owner_execution_receipt` | MOVE | `integration/warehouse_execution/test_runtime_cli.py::test_cli_run_waits_for_long_owner_execution_receipt` | Real process and synchronized long-owner receipt retained. |
| `tests/runtime/test_cli.py::test_cli_plan_refuses_unknown_scan_under_strict_budget` | REWRITE | `tests/runtime/test_cli.py::test_cli_plan_refuses_unknown_scan_under_strict_budget` | Real CLI main/parser and owner IPC, without repeated interpreter startup; all original output assertions retained. Red: wrong cancel dispatch / refusal exit status (artifact://9765). |
| `tests/adapters/test_airflow.py::test_airflow_operator_runs_and_retries_persisted_owner_invocation` | MOVE | `integration/warehouse_execution/test_orchestrator_adapters.py::test_airflow_operator_runs_and_retries_persisted_owner_invocation` | Ruling A: original snapshot or same-root retry assertions are not all in the cited twin; retain unchanged in integration. |
| `tests/agent/test_runtime_feedback.py::test_status_events_and_cancel_require_run_destination_ownership` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_status_events_and_cancel_require_run_destination_ownership` | Assertions preserved at the composition boundary. |
| `tests/agent/test_runtime_feedback.py::test_dataset_preview_resolves_persisted_destination_before_reading` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_dataset_preview_resolves_persisted_destination_before_reading` | Assertions preserved at the composition boundary. |
| `tests/runtime/test_control_ipc.py::test_control_returns_full_bounded_event_page` | REWRITE | `tests/runtime/test_control_ipc.py::test_control_returns_full_bounded_event_page` | Same 1,000-event full transport page; bulk seed persisted events rather than 1,000 producer transactions. Red: client truncation to 100 fails (artifact://9765). |
| `tests/runtime/test_backend_capabilities.py::test_dataset_arrow_gate_precedes_read_and_export_destination_work` | KEEP | `tests/runtime/test_backend_capabilities.py::test_dataset_arrow_gate_precedes_read_and_export_destination_work` | Existing tiny owner-boundary test already exercises the owning operation; no narrower substitute preserves its public outcome. |
| `tests/runtime/test_backend_capabilities.py::test_unknown_snapshot_refuses_only_retained_execution_before_source_work` | MOVE | `integration/warehouse_execution/test_runtime_api.py::test_unknown_snapshot_refuses_only_retained_execution_before_source_work` | Must prove ordinary materialization still succeeds while retained capture refuses before source/filesystem work; keep composition. |
| `tests/runtime/test_backend_capabilities.py::test_dataset_export_uses_arrow_without_native_unload_and_checks_scan_policy` | KEEP | `tests/runtime/test_backend_capabilities.py::test_dataset_export_uses_arrow_without_native_unload_and_checks_scan_policy` | Existing tiny owner-boundary test already exercises the owning operation; no narrower substitute preserves its public outcome. |
| `tests/runtime/test_backend_capabilities.py::test_dataset_reopen_refuses_unverified_backend_before_native_attributes` | KEEP | `tests/runtime/test_backend_capabilities.py::test_dataset_reopen_refuses_unverified_backend_before_native_attributes` | Existing tiny owner-boundary test already exercises the owning operation; no narrower substitute preserves its public outcome. |
| `tests/runtime/test_snapshot.py::test_rosterless_event_snapshot_executes_all_required_sources[relative_metric]` | MOVE | `integration/warehouse_execution/test_runtime_api.py::test_rosterless_event_snapshot_executes_all_required_sources[relative_metric]` | Capture, live-source deletion, retained reopen and full compiled values cannot be reduced to root-discovery output. |
| `tests/runtime/test_snapshot.py::test_snapshot_captures_only_sources_in_design_dependency_closure` | KEEP | `tests/runtime/test_snapshot.py::test_snapshot_captures_only_sources_in_design_dependency_closure` | Existing tiny owner-boundary test already exercises the owning operation; no narrower substitute preserves its public outcome. |
| `tests/runtime/test_snapshot.py::test_rosterless_event_snapshot_executes_all_required_sources[metric]` | MOVE | `integration/warehouse_execution/test_runtime_api.py::test_rosterless_event_snapshot_executes_all_required_sources[metric]` | Capture, live-source deletion, retained reopen and full compiled values cannot be reduced to root-discovery output. |
| `tests/agent/test_plan_authorization.py::test_receipt_binds_dataset_recipe_change_evidence_and_base_generation` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_receipt_binds_dataset_recipe_change_evidence_and_base_generation` | Assertions preserved at the composition boundary. |
| `tests/agent/test_plan_authorization.py::test_execute_replays_before_source_freshness_and_resolves_exact_invocation` | MOVE | `integration/agent/test_dataset_tool_runtime.py::test_execute_replays_before_source_freshness_and_resolves_exact_invocation` | Assertions preserved at the composition boundary. |
| `tests/runtime/test_planning.py::test_owner_planner_persists_same_snapshot_and_manifest_by_invocation` | MOVE | `integration/warehouse_execution/test_runtime_api.py::test_owner_planner_persists_same_snapshot_and_manifest_by_invocation` | Invocation identity, retained snapshot, output validation and manifest binding require real owner composition. |
| `tests/runtime/test_planning.py::test_roster_only_owner_plan_runs_without_fact_source_work` | KEEP | `tests/runtime/test_planning.py::test_roster_only_owner_plan_runs_without_fact_source_work` | Existing tiny owner-boundary test already exercises the owning operation; no narrower substitute preserves its public outcome. |
| `tests/runtime/test_planning.py::test_owner_planning_refuses_before_staging_on_pinned_inputs[False]` | KEEP | `tests/runtime/test_planning.py::test_owner_planning_refuses_before_staging_on_pinned_inputs[False]` | Existing tiny owner-boundary test already exercises the owning operation; no narrower substitute preserves its public outcome. |
| `tests/runtime/test_planning.py::test_plan_request_uses_owner_ipc_from_process_without_database_access` | KEEP | `tests/runtime/test_planning.py::test_plan_request_uses_owner_ipc_from_process_without_database_access` | Existing tiny owner-boundary test already exercises the owning operation; no narrower substitute preserves its public outcome. |
| `tests/runtime/test_planning.py::test_owner_plan_rolls_back_snapshot_pin_when_invocation_persistence_fails` | KEEP | `tests/runtime/test_planning.py::test_owner_plan_rolls_back_snapshot_pin_when_invocation_persistence_fails` | Existing tiny owner-boundary test already exercises the owning operation; no narrower substitute preserves its public outcome. |
| `tests/runtime/test_imports.py::test_runtime_metadata_imports_do_not_require_duckdb_backend` | KEEP | `tests/runtime/test_imports.py::test_runtime_metadata_imports_do_not_require_duckdb_backend` | Already one fresh-process import guard: process isolation is the behavior, not incidental setup. |
| `tests/runtime/test_execute.py::test_lease_heartbeat_keeps_long_task_alive` | KEEP | `tests/runtime/test_execute.py::test_lease_heartbeat_keeps_long_task_alive` | Already tiny executor with real lease/cancellation synchronization; reducing work would remove heartbeat behavior. |
| `tests/runtime/test_execute.py::test_cancellation_serializes_with_heartbeat_persistence` | KEEP | `tests/runtime/test_execute.py::test_cancellation_serializes_with_heartbeat_persistence` | Already tiny executor with real lease/cancellation synchronization; reducing work would remove heartbeat behavior. |

## Commit validation

- `9fbe5e6` — persisted-layer parity moves; all four static gates passed before
  commit. The complete six-file moved-tier run above passed before this commit.
- `6d96260` — direct CLI entrypoint tests and owner composition moves. The four
  static gates passed; `uv run pytest -q -n 4 tests/runtime/test_api.py tests/runtime/test_cli.py tests/runtime/test_control_ipc.py tests/runtime/test_snapshot.py tests/runtime/test_planning.py tests/runtime/test_backend_capabilities.py integration/warehouse_execution/test_runtime_api.py integration/warehouse_execution/test_runtime_cli.py`
  returned `61 passed, 257 warnings in 16.52s` (artifact://9806).
- `a152656` — orchestrator retry and historical publication moves. All four static
  gates passed; `uv run pytest -q -n 4 tests/adapters integration/warehouse_execution/test_orchestrator_adapters.py`
  returned `21 passed, 197 warnings in 38.70s` (artifact://9813).
- Automatic review 9123 (`9fbe5e6`) passed with no actionable issues; inspected,
  commented and closed.
- Agent group: all four static gates passed;
  `uv run pytest -q -n 4 tests/agent integration/agent/test_dataset_tool_runtime.py`
  returned `81 passed, 557 warnings in 30.13s` (artifact://9823).
- Automatic reviews 9124 (`6d96260`) and 9125 (`a152656`) passed with no actionable
  issues; both inspected, commented and closed.
