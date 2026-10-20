# Compiler and temporal test dispositions

## Scope and measurement model

Area: `tests/compile`, `tests/temporal`, `tests/simulate`, `tests/semantic`, `tests/compat`, `tests/parity`, `tests/reference`, and the in-tests `tests/integration` folder. There are 111 inventoried cases in this area. Updated dispositions after the integrated duration pass: **0 DELETE, 20 MOVE, 58 REWRITE, 4 SLIM, 29 KEEP**. No library changes or shared ledger/fixture/CI changes are included in this area's commits.

Ruling: inventory rows are proposals, not mandates. Retain tests already at their owning function on tiny inputs, and retain actual optional-dependency/process isolation and independent-path parity rather than replacing these with weaker helper assertions. The final uncontended duration gate belongs to the integrator.

Ruling (parent, 2026-10-19): sibling workers are running concurrently and no quiet window exists until all finish. All observations here are **loaded**; rank relative costs within the run. The parent will perform the single uncontended integrated-suite timing pass. Do not interpret loaded per-test samples as uncontended acceptance evidence.

The first command accidentally cleared the slow exclusion and was cancelled; it is not a measurement. Explicit `-o addopts='' -p no:xdist -m 'not slow'` preserves the ordinary tier without passing the default `-n 8` to a disabled plugin.

## Numerical contracts and decisions

For each cohort bucket/age, deduplicate members by unit. `at_risk` counts age-zero entrants, `observable` counts complete member values, and a result is available only when mature and every entrant value is available. The mean is `sum(x_i) / n`. Integer and decimal inputs accumulate before conversion; checked floating means use scaled reduction and publish an absolute-error certificate enclosing the exact stored-input result. Refuse when the certificate exceeds the library tolerance. Tests retain cancellation, near-overflow, decimal scale, exact reasons, and many-member/large-zero cases.

Ruling: cohort reductions receive a real, already-compiled LTV-shaped relation rather than reconstructing event prefixes for already-known member values. The many-member and large-zero cardinalities remain because the member-count-dependent arithmetic is the contract. Upstream trajectory values still have independent hand-sum tests. The decimal cancellation fixture now actually contains positive and negative members.

Ruling: snapshot freshness, budget enforcement, parent provenance, first-entry anchor deduplication, surrogate export, winsorization stage/expr refusal, and retention serialization/provenance remain composition tests. Move their assertions rather than replacing them with a helper return value that cannot publish their metadata. Existing library boundaries do not expose all those results without the composition; no new library seam is introduced.

Ruling: survival collection uses the existing compiler validation and `Frame._normalize_output` on the actual Arrow result. Direct compiler execution alone omits the public UTC normalization. The first functional replacement run caught six timezone-contract failures (`6 failed, 97 passed`); tracing `Frame._materialize` to `_normalize_output` identified the missing production step. No timestamp casts or expected-value weakening were copied into tests.

Ruling: aggregate fixtures compute their real sparse prefix once into a fresh per-test native table. The reduction then reads that physical state, instead of recomputing the same lazy prefix for every deferred check and output query. Native BIGNUM coefficients are not narrowed through Arrow. Prefix arithmetic retains its own direct suite; no query, deferred check, or safety rule is skipped.

Ruling: the inventory's in-tests integration filenames are not a separate tier. Their public composition cases move to the already-collected root backend-capability integration file. The independence test uses `integration/test_independence.py`, not the inventory's illustrative `integration/independence.py`, so recursive pytest discovery collects it without a special selector.

## Executed red checks

Temporary process-local mutations wrap or recompile the real production owner, run pytest against the replacements, and restore every patched function in `finally`. Nothing is patched in the committed library. Probe command for each mode:

```sh
env PYTHONPATH=. uv run python /tmp/terrine_compile_mutations.py MODE
```

The scratch runner is removed after verification. These modes describe the complete mutations, not permanent test fixtures:

| mode | temporarily broken production rule | observed pytest output |
|---|---|---|
| cohorts | Replace cohort value/bucket reductions with constants, zero the certificate, and omit final numeric checks. | 14 failed |
| ltv | Replace period/cumulative values and period certificate, remove checked feature policy, and omit final numeric checks. | 6 failed, 1 deselected |
| survival_empty | Drop all rows at the actual survival compiler return. | 16 failed, 3 passed, 10 deselected |
| survival_prevalence | Remove `retained = result.filter(~prevalent)` filtering. | 1 failed |
| survival_utc | Omit existing `Frame._normalize_output` UTC normalization. | 1 failed |
| aggregate | Replace final aggregate values and omit deferred scalar checks. | 25 failed, 1 passed, 13 deselected |
| retention | Publish out-of-domain retention bands from compile_retention. | 2 failed, 4 deselected |
| state_gap | Ignore explicit zero gap for binned state periods. | 1 failed |
| panel_checks | Omit sequential deferred history/initiation checks. | 2 failed, 2 passed, 14 deselected |
| panel_horizon | Extend trial-specific follow-up by a day. | 2 failed, 2 passed, 14 deselected |
| panel_eligibility | Include all candidate anchors rather than eligible anchors in unit-history validation. | 1 failed |

Parsing the actual failed node IDs and comparing them with every REWRITE row returned `Rewritten inventory rows without observed failure: []`.

## Coverage rows

| test id | disposition | carrier test id | evidence |
|---|---|---|---|
| `tests/compile/test_cohorts.py::test_cohort_collection_observes_fresh_snapshot_and_budget` | MOVE | `integration/warehouse_execution/test_backend_capabilities.py::test_cohort_collection_observes_fresh_snapshot_and_budget` | Original assertions at test_cohorts.py:77-86 retained; freshness, budget, and published provenance require collection. Collected in the 24-case focused integration selection. |
| `tests/compile/test_cohorts.py::test_cohort_mean_preserves_fractional_decimal_and_float_values` | REWRITE | `tests/compile/test_cohorts.py::test_cohort_mean_preserves_fractional_decimal_and_float_values` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_zero_partial_and_unavailable_cohort_denominators` | REWRITE | `tests/compile/test_cohorts.py::test_zero_partial_and_unavailable_cohort_denominators` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_cohort_grouping_bucket_statistic_and_denominators_match_hand_calculation` | REWRITE | `tests/compile/test_cohorts.py::test_cohort_grouping_bucket_statistic_and_denominators_match_hand_calculation` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_large_zero_float_cohort_mean_remains_available` | REWRITE | `tests/compile/test_cohorts.py::test_large_zero_float_cohort_mean_remains_available` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_float_cohort_mean_avoids_overflow_and_retains_certificate` | REWRITE | `tests/compile/test_cohorts.py::test_float_cohort_mean_avoids_overflow_and_retains_certificate` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_float_mean_certificate_covers_many_member_rounding` | REWRITE | `tests/compile/test_cohorts.py::test_float_mean_certificate_covers_many_member_rounding` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_cohort_summary_surrogate_export_retains_age_and_denominators` | MOVE | `integration/warehouse_execution/test_backend_capabilities.py::test_cohort_summary_surrogate_export_retains_age_and_denominators` | Original assertions at test_cohorts.py:482-483 retained; surrogate export requires published frame metadata. Collected in the 24-case focused integration selection. |
| `tests/compile/test_cohorts.py::test_float_cohort_total_refuses_nonfinite_sum_and_certificate` | REWRITE | `tests/compile/test_cohorts.py::test_float_cohort_total_refuses_nonfinite_sum_and_certificate` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_cohort_float_total_enforces_final_tolerance` | REWRITE | `tests/compile/test_cohorts.py::test_cohort_float_total_enforces_final_tolerance` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_mean_does_not_overflow_intermediate_sum` | REWRITE | `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_mean_does_not_overflow_intermediate_sum` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_total_accumulates_cancellation_exactly` | REWRITE | `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_total_accumulates_cancellation_exactly` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_exact_integer_values_are_summed_before_cohort_mean_conversion` | REWRITE | `tests/compile/test_cohorts.py::test_exact_integer_values_are_summed_before_cohort_mean_conversion` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_bucket_index_normalizes_timestamp_remainders_across_unix_epoch` | REWRITE | `tests/compile/test_cohorts.py::test_bucket_index_normalizes_timestamp_remainders_across_unix_epoch` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_cohort_table_reuses_parent_ltv_relation_and_provenance` | MOVE | `integration/warehouse_execution/test_backend_capabilities.py::test_cohort_table_reuses_parent_ltv_relation_and_provenance` | Original assertions at test_cohorts.py:106-111 retained; parent identity and operation provenance are composition. Collected in the 24-case focused integration selection. |
| `tests/compile/test_cohorts.py::test_repeated_first_entry_events_count_one_cohort_member` | MOVE | `integration/warehouse_execution/test_backend_capabilities.py::test_repeated_first_entry_events_count_one_cohort_member` | Original assertion at test_cohorts.py:169-170 retained; first-event anchor generation is not a cohort reduction rule. Collected in the 24-case focused integration selection. |
| `tests/compile/test_cohorts.py::test_bucket_index_respects_subsecond_origin_boundaries` | REWRITE | `tests/compile/test_cohorts.py::test_bucket_index_respects_subsecond_origin_boundaries` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_total_refuses_unrepresentable_result` | REWRITE | `tests/compile/test_cohorts.py::test_high_scale_decimal_cohort_total_refuses_unrepresentable_result` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: cohorts. |
| `tests/compile/test_ltv.py::test_cutoff_nulls_period_error_certificate_with_its_value` | REWRITE | `tests/compile/test_ltv.py::test_cutoff_nulls_period_error_certificate_with_its_value` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: ltv. |
| `tests/compile/test_ltv.py::test_cumulative_float_results_enforce_final_tolerance` | REWRITE | `tests/compile/test_ltv.py::test_cumulative_float_results_enforce_final_tolerance` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: ltv. |
| `tests/compile/test_ltv.py::test_float_feature_certificates_survive_exact_trajectory_compilation` | REWRITE | `tests/compile/test_ltv.py::test_float_feature_certificates_survive_exact_trajectory_compilation` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: ltv. |
| `tests/compile/test_ltv.py::test_ltv_period_and_cumulative_values_match_hand_sum` | REWRITE | `tests/compile/test_ltv.py::test_ltv_period_and_cumulative_values_match_hand_sum` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: ltv. |
| `tests/compile/test_ltv.py::test_trajectory_preserves_event_feature_timing_through_arrow_round_trip` | MOVE | `integration/warehouse_execution/test_backend_capabilities.py::test_trajectory_preserves_event_feature_timing_through_arrow_round_trip` | Original assertions at test_ltv.py:291-300 retained; portable provenance is an Arrow/public-frame contract. Collected in the 24-case focused integration selection. |
| `tests/compile/test_ltv.py::test_scale_zero_decimal_cumulative_retains_wide_values` | REWRITE | `tests/compile/test_ltv.py::test_scale_zero_decimal_cumulative_retains_wide_values` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: ltv. |
| `tests/compile/test_ltv.py::test_cumulative_partitions_repeated_anchors_and_propagates_missing_period` | KEEP | `tests/compile/test_ltv.py::test_cumulative_partitions_repeated_anchors_and_propagates_missing_period` | Assertions unchanged; already direct compilation or unique consumer Arrow/provenance coverage. |
| `tests/compile/test_winsorization.py::test_winsorization_uses_frozen_explicit_bounds_at_declared_stage` | MOVE | `integration/warehouse_execution/test_backend_capabilities.py::test_winsorization_uses_frozen_explicit_bounds_at_declared_stage` | Original assertions at test_winsorization.py:23-37 retained, including metadata and expr refusal; apply_winsorization alone cannot assert these. Collected in the 24-case focused integration selection. |
| `tests/compile/test_retention.py::test_retention_composes_with_bounded_repeated_shapes[schedule]` | REWRITE | `tests/compile/test_retention.py::test_retention_composes_with_bounded_repeated_shapes[schedule]` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: retention. |
| `tests/compile/test_retention.py::test_retention_composes_with_bounded_repeated_shapes[event_panel]` | REWRITE | `tests/compile/test_retention.py::test_retention_composes_with_bounded_repeated_shapes[event_panel]` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: retention. |
| `tests/compile/test_retention.py::test_named_retention_roundtrip_frame_matches_independent_occurrences[2d]` | MOVE | `integration/warehouse_execution/test_backend_capabilities.py::test_named_retention_roundtrip_frame_matches_independent_occurrences[2d]` | Original assertions at test_retention.py:109-147 retained, including serialization, independent oracle, exact reasons, and portable provenance. Collected in the 24-case focused integration selection. |
| `tests/compile/test_retention.py::test_named_retention_roundtrip_frame_matches_independent_occurrences[open_ended]` | MOVE | `integration/warehouse_execution/test_backend_capabilities.py::test_named_retention_roundtrip_frame_matches_independent_occurrences[open_ended]` | Original assertions at test_retention.py:109-147 retained, including serialization, independent oracle, exact reasons, and portable provenance. Collected in the 24-case focused integration selection. |
| `tests/compile/test_retention.py::test_calendar_open_retention_history_includes_anticipation[6-False]` | REWRITE | `tests/compile/test_retention.py::test_calendar_open_retention_history_includes_anticipation[6-False]` | Retain acceptance at the exact exclusive history bound using the owning history check, real roster rows, and the retention expression's actual outcome cutoff. Integrated follow-up below records the red check. |
| `tests/compile/test_retention.py::test_calendar_open_retention_history_includes_anticipation[5-True]` | REWRITE | `tests/compile/test_retention.py::test_calendar_open_retention_history_includes_anticipation[5-True]` | Retain the incomplete-unit-treatment-history refusal and anticipation horizon using the owning history check on real roster rows. Integrated follow-up below records the red check. |
| `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[1d-totals0-fresh_totals0-observed]` | MOVE | `integration/warehouse_execution/test_saved_plan_restart.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[1d-totals0-fresh_totals0-observed]` | Original assertions at test_persisted_layers.py:580-607 retained, including mutation isolation, frozen values, fresh values, identities, and metadata. Collected in the 24-case focused integration selection. |
| `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[12h-totals1-fresh_totals1-observed]` | MOVE | `integration/warehouse_execution/test_saved_plan_restart.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[12h-totals1-fresh_totals1-observed]` | Original assertions at test_persisted_layers.py:580-607 retained, including mutation isolation, frozen values, fresh values, identities, and metadata. Collected in the 24-case focused integration selection. |
| `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[1d-totals0-fresh_totals0-binned]` | MOVE | `integration/warehouse_execution/test_saved_plan_restart.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[1d-totals0-fresh_totals0-binned]` | Original assertions at test_persisted_layers.py:580-607 retained, including mutation isolation, frozen values, fresh values, identities, and metadata. Collected in the 24-case focused integration selection. |
| `tests/compile/test_persisted_layers.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[12h-totals1-fresh_totals1-binned]` | MOVE | `integration/warehouse_execution/test_saved_plan_restart.py::test_state_panel_consumes_frozen_prefixes_and_scheduled_anchors[12h-totals1-fresh_totals1-binned]` | Original assertions at test_persisted_layers.py:580-607 retained, including mutation isolation, frozen values, fresh values, identities, and metadata. Collected in the 24-case focused integration selection. |
| `tests/compile/test_persisted_layers.py::test_persisted_shape_matrix_preserves_panel_rows_and_metadata[sequential]` | MOVE | `integration/warehouse_execution/test_saved_plan_restart.py::test_persisted_shape_matrix_preserves_panel_rows_and_metadata[sequential]` | Original assertions at test_persisted_layers.py:461-468 retained; persisted and ordinary compiler paths keep all row/metadata comparisons. Collected in the 24-case focused integration selection. |
| `tests/compile/test_persisted_layers.py::test_layer_definition_identity_binds_catalog_and_design` | REWRITE | `tests/compile/test_persisted_layers.py::test_layer_definition_identity_binds_catalog_and_design`; `tests/compile/test_persisted_layers.py::test_runtime_layer_composition_matches_direct_state_audit` | Preserve every original identity/refusal/parity assertion; separate catalog-identity behavior from runtime state-audit parity so each test has one owning contract. Integrated follow-up below records both red checks. |
| `tests/compile/test_persisted_layers.py::test_runtime_and_frame_share_unknown_eligibility_refusal` | MOVE | `integration/warehouse_execution/test_saved_plan_restart.py::test_runtime_and_frame_share_unknown_eligibility_refusal` | Original assertions at test_persisted_layers.py:343-344 retained; runtime/public-frame code and context parity is composition. Collected in the 24-case focused integration selection. |
| `tests/compile/test_persisted_layers.py::test_same_fact_measures_keep_filter_and_aggregation_identity` | KEEP | `tests/compile/test_persisted_layers.py::test_same_fact_measures_keep_filter_and_aggregation_identity` | Already direct layer planning/composition with the tiny column_model; retain definition, measure, and row-parity assertions. |
| `tests/compile/test_persisted_layers.py::test_persisted_daily_prefix_anchor_reuse_matches_ordinary_frame_compiler` | MOVE | `integration/warehouse_execution/test_saved_plan_restart.py::test_persisted_daily_prefix_anchor_reuse_matches_ordinary_frame_compiler` | Original assertions at test_persisted_layers.py:68-74 retained, including anchor reuse, row equality, and metadata. Collected in the 24-case focused integration selection. |
| `tests/compile/test_persisted_layers.py::test_persisted_shape_matrix_preserves_panel_rows_and_metadata[event_panel]` | MOVE | `integration/warehouse_execution/test_saved_plan_restart.py::test_persisted_shape_matrix_preserves_panel_rows_and_metadata[event_panel]` | Original assertions at test_persisted_layers.py:461-468 retained; persisted and ordinary compiler paths keep all row/metadata comparisons. Collected in the 24-case focused integration selection. |
| `tests/compile/test_panels.py::test_sequential_anticipation_extends_control_risk_horizon` | MOVE | `integration/warehouse_execution/test_backend_capabilities.py::test_sequential_anticipation_extends_control_risk_horizon` | Original assertions at test_panels.py:457-470 retained, including refusal and Arrow provenance round trip. Collected in the 24-case focused integration selection. |
| `tests/compile/test_panels.py::test_later_initiation_at_trial_specific_exclusive_horizon_is_not_refused` | REWRITE | `tests/compile/test_panels.py::test_later_initiation_at_trial_specific_exclusive_horizon_is_not_refused` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: panel_horizon. |
| `tests/compile/test_panels.py::test_sequential_roster_history_ignores_ineligible_anchor_rows` | REWRITE | `tests/compile/test_panels.py::test_sequential_roster_history_ignores_ineligible_anchor_rows` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: panel_eligibility. |
| `tests/compile/test_panels.py::test_sequential_roster_history_uses_exclusive_trial_horizon[2025-01-10 00:00:00+00-False]` | REWRITE | `tests/compile/test_panels.py::test_sequential_roster_history_uses_exclusive_trial_horizon[2025-01-10 00:00:00+00-False]` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: panel_horizon. |
| `tests/compile/test_panels.py::test_sequential_later_initiation_uses_reanchored_outcome_support` | REWRITE | `tests/compile/test_panels.py::test_sequential_later_initiation_uses_reanchored_outcome_support` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: panel_checks. |
| `tests/compile/test_panels.py::test_sequential_roster_history_uses_exclusive_trial_horizon[2025-01-09 23:59:59+00-True]` | REWRITE | `tests/compile/test_panels.py::test_sequential_roster_history_uses_exclusive_trial_horizon[2025-01-09 23:59:59+00-True]` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: panel_checks. |
| `tests/compile/test_time_to_event.py::test_wide_exit_source_timestamp_is_not_cast_to_nanoseconds` | REWRITE | `tests/compile/test_time_to_event.py::test_wide_exit_source_timestamp_is_not_cast_to_nanoseconds` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_wide_exit_coverage_start_is_intersected_before_narrowing` | REWRITE | `tests/compile/test_time_to_event.py::test_wide_exit_coverage_start_is_intersected_before_narrowing` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_nanosecond_wide_outcome_and_exit_coverages_do_not_narrow_out_of_range` | REWRITE | `tests/compile/test_time_to_event.py::test_nanosecond_wide_outcome_and_exit_coverages_do_not_narrow_out_of_range` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_nanosecond_exit_before_outcome_coverage_is_not_ignored` | REWRITE | `tests/compile/test_time_to_event.py::test_nanosecond_exit_before_outcome_coverage_is_not_ignored` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_wide_entry_source_timestamp_is_not_cast_to_nanoseconds` | REWRITE | `tests/compile/test_time_to_event.py::test_wide_entry_source_timestamp_is_not_cast_to_nanoseconds` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_nanosecond_event_source_accepts_utc_timestamptz_entry_source` | REWRITE | `tests/compile/test_time_to_event.py::test_nanosecond_event_source_accepts_utc_timestamptz_entry_source` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_wide_entry_coverage_start_is_intersected_before_narrowing` | REWRITE | `tests/compile/test_time_to_event.py::test_wide_entry_coverage_start_is_intersected_before_narrowing` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_nanosecond_delayed_entry_keeps_entry_before_outcome_coverage` | REWRITE | `tests/compile/test_time_to_event.py::test_nanosecond_delayed_entry_keeps_entry_before_outcome_coverage` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_nanosecond_events_survive_time_to_event_compilation_in_any_session_timezone` | REWRITE | `tests/compile/test_time_to_event.py::test_nanosecond_events_survive_time_to_event_compilation_in_any_session_timezone` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_utc. |
| `tests/compile/test_time_to_event.py::test_wide_roster_observation_end_is_intersected_before_narrowing` | REWRITE | `tests/compile/test_time_to_event.py::test_wide_roster_observation_end_is_intersected_before_narrowing` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_life_table_matches_independent_pandas_risk_set_oracle` | KEEP | `tests/compile/test_time_to_event.py::test_life_table_matches_independent_pandas_risk_set_oracle` | Assertions unchanged; already direct compilation or unique consumer Arrow/provenance coverage. |
| `tests/compile/test_time_to_event.py::test_failure_before_non_delayed_entry_is_not_discarded` | REWRITE | `tests/compile/test_time_to_event.py::test_failure_before_non_delayed_entry_is_not_discarded` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_prevalence. |
| `tests/compile/test_time_to_event.py::test_survival_preserves_event_feature_timing_through_arrow_round_trip` | KEEP | `tests/compile/test_time_to_event.py::test_survival_preserves_event_feature_timing_through_arrow_round_trip` | Assertions unchanged; already direct compilation or unique consumer Arrow/provenance coverage. |
| `tests/compile/test_time_to_event.py::test_event_before_declared_delayed_entry_is_prevalent` | REWRITE | `tests/compile/test_time_to_event.py::test_event_before_declared_delayed_entry_is_prevalent` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_administrative_cutoff_changes_followup_without_changing_readiness` | REWRITE | `tests/compile/test_time_to_event.py::test_administrative_cutoff_changes_followup_without_changing_readiness` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_non_delayed_entry_advance_without_prior_event_is_incomplete` | REWRITE | `tests/compile/test_time_to_event.py::test_non_delayed_entry_advance_without_prior_event_is_incomplete` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_nanosecond_event_after_python_microsecond_clamp_is_retained` | REWRITE | `tests/compile/test_time_to_event.py::test_nanosecond_event_after_python_microsecond_clamp_is_retained` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/compile/test_time_to_event.py::test_nanosecond_events_support_coverage_outside_nanosecond_range` | REWRITE | `tests/compile/test_time_to_event.py::test_nanosecond_events_support_coverage_outside_nanosecond_range` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: survival_empty. |
| `tests/temporal/test_window_aggregates.py::test_all_null_ratio_numerator_is_not_a_zero_denominator` | REWRITE | `tests/temporal/test_window_aggregates.py::test_all_null_ratio_numerator_is_not_a_zero_denominator` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/temporal/test_window_aggregates.py::test_float_window_certifies_ordinary_history_but_refuses_cancellation` | REWRITE | `tests/temporal/test_window_aggregates.py::test_float_window_certifies_ordinary_history_but_refuses_cancellation` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/temporal/test_window_aggregates.py::test_exact_instants_half_open_boundaries_and_named_results[union_window]` | REWRITE | `tests/temporal/test_window_aggregates.py::test_exact_instants_half_open_boundaries_and_named_results[union_window]` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[avg_calendar_day-mean-expected4-reasons4]` | REWRITE | `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[avg_calendar_day-mean-expected4-reasons4]` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[avg_event-mean-expected3-reasons3]` | REWRITE | `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[avg_event-mean-expected3-reasons3]` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[sum-mean-expected0-reasons0]` | REWRITE | `tests/temporal/test_window_aggregates.py::test_complete_empty_and_all_null_remain_distinct[sum-mean-expected0-reasons0]` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/temporal/test_window_aggregates.py::test_historical_invalid_float_state_is_not_erased_by_a_later_group` | REWRITE | `tests/temporal/test_window_aggregates.py::test_historical_invalid_float_state_is_not_erased_by_a_later_group` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/temporal/test_window_aggregates.py::test_unknown_filter_history_cancels_and_unavailable_suppresses_refusal` | REWRITE | `tests/temporal/test_window_aggregates.py::test_unknown_filter_history_cancels_and_unavailable_suppresses_refusal` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/temporal/test_window_aggregates.py::test_exact_instants_half_open_boundaries_and_named_results[native]` | REWRITE | `tests/temporal/test_window_aggregates.py::test_exact_instants_half_open_boundaries_and_named_results[native]` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/temporal/test_window_aggregates.py::test_integer_sum_widens_without_losing_units_above_binary64_precision` | REWRITE | `tests/temporal/test_window_aggregates.py::test_integer_sum_widens_without_losing_units_above_binary64_precision` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/temporal/test_window_aggregates.py::test_count_and_conversion_ignore_unused_float_certificates` | REWRITE | `tests/temporal/test_window_aggregates.py::test_count_and_conversion_ignore_unused_float_certificates` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/temporal/test_window_aggregates.py::test_exact_wide_history_retracts_and_retains_decimal_scale` | REWRITE | `tests/temporal/test_window_aggregates.py::test_exact_wide_history_retracts_and_retains_decimal_scale` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: aggregate. |
| `tests/compile/test_state_panel.py::test_binned_state_periods_ignore_covariate_gap` | REWRITE | `tests/compile/test_state_panel.py::test_binned_state_periods_ignore_covariate_gap` | Same consumer-visible assertions at the owning compiler/reduction boundary; red: state_gap. |
| `tests/compat/test_semantic_interop.py::test_native_catalog_executes_a_frame_without_increment` | KEEP | `tests/compat/test_semantic_interop.py::test_native_catalog_executes_a_frame_without_increment` | Retain native-catalog execution with increment unavailable; not equivalent to a mocked compatibility check. |
| `tests/simulate/test_independence.py::test_generator_and_reference_are_independent_of_compiler_and_warehouse` | MOVE | `integration/test_independence.py::test_generator_and_reference_are_independent_of_compiler_and_warehouse` | Original subprocess and embedded assertions at test_independence.py:20-32 retained; forbidden-import isolation remains real. Collected in the 24-case focused integration selection. |
| `tests/temporal/test_retention.py::test_bounded_retention_matches_exact_path_and_independent_pandas[True]` | KEEP | `tests/temporal/test_retention.py::test_bounded_retention_matches_exact_path_and_independent_pandas[True]` | Already retention_threshold_bands on tiny Arrow-backed relations; retain bounded/exact/independent-oracle and nanosecond checks. |
| `tests/temporal/test_retention.py::test_bounded_retention_preserves_submicrosecond_anchor_lower_bound[True]` | KEEP | `tests/temporal/test_retention.py::test_bounded_retention_preserves_submicrosecond_anchor_lower_bound[True]` | Already retention_threshold_bands on tiny Arrow-backed relations; retain bounded/exact/independent-oracle and nanosecond checks. |
| `tests/temporal/test_retention.py::test_bounded_retention_preserves_submicrosecond_anchor_lower_bound[False]` | KEEP | `tests/temporal/test_retention.py::test_bounded_retention_preserves_submicrosecond_anchor_lower_bound[False]` | Already retention_threshold_bands on tiny Arrow-backed relations; retain bounded/exact/independent-oracle and nanosecond checks. |
| `tests/temporal/test_retention.py::test_bounded_retention_matches_exact_path_and_independent_pandas[False]` | KEEP | `tests/temporal/test_retention.py::test_bounded_retention_matches_exact_path_and_independent_pandas[False]` | Already retention_threshold_bands on tiny Arrow-backed relations; retain bounded/exact/independent-oracle and nanosecond checks. |
| `tests/temporal/test_retention.py::test_open_ended_retention_requires_complete_source_cutoff` | KEEP | `tests/temporal/test_retention.py::test_open_ended_retention_requires_complete_source_cutoff` | Already retention_threshold_bands on tiny Arrow-backed relations; retain bounded/exact/independent-oracle and nanosecond checks. |
| `tests/temporal/test_retention.py::test_finite_retention_preserves_nanosecond_end_and_followup[-1-999999999-True]` | KEEP | `tests/temporal/test_retention.py::test_finite_retention_preserves_nanosecond_end_and_followup[-1-999999999-True]` | Already retention_threshold_bands on tiny Arrow-backed relations; retain bounded/exact/independent-oracle and nanosecond checks. |
| `tests/semantic/test_rules_import.py::test_rules_import_without_query_or_temporal_packages` | KEEP | `tests/semantic/test_rules_import.py::test_rules_import_without_query_or_temporal_packages` | Already an isolated forbidden-import subprocess plus one tiny Catalog/Window alignment result. |
| `tests/compile/test_columns.py::test_false_conjunct_excludes_unknown_without_hiding_candidate_audit` | KEEP | `tests/compile/test_columns.py::test_false_conjunct_excludes_unknown_without_hiding_candidate_audit` | Already compile_columns on a fresh three-event/two-unit relation; retain exact audit, refusal, and metadata assertions. |
| `tests/compile/test_columns.py::test_exact_attribute_predicates_do_not_narrow_large_integer_or_decimal` | KEEP | `tests/compile/test_columns.py::test_exact_attribute_predicates_do_not_narrow_large_integer_or_decimal` | Already compile_columns on a fresh three-event/two-unit relation; retain exact audit, refusal, and metadata assertions. |
| `tests/compile/test_columns.py::test_event_time_selector_metadata_distinguishes_known_absence_and_missing_origin` | KEEP | `tests/compile/test_columns.py::test_event_time_selector_metadata_distinguishes_known_absence_and_missing_origin` | Already compile_columns on a fresh three-event/two-unit relation; retain exact audit, refusal, and metadata assertions. |
| `tests/compile/test_columns.py::test_reobserved_null_is_known_and_duplicate_inline_observations_dedupe` | KEEP | `tests/compile/test_columns.py::test_reobserved_null_is_known_and_duplicate_inline_observations_dedupe` | Already compile_columns on a fresh three-event/two-unit relation; retain exact audit, refusal, and metadata assertions. |
| `tests/parity/test_increment_frames.py::test_exposure_date_bucket_is_not_an_instant_equality_boundary` | KEEP | `tests/parity/test_increment_frames.py::test_exposure_date_bucket_is_not_an_instant_equality_boundary` | Retain real native/increment temporal-boundary parity; narrowing to one helper would lose independent-path evidence. |
| `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values0]` | SLIM | `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_reordered_and_partitioned_leaves[values0]` | Full original three-order/two-partition assertion loop preserved under slow; three fast representatives retain ordinary, cancellation, and subnormal inputs. |
| `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values1]` | SLIM | `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_reordered_and_partitioned_leaves[values1]` | Full original three-order/two-partition assertion loop preserved under slow; three fast representatives retain ordinary, cancellation, and subnormal inputs. |
| `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values2]` | SLIM | `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_reordered_and_partitioned_leaves[values2]` | Full original three-order/two-partition assertion loop preserved under slow; three fast representatives retain ordinary, cancellation, and subnormal inputs. |
| `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_exact_stored_leaves[values3]` | SLIM | `tests/temporal/test_sparse_state.py::test_float_prefix_certificates_enclose_reordered_and_partitioned_leaves[values3]` | Full original three-order/two-partition assertion loop preserved under slow; three fast representatives retain ordinary, cancellation, and subnormal inputs. |
| `tests/integration/test_calendar_panel_quantile.py::test_calendar_comparison_compiles_quantile_outcome_horizon` | MOVE | `integration/warehouse_execution/test_backend_capabilities.py::test_calendar_comparison_compiles_quantile_outcome_horizon` | Original assertions at test_calendar_panel_quantile.py:50-51 retained; public calendar/quantile composition. Collected in the 24-case focused integration selection. |
| `tests/compile/test_nonadditive.py::test_approximate_quantile_frame_matches_type7_tolerance_and_is_reproducible` | KEEP | `tests/compile/test_nonadditive.py::test_approximate_quantile_frame_matches_type7_tolerance_and_is_reproducible` | Retain tiny compiler value, null-reason, approximation metadata, and reproducibility contracts together. |
| `tests/compile/test_nonadditive.py::test_approximate_distinct_executes_in_duckdb_and_marks_column_metadata` | KEEP | `tests/compile/test_nonadditive.py::test_approximate_distinct_executes_in_duckdb_and_marks_column_metadata` | Retain tiny compiler value, null-reason, approximation metadata, and reproducibility contracts together. |
| `tests/compile/test_nonadditive.py::test_min_max_and_distinct_windows_preserve_empty_and_null_rows` | KEEP | `tests/compile/test_nonadditive.py::test_min_max_and_distinct_windows_preserve_empty_and_null_rows` | Retain tiny compiler value, null-reason, approximation metadata, and reproducibility contracts together. |
| `tests/compile/test_nonadditive.py::test_quantile_frame_matches_independent_numpy_type7_and_null_reasons` | KEEP | `tests/compile/test_nonadditive.py::test_quantile_frame_matches_independent_numpy_type7_and_null_reasons` | Retain tiny compiler value, null-reason, approximation metadata, and reproducibility contracts together. |
| `tests/integration/test_sequential_panels.py::test_all_shapes_build_materialize_and_arrow_round_trip` | MOVE | `integration/warehouse_execution/test_backend_capabilities.py::test_all_shapes_build_materialize_and_arrow_round_trip` | Original assertions at test_sequential_panels.py:37-49 retained, including shape, panel metadata, trial identities, and Arrow rows. Collected in the 24-case focused integration selection. |
| `tests/compile/test_matched_t0.py::test_matched_t0_samples_controls_without_replacement_and_records_probability` | KEEP | `tests/compile/test_matched_t0.py::test_matched_t0_samples_controls_without_replacement_and_records_probability` | Retain tiny matching population, assignment probability, convenience-design, and coded-refusal contracts. |
| `tests/compile/test_matched_t0.py::test_matched_t0_convenience_lowers_policy_onto_design` | KEEP | `tests/compile/test_matched_t0.py::test_matched_t0_convenience_lowers_policy_onto_design` | Retain tiny matching population, assignment probability, convenience-design, and coded-refusal contracts. |
| `tests/compile/test_matched_t0.py::test_matched_t0_refuses_insufficient_controls_during_materialization` | KEEP | `tests/compile/test_matched_t0.py::test_matched_t0_refuses_insufficient_controls_during_materialization` | Retain tiny matching population, assignment probability, convenience-design, and coded-refusal contracts. |
| `tests/compile/test_range_scaling.py::test_shape_outputs_preserve_range_bound_metadata[observed_state]` | KEEP | `tests/compile/test_range_scaling.py::test_shape_outputs_preserve_range_bound_metadata[observed_state]` | Retain range-bound metadata on actual shape output; a temporal helper alone does not publish that metadata. |
| `tests/compile/test_range_scaling.py::test_shape_outputs_preserve_range_bound_metadata[binned_state]` | KEEP | `tests/compile/test_range_scaling.py::test_shape_outputs_preserve_range_bound_metadata[binned_state]` | Retain range-bound metadata on actual shape output; a temporal helper alone does not publish that metadata. |
| `tests/compile/test_event_sequence.py::test_sequence_compiler_excludes_known_false_eligibility_anchors` | KEEP | `tests/compile/test_event_sequence.py::test_sequence_compiler_excludes_known_false_eligibility_anchors` | Retain actual sequence compiler eligibility exclusion on tiny inputs. |
| `tests/reference/test_event_sequences.py::test_overlapping_event_anchors_match_independent_pandas_oracle` | KEEP | `tests/reference/test_event_sequences.py::test_overlapping_event_anchors_match_independent_pandas_oracle` | Retain independent pandas oracle versus overlapping compiler event anchors. |
| `tests/temporal/test_sketches.py::test_tdigest_partition_merge_is_canonical_and_fresh_process_reproducible` | KEEP | `tests/temporal/test_sketches.py::test_tdigest_partition_merge_is_canonical_and_fresh_process_reproducible` | Retain real fresh-process reproducibility and canonical partition-merge assertions. |

## CI selector changes

- The original area handoff required adding `tests/temporal/test_sparse_state.py` to the checks job's explicit `-m slow` selector. Integration now runs `uv run pytest -q -n 8 tests -m slow`, which includes the file and its four full original matrices. No additional selector edit is needed for this follow-up.
- Root `pytest -q -n 4 integration` already recursively discovers every move. The focused collection command below collected all 20 moved cases alongside four existing integration cases. All need only the DuckDB/Pandas dependencies already installed by the checks job; no new optional extra is introduced.
- No active selector names either removed `tests/integration` file. Historical implementation plans and the shared ledger are intentionally unchanged; the parent integrates these coverage rows.

## Verification evidence

Baseline (loaded):

```sh
uv run pytest -q -o addopts='' -p no:xdist -m 'not slow' tests/compile tests/temporal tests/simulate tests/semantic tests/compat tests/parity tests/reference tests/integration --durations=0 --durations-min=2.0
```

Output: `561 passed, 50 deselected, 1902 warnings in 534.90s`. The largest call was cohort grouping at `37.93s`; initial fixture setup was `4.38s`. These are observations under concurrent sibling load, not performance guarantees.

Collection:

```sh
uv run pytest --collect-only -q -n 0 integration/warehouse_execution/test_backend_capabilities.py integration/warehouse_execution/test_saved_plan_restart.py integration/test_independence.py
```

Output: `24 tests collected in 2.51s`. All 20 moved IDs above are included.

Static gates:

```sh
uv run ruff format terrine tests examples integration
uv run ruff check terrine tests examples integration
uv run ty check terrine tests
uv run tach check
```

Output: `321 files left unchanged`; Ruff and ty: `All checks passed!`; Tach: `All modules validated!`.

Intermediate ordinary area run used the same baseline command. Output: `540 passed, 54 deselected, 1474 warnings in 382.63s`. Concurrent sibling activity changed during the run, so this is not an uncontended speed estimate. The twenty moved cases and one fewer fast sparse-prefix parameter account for the ordinary count change. All four full sparse-prefix matrices remain in the explicit slow tier.

The largest intermediate loaded call was the LTV cutoff check at `17.05s`, prompting the subsequent two-period input reduction below. No uncontended per-test claim is made. The parent must evaluate the <=2.0s phase gate after integrating the library-overhead fix and all area work.

```sh
uv run pytest -q -n 4 integration/warehouse_execution/test_backend_capabilities.py integration/warehouse_execution/test_saved_plan_restart.py integration/test_independence.py
uv run pytest -q -n 0 tests/temporal/test_sparse_state.py -m slow
```

Outputs: `24 passed, 299 warnings in 46.32s`; `4 passed, 55 deselected, 24 warnings in 3.03s`. The full moved integration files, not only newly added cases, were exercised. All temporary production mutations were restored before these green runs; the scratch runner is removed before handoff.

Per-group persisted-layer gate repeated all four static commands and ran:

```sh
uv run pytest -q -n 0 tests/compile/test_persisted_layers.py integration/warehouse_execution/test_saved_plan_restart.py
```

Output: `29 passed, 174 warnings in 36.93s`; static gates remained clean.

## Commits and automatic review

- `e1064c3` — separate cohort reductions from frame composition. Review job `9130`: **Pass**, no actionable issues. Shown, commented as `roborev-fix`, and closed.
- `4533586` — exercise persisted layer composition in integration. Review job `9131`: **Pass**, no actionable issues. Shown, commented as `roborev-fix`, and closed.
- `ac901ad` — check survival and state rows at compiler boundaries. Repeated all four static gates and ran `uv run pytest -q -n 0 tests/compile/test_time_to_event.py tests/compile/test_state_panel.py`: `56 passed, 29 deselected, 316 warnings in 26.16s`. Static gates remained clean. Review job `9132`: **Pass**, no actionable issues; shown, commented, closed.
- `2b968e1` — reuse concrete prefix inputs and bound fast permutations. Repeated all four static gates; `uv run pytest -q -n 0 tests/temporal/test_window_aggregates.py tests/temporal/test_sparse_state.py` passed `81 passed, 17 deselected, 149 warnings in 16.16s`; the explicit `-m slow` sparse-state selection passed `4 passed, 55 deselected, 24 warnings in 3.10s`. Review job `9133` raised one valid medium finding: the CI slow selector needs this file. Per the area contract, CI edits are owned by Main; the selector change is recorded above and explicitly assigned to Main. The review was commented and closed as a handoff, not as a claim that CI is changed.

The two expensive trajectory certificate examples were subsequently reduced from three periods to two: age one still crosses the cutoff, and both opposite-sign events still occur in the cumulative result. The exact assertions are unchanged. Repeated red probe:

```sh
env PYTHONPATH=. uv run python /tmp/terrine_ltv_period_probe.py
```

This process-local compiler-output mutation replaced period values with `1.0` and removed deferred checks; it was restored in `finally`. Output: `2 failed, 6 warnings in 2.02s`, specifically the cutoff-null assertion and the missing precision refusal. The runner asserted those exact failures and was removed after use.

Follow-up commit `fccf976` repeated the four static gates and ran `uv run pytest -q -n 0 tests/compile/test_ltv.py`: `7 passed, 20 warnings in 10.10s`. The slowest loaded calls in that focused run were the cutoff certificate (`2.45s`) and final precision refusal (`2.39s`). These are loaded timings, not evidence of the uncontended two-second gate.

Commit `44f4c91` moved generator/reference isolation without content changes. It repeated all four static gates and ran `uv run pytest -q -n 0 integration/test_independence.py`: `1 passed in 0.53s`.

Reviews `9135` (`fccf976`) and `9136` (`44f4c91`): **Pass**, no actionable findings. Both were shown, commented as `roborev-fix`, and closed. The sole outstanding integration dependency is the explicit slow selector described above.

## Final loaded verification

After the two-period change and all six code commits, repeated all four static gates above: `321 files left unchanged`, Ruff/ty `All checks passed!`, Tach `All modules validated!`. Then ran:

```sh
uv run pytest -q -o addopts='' -p no:xdist -m 'not slow' tests/compile tests/temporal tests/simulate tests/semantic tests/compat tests/parity tests/reference tests/integration --durations=0 --durations-min=2.0
uv run pytest -q -n 4 integration/warehouse_execution/test_backend_capabilities.py integration/warehouse_execution/test_saved_plan_restart.py integration/test_independence.py
uv run pytest -q -n 0 tests/temporal/test_sparse_state.py -m slow
```

Outputs:

- Ordinary area: `540 passed, 54 deselected, 1474 warnings in 130.42s`.
- Complete moved integration files: `24 passed, 299 warnings in 33.26s`.
- Full sparse-prefix slow matrix: `4 passed, 55 deselected, 24 warnings in 2.96s`.

Observed loaded before/after area seconds: **534.90 / 130.42**. Sibling load differed, so these measurements do not establish an uncontended speedup or the final two-second acceptance gate.

The final loaded ordinary run reported these calls above two seconds:

| Test suffix (under `tests/compile/`) | Seconds |
| --- | ---: |
| `test_time_to_event.py::test_nanosecond_exit_before_outcome_coverage_is_not_ignored` | 2.74 |
| `test_persisted_layers.py::test_layer_definition_identity_binds_catalog_and_design` | 2.74 |
| `test_ltv.py::test_cutoff_nulls_period_error_certificate_with_its_value` | 2.73 |
| `test_ltv.py::test_cumulative_float_results_enforce_final_tolerance` | 2.71 |
| `test_time_to_event.py::test_nanosecond_wide_outcome_and_exit_coverages_do_not_narrow_out_of_range` | 2.38 |
| `test_time_to_event.py::test_wide_exit_coverage_start_is_intersected_before_narrowing` | 2.19 |
| `test_retention.py::test_calendar_open_retention_history_includes_anticipation[6-False]` | 2.17 |
| `test_ltv.py::test_ltv_period_and_cumulative_values_match_hand_sum` | 2.13 |
| `test_time_to_event.py::test_wide_exit_source_timestamp_is_not_cast_to_nanoseconds` | 2.13 |

All other reported ordinary durations were below two seconds in this loaded run. Main owns the explicitly deferred uncontended integrated-suite measurement and the required sparse-state CI selector.

## Integrated uncontended follow-up

Main's integrated single-process measurement reported eight remaining calls above two seconds after the area merges and library overhead change. Work continued in `/Users/kylejcaron/terrine/.worktrees/fast-tests` on `feat/fast-tests`, with exclusive edit ownership, after `uv sync --group dev --group docs --all-extras` (`Resolved 261 packages`; `Checked 259 packages`).

Ruling: keep the actual compiler, assertions, and wide timestamp domains, but remove irrelevant event-anchor discovery from four survival exit cases. Their known anchor instant and one asserted unit suffice; event-anchor behavior remains covered by the other survival cases. The trajectory cases keep their required first-event anchors, use only the asserted unit, and the cutoff-projection test reads the output directly rather than repeating unrelated numeric checks. The cumulative precision case still runs deferred validation. Fixture defaults used by other unit/integration cases are unchanged.

Ruling: calendar retention tests own the roster history boundary, not construction of an entire comparison stack. Evaluate the real history-check query on the same two roster units and actual retention cutoff, with the same refusal/no-refusal assertions. For a Jan 4 open-ended cutoff plus two anticipation days, an exclusive Jan 6 observation bound suffices and Jan 5 does not; both sides are checked by the mutations below.

Ruling: the layer-identity test mixed catalog hash/refusal/parity and runtime state-audit parity. Separate those distinct contracts while preserving every assertion and real compiler/composer call; the runtime audit comparison uses one period instead of two. Do not split a single numerical behavior into fragments.

Red checks used process-local replacements and restored every patched function in `finally`:

```sh
env PYTHONPATH=. uv run python /tmp/terrine_residual_rule_probe.py <mode>
```

| Mode | Broken production rule | Observed output |
| --- | --- | --- |
| `cutoff` | Remove the administrative-cutoff nulling of period values. | `1 failed` at the period-value null assertion. |
| `precision` | Disable the final cumulative-tolerance failure predicate, retaining upstream checks. | `1 failed`: the expected `TerrineError` did not occur. |
| `exit` | Ignore the actual exit time in the survival follow-up end. | `1 failed`: event was true instead of false. |
| `bounds` | Directly cast wide bounds to nanoseconds rather than bounding them. | `2 failed`: DuckDB rejected timestamp-us to timestamp-ns conversion. |
| `source` | Cast exit-source timestamps to the nanosecond outcome axis. | `1 failed`: DuckDB rejected timestamp-us to timestamp-ns conversion. |
| `identity` | Omit the persisted definition-hash mismatch guard. | `1 failed`: expected identity `ValueError` did not occur. |
| `audit` | Drop all runtime-composed audit rows. | `1 failed`: real audit-table equality assertion. |
| `anticipation` | Omit anticipation from the history bound. | `1 failed`: expected incomplete-history refusal did not occur. |
| `exclusive` | Reject an observation bound equal to an exclusive outcome cutoff. | `1 failed`: the valid bound raised incomplete-history refusal. |

An initial fixed-date trajectory attempt was rejected by `CohortSpec` construction and is not red-check evidence; those probes were repeated after restoring its required first-event anchor. All ten changed/split cases then passed together (`10 passed, 64 warnings in 14.13s`); that measurement still showed four calls above two seconds, so it is not the final duration gate.

Profiling the remaining cumulative-precision and wide-exit-source cases:

```sh
uv run python -m cProfile -o /tmp/terrine-residual.prof -m pytest -q -o "addopts=-ra --strict-markers -m 'not slow'" -p no:xdist tests/compile/test_ltv.py::test_cumulative_float_results_enforce_final_tolerance tests/compile/test_time_to_event.py::test_wide_exit_source_timestamp_is_not_cast_to_nanoseconds
uv run python -c "import pstats; pstats.Stats('/tmp/terrine-residual.prof').strip_dirs().sort_stats('cumulative').print_stats(35)"
```

Output: `2 passed, 10 warnings in 11.09s` under profiling. The profile recorded 14 SQL compilation calls with 7.749 cumulative seconds and 3.397 cumulative seconds in select/select rewriting. Profiled times are diagnostic, not acceptance measurements.

Ruling: query only the survival columns consumed by each assertion, retaining `censor_time` in every wide-exit projection so the problematic timestamp path is still executed. Keep every deferred validation and the existing output normalizer. This removes irrelevant output expressions, not the wide numerical domain.

Ruling: the final cumulative certificate rule consumes certified period values. Supply one real, physically bound, two-period relation to that owner through its column-compiler dependency, with values `1e12` and `-1e12`, one-ULP input certificates, complete observation metadata, and real anchor validation. Execute the actual trajectory reduction and deferred check; also assert the public error context names `cumulative_value`. Only the upstream column builder is substituted, not numerical output or validation. The existing full-column trajectory and temporal-window tests retain upstream certificate coverage. Cost if wrong: missing an upstream-to-cumulative composition regression; the real compiler path remains covered by the other trajectory tests, while this case independently checks its final refusal.

The narrowed projections still left a wide-exit call at `2.01s` in the ten-case focused run (`10 passed, 65 warnings in 11.96s`). Ruling: expression width alone does not remove the repeated anchor subquery embedded in each first-occurrence query. Materialize the real fixed-date candidate relation before handing it to the survival compiler, preserving its metadata and all deferred checks; the compiler still performs the real exit/event lookups and timestamp arithmetic. This is a physical input-boundary change, not a replacement result. For runtime audit parity, bind real physical prefix inputs through the existing persisted-layer API rather than reconstructing the same prefix inside each composed query.

The first full integrated gate passed (`443 passed, 49 deselected, 1449 warnings in 88.94s`) but reported the wide-outcome/exit case at `2.02s`. Ruling: the fixed-date roster declares no observation-bound columns, so its minimal candidate input should contain only `unit`, `anchor_id`, and `t0`, not materialized nullable observation placeholders. This preserves the same unknown-observation domain while avoiding needless nanosecond bound expressions. The source-coverage and exit-time guards remain real and are red-checked again. The complete affected integration files passed (`28 passed, 321 warnings in 21.23s`).

### Final integrated verification

After the minimal candidate input change, repeated the `exit`, `bounds`, and `source` red probes: the same four survival cases failed on the intended wrong event flag or rejected out-of-range cast. Restored every production function before the green run.

```sh
uv run ruff format terrine tests examples integration
uv run ruff check terrine tests examples integration
uv run ty check terrine tests
uv run tach check
uv run pytest -q -o "addopts=-ra --strict-markers -m 'not slow'" -p no:xdist tests/compile tests/temporal --durations=0 --durations-min=2.0
```

Outputs: `317 files left unchanged`; Ruff and ty `All checks passed!`; Tach `All modules validated!`; pytest **`443 passed, 49 deselected, 1449 warnings in 87.06s`**, with **`(1329 durations < 2s hidden.)`** and no reported phase at the two-second threshold. This is the requested uncontended single-process compiler/temporal gate, not an extrapolation from parallel timings.

Affected integration verification used:

```sh
uv run pytest -q -n 4 integration/warehouse_execution/test_backend_capabilities.py integration/warehouse_execution/test_saved_plan_restart.py integration/test_independence.py
```

Output: `28 passed, 321 warnings in 21.23s`. The extra cases relative to the original area run came from the integrated branch. No integration file, library file, shared fixture, or CI file is changed by this follow-up. The temporary red runner and profile are removed before handoff.
