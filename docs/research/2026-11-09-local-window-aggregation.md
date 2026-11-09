# Local window aggregation: integrated verification

The compiler uses one window-local aggregation pipeline: raw selected rows, or
noncumulative unit/day payloads for declaration-proven aligned repeated origins.
Additive leaves are collected before physical lowering; independent sources reduce
before joining at full `(unit, anchor_id)` grain. General prefix production,
lookup and retained-prefix execution have been removed.

Native source-window arithmetic is backend binary64 over selected contributions,
not lifetime-prefix subtraction. Exact counts, integers, declared decimals and
per-input `exact=True` quantization retain their domains and refusal checks.
Specialized cumulative LTV and cohort magnitude, quotient, product and underflow
safeguards remain unchanged. No new public strategy option, density probe,
persistent summary cache, numerical mode or warehouse support was introduced.

## Executed correctness gates

Production source: `2d6b0b00f802d0367d8d21d88822bbe8a937d042`.
The local PostgreSQL service was PostgreSQL 16.15. Scale tests and unconfigured
live BigQuery/Snowflake selections were not run.

| Command | Observed result |
|---|---|
| `make check TESTS="tests integration" PYTEST_WORKERS=auto` | Static gates passed; 2,836 passed, 7 skipped |
| `uv run pytest integration/conformance --generic-dialect` | 16 passed; 45 nonselected warehouse cases deselected |
| `make test-postgres`, native policy | 115 passed, 1 skipped |
| `make test-postgres`, portable-policy conformance | 15 passed |
| `uv run python examples/materialization_runtime.py` | Published totals `3,4`, then corrected totals `8,4`; tier changed from gold to silver |

The local skips were six optional third-party statistical handoffs (lifelines,
doubleml, diff-diff, dowhy and duckreg) and one exact-domain case whose expected
refusal does not apply to the native coefficient domain. The PostgreSQL skip was
the same admitted-domain case. These skips are not evidence that the optional
third-party fits passed. The installed dataframe, Increment and orchestration
paths were exercised by the ordinary selection.

### Regression evidence

- **Selected native inputs:** small recent values survive excluded large history;
  selected invalid inputs cannot publish successfully. Ordinary native underflow
  or backend arithmetic exceptions remain permitted, with the original cause,
  rather than being mislabeled as exact overflow.
- **Validation order:** physical batch attachment is separate from logical check
  registration. Interleaved additive/nonadditive outputs and later-source history
  checks cannot preempt an earlier logical refusal. Empty additive outputs and
  conversions without row checks retain their source integrity checks.
- **Count admission:** a retained count-only plan previously mistook decimal-valued
  timestamp coordinates for exact measure coefficients. Admission now requires
  the DuckDB coefficient witness for the BIGNUM coefficient domain, not arbitrary
  decimals. Real ordinary and retained count materializations both returned
  `0,0,0,1,1,2`; an intentionally failing witness still refuses exact staging.
- **Mixed arithmetic:** a native numerator with a withheld exact denominator
  previously raised PostgreSQL `DivisionByZero` before reporting exact overflow.
  The regression uses two individually admitted declared-decimal facts whose
  scaled sum exceeds the bounded coefficient domain. Physical zero divisors are
  now masked independently of logical `known_zero`, preserving
  `terrine.temporal.exact_overflow`. The pre-fix real-PostgreSQL case failed;
  the corrected case and the other existing exact-overflow consumers passed.
  A separate real portable-policy smoke retained the valid ratio `6/2 = 3` and
  a genuine zero denominator's null/`zero_denominator` result.
- **Warehouse fixtures:** BigQuery/Snowflake selected-NaN refusals now put NaN
  inside the requested window; their existing positive cases include excluded
  nonfinite history. Equivalent selected/excluded inputs were executed on
  DuckDB, not represented as live cloud execution.

## Pathway coverage ledger

These are existing consumer assertions exercised by the ordinary selection,
not a claim that every route received its own performance benchmark. Paths are
repository-relative; parameterized cases retain the qualifications below.

| Pathway | Owning cases | Consumer evidence |
|---|---|---|
| Fixed/scheduled metrics, relative selectors, eligibility | `integration/compile/test_column_runtime_contracts.py::test_multisource_batch_preserves_selected_origins_and_full_anchor_support`; `tests/compile/test_columns.py::test_time_series_columns_are_values_with_named_support` | Independent source sums `5` and `20`, ratio `0.25`, mean/count/filter values, duplicate anchor identities, nested selected origins and missing-origin reasons; eligible support retained |
| Sequential/trial/calendar/event panels | `tests/compile/test_panels.py::test_sequential_schedule_emits_one_row_per_unit_trial`, `test_month_schedule_trials_are_the_schedule_count`, `test_event_panel_emits_only_declared_relative_periods`; saved-panel cases in `integration/warehouse_execution/test_saved_plan_restart.py` | Independent schedules, composite identities and event periods; retained rows and metadata equal ordinary results |
| Calendar comparison | `integration/compile/test_frame_runtime_window_export.py::test_row_checks_count_base_candidates_not_shape_rows` | Base-candidate NaN refuses despite stacking; corrected input publishes independent stacked values `0,1` with complete support and no reasons |
| LTV and cohort reductions | `integration/compile/test_ltv.py::test_ltv_period_and_cumulative_values_match_hand_sum`; `tests/compile/test_cohorts.py::test_cohort_grouping_bucket_statistic_and_denominators_match_hand_calculation` | Period values `10,5,0`, cumulative `10,15,15`, independent cohort denominators; owning cumulative/cohort overflow and underflow regressions preserve specialized safeguards |
| Binned/observed state panels, baselines and endpoints | `integration/state_panels/test_state_panel_workflows.py::test_baseline_is_computed_for_delayed_entry_at_panel_start`; `test_adjacent_binned_state_relation_pairs_response_with_successor_prior`; `integration/warehouse_execution/test_saved_plan_restart.py::test_state_panel_consumes_frozen_snapshot_and_scheduled_anchors` | Delayed-entry baseline, adjacent response/prior relationship, frozen scheduled totals and independent corrected fresh totals for binned and observed panels |
| Sequence/session/funnel base metrics | `tests/compile/test_event_sequence.py::test_sequence_sessions_are_compiled_per_anchor`, `test_sequence_compiler_publishes_funnel_attempt_schema` | Existing session/funnel behavior plus independent header counts `0,0,2`, including tied origins and complete/no-reason metadata |
| Survival and life-table additive grouping | `integration/compile/test_time_to_event.py::test_life_table_grouping_features_retain_conservative_surrogate_timing`, `test_prevalent_events_are_excluded_and_cutoff_equality_is_censored` | Additive-derived group keys distinguish risk/event counts `(2,1)` and `(1,0)`; original conservative timing, censoring and support assertions retained |
| Nonadditives, retention and as-of attributes | `tests/compile/test_nonadditive.py::test_quantile_frame_matches_independent_numpy_type7_and_null_reasons`; `tests/temporal/test_retention.py::test_retention_band_uses_distinct_occurrences_and_finite_followup`; `tests/temporal/test_scd2_attributes.py` | Independent quantile and availability reasons, deduplicated retention membership/follow-up, event-time dimension enrichment and overlapping-history checks |
| Expression, preview, materialize and dataframe conversion | `integration/compile/test_frame_runtime_window_export.py::test_many_window_columns_compile_and_export_together`, `test_materialize_gates_an_empty_output`; `tests/test_frame_runtime.py::test_bounded_preview_caps_the_summary_read_but_not_the_checks`; `integration/test_frame.py` | Known output values/support, invalid excluded-all candidates still gated, bounded preview does not bypass checks, pandas/Polars and typed metadata parity |
| Catalog, from-tables and Increment ingress | `integration/test_model_from_tables.py::test_to_catalog_round_trip_builds_equal_dataset`; `tests/compat/test_increment_semantics.py::test_increment_and_native_catalogs_preserve_dataset_meaning`; `integration/parity/test_increment_panel_parity.py::test_increment_panel_per_unit_matches_terrine_on_day_aligned_support` | Catalog round-trip values/counts/reasons; Increment adapter/catalog meaning plus downstream catalog execution and independent day-aligned results. This is chained ingress evidence, not a separate Increment-to-Frame numerical benchmark |
| Stage/capture/refresh, bare/saved plans, restart/replay | `integration/warehouse_execution/test_runtime_api.py::test_saved_plan_execution_reopens_retained_snapshot_for_each_task`; `test_saved_plan_restart.py::test_saved_plan_materializes_retained_model_and_design_in_fresh_process`; `test_incremental_equivalence.py::test_saved_runtime_correction_matches_clean_rebuild_of_new_generation`; `test_runtime_cost_budget.py::test_duckdb_runtime_reports_unknown_scan_and_observed_compiler_output` | Frozen source rather than later live mutation; corrections equal clean rebuilds; replay/retry and fresh-process execution; bare execution/publication and budget refusal at their owning boundary |
| Immutable reopen, export, training and orchestration | `integration/warehouse_execution/test_ml_staging_reads.py::test_dataset_batches_and_parquet_preserve_typed_metadata`; `test_ml_staging_training.py::test_training_split_assigns_support_without_reassigning_overlap`; `test_orchestrator_adapters.py::test_two_dbt_build_cycles_stop_owner_and_replay_from_separate_processes`, `test_dagster_historical_partitions_publish_distinct_owner_outputs` | Rows/schema/roles/reasons survive batch/Parquet conversion; split support preserved; dbt totals `4→9` leave old `4` immutable; distinct historical Dagster publications retain independent values |
| Generic read-only admission and verified engine policy | `integration/execution/test_generic_provider.py::test_explicit_generic_model_and_real_integer_read`, `test_frame_admission_precedes_every_source_read`; `integration/backends/test_generic.py`; `integration/conformance/test_engines.py` | Real opt-in SQLite reads, refusal before protected effects/source reads, bounded portable numeric known answers, native and forced-portable execution on DuckDB/PostgreSQL |

Calendar, sequence and additive life-table gaps were filled by extending their
existing cases, without adding test items. Isolated shape-specific value
mutations passed the old assertions and failed the strengthened consumer
assertions. The mixed-ratio overflow edge adds one case on each of the two real
backend selections: the PostgreSQL case uniquely catches premature division by
zero. Retired prefix ABI, SQL-text and exact-join-count assertions were deleted,
not updated to pin the replacement implementation.

## Immutable compatibility

- All 42 files under `tests/artifacts/1` retain their original SHA-256 hashes.
  New current provenance is stored in the separate generation `2`; historical
  expected bytes were not rewritten.
- Version-one manifests with and without `frame_metadata` remain readable.
  Stored metadata stays historical; reopening is not relabeling or recomputation.
- Valid older saved/bare plans can still be checksum-verified, inspected and
  administratively released. They cannot execute or obtain a stale request-key
  replay under execution revision 2. An empty-change refresh of an older
  generation recomputes; explicit old-generation reads remain unchanged.
- Execution evidence is persisted in existing immutable Arrow schema metadata;
  runtime schema version 1 did not require a database migration. Recovery, replay
  and reuse validate the exact generation, including typed-empty/plain-Arrow
  publication paths.
- `tests/artifacts/RELEASES`, package version `0.1.0a1` and dependency lock were not
  changed. No user data, release mapping or published generation was migrated.

## Measurement boundaries

The public benchmark is the extended `--frame-only` path of
`benchmarks/query_strategy_spike.py`, with the same fixtures/oracle at baseline
production `5705ff7fde0f781c7be5f3386ab4e5f1c81425db` and the integrated source.
Fixture and oracle copies are byte-identical between the isolated baseline and
current checkout; the dependency lock is also identical. Baseline execution
imports were attested during that run. Its original per-process file-hash and
DuckDB-version receipts were not recorded; they have not been fabricated
retrospectively. Final measurements capture actual imported production,
helper/fixture/oracle hashes, engine version and output SQL/plan hashes in each
process, checking source stability before/after execution.

`Frame.materialize(...).table` includes recompilation, admission, scheduled
validation, output and Arrow collection. Independent support/value checks run
after its timer. Standalone build and SQL compilation are separate observations,
not disjoint phases to add to the materialization duration. Every sample checks
the complete declared-origin multiset, unique anchor identities and all 50
values, reasons, completeness flags and observation endpoints against independent
selected-input Python arithmetic. Native fixture tolerances are not a universal
floating-point accuracy guarantee.

Stage and refresh timers measure their publication/runtime API calls; they
exclude the subsequent Dataset-to-Arrow reads, independent oracle and fresh
ordinary comparison rebuild. Corrections accumulate across the three retained
samples in the same way on both revisions. Each sample checks refreshed/fresh
parity and re-reads its old generation unchanged.

The internal raw/day probe calls the production reducer, including inline daily
construction. Server CTAS and validation-to-Arrow are separate complete
evaluations, not additive timing phases. CTAS order alternates; validation reads
are grouped, and preceding oracle/plan queries can warm data. Stored public
plans describe `.expr`, not every scheduled validation statement. Reference
`matched_events`/join counts belong to the independent prototype relation, not
unobserved production intermediates. Public SQL sizes are UTF-8 bytes; kernel
character counts require an explicit encoding check before being called bytes.

All timings are informational on a shared machine, using one DuckDB thread and
three bounded samples. There is no cold-cache, isolated-machine, statistical
confidence, all-pathway speedup or warehouse-cost claim. Original 20261108
experiments and support-verified pre-change baselines remain intact; intermediate
or interrupted runs are not final-source evidence.

## Final integrated measurements

All ten integer/native records completed on the reviewed production source above,
with DuckDB 1.5.6, Ibis 12.0.0 and Arrow 23.0.1. Five per-process identity receipts
match that source before/after execution; all 39 recorded output-file digests
were checked. The public samples independently checked 18.6 million values,
with additional retained-correction checks, complete origin support and metadata.

Primary records, raw SQL, expression plans, environments and exact arguments:

- `benchmarks/results/query-frame-final2-fixed-20261109/`
- `benchmarks/results/query-frame-final2-dense-20261109/`
- `benchmarks/results/query-frame-final2-irregular-20261109/`
- `benchmarks/results/query-frame-final2-retained-20261109/`
- `benchmarks/results/query-local-kernel-final2-dense-20261109/`

Each directory's `source-identity.json` includes the identity wrapper's complete
source and output hashes. Matched pre-change records remain under
`benchmarks/results/query-frame-verified-baseline-*-20261109/`.

### Fixture sizes

Input events are the deterministic loader's declared product. Distinct unit/day
buckets and final rows are measured. All fixtures request five metrics over ten
windows, with complete observations and one event source.

| Fixture | Units | Input events | Distinct unit/day buckets | Final rows | Origin layout |
|---|---:|---:|---:|---:|---|
| Fixed | 10,000 | 300,000 | 133,335 | 10,000 | One fixed origin |
| Dense | 1,000 | 300,000 | 18,345 | 21,000 | 21 aligned daily origins |
| Irregular | 1,000 | 30,000 | 30,000 | 30,000 | Event-relative repeated origins; genuinely poor day compression |
| Retained | 1,000 | 30,000 | 13,335 | 1,000 | One fixed origin; three distinct stage/correct/refresh samples |

### Public materialization

Entries are median `[minimum–maximum]` of three samples. A baseline/current ratio
below one means the current median is slower. These are matched public API
boundaries, not prototype server-time ratios.

| Fixture / mode | Baseline materialize, s | Current materialize, s | Baseline / current |
|---|---:|---:|---:|
| fixed / integer | 37.176 [28.179–46.655] | 18.141 [17.944–18.483] | 2.05× |
| fixed / native | 29.390 [29.127–37.039] | 17.274 [16.679–18.738] | 1.70× |
| dense / integer | 29.498 [19.326–31.603] | 10.681 [10.527–10.808] | 2.76× |
| dense / native | 19.118 [18.880–29.135] | 10.224 [10.163–10.377] | 1.87× |
| irregular / integer | 26.732 [26.181–37.963] | 24.019 [23.955–24.732] | 1.11× |
| irregular / native | 22.608 [22.022–26.045] | 24.004 [23.812–24.382] | 0.94× |
| retained / integer | 17.488 [16.395–22.941] | 13.252 [13.080–13.611] | 1.32× |
| retained / native | 13.114 [12.368–13.258] | 13.174 [12.312–13.491] | 1.00× |

### Compilation and SQL size

Build includes `Design.build` and `.expr`. Compilation below is expression-to-SQL;
these observations are not added to materialization, which recompiles internally.

| Fixture / mode | Build, s | SQL compile, s | Baseline compact bytes | Current submitted/compact bytes | Current formatted bytes |
|---|---:|---:|---:|---:|---:|
| fixed / integer | 4.920 | 1.281 | 226,671 | 1,078,242 | 3,460,106 |
| fixed / native | 5.602 | 1.602 | 208,473 | 1,067,895 | 3,649,365 |
| dense / integer | 5.830 | 0.792 | 226,992 | 278,520 | 455,376 |
| dense / native | 6.184 | 0.816 | 208,792 | 265,393 | 437,573 |
| irregular / integer | 5.069 | 1.393 | 228,658 | 1,078,489 | 3,460,333 |
| irregular / native | 6.136 | 1.398 | 210,350 | 1,068,156 | 3,649,606 |
| retained / integer | 4.967 | 1.538 | 226,671 | 1,078,242 | 3,460,106 |
| retained / native | 5.423 | 1.576 | 208,473 | 1,067,895 | 3,649,365 |

SQL text grew, especially for raw wide queries. Shared reduction does not imply
compact final arithmetic/availability projections. The final fixed-native
expression plans show the structural change below, rather than a smaller-text
claim. Both plans scan the event source once; current table scans also include
two roster scans. These are `.expr` operators, not the complete materialization
validation schedule.

| Physical operator | Baseline | Current |
|---|---:|---:|
| Table scan | 2 | 3 |
| Hash join | 50 | 3 |
| ASOF join | 2 | 0 |
| Hash group by | 2 | 1 |
| Window | 2 | 0 |

### Dense production raw/day kernel

Both variants call the actual production reducer. Daily construction is inline
and included; no summary was supplied for free. Both produced the same values
and availability metadata as the independent raw reference, plus the independent
selected-input arithmetic oracle. Every submitted kernel SQL string was checked
to be ASCII, so the recorded character counts equal UTF-8 bytes for this fixture.

| Mode / representation | CTAS, s | Validation→Arrow, s | Build / SQL compile, s | Compact / formatted bytes |
|---|---:|---:|---:|---:|
| integer / local_raw | 64.195 [63.954–64.460] | 132.503 [132.320–132.909] | 4.990 / 1.482 | 1,139,099 / 3,429,757 |
| integer / local_day | 0.635 [0.629–0.654] | 4.356 [4.277–4.580] | 6.247 / 1.575 | 342,515 / 523,423 |
| native / local_raw | 64.039 [63.445–64.175] | 132.304 [131.156–132.379] | 5.003 / 1.614 | 1,140,932 / 3,640,244 |
| native / local_day | 0.509 [0.505–0.512] | 4.282 [3.965–4.546] | 6.052 / 1.341 | 340,239 / 528,973 |

The independent reference's matched-event relation has 1,976,690 rows. That is
not a measured production intermediate count. Production operator details remain
in the stored profiles; no inferred intermediate cardinality or warehouse bytes
scanned is substituted. The ordinary compiler selected `local_day` for the dense
fixture and `local_event` for fixed/irregular origins.

### Retained publication and correction

These API timers exclude subsequent output reads, the fresh rebuild and oracle
work. Each of the three samples passed independent refreshed-value/support checks,
matched a fresh ordinary build and left its old generation unchanged.

| Mode / operation | Baseline median [min–max] | Current median [min–max] | Unit |
|---|---:|---:|---|
| integer / stage | 25.011 [20.732–30.407] | 24.378 [23.831–24.549] | s |
| integer / source correction | 2.455 [1.670–2.829] | 1.325 [1.130–1.874] | ms |
| integer / refresh | 30.668 [28.286–32.332] | 37.191 [36.325–37.759] | s |
| native / stage | 37.762 [20.803–39.412] | 24.113 [23.895–24.342] | s |
| native / source correction | 2.364 [1.698–4.469] | 1.374 [1.106–1.696] | ms |
| native / refresh | 40.338 [30.518–50.235] | 35.435 [34.480–36.134] | s |

## Acceptance and limits of the result

The shared local design removes per-output relational assembly and avoids the
measured dense raw-overlap penalty. Matched full-Frame fixed/dense measurements
improve while preserving the numerical, pathway and retained-compatibility
contracts. This supports the cutover, not a universal speedup claim.

The native irregular median and integer refresh median regressed. Native
retained materialization is effectively unchanged within these sample ranges.
SQL text is larger, and retained refresh remains substantial compiler/runtime
work. These are reported costs, not hidden behind prototype ratios, a new
optimizer or restored prefixes. Baseline noise, shared-machine contention and
fixed run order preclude stronger statistical or cold-start conclusions.

Exact-float quantization, incomplete coverage, unavailable filters and specialized
reducers are correctness evidence from their owning cases, not performance claims
from these all-observed integer/native fixtures. BigQuery/Snowflake live checks
remain conditional on credentials and authorization; this run provides no new
live cloud conformance, performance or billing evidence. The earlier PostgreSQL
prototype timeout remains a censored historical observation, not a new timed run.
