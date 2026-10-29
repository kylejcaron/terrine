# Changelog

## Unreleased

### Performance

- Run a build's integrity checks, eligibility summary and output in one
  statement. Window checks are boolean flag columns of the study's audit
  relation instead of one statement per check that recomputed the metric
  from the source tables; the deferred checks form one schedule in
  registration order, folded into gate statements, and the final run's
  counts and the eligibility summary ride on the output statement as a
  one-row cross join that is decoded before the first batch leaves. Refusal
  codes, their context (`failed_count` included) and the first refusal for a
  given dataset are unchanged; an empty output still refuses a failing check
  on an excluded candidate, and `Budget.max_rows` and `max_output_bytes`
  count output rows and bytes only. On the repro (DuckDB 1.5.6; 10,000
  units, 300,000 events, ten snapshots, three metrics; Apple M3 Pro, 12
  cores) a warm build went from 14.0 s and eight statements over the source
  tables to 0.8 s and one. Designs with as-of attributes or event-history
  checks keep one statement per such check between gate statements.
  Composition for runtime staging runs the schedule once and each staging
  task reads its output with the summary alone;
  `preview(..., bounded_count=N)` still runs every check unrestricted and
  caps only its eligibility read.
- Compare DuckDB lookup timestamps natively: a microsecond cutoff and state
  are their own as-of keys, a naive side is attached to UTC explicitly so
  the session `TimeZone` cannot shift it, and a nanosecond axis keeps the
  structured order key. Hold counts in native integers there: `BIGINT` 0/1
  terms accumulated in `HUGEINT` and narrowed by `TRY_CAST`, so a count
  beyond `int64` refuses `terrine.temporal.exact_overflow` rather than
  wrapping; integer, decimal and float sums keep their `BIGNUM`
  coefficients. A window's state carries only the fields its measure reads,
  and an unfiltered measure's state no longer holds the five constant
  unknown-filter counters. PostgreSQL, BigQuery, Snowflake and the portable
  policy keep their numeric and timestamp state; their compiled statements
  change only in the gate shape, the pruned state columns and integer count
  literals.

### Public API

- Refusal messages describe units, events, labels, metrics, history, t0 and
  windows, explain what was wrong and give a route forward. Unknown names
  list the available declarations, feature and outcome timing errors explain
  their side of t0, and reversed schedule and window bounds explain which
  endpoints are included. Refusal codes and existing context fields are retained.
  Timing refusals also identify the output, normalized start/end seconds, offending
  bound and allowed limit; reversed-window refusals include both bounds.
- `Events(table, *, time, key, unit=None, metrics=None, labels=(), complete=None)`
  declares user-named metrics with the constructors `tr.count`, `tr.sum`,
  `tr.mean` (per event, or `per="day"`), `tr.min`, `tr.max`,
  `tr.count_distinct` and `tr.ratio`, each with a `where=` mapping keyed by
  `<label>` or `<label>__<op>` (`ne`, `in`, `not_in`, `gt`, `gte`, `lt`,
  `lte`, `between`; a scalar means equality, a list membership, several keys
  a conjunction) that becomes the catalog's `filters`. `labels=` names event
  columns that bind as event-time properties with their dtype read from the
  column, usable in `where=`, `event_time(label_property=, equals=)`, `asof`
  and `Treatment`. `tr.History(table, *, key, column, unit=None, valid_from=,
  valid_to=, changed_at=, complete=)` on `Model.from_tables(history={...})`
  binds one as-of attribute per declaration, as a range-validity dimension
  (`valid_from`, optional `valid_to`) or a change log (`changed_at`), and
  `unit` is inferred from a single `UnitSource`. `Model.to_catalog()` writes
  the equivalent schema-version-2 catalog as YAML; rebinding it with
  `from_catalog` gives an equal `model_hash`. New refusals: an `Events` or
  `History` omitting `unit` with several `UnitSource`s refuses with
  `terrine.model.ambiguous_unit` (context `owner`, `choices`, `parameter`);
  a bad filter key refuses at the constructor with
  `terrine.model.invalid_binding` (`reason="invalid_filter"`, `key`); an
  undeclared `where=` label, ratio operand or unit refuses at `from_tables`
  with `terrine.semantic.missing_reference` (context `kind`, `name`, `owner`,
  `available`); a label or history column of an unsupported type with
  `reason="unsupported_column_type"`; a `History` with two time axes or none
  with `reason="history_time_axis"`; and a `History` without `complete=` with
  `terrine.model.missing_event_completeness`.
- `UnitSource`, `Events` and `History` take a pandas DataFrame, a Polars
  DataFrame or a pyarrow Table as `table`, stored as Arrow (a pandas index is
  dropped; neither library is imported by Terrine). `Model.from_tables`'s
  `con` is optional: with every table in memory, the tables are registered
  under their mapping keys on a private in-process DuckDB that becomes
  `model.con`, so `Model.units`, the catalog relations, `model_hash` and
  `to_catalog()` carry those names exactly as for a warehouse, and nothing
  is uploaded. The README binds its DataFrames this way without an Ibis
  import. Such a model materializes only: `frame.stage(...)`, and reopening
  or refreshing a dataset on any connection that is not a file-backed
  DuckDB, refuse with `terrine.dataset.invalid_destination`
  (`reason="file_backed_connection_required"`, `route`). New refusals, all
  `terrine.model.invalid_binding`: a connection beside an in-memory table
  (`reason="in_memory_input_with_connection"`, sorted `inputs` such as
  `("events:orders", "units:customer")`; also from `from_catalog(units=)`),
  a table name without a connection (`reason="connection_required"`, sorted
  `tables`), a unit name equal to an event or history name when both are in
  memory (`reason="name_collision"`, `kind="table"`, `name`, `owners`), and
  any other `table` object such as an Ibis table, a path or a mapping
  (`reason="unsupported_table_input"`, `field`, `type`) at the descriptor.
- `Schedule`, and so `time_series` and `trial_panel`, accept calendar-month
  steps: `every="1mo"`, `"3mo"` or `"1y"` (twelve months) alongside elapsed
  durations. Anchor `k` is `start` advanced by `k * months` and clamped to that
  month's last day, never chained from the previous anchor, so a monthly
  schedule from `2025-01-31` through `2025-04-30` lands on Jan 31, Feb 28,
  Mar 31 and Apr 30, and a yearly one from `2024-02-29` on Feb 28 in 2025 and
  2026; anchors stop at the last `k` whose day is on or before the inclusive
  `end`. Each anchor's instant is computed in Python and selected by ordinal in
  the query, so every backend agrees on end-of-month clamping. `Schedule.count`,
  `Schedule.instant` and `Schedule.instants` expose the anchor arithmetic to
  callers that hold the model's day boundary. Calendar units stay a `Schedule`
  feature: `window=`, `asof(before=)`, `gap=`, `EventAnchor(offset=)`,
  `ltv(period=)`, `event_panel(period=)`, `life_table(bin=)`,
  `event_sequence(horizon=, session_gap=)` and the other elapsed fields refuse
  `"1mo"` with `terrine.window.invalid_duration`, whose context now carries
  `field` and `value` and whose message routes to `"30d"` or `"4w"`.
  `state_panel(every="1mo")` and a `StatePanel` design on a month schedule
  refuse with `terrine.state_panel.invalid_schedule`
  (`reason="calendar_months_unsupported"`, context `every`, `route="4w"`);
  the recipe checks the schedule's step and whole periods at construction
  whether or not `periods` is given; a
  `CalendarComparisonSpec` on a month schedule refuses with
  `terrine.design.invalid_calendar_comparison` (same reason, `every`, `route`),
  because both shapes step by a fixed elapsed width.
- `Design.unit` may be `None`, and every `terrine.recipes` constructor takes
  `unit: str | None = None`: an unset unit means the model's only `UnitSource`.
  The design is resolved wherever it meets a model (`validate`, `build`,
  `snapshot`, `preview`, `plan_layers`, the agent tools and staging); with
  several `UnitSource`s it refuses with `terrine.model.ambiguous_unit`
  (context `owner="design"`, sorted `choices`, `parameter="unit"`) and with
  none with `terrine.design.invalid` (`reason="no_unit_source"`). `to_yaml`
  and `to_json` write `unit: null` and read it back; `spec_hash` of such a
  design hashes that document, while the identity recorded by staging, saved
  definitions and scoring views is the resolved design's, so a typed and an
  inferred unit give the same staged `design_hash`, definition digest and
  scoring `view_hash`. The README and the guides omit the unit on single-unit
  models.
- `Frame.to_pandas` and `Frame.to_polars` take `values_only: bool = False`.
  `values_only=True` drops every `<name>__complete` and `<name>__reason`
  column as well as the bookkeeping columns, keeping identity, role and
  value columns (`unit`, `anchor_id`, `t0`, treatment, trial, weight and
  structural shape columns such as `age` and `cumulative_value`); an
  incomplete value stays null and is never imputed. The default view is
  unchanged. `training_set(...).to_pandas(values_only=True)` is the in-memory
  feature and label table. Combining `values_only=True` with
  `include_observation_metadata=True` refuses with
  `terrine.dataset.invalid_view`
  (`reason="values_only_excludes_observation_metadata"`) before any query
  runs. `to_pyarrow()` and `MaterializedFrame.to_arrow()` stay lossless and
  gain no flag.
- `Design.describe(model)` returns a validated, plain-language summary of the
  rows, anchors, named expressions, treatment, shape, declared completeness and
  output columns, using bound metadata without compiling or querying sources.
  Long schedules show their first twelve dates and the remaining count.
  `DatasetTools.validate_design` and `propose_design` include the same text in
  the required `ValidationOutput.summary` field after source authorization.
  Summaries distinguish sequence headers from child rows and arrays, name
  eligibility support columns, and report empty-window values and completeness
  reasons according to the bound aggregation and shape.
  Survival summaries distinguish pre-entry exclusions from incomplete initial
  history retained without an entry selector.

### Breaking changes

- `Events` takes `table` positionally and everything else by keyword; `keys`
  and `values` are gone, replaced by one `key` column, `metrics=` and
  `labels=`. Generated metric names (`<source>_count`, `<source>_<value>_sum`,
  `<source>_<value>_mean`) are gone: an `Events` without `metrics` declares
  its event only, and every metric is named by the user. `Model.from_tables`
  loses `readiness=` (completeness comes from each `Events.complete` and
  `History.complete`; `from_catalog` keeps it) and gains `history=`.
  `Events.complete` and `History.complete` store `date` values. `__` is
  reserved: a label containing it refuses at `Events(...)` with
  `terrine.model.invalid_binding` (`reason="reserved_name"`, `kind="label"`,
  `name`). Event names, history names and labels share one namespace, metric
  names are unique across every `Events`, and each metric column reserves the
  value event `<events name>_<column>` against every event, label and history
  name; a clash refuses with `reason="name_collision"` (context `kind`,
  `name`, sorted `owners`). The codes
  `terrine.model.generated_metric_collision` and
  `terrine.model.multiple_event_units` are removed.
- Catalog documents are `schema_version: 2` and use unit, time and event
  vocabulary: a source declares `time`, `units` and `events` (formerly
  `timestamp`, `entities`, `facts`); the root `facts` mapping is `events`
  and its record is `Event`; a dimension declares `unit` and `key` (formerly
  `source_entity`, `entity_column`); a measure names its `event` (formerly
  `fact`); a metric names its `unit` (formerly `entity`). A document that
  declares any other `schema_version` refuses with
  `terrine.catalog.invalid_definition` carrying one issue of kind
  `unsupported_schema_version` at `("schema_version",)` before any other
  field is checked. `Model.from_tables`, `Model.from_increment` and
  `dump_catalog` emit version 2, the shipped simulation baseline is
  version 2, and the agent's `SourceMetadata` and `MetricMetadata` carry
  `units`, `events` and `unit`. Bound descriptors rename with their fields
  (`BoundSource.time_column`, `.units`, `.events`; `BoundMetric.unit`,
  `.unit_columns`; `BoundMeasure.event`; `BoundDimension.unit`, `.key`), as
  do the definition-hash payload keys, so feature definition hashes and
  `Model.model_hash` differ from earlier versions for the same study.
- Design specifications name events with `event`: `EventAnchor(event, ...)`,
  `event_time(event, ...)` and `EventTime.event`, `retention(event, ...)` and
  `RetentionMetric.event`, `EventSequence(event, ...)` and
  `SequenceMeta.event` (formerly `fact`). `Initiation(event, *, history_start,
  knowledge)` and `PersistentAdoption(event, *, adopted_value, history_start,
  persistence, knowledge)` lose `source`: `event` names an event or a label and
  the owning source is the one that declares it; a name declared by two
  sources refuses with `terrine.design.invalid` (`reason="ambiguous_reference"`,
  context `name`, `owners`) and an unknown name with
  `terrine.design.missing_reference` (`kind="event"`). Recorded provenance
  still carries the resolved source. Design documents written by `to_json` and
  `to_yaml` and `DatasetDefinition` documents are `schema_version: 2` with the
  `event` keys; a version 1 design document refuses with
  `terrine.design.invalid` (`reason="unsupported_schema_version"`) and text
  that does not parse with `reason="malformed_document"`, a version 1
  definition refuses with `terrine.dataset.invalid_definition`, and manifests
  and saved plans that embed them refuse through their existing codes. There
  is no migration: construct and save the design again. The
  `terrine.design.missing_reference` context uses `kind="event"` and
  `"bound event"` where it named a fact.
- A calendar date typed as an end bound includes that whole local day.
  `Schedule(every, start, end)`, and so `time_series`, `trial_panel` and
  `state_panel`, anchor every `t0 = day(start) + k * every` before
  `day(end + 1 day)`; `start == end` is one anchor. A date `AnchorRange.end`
  keeps anchors through that day; an aware datetime end is still an exact
  exclusive instant, relative windows and `TimeSplit` are unchanged. Both
  carry a read-only `end_exclusive` (`end + 1 day` for a date; an instant
  unchanged) that the compiler reads. `start > end` on dates refuses with
  `terrine.design.invalid` (`reason="schedule_end_before_start"` or
  `"range_end_before_start"`, context `field`, `start`, `end`), a datetime
  range still needs `start < end`, and a date end of `date.max` refuses with
  `reason="date_overflow"`. Design documents keep the same `end` key with the
  new meaning; version 1 documents already refuse under the gate above.
  `recipes.state_panel(periods=None)` derives the period count from the
  schedule and refuses `terrine.state_panel.invalid_schedule`
  (`reason="schedule_not_whole_periods"`, context `every`, `start`, `end`)
  when the span is not whole periods; an explicit `periods` is an upper
  bound and a longer schedule refuses with `reason="schedule_exceeds_max_periods"`
  (context `periods`, `max_periods`) rather than truncating.
- `Schedule.every` is `int | None`: a calendar-month schedule stores `months`
  and leaves `every` as `None`; exactly one of the two is set. Design
  documents written by `to_json` and `to_yaml` carry both keys
  (`"every": 2419200, "months": null` or `"every": null, "months": 1`), a
  document that sets both, neither, or a non-positive value refuses as
  invalid node data, and the agent design schema describes both fields.
  `terrine.windows.seconds` requires the keyword `field` naming the caller's
  parameter, and `SessionSpec(gap=)` and `FunnelSpec(within=)` let a
  malformed duration refuse with `terrine.window.invalid_duration` instead of
  rewrapping it as `terrine.sequence.invalid_spec`.
- `recipes.life_table` takes `group_by` as a mapping from each grouping
  column's name to its expression or predicate and no longer accepts
  `features=`; the names are the design's features and the life table's
  groups, so a mismatch cannot arise and the former `ValueError` is gone.
- Output row order on DuckDB may differ from anchor order for a small study
  (DuckDB rewrites an as-of join with fewer than 64 estimated cutoffs into a
  hash aggregate). Row order was never part of the dataset contract on any
  backend; sort explicitly when an order is needed.
- Persisted runtime prefix layers written by this version hold counts as
  Arrow `decimal128(38, 0)` on DuckDB and cannot be read by earlier
  versions; layers written by earlier versions still restore and compose
  identically.
- Constructor refusals are coded. An argument value that a public constructor,
  recipe, expression, split policy or model binding input cannot accept raises
  `TerrineError` at construction (still a `ValueError`; formerly a bare
  `ValueError` or `TypeError`), with context `field` (the parameter name) and
  `reason` (a stable token such as `empty_name`, `wrong_type`, `negative`,
  `not_positive`, `not_positive_integer`, `naive_datetime`, `invalid_date` or
  `unsupported_value` with `choices`). A refusal raised by a constructor a
  recipe or convenience function calls names the caller's own parameter
  (`occurrence`, `period`, `bin`, `start_event`, `end_event`, `state_source`,
  `periods` in the recipes; `thresholds` and `horizon` in `retention`;
  `anticipation` in `Sequential`; `complete` in `Events` and `History`), and
  the record constructors name theirs (`period_seconds`,
  `anticipation_seconds`, `interval`, `count_thresholds`,
  `finite_horizon_seconds`). `terrine.design.invalid` covers
  `FixedDate`, `EventAnchor`, `Schedule` (and `Schedule.instants` on an elapsed
  schedule), `AnchorRange`, `Treatment`, `Design`, every expression constructor
  and node, `Trajectory`, `CohortSpec`, `CohortGrouping`, `TimeToEvent`,
  `LifeTable`, `Initiation`, `PersistentAdoption`, `TrialWeight`,
  `MatchedT0Sample`, `SequentialSpec`, `EventPanelSpec`, the argument types of
  `CalendarComparisonSpec`, `Sequential`, `CalendarPanel`,
  `CalendarComparisonPanel`, `EventPanel`, `matched_t0` and `to_json`/`to_yaml`
  of a value that is not a `Design`; `from_json` and `from_yaml` refuse every
  structural defect of a parsed document (a missing or extra field, an unknown
  node, a value of the wrong JSON type) with `reason="malformed_document"`
  instead of a bare error, and keep a node constructor's own refusal.
  `StateObservations`, `BinnedMetricState` and `StatePanel` refuse with
  `terrine.state_panel.invalid_state_source`; `Coverage`, `BoundCoverage`,
  `UnitSource`, `Events` and `History` with `terrine.model.invalid_binding`;
  `DatasetDefinition(base_design=)` and `DatasetDefinitionStore` with
  `terrine.dataset.invalid_definition`; and duplicate or empty
  `training_set` selections with `terrine.dataset.invalid_selection`, an
  unknown `incomplete_labels` with `terrine.dataset.invalid_view`. Two codes
  are new: `terrine.dataset.invalid_split` for `TimeSplit`, `GroupSplit`,
  `TimeSplit.bind`, split records restored from a dataset definition and
  `training_set(split=...)` (a value that is not a split policy, a group that
  is not the unit or cluster unit), and `terrine.budget.invalid_limit` for a
  `Budget` limit that is not a nonnegative integer or `None` and a
  non-boolean `strict_scan`. Existing refusals change context: a negative
  `EventAnchor` offset carries `reason="negative"`, `field="offset"`;
  `survival` and `life_table` with `occurrence="each"` carry
  `reason="single_entry_required"`, `field="occurrence"`, `choices`; a naive
  `history_start` keeps `terrine.design.invalid_treatment_history` with
  `reason="naive_datetime"`, `field="history_start"`. The
  `terrine.design.missing_reference` context gains `available`, the sorted
  declared names of the same kind (the model's metric names for
  `kind="metric"`), and its message lists them. The dataset agent tools return
  `terrine.design.invalid` (formerly `terrine.agent.invalid_request`) for a
  design document with unknown fields or wrongly typed values, and
  `terrine.dataset.invalid_split` (formerly `terrine.dataset.invalid_definition`)
  when a training view's split group is not the unit or its split record does
  not restore (`propose_definition`, `validate_definition`, `save_design`).
- Owner, runtime, manifest, export, orchestration, agent-policy and simulation
  inputs refuse with coded `TerrineError`s as well (formerly bare `ValueError`,
  `TypeError`, `RuntimeError` or `FileExistsError`), with `reason` and `field`,
  reusing the code of the same hazard. A runtime namespace or destination name
  that is not a simple identifier (`LeaseStore`, `RuntimeStore`,
  `DuckDBBackend`, `DestinationRef`, `DatasetTools`, `SavedPlan.create`, the
  Airflow and Dagster builders) refuses `terrine.dataset.invalid_destination`;
  `find_run_by_request_key`, `PlanRequest.from_dict`, `Dataset.plan_refresh(changes=)`
  and an orchestration provider that does not return a `PlanRequest` refuse
  `terrine.runtime.plan_request_invalid`; `RefreshPlan.deserialize` and
  `SavedPlan.create` refuse `terrine.runtime.saved_plan_mismatch` (`reason`
  `malformed_plan`, `unsupported_plan_version`, `fingerprint_mismatch`, ...);
  manifest records and `DatasetManifest.from_json` refuse
  `terrine.dataset.invalid_manifest` with token reasons and keep a record's own
  `field`; `Dataset(manifest=)` refuses `terrine.dataset.invalid_manifest`;
  `ExportManifest.from_json` refuses `terrine.dataset.invalid_export`;
  `ArrowSurrogate` refuses `terrine.export.invalid_surrogate`; `AgentPolicy`,
  `DatasetTools` and an owner budget refuse `terrine.agent.owner_configuration`
  and a requested budget `terrine.agent.invalid_request`; `dump_catalog` refuses
  `terrine.catalog.invalid_definition`; `Readiness` refuses
  `terrine.model.invalid_binding`; `Frame.cohorts` refuses
  `terrine.cohort.invalid_operation`; and `ScaleConfig`'s seed refuses
  `terrine.simulate.invalid_seed`. Three codes are new:
  `terrine.runtime.invalid_configuration` (Airflow and Dagster builder
  settings, `OpenTelemetryAdapter(queue_size=)`, `SnapshotAuthority(owner=)`, a
  `ControlServer` namespace that differs from its store, and an endpoint that
  exists and is not a socket or is already served, formerly `FileExistsError`),
  `terrine.export.invalid_input` (`preflight_export` data and consumer) and
  `terrine.simulate.invalid_config` (`ScaleConfig` dimensions and version,
  stream arguments). An Airflow or Dagster materialization whose run does not
  succeed raises `terrine.runtime.execution_failed` with context `run_id`,
  `state`, `error_code` (formerly `AirflowException` or `RuntimeError`).
  `Model.from_tables` and `Model.from_catalog` binding refusals carry token
  reasons with `field` (`wrong_type`, `undeclared_name`) instead of prose.
  Decoders keep a coded refusal raised while rebuilding a record instead of
  rewrapping it: `PlanRequest.from_dict`, `PlanInvocation.from_dict` and
  `SavedPlan.from_json` surface `terrine.dataset.invalid_destination` for a
  stored destination that is not a simple identifier (and the record's other
  codes, such as `terrine.budget.invalid_limit` for a stored budget, also from
  `RefreshPlan.deserialize`), and a scoring definition's malformed eligibility
  refuses `terrine.design.invalid` through the agent tools (formerly
  `terrine.dataset.invalid_definition`). A
  public constructor given a non-collection where it iterates one
  (`MatchedT0Sample(strata=5)`, `retention(thresholds=5)`, `EventPanelSpec`,
  `CalendarComparisonSpec`, `Events(labels=)`, `DatasetDefinition(transforms=)`,
  `life_table(group_by=)`, `training_set(label=)`, `from_tables(units=, event_ids=)`,
  `AgentPolicy` collections) refuses with its family's code and
  `reason="wrong_type"` instead of a bare `TypeError`, and an unhashable
  `event_time(occurrence=)` or `EventSequence(representation=)` refuses instead
  of raising `TypeError`. Pydantic models (`WorldConfig`, the agent's view and
  response schemas) keep raising pydantic's `ValidationError`; the agent tools
  report it as a coded refusal.

## 0.1.0.dev1

### Backend compatibility

- Admit any other genuine Ibis backend for read-only use only after an
  explicit `allow_unverified=True` on `Model.from_catalog`, `Model.from_tables`
  or `Model.from_increment`. By default such a backend refuses with
  `terrine.backend.capability_unavailable` (`operation="query"`,
  `status="unverified"`, a route naming the flag and the verified engines)
  before its schema is read; a non-Boolean choice refuses with
  `terrine.model.invalid_binding`. Admission emits the new filterable
  `terrine.UnverifiedBackendWarning` once at the caller's line and binds
  consent to that Model, not the connection. Registered identity failures,
  raw drivers and name-only connections remain refused with or without the
  flag. The separate `GENERIC` engine record is read-only: retained input
  capture, staging, publication, published Dataset reopening, native Parquet
  unload and transaction snapshots refuse before any write, and strict scan
  budgets still refuse unknown source costs. `Catalog.dialect` keeps the
  connection's real SQL dialect; the expression policy is `generic`.
- Execute admitted reads on the caller's borrowed connection, one independent
  statement at a time: Terrine issues no transaction control, session
  settings, temporary objects, cloning or disconnection, but Ibis schema
  inspection may end an open caller transaction, so pass a connection with no
  pending writes or a dedicated read connection. The execution snapshot has
  no identity and `replay_equivalence="unverified"`. Admitted SQL and every
  compiled statement are parsed for read-only safety in the real dialect and
  fail closed with `terrine.sql.invalid_read_query`. Every SQL verification
  step on every engine (parsing, rendering, scope analysis, the PostgreSQL
  function check, the BigQuery and Snowflake routine checks and Snowflake's
  Time Travel pinning) runs on a fresh thread, whose recursion depth is
  counted separately, so a deeply nested compiled statement is verified
  whatever the caller's own stack depth without changing the interpreter's
  recursion limit, and relation discovery reads the verified scope graph
  rather than traversing nested derived tables again; a statement too deep to
  verify even then refuses with `terrine.sql.invalid_read_query` and
  `reason="too_deep_to_verify"`, never as unsafe SQL or a raw
  `RecursionError`. Unsupported primitives
  refuse with `capability_unavailable` naming the backend and operation,
  chained from the backend's exception; owned Arrow readers are released on
  exhaustion, error, early close or context closure, and a failure during
  batch consumption carries the same code.
- Run lazy scalar known-answer witnesses once per execution context, before
  any source row and only when a relation needs them; materialization and the
  bounded preview submit every relation they will run (output, audit and each
  integrity check) as soon as the study is compiled, so an unsupported
  primitive refuses before its roster and event checks scan, while an
  independent statement on an execution context is admitted when issued:
  exact decimal arithmetic
  (scale-9 low digits at a large offset, cancellation, the domain edges,
  signed quotients, both `int64` endpoints and a 38-digit coefficient), which
  refuses `operation="exact_state"` on a backend such as SQLite that stores
  decimals by affinity, and UTC wall clocks of aware-timestamp deltas at four
  instants across daylight-saving, pre-epoch and local-mean-time offsets,
  which refuses `operation="utc_wall_clock"` where Ibis lowers the delta
  through session wall clocks (PostgreSQL-family backends in a non-UTC
  session) rather than answering by the session offset. Nanosecond
  timestamps refuse the portable UTC conversions (`utc_wall_clock`,
  `utc_instant`) while the expression is built; sub-second parts keep
  nanosecond support.
- Complete the portable base expression policy used after unverified
  admission, so every study expression is an ordinary Ibis expression without
  native bindings. Float facts enter as `DECIMAL(38, 9)` state by truncating
  the backend's decimal text toward zero at nine places (`|Q9(t) - t| <
  10**-9`; magnitudes at or above `10**29` and non-finite values refuse with
  the existing `float64_precision` failures); counts and source-decimal
  coefficients stay in a separate `DECIMAL(38, 0)` domain whose literals,
  typed NULLs and narrowing bounds take the coefficient's type. Prefix
  differences leaving the fixed domain and outputs leaving their target type
  refuse with the existing codes. Accumulations are admitted only when a
  binary64 bound on their magnitudes proves the exact sum fits: with `n`
  contributing values the computed bound `B` lies within
  `e = (1 + u)(1 + γ_{n-1}) - 1` of the true magnitude sum (`u = 2**-53`,
  `γ_k = k·u / (1 - k·u)`), so a partition is admitted iff
  `B < limit · (1 - e)`, evaluated with its own rounding on the conservative
  side; otherwise its values are withheld before the backend adds them and
  every consumer of the state (sums, averages, ratios, cohort totals and
  means, cumulative trajectories) refuses with `float64_precision`
  (`bound_overflow`) for float facts or `exact_overflow` for coefficients,
  never a backend error, a narrowed value or a silent NULL. The bound is
  deliberately conservative: opposite-sign history whose magnitudes alone
  exceed the domain is refused even where the exact difference would fit.
  Exact-state ratios divide correctly rounded binary64 operands once (within
  `3.1u` before later rounding); integer quotients truncate toward zero
  through a certified decimal residual correction and are NULL beyond int64
  or for a zero divisor. Timestamps use native order, typed UTC epoch deltas
  and signed parts; fixed-offset local days and their start instants derive
  from one epoch delta and one floor quotient (`Dialect.local_day`,
  `Dialect.day_start`) with every intermediate inside `int64` at both
  endpoints under the extreme offsets, which keeps day buckets and endpoint
  alignment shallow where chained conversions would nest (registered engines
  keep their cast-based forms, compiled unchanged); the union/window as-of
  fallback, coalesced extrema and portable integer sequences are the
  defaults. Measured on DuckDB and
  PostgreSQL 16 with the policy forced: shortest round-trip float text within
  half an ulp, exact `0.1 + 0.2` and scale-2 cancellation, both signed
  `int64` quotient endpoints, and nested quotients within `8u`. Registered
  engines keep their native state and every previously refused operation
  (`exact_ratio` and `exact_is_nonfinite` on DuckDB and Snowflake; normalized
  significand division on PostgreSQL and BigQuery) through explicit
  overrides; their native conversions, refusals and session lifetimes are
  unchanged.
- Record the engine stamp on every result: required `Provenance.engine_name`
  (actual backend), `dialect` (admitted expression policy) and `engine_status`
  (its registered status), written by direct materialization, the staged
  runtime and every fixture builder, and retained through views, cohorts,
  life tables, Arrow, JSON, manifests and exports. Arrow metadata stays
  version 2 under `terrine.materialized_frame.v2` and JSON metadata version 1.
  Payloads saved before the stamp existed decode as their recorded verified
  execution dialect; a partial stamp, one contradicting the registered policy
  or the execution snapshot's backend, or an absent stamp with an unregistered
  execution backend, refuses with the new
  `terrine.metadata.invalid_engine_stamp` (`reason`, present `fields`) instead
  of acquiring verified status.
- Centralize verified engine admission, capability records and expression
  selection without changing engine execution behavior, supported builds,
  session lifetimes or connection ownership.
  Unsupported-operation refusal routes now name registered verified engines
  implementing the operation, correcting stale DuckDB/PostgreSQL-only advice;
  exact-ratio routes name PostgreSQL and BigQuery. Refusal codes and non-route
  context are unchanged.
  Admission gates and static inspection both honor the selected provider's
  effective capability evidence; unavailable dialect admission still refuses
  before materialization.
- Encapsulate native numeric types, exact arithmetic and float conversion in
  each engine's expression policy. Shared normalization retains the existing
  coefficient bounds, NULL/nonfinite flags, overflow behavior and rounding
  tolerances.
- Encapsulate timestamp precision, UTC conversion, infinity ordering and exact
  output narrowing in each engine's expression policy. Explicit timestamp
  selection also governs nested operations without changing the enclosing
  expression scope; temporal windows and cohort bucket remainders are unchanged.
- Centralize as-of joins, NULL-ignoring extrema, integer sequences and feature
  admission in the registered engine policies. Strict/inclusive cutoffs, duplicate
  requests, aliases and existing feature/build boundaries are unchanged.

### Fixed

- The registered DuckDB, PostgreSQL and Snowflake UTC conversions treated a
  naive timestamp as a value to attach UTC to, so the wall clock they returned
  was zone-aware and any date read from it followed the session time zone: the
  start of the day containing naive `2025-01-01 00:00:00` came out as
  `2024-12-31` in an `America/New_York` session. A naive value is its own UTC
  wall clock and is now returned unchanged, and a UTC-aware value is its own
  instant. Dataset features were not affected: anchors are UTC-aware and naive
  source timestamps enter through the instant conversion; the wrong answer was
  reachable only by applying the wall-clock conversion directly to a naive
  value, as endpoint alignment on naive values now does.
- Source distributions no longer include internal planning documents or local
  build and test output.

### Tests

- Centralize known-answer engine conformance cases for whole-day schedules,
  NULL-ignoring extrema, cancellation-safe float and exact decimal windows,
  UTC timestamp conversion, tied event ordering, censor times and strict as-of
  cutoffs. Run the same cases on DuckDB and live PostgreSQL. Add the day
  start of a naive timestamp in a non-UTC session, with a day-aligned feature
  over naive source events.
- Compile the same known answers with the portable base expression policy on
  each real connection under `--generic-dialect` (DuckDB by default, PostgreSQL
  with `-m postgres`), about twenty seconds per backend inside the existing
  integration and `make test-postgres` checks, and check the policy's
  numerical and relational contracts against independent oracles on both, plus
  real unregistered-backend reads and refusals on the Ibis SQLite backend.

### Documentation

- Describe the verified, opted-in unverified and refused backend tiers, the
  portable numerical contract and its admission witnesses, and the engine
  stamp in the compatibility, capabilities and public API guides. Add
  `examples/unverified_backend.py`, which opts in to the Ibis SQLite backend,
  shows the default refusal, a coded operation refusal, a real read and its
  stamp.
- Rewrite the README around a customer spend forecasting table built from
  pandas DataFrames, with the shapes Terrine builds and how to read its output.
  The first-dataset guide follows the same example.

### Public API

- `EventAnchor(..., offset="1d")` shifts analysis time zero by a non-negative
  elapsed duration for landmark datasets, including survival after a baseline
  feature period. First/nth selection and `anchor_range` retain original-event
  semantics. Nonzero offsets participate in anchor identity, provenance and
  Design hashes; zero-offset serialization and hashes remain unchanged.
  Negative offsets refuse with `terrine.design.invalid`; persisted-layer
  substitution for shifted anchors refuses before source access with
  `terrine.compile.runtime_shape_unsupported`. Recipes are unchanged.

### Breaking changes

- Constructing `Provenance` requires `engine_name`, `dialect` and
  `engine_status`; saved payloads without them still decode as their recorded
  verified execution dialect.

## 0.1.0.dev0

### Documentation

- Start the README with a runnable table-first dataset and its completeness
  columns, then choose a dataset type. Add executable feature patterns for
  rolling windows, conditional events, eligibility, as-of attributes, recency
  and Boolean labels. Keep output budgets, staging, owned inputs and development
  details in their guides.

### Public API

- Construct common dataset types with `terrine.recipes`: time series, trial
  panels, event panels, cumulative LTV, survival, life tables, state panels and
  bounded event sequences. Each returns a plain low-level `Design`, retains
  supported row/role options and refuses unsupported keywords. Life-table
  features must exactly match its explicit grouping names.
  Start with these constructors for common row meanings; the unchanged public
  Design, anchor, expression and shape API remains their full-flexibility layer.
  Constructor-first guides include equivalent low-level Designs.
  LTV requires an untransformed native metric with exactly one SUM measure;
  generated count and mean metrics are not accepted by that shape.
- Recipe examples use the same adjacent per-value completeness columns as their
  materialized datasets, including LTV values and empty event-sequence counts.
- Bind table-backed events with immutable `Events` descriptors and
  `Model.from_tables`, retaining explicit unit rosters and completeness.
  Generate event/value facts and count, sum and event-mean metrics; refuse
  ambiguous names and direct multi-unit sources to `Model.from_catalog`.
  Run `examples/table_first_dataset.py` for zero-activity roster retention,
  generated metrics, bounded materialization and event-origin LTV; all dataset
  constructors have executable examples in CI.
- `Model.from_catalog` accepts plain Catalog mappings and validates them with
  the same coded refusal contract as YAML-loaded definitions. `GroupSplit` and
  `TimeSplit` are public through `terrine.dataset` for training-set workflows.

### Breaking changes

- Publish per-value completeness as adjacent `<name>__complete` and
  `<name>__reason` columns across every dataset shape, including boolean outputs
  and eligibility. Life-table reason columns retain lists of exact reasons.
  Event properties such as `kind_complete` remain values, not status evidence.
  User output names ending in either suffix now refuse during Design validation
  with `terrine.spec.reserved_output_name`.
- Default Frame dataframe conversions are compact, lossy views that omit
  observation bounds, resolved times and anchor provenance. Pass
  `include_observation_metadata=True` to keep them. Arrow conversions, staged
  datasets, manifests and saved fixtures retain every column, with identity
  first, adjacent value/support triples next, and bookkeeping last. Missing
  required observation columns refuse Arrow reconstruction with
  `terrine.metadata.missing_columns`. Anchor provenance and frame observation-bound
  names are reserved at Design validation.
- Rename the saved recipe family without compatibility aliases:
  `DatasetRecipe` → `DatasetDefinition`,
  `DatasetRecipeStore` → `DatasetDefinitionStore`,
  `DatasetRecipeReference` → `DatasetDefinitionReference`, and
  `SavedPlanRecipe` → `SavedPlan`. The canonical runtime `PlanRecipe` becomes
  `PlanDefinition`, available through `RefreshPlan.definition`.
- Move `terrine.runtime.recipe` to `terrine.runtime.saved_plan` and
  `terrine.agent.recipes` to `terrine.agent.definitions`.
- Rename refusal codes `terrine.dataset.invalid_recipe` →
  `terrine.dataset.invalid_definition` and `terrine.runtime.plan_recipe_mismatch`
  → `terrine.runtime.saved_plan_mismatch`. Malformed, unknown-field and old-format
  manifest JSON now refuses with `terrine.dataset.invalid_manifest`.
- Rename saved-plan wire fields `dataset_recipe_json` → `dataset_definition_json`
  and `recipe_hash` → `plan_hash`. Manifest fields become `definition`,
  `definition_reference` and `definition_hash` instead of `recipe`,
  `recipe_reference` and `recipe_hash`. Owner requests use `dataset_definition`
  instead of `dataset_recipe`; invocations use `saved_plan_json` instead of
  `recipe_json`. Fitted winsorization identities use `definition_hash` instead of
  `recipe_hash`. Agent receipts and dataset references use the matching
  `plan_hash`, `definition_reference` and `definition_digest` names.
- Rename agent tools `get_dataset_recipe_schema` →
  `get_dataset_definition_schema`, `propose_recipe` → `propose_definition`, and
  `validate_recipe` → `validate_definition`.
- Keep canonical bare definitions without transforms readable with identical
  bytes and digest. Old manifest and saved-plan envelopes refuse with the codes
  above; old owner planning requests refuse with
  `terrine.runtime.plan_request_invalid` rather than dropping their saved view.
  Definitions and fitted transforms carrying the old fitted `recipe_hash` refuse
  with `terrine.dataset.invalid_definition`; refit from the declared training
  population and save a new definition. No artifact migration is performed.
- Preserve persisted owner SQL identifiers: table `runtime_dataset_recipes` and
  the `recipe_json` columns in `runtime_dataset_requests` and
  `runtime_dataset_recipes` are unchanged. Existing owner databases still open
  and accept new generations even when their old manifests cannot be reopened.
- Resolve agent receipts by persisted invocation identity before decoding their
  saved plans. Unrelated historical invocations do not block new execution or
  staging; directly resolving an old receipt still refuses with
  `terrine.runtime.saved_plan_mismatch`.

### Dataset construction

- Build typed, point-in-time analytical and ML datasets with a standalone native
  Model → Design → Frame → Dataset workflow and a complete executable tutorial.
- Lead onboarding with consumer installation, bounded collection and a real
  output-refusal recovery; stream common-stack batches incrementally.
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
- Accept `gt`, `eq`, `is_null` and `all_of` rules as boolean features and
  outcomes, preserving incomplete-window nulls and reasons, three-valued
  conjunctions and full temporal support for follow-up and training splits.
- Refuse approximate-quantile operands in boolean outputs before reading data;
  reuse `terrine.metric.approx_eligibility_unavailable` across predicate roles
  and ordinary/persisted-runtime admission.
  Use exact quantiles or output the approximate value without a predicate.
- Include every nested event-selector dependency in sequential outcome
  follow-up, even when a plain or boolean outcome measures before its selector.
  Preserve sustained-control initiation checks through the full search horizon.
- Preserve evaluated null reasons on event-relative results after a selector
  resolves, independently of completeness.
- Select the last matching event in event-time selector windows with deterministic
  timestamp and event-ID ordering; keep complete empty lookbacks as null.
- Build sequential/calendar/event panels, bounded state pairs, LTV trajectories,
  cohort summaries, per-unit time-to-event records and risk-set counts/plain
  rates. Retain row identity, temporal support and typed shape provenance.
- Refuse exact cumulative LTV totals that exceed the declared decimal output
  type; preserve unavailable cumulative values and their reasons.
- Retain declared treatment values and support metadata in time-to-event output.
- **Breaking:** `TimeToEvent.duration` is now an elapsed Arrow duration from
  `t0`, not a terminal timestamp. `event_time` and `censor_time` remain the
  absolute timestamps; lifelines and LifeTable consume elapsed durations.
- Build deterministic bounded event sequences, sessions, funnels, transitions
  and event-relative selectors with explicit tie, missing-predecessor and
  temporal-leakage rules.
- Support exact retention occurrence bands, exact min/max/distinct/type-7
  quantiles, explicit native approximate distinct, and optional cross-sectional
  DataSketches quantile sidecars.
- Admit PostgreSQL execution only when `pg_catalog.version()` identifies the
  measured PostgreSQL 16 server; PostgreSQL-wire-compatible forks and unverified
  PostgreSQL versions refuse before source schema access.

### Reproducibility and ML

- Stage immutable DuckDB generations with bounded Arrow transfers and pinned
  reads. Preserve logical types, column roles, coverage, completeness, precision
  metadata and construction definitions through manifests and Arrow serialization.
- Reopen and refresh saved training/scoring definitions in a fresh process. Explicit
  source corrections publish a new generation without mutating existing readers.
- Select training features/labels with exact incomplete-label exclusions and
  leakage-aware group/time splits, purging, embargo and unassigned-row reasons.
- Resolve scoring instants once, select an explicit eligible roster and omit
  unused labels, treatment and label-source reads.
- Apply fixed or train-only fitted post-aggregation winsorization, retain exact
  frozen limits and require matching feature/definition/snapshot identities on reuse.
- Export resumable checksum-verified local Parquet shards with semantic metadata,
  exact decimal/timestamp/null values and typed empty outputs.
- Enforce output-byte budgets against cumulative Arrow batches before Parquet
  writes; capped export replays re-read the pinned stream while manifests retain
  physical Parquet byte counts.
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
- Materialize Frames directly from Snowflake (`terrine[snowflake]`): each run
  pins every table and view read to one Time Travel instant on the caller's
  connection (reads only; tables need `DATA_RETENTION_TIME_IN_DAYS >= 1`, the
  session `TIMEZONE` must be `UTC`) and re-verifies every relation's `CREATED`
  and `LAST_DDL` once each statement completes, before any row reaches the
  caller, so a table swapped, renamed or replaced into place during a run
  refuses, and one without identity metadata refuses with a route. Row access
  and masking policies are evaluated for the reading role at query time and
  are not pinned; views calling user routines refuse, while built-ins inside
  views are evaluated per query. Names follow Snowflake's identifier rules,
  inside admitted SQL too,
  and every physical reference is rewritten fully qualified at binding, so a
  later `USE SCHEMA` cannot redirect a pinned read. `NUMBER(p, 0)` columns
  beyond int64's 18 digits resolve as `decimal(p, 0)`, so wide integer keys
  and properties reach the output (integer properties accept scale-zero
  decimal columns on every backend). The PostgreSQL coverage and tolerance
  contract carry over with exact state in `NUMBER(38, 0)`: float facts follow
  the DuckDB design (per-unit scale, one correctly rounded `TRY_TO_DOUBLE`
  rounding) from each value's 18-digit `DECFLOAT` text, since Snowflake's
  row-wise text renderings of a double are lossy, so float windows match
  DuckDB within `2⁻⁵¹ · Σ|x| + (n + 1) · 2⁻¹⁰⁷⁴` (`n` subnormal inputs) rather
  than bit for bit; coefficients or sums beyond 38 digits and results beyond
  binary64's range refuse with a code; integer filters compare `NUMBER(p, 0)`
  columns against their physical range; event-anchor identities render
  timestamps at full precision; timestamp scales other than 6 and 9 promote
  losslessly for time-to-event and first-event lookups; and nanosecond
  timestamps are exact in SQL while Arrow outputs outside 1677-2262 refuse
  (`terrine.execution.arrow_timestamp_range`). Live Snowflake probes run
  weekly and on dispatch (`make test-snowflake`).
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
- Stage window-arithmetic intermediates in shared projections and resolve the
  compile dialect from the Design's scope, so one-row float averages, ratios and
  multi-column Frames materialize in roughly half the time with identical
  values, metadata and refusals (`benchmarks/run_overhead.py`).
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

- **Breaking:** compute DuckDB float windows from exact state. Each input now
  enters as its shortest decimal (at most half an ulp from its binary value) at
  a scale derived per unit from the data, the window's exact difference is
  rounded to binary64 once, and averages and ratios divide before that
  rounding, so a published quotient `r` of exact `p/q` satisfies
  `|r − p/q| ≤ 6·2⁻⁵³·|p/q| + 2⁻¹⁰⁷⁵`; PostgreSQL and BigQuery keep their
  exact division and single rounding. Results no longer carry
  earlier floating-point accumulation error, so they can differ from earlier
  DuckDB results (`0.1 + 0.2 − 0.3` is `0.0`), and long histories no longer
  refuse. The certified-interval machinery is removed: `absolute_error_column`
  and the `checked_float64_v1` numeric policy leave `ColumnMeta` and saved
  frame metadata (float outputs carry `numeric_policy=None`; metadata saved
  with the removed fields does not decode), `terrine.temporal.float64_unsupported`
  and the `float64_precision` failure kinds other than `nonfinite_input` and
  `bound_overflow` are gone, the state-panel reason `ambiguous_state_bin` and
  the eligibility reason `uncertain_comparison` no longer occur because bins
  and predicates compare the rounded value, and runtime plans no longer carry
  a prefix-certificate layer (`benchmarks/README.md` records the cost).
- Preserve integer/decimal scale; reject unrepresentable temporal/numeric
  results instead of narrowing or overflowing.
- Report native Catalog schema errors as `terrine.catalog.invalid_definition`
  with immutable, field-located validation categories and recovery hints.
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
