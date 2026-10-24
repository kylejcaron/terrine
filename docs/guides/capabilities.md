# Dataset capabilities

Verified execution means an actual local DuckDB path has an executable contract,
not merely SQL compilation. The combinations below point to their acceptance
cases. They are not a Cartesian promise that every metric is valid on every
shape. Source type, history, overlap and temporal validation still apply.

## Metric × shape × backend

| Metric/construction | Shape with local DuckDB evidence | Ordinary Frame | Retained runtime | Other backends |
| --- | --- | --- | --- | --- |
| Additive counts/sums/event or aligned-day means/ratios | [Cross-section](../../tests/compile/test_columns.py), [sequential/event panels](../../tests/compile/test_panels.py), [state panels](../../tests/compile/test_state_panel.py) | Sparse prefix windows with completeness and checked precision | Cross-section, sequential, event and state-panel substitutions | PostgreSQL/BigQuery/Snowflake: only operation-level support in [backend matrix](backend-compatibility.md#operation-evidence); unlisted backends: unverified |
| Exact min/max/distinct/type-7 quantile | [Cross-section](../../tests/compile/test_nonadditive.py), [bounded repeated windows](../../tests/compile/test_range_scaling.py) | Sparse range joins; compiler-derived overlap bound required | Raw-event bounded range route, not prefix subtraction | PostgreSQL/BigQuery/Snowflake: only operation-level support in [backend matrix](backend-compatibility.md#operation-evidence); unlisted backends: unverified |
| Approximate distinct | [Cross-section and bounded windows](../../tests/compile/test_nonadditive.py) | Explicit native HLL, approximation metadata retained | Bounded raw-event route where the shape is admitted | PostgreSQL/BigQuery/Snowflake: only operation-level support in [backend matrix](backend-compatibility.md#operation-evidence); unlisted backends: unverified |
| Approximate quantile | [Cross-section only](../../tests/temporal/test_sketches.py) | Explicit DataSketches t-digest sidecar; `sketches` extra | Refused; use ordinary materialization or exact quantile | PostgreSQL/BigQuery/Snowflake: only operation-level support in [backend matrix](backend-compatibility.md#operation-evidence); unlisted backends: unverified |
| Retention occurrence bands | [Cross-section, schedule and event panel](../../tests/compile/test_retention.py) | Exact distinct occurrences, null for incomplete follow-up | Bounded raw-event route on admitted shapes | PostgreSQL/BigQuery/Snowflake: only operation-level support in [backend matrix](backend-compatibility.md#operation-evidence); unlisted backends: unverified |
| Trajectory/cumulative value and cohort summaries | [LTV](../../tests/compile/test_ltv.py), [cohorts](../../tests/compile/test_cohorts.py) | Explicit age/denominator/completeness metadata | Shape substitution refused; ordinary Frame route | PostgreSQL/BigQuery/Snowflake: only operation-level support in [backend matrix](backend-compatibility.md#operation-evidence); unlisted backends: unverified |
| Time-to-event and life tables | [Per-unit records](../../tests/compile/test_time_to_event.py) | Counts/plain rates, event/censor/entry roles | Shape substitution refused; ordinary Frame route | PostgreSQL/BigQuery/Snowflake: only operation-level support in [backend matrix](backend-compatibility.md#operation-evidence); unlisted backends: unverified |
| Ordered events/sessions/funnels | [Bounded event sequences](../../tests/compile/test_event_sequence.py) | Position rows or arrays, deterministic event ties | Shape substitution refused; ordinary Frame route | PostgreSQL/BigQuery/Snowflake: only operation-level support in [backend matrix](backend-compatibility.md#operation-evidence); unlisted backends: unverified |
| Calendar-comparison dataset rows | [Panel construction](../../tests/compile/test_panels.py) | Explicit treatment-history and control definitions; no estimation | Shape substitution refused; ordinary Frame route | PostgreSQL/BigQuery/Snowflake: only operation-level support in [backend matrix](backend-compatibility.md#operation-evidence); unlisted backends: unverified |

Retained composition support is specified in the
[runtime shape matrix](materialization-runtime.md#runtime-shape-support) and
[verified-layer tests](../../tests/compile/test_persisted_layers.py). Unsupported
shape substitution or sidecars refuse before source work rather than selecting a
hidden full rebuild. For a fresh ordinary build, use `Design.build(model)`.

## Backend, conversion and adapter status

Local **Ibis DuckDB** is the reference dataset execution provider, and **Ibis
PostgreSQL**, **Ibis BigQuery** and **Ibis Snowflake** backends materialize
Frames for counts, sums, averages, ratios, as-of attributes and the panel and
time-to-event shapes. Raw DuckDB connections are valid for owner runtime state,
not as an Ibis Model provider. Arbitrary Ibis backends are unverified; an
accepted backend name or compiled SQL is not execution evidence. Remote driver,
object-store and reviewed dbt relation bridges are not advertised by the local
path. See [backend compatibility](backend-compatibility.md) for operation-level
capabilities and [semantic-layer interoperability](semantic-layer-interop.md)
for definition mappings.

Arrow is a base dependency. pandas and Polars conversions, orchestration
frameworks, sketches, increment and downstream modeling libraries are optional
extras. Their absence cannot prevent base/focused-facade imports. The
[common-stack guide](common-stack-interop.md), [orchestrators](orchestrators.md)
and [library hand-offs](library-handoffs.md) separate real examples from
unverified variants. A dataset hand-off never adds a native effect estimator.

For a coded refusal, use the [reference](../reference/refusals.md) and its linked
recovery category. Inherent information/validity limits are listed separately in
[limitations](limitations.md). Missing backend or adapter implementations are
support status, not inherent statistical limitations.
