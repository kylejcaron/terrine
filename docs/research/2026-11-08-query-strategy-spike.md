# Window-local aggregation experiment

2026-11-08. Research artifacts only; no production compiler behavior changed.

## Decision

Use one batched, window-local aggregate pipeline with raw rows or eligible noncumulative summaries. Do not retain a second general prefix engine merely to preserve existing machinery. Exact arithmetic remains an arithmetic policy, including existing `exact=True` per-input quantization.

Raw direct aggregation is not universally faster: overlapping anchors exposed a regression. Local daily summaries resolved it in both native and integer workloads, including summary construction. An independent Astra xhigh reviewer agreed with this revised direction after inspecting the results and runtime code; its initial recommendation had retained restricted exact prefixes.

The implementation proposal and complete pathway matrix are in [the design specification](../superpowers/specs/2026-11-08-practical-window-aggregation-design.md). Implementation, migration and release have not been performed.

## Method and reproducibility

Base: `d0a029901890066bc10314aec24f1bdfab8ebba5` (0.1.0a1). Python 3.12.11, Ibis 12.0.0, DuckDB 1.5.6, PyArrow 23.0.1, PostgreSQL 16.15 (Homebrew), macOS 15.7.4 arm64. Experiments ran sequentially on a shared workstation, with one DuckDB thread and PostgreSQL parallel query workers disabled. Timings are exploratory local observations, not CI thresholds or universal cost guarantees. A later health check reported machine memory pressure; no isolated peak-memory claim is made.

JSON records remain directly readable under `benchmarks/results/query-spike-*`. The 68 generated SQL and execution-plan files are preserved byte-for-byte in `benchmarks/results/query-spike-20261108-raw.tar.gz` rather than adding their expanded text to the repository. Restore them from the repository root with `tar -xzf benchmarks/results/query-spike-20261108-raw.tar.gz`; archive members retain the paths cited below.

Inputs reuse `benchmarks/workloads.py:load_tables`, `_cost_catalog` and `terrine/simulate/scale.py`. Each design requests five metrics (event count, sum, event mean, sum/event-count ratio, activity) over ten trailing UTC-day windows. The original cost workload uses integer values. The native variant converts the same generated values to binary64 tenths.

The current and candidate expressions publish the same 50 values plus completeness, reason, observed-from and observed-until fields for each output, with canonicalized names for comparison. Candidate check columns are also evaluated; validation flags cannot be silently ignored by the probe. The equality check includes all selected values and support metadata, not only sums.

Server timing: median of three `CREATE TEMPORARY TABLE ... AS <compiled expression>` evaluations, dropping the output outside the timed interval. Paired variants alternate order. Summary timings include source grouping; no prebuilt-summary advantage. Compilation, query-to-Arrow and a separate `EXPLAIN ANALYZE` are recorded independently. Ibis graph construction and SQL compilation are separate measurements. SQL-size counts distinguish formatted, compact and backend-submitted strings.

Only the **current** implementation has full `Frame.to_pyarrow()` timing. Candidate timings cover a standalone Ibis expression plus its simple checks, not all Frame snapshot/admission/integrity machinery. Server ratios must not be described as measured end-to-end Frame speedups.

### Fixed anchor: 10,000 units / 300,000 events

| Arithmetic | Current server median (s) | Raw local median (s) | Current compact/submitted SQL chars | Raw local SQL chars |
|---|---:|---:|---:|---:|
| Integer | 5.996 | 0.537 | 229,653 | 77,949 |
| Native float | 4.573 | 0.625 | 211,455 | 84,381 |

All 10,000 output rows match in values and support metadata. The current full Frame materializations took 22.189 s and 20.539 s respectively; these have no equivalent candidate-Frame measurements and are not used to compute a speedup.

The direct physical plans contain one hash join, one grouped aggregate and the small roster/time cross product. Three table scans are present (events, roster, anchor times); this is not a claim of a single physical scan for the complete operation.

Evidence: `benchmarks/results/query-spike-duckdb-fixed-20261108/`.

### Overlap: 1,000 units / 300,000 events, native floats

| Anchors per unit | Current median (s) | Raw local median (s) | Daily-summary median (s) | Raw matched event rows | Joined summary rows |
|---|---:|---:|---:|---:|---:|
| 1 | 1.759 | 0.437 | 0.291 | 96,670 | 6,004 |
| 7 | 1.948 | 2.225 | 0.558 | 696,670 | 42,694 |
| 21 | 2.505 | 6.312 | 0.547 | 1,976,690 | 121,412 |

Current/raw were measured together; daily summaries were a subsequent experiment against the identical deterministic workload. No claim is made about tight confidence intervals between those runs. Daily outputs match the raw outputs and the independent Python reference at every metric value/support field. The 21-anchor oracle checks 1,050,000 metric values plus their metadata.

Evidence: `query-spike-duckdb-overlap-20261108/` and `query-spike-duckdb-buckets-20261108/` under `benchmarks/results/`.

### Exact-integer repeated windows

All three strategies were measured in the same run for each row.

| Workload (1,000 units, 21 anchors/unit) | Events | Current median (s) | Raw local median (s) | Daily-summary median (s) | Raw join rows, including empty anchors | Summary join rows |
|---|---:|---:|---:|---:|---:|---:|
| Dense | 300,000 | 4.241 | 8.900 | 0.541 | 1,976,690 | 121,412 |
| Sparser | 30,000 | 2.660 | 0.680 | 0.215 | 167,972 | 79,662 |

Both cases match current output and the independent oracle. The sparser case provides only about 2.1-fold join compression; it is **not** a truly uncompressible or irregular-boundary benchmark. The evidence does not establish a universal raw-versus-summary crossover.

Evidence: `query-spike-duckdb-exact-dense-20261108/`, `query-spike-duckdb-exact-sparse-20261108/`.

### PostgreSQL observations

After explicitly running `ANALYZE` on source, roster and anchor-time tables, the current integer query still exceeded its 180-second statement timeout during the first server evaluation. This is a censored observation, not an exact duration. A separately executed raw local candidate completed with all 500,000 metric values and their support metadata matching the independent Python oracle.

| Integer, 10,000 units / 300,000 events / 50 outputs | Server evaluation (s) | Query-to-Arrow, including compilation/checking (s) |
|---|---:|---:|
| Current | Timed out at 180 | Not run |
| Raw local | 0.948 median | 5.205 |

Evidence: `query-spike-postgres-analyzed-20261108/timeout.json` and `query-spike-postgres-analyzed-direct-20261108/postgres-integer-anchors1.json`. Do not compute an exact speedup from a timeout.

Earlier unanalysed-schema runs are retained as diagnostics in `query-spike-postgres-fixed-20261108/` and `query-spike-postgres-direct-20261108/`. The native candidate also passed its oracle there, but no native current baseline was run. Query plans record temporary block use; no warehouse scan-cost or isolated peak-memory estimate is inferred from it.

### BigQuery compilation only

Metadata-only compilation used a real Ibis BigQuery backend policy, with no credentials, network queries or billed warehouse execution.

| Arithmetic | Current formatted chars | Raw local formatted chars | Current compact chars | Raw local compact chars |
|---|---:|---:|---:|---:|
| Integer | 1,063,112 | 126,982 | 525,500 | 87,078 |
| Native float | 2,273,557 | 137,431 | 912,025 | 93,767 |

The metadata-canonicalized statement differs from the older 1,050,208-character benchmark; these are fresh counts for this experiment, not a reproduction of that exact string. Formatted length alone does not establish rejection by BigQuery. SQL shape evidence does not establish warehouse runtime, bytes scanned or complete exact-domain correctness.

Evidence: `query-spike-bigquery-20261108/results.json` and its SQL files.

## Semantic checks

`benchmarks/query_strategy_semantics.py` exercises both raw and daily inputs with independently specified outcomes:

- Empty count/sum zero, unavailable empty mean, zero-denominator ratio.
- All-null sum remains null, not zero.
- Half-open endpoints and selected predicates.
- Source completeness remains per window; observation bounds withdraw only the affected result.
- Unresolved predicate flags are checked only where the output is available.
- An excluded large native history does not erase the requested value.
- Excluded nonfinite history does not poison the requested window; selected nonfinite input produces a blocking flag.

These checks are narrower than production label semantics: they do not distinguish unavailable dimension history from a known-null label. They do not cover arbitrary time zones, partial buckets, multiple sources or selector-dependent anchors.

`benchmarks/query_strategy_exact.py` executes independent DuckDB known answers:

- Two `6e-10` inputs and one `4e-10` input quantize individually to a combined `2e-9`, with one discarded nonzero term. Quantizing an already summed day would give the wrong answer. The current exact-float Frame agrees and carries `below_resolution`.
- Summaries preserve `9007199254740995` without a float round trip.
- Declared decimal summaries preserve `100000000000000000000.000000003`.

This proves selected arithmetic examples, not a general exact reducer, all-engine decimal correctness or overflow certification. Existing backend exact primitives remain necessary.

The Python workload oracle (`query_strategy_oracle.py`) independently filters each unit's source events by `[start,end)`, uses exact Python integers or `math.fsum`, derives means/ratios/counts, and checks reason/completeness/observation fields. Its `1e-10` relative / `1e-9` absolute float comparison applies to these bounded fixtures, not a proposed package-wide numerical promise.

## Retained execution and pathway coverage

Read-only mapping found one shared additive consumer: the `MetricAgg` branch of `compile_columns.expression`, which calls `lower_aggregate` once per expression. All recipes reach it for their additive leaves, including generated LTV periods, state-panel state/baseline windows, outcomes, eligibility and selector-relative windows. Specialized event-sequence, survival, retention, cohort and life-table algorithms are separately identified in the specification; source-window batching is not falsely advertised as replacing them.

Retained execution captures immutable source roots, rebuilds prefix layers, spools/restores them and calls the same compiler with substitutions. There is no cross-generation prefix delta/extension reuse in that path. Old final-generation reads require final partitions, not the prefix executor. Sources: `terrine/compile/numeric.py:29-120`, `compile/columns.py:1024-1080`, `compile/layers.py:154-267`, `compile/materialize.py:35-92`, `runtime/api.py:522-584,767-791`, `runtime/planner.py:313-351`.

The existing public `examples/materialization_runtime.py` was executed successfully: initial published totals `(3,4)` matched ordinary materialization; corrected totals `(8,4)` matched a fresh ordinary rebuild; reopening the original receipt preserved the initial generation. This exercises the **existing** retained architecture, not a prototype retained-local implementation. Its purpose here is to verify the migration surface is runnable.

The reviewed spec also covers bare `RefreshPlan` execution, not merely `SavedPlan`, and preserving verified inspection/snapshot-pin release for old plans while rejecting obsolete execution. Sources: `runtime/api.py:232-234`, `runtime/saved_plan.py:243-251,291-334`.

## Reproduce the representative experiments

Use the isolated environment with DuckDB/PostgreSQL extras; offline BigQuery compilation additionally needs the BigQuery extra. Each command requires a new output directory so earlier evidence cannot be overwritten. Actual run configurations are captured in each directory's `environment.json`.

```sh
python -m benchmarks.query_strategy_spike --units 10000 --anchors 1 --repeats 3 --full --output /tmp/query-fixed-new
python -m benchmarks.query_strategy_spike --units 1000 --events-per-unit 300 --modes native --anchors 1,7,21 --variants current,direct,bucketed --repeats 3 --output /tmp/query-overlap-new
python -m benchmarks.query_strategy_spike --units 1000 --events-per-unit 300 --modes integer --anchors 21 --variants current,direct,bucketed --repeats 3 --output /tmp/query-exact-dense-new
python -m benchmarks.query_strategy_spike --units 1000 --events-per-unit 30 --modes integer --anchors 21 --variants current,direct,bucketed --repeats 3 --output /tmp/query-exact-sparse-new
python -m benchmarks.query_strategy_semantics /tmp/query-semantics-new.json
python -m benchmarks.query_strategy_exact /tmp/query-exact-new.json
python -m benchmarks.query_strategy_bigquery /tmp/query-bigquery-new
```

For PostgreSQL, use a uniquely owned local scratch server and supply `--backend postgres --postgres-url ...`. The harness now explicitly analyzes source, roster and anchor-time tables before timing, and applies a 180-second statement timeout. It drops only its uniquely named schema. Do not run independent timing campaigns concurrently.

## Limits and consequences

- No production compiler or runtime code was changed. The prototype is intentionally restricted to one source, aligned UTC-day windows and the five benchmark metrics.
- The day-summary prototype groups the full source before matching; production must scope to consumed units/buckets before arithmetic. Global min/max bounds alone are insufficient for disjoint requests. The cost of that complete production scoping is not measured here.
- Raw versus summary reduction orders may differ in floating rounding or exceptional overflow/underflow. Selected-input locality is not a bit-identical result guarantee.
- Exact-float quantization and wide domains have tiny DuckDB known answers, not a large exact-float or warehouse-wide performance matrix.
- No live BigQuery/Snowflake execution, warehouse cost measurement, native peak-memory certification or fully integrated candidate Frame/retained performance measurement.
- Truly uncompressible and irregular repeated-anchor workloads remain unmeasured. No existing evidence warrants retaining an entire cumulative execution architecture as insurance against them.
- Local measurements support the architectural decision, not a promise that every public pathway becomes faster by the same factor.

Next implementation work should follow the specification as one complete compiler/runtime cutover, with every pathway given an explicit disposition. It must remeasure the integrated pipeline rather than promoting these throwaway helpers into production.
