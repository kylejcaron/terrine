# Practical batched window aggregation

Date: 2026-11-08. Status: proposed design, not an implemented compiler change.

## Intent and scope ledger

| Capability or decision | Source | Scope |
|---|---|---|
| Improve all pathways rather than one recipe | Owner: “the goal is to apply this improvement to every pathway” | Confirmed; every additive consumer and execution wrapper is covered below |
| Keep guarantees only when useful and practical | Owner: “only if theyre both useful and more importantly practical” | Confirmed design criterion; no obligation to preserve implementation-driven refusals |
| Benchmark representative strategies before specifying the replacement | Owner: “lets do it”, following the benchmark-then-spec discussion | This deliverable: experiment, evidence, pathway map and specification; no production changes |
| Obtain independent architecture assessment | Owner explicitly requested independent Astra xhigh review | Completed before the experiment and revisited with measured overlap results |
| Remove `exact=True`, change Catalog defaults, add support for new backends/shapes | Not requested | Excluded; existing exact declarations retain their meaning |
| Publish, release, merge or migrate user artifacts | Not authorized by this experiment | Excluded |

Goal: efficient multi-metric/window queries through a maintainable shared compiler, without making ordinary use pay for general numerical certification or a cumulative-state algorithm it does not need.

The proposed numerical/refusal and executable-plan changes below require approval as part of implementation. This document does not itself authorize migration. Existing published data is never rewritten or deleted.

## Evidence and decision

The reproducible experiment and limitations are recorded in `docs/research/2026-11-08-query-strategy-spike.md`; machine-readable records are under `benchmarks/results/query-spike-*20261108/`, with raw SQL and execution plans preserved in `benchmarks/results/query-spike-20261108-raw.tar.gz`. The experiment helpers are explicitly throwaway, not an alternative public compiler.

The investigation establishes two distinct problems: per-output relational duplication, and native-float lifetime-prefix subtraction. A weaker precision promise alone fixes neither. The wide cost workload is integer-valued; native floats already have no bitwise-parity guarantee. Removing floating-point guards is not sufficient to fix the documented wide-query cost.

Raw batched joins improve the fixed-anchor workload but regress on dense overlapping anchors. Therefore the target is **one window-local aggregate pipeline with two input representations: raw rows or noncumulative sufficient-statistic summaries**. Native and exact arithmetic are policies within that pipeline, not separate general query engines.

Do not build a new batched exact-prefix engine merely to preserve the existing mechanism. Existing retained execution rebuilds prefixes for each changed snapshot; it does not incrementally extend those prefixes across generations. The measured dense and sparser integer cases both favor local summaries, including construction. **No exact-prefix exception is included.** A future optimization proposal would need a concrete workload showing a practical requirement the integrated local pipeline cannot meet.

## Practical result contract

### Preserve dataset meaning

- Preserve unit and anchor identity, duplicate anchor instances, output order/names, half-open `[start,end)` membership, cutoffs and supported timestamp precision.
- Preserve source completeness separately from unit observation completeness. Availability is per output; one incomplete window cannot withdraw another compatible output in the same batch.
- Preserve zero versus all-null versus no observations; means divide by the appropriate nonnull count or calendar-day denominator. Ratios preserve numerator/denominator availability and zero-denominator reasons.
- Preserve known-null versus unavailable filter information and three-valued eligibility semantics. A batch must not silently drop events whose predicate could not be determined.
- Preserve provenance, immutable published generations, snapshot consistency, admission before source reads where required, and rejection before partial output is exposed.
- Retain current supported exact output domains for counts, integers and declared decimals. A representation that cannot hold a result fails; it never silently becomes float or wraps.

### Native floating point

Aggregate selected contributions in binary64 using the backend. Normal order dependence is allowed; no bitwise cross-engine parity, no universal relative-error bound for ill-conditioned sums or ratios, and no history-relative numerical certificate.

Windows must not depend on lifetime cumulative state or fail because an unconsumed historical bucket was summarized. This is selected-input locality, not bitwise invariance under every physical regrouping. Raw and summary execution preserve the same selected contributions, availability rules and logical validation scope. An input selected by another requested output remains relevant to that output.

Selected nonfinite inputs and nonfinite results cannot be returned as successful finite metrics. Use cheap aggregate/final-result checks where the backend returns nonfinites, and ordinary backend errors where arithmetic raises. Do not add magnitude-window scans merely to guarantee that an engine exception can never occur. Ordinary underflow and harmless rounding follow backend behavior. Keep stable package errors where the cause is reliably known; otherwise surface an execution error with the original cause, rather than misclassifying native overflow as exact overflow from a broad message match.

A summary introduces intermediate sums. Their overflow/underflow outcomes need not match a differently ordered raw reduction on every extreme input. Explicit arithmetic failure is allowed under the backend's supported domain; silently publishing corrupted values or discarded contributions is not. Do not reintroduce certification or automatic retries to make all physical orderings behave identically.

Validation follows the same policy in window results, cumulative trajectories and cohort reductions. Those specialized reducers do not need to use the source-window join algorithm, but must not retain obsolete native certification solely because it lived outside `lower_aggregate`.

### Exact arithmetic and `exact=True`

Exactness is independent of access strategy. Preserve existing backend exact-integer/decimal primitives and output-domain checks. Native SQL casts alone are not evidence that every engine preserves the existing wide domains.

For exact floats, quantize **each input** at the current scale and rounding semantics before summarization. Combine exact coefficients, event/nonnull counts and discarded-term counts. Convert once at publication/division as currently promised. Preserve `below_resolution` notes, including when a nonzero term disappears; never quantize an already summed bucket instead. This is quantized exactness, not exact summation of the original binary64 values.

Do not change omitted-`exact` Catalog defaults in this change. Do not remove `exact=True` or widen exact-mode promises. Exact representation overflow remains explicit; native and exact domain errors need not have identical backend evaluation order. Remove history-specific refusal behavior made obsolete by retiring cumulative state.

## Compiler architecture

### Batch before physical lowering

Collect resolved additive requests in `compile_columns`, above the current one-request `lower_aggregate` boundary. Reuse existing `BoundMetric`/`BoundMeasure`, `Window`, `TemporalResult`, `ColumnMeta` and compiled-study structures. Add only the internal request/group records needed to describe a batch; no new public expression vocabulary or general optimizer.

Collect feature, outcome and eligibility leaves together where their dependencies are ready. Resolve event selectors before batching their dependent windows. A dependency-ready group is not necessarily the whole design: state panels have different endpoint, row and baseline anchor relations; selectors can change origins; event panels expand anchors before aggregation.

Distinguish:

1. **Join compatibility:** same normalized source, unit mapping and anchor relation/grain. Different windows/predicates can share bounded source access.
2. **Primitive equivalence:** same value expression, predicate/label semantics, interval, arithmetic and primitive operation. Share sum/event count/nonnull count only when genuinely equivalent; an event mean does not use every count metric as its denominator.

Metric display names and existing aggregation-specific `measure_identity` are not adequate primitive-sharing keys. Preserve stable outward column order even when internal computation is shared.

Aggregate each independent source before joining summaries at the full `(unit, anchor_id)` grain. A cross-source ratio must never join both raw event tables and multiply contributions.

### Raw and summary inputs

Raw execution is the general path for exact-time, event-relative, irregular and otherwise ineligible bounds. Keep temporal comparisons and engine-specific operations in the existing backend helpers.

For statically aligned windows, a noncumulative daily summary may carry shared counts, sums and applicable filter/invalid-input/discarded-term indicators. Each consumed bucket must be wholly contained in the requested interval. Start with the existing fixed-offset day-boundary alignment rules; the experiment's UTC-midnight shortcut is not a production eligibility rule.

Do not introduce partial-bucket reconstruction in the first implementation. Ineligible requests use raw rows. Raw and summary inputs feed the same aggregate combination and semantic finalization. Scope summarization to consumed units and buckets, not merely a global minimum-start/maximum-end envelope: disjoint requests can leave unused gaps inside that envelope. A bounded semijoin of required unit/bucket identities, followed by the exact window and predicate conditions, is one suitable implementation. Unconsumed buckets must not be aggregated first and only discarded afterward.

Choose representations with small declaration-derived rules and demonstrated workload evidence, not hidden row-count probes or arbitrary anchor-count thresholds. A fixed-anchor raw plan and an aligned repeated-anchor summary plan are supported shapes. Physical changes preserve selected inputs, exact result domains and availability/validation rules; they do not promise identical exceptional-arithmetic behavior under different reduction orders. Explain the chosen representation through existing strategy/preview metadata.

No promise of one SQL statement or one scan: a shared scoped intermediate can be legitimate if total execution improves. No new persisted-summary/cache subsystem is included. Inline Ibis relations are the initial representation; future persistence must earn its complexity.

### Shared finalization and execution

Have both input representations produce the same primitive-result schema. One finalization path owns completeness, observation bounds, null reasons, exact output narrowing and practical checks. Return multiple outputs through `TemporalResult`, rather than rebuilding completeness and joining one result per metric/window.

Preserve the existing validation/publication boundary and snapshot ownership. Combining checks must not let ineligible filtering conceal invalid candidate data where the current study contract requires validation. The exact order of two simultaneous numeric failures and conservative historical rejection thresholds are not promises to preserve. Retain deterministic logical error selection where practical without requiring separate scans per output.

Keep all study computation in Ibis. Concrete backend modules retain only necessary type/operation/error differences. The engine registry remains the sole capability source. The runtime consumes compiled work; the compiler does not import runtime orchestration.

## Every-pathway rollout matrix

“Every pathway” means every additive consumer uses the replacement, not that unrelated statistical algorithms are falsely labeled as faster.

| Surface | Owning location | Required disposition |
|---|---|---|
| Ordinary fixed/scheduled Design; time-series recipe | `compile/columns.py`, `compile/numeric.py` | Batch all additive leaves, including features/outcomes/eligibility |
| Relative windows and nested selectors | `compile/columns.py` selector resolution | Batch only after the relevant origin is resolved; retain full anchor identity |
| Trial/Sequential/CalendarPanel | `compile/shapes/sequential.py` | Replace its shared additive compilation; keep treatment/risk-set/weight semantics |
| Event panel | `compile/shapes/event_panel.py` | Batch after event-period expansion; preserve composite anchor IDs |
| Calendar comparison | `compile/shapes/calendar_comparison.py` | Replace base scheduled aggregation before cohort/control stacking |
| LTV period outputs | `compile/shapes/ltv.py` | Batch generated age-window requests, not only user-named features |
| LTV cumulative and cohort reductions | `compile/shapes/ltv.py`, `cohorts.py` | Apply the practical arithmetic policy; preserve cumulative missingness/cohort grain; no source-window join required |
| Binned state panel | `compile/shapes/state_panel.py` | Migrate state-period, endpoint, row and baseline compilations; no implicit cross-grain sharing |
| Observed-state panel | `compile/shapes/_observations.py` | Migrate row/baseline additive leaves; preserve state observation/absorption logic |
| Event sequence/session/funnel | `compile/shapes/event_sequence.py` | Migrate base additive features/outcomes/eligibility; sequence payload remains specialized |
| Survival | `compile/shapes/time_to_event.py` | Migrate additive base expressions; preserve entry/event/censor occurrence semantics |
| Life table | `compile/dispatch.py`, `shapes/life_table.py`, `frame.py` | Migrate additive grouping leaves; Arrow risk/event/censor reduction remains specialized |
| Min/max/distinct/quantiles/sketches | `temporal/nonadditive.py`, `compile/columns.py` | Already local-range operations; preserve behavior and apply relevant arithmetic policy, no prefix-retirement speedup claim |
| Retention and as-of/attribute lookups | `compile/retention.py`, selector/as-of paths | Preserve their specialized algorithms; resolve dependencies for additive batching |
| `.expr`, preview, materialize and dataframe conversions | `frame.py`, `compile/cost.py`, execution providers | All use the same new compiler; retain documented raw-expression safeguard bypass |
| Stage/capture/refresh/saved-plan execution | `compile/layers.py`, `compile/materialize.py`, `runtime/api.py`, `runtime/graph.py`, `dataset/reference.py` | Remove new prefix production and substitution; compile local aggregation against the retained snapshot |
| Published reopen/export | `runtime/api.py`, `dataset/reference.py`, export modules | Preserve immutable final-generation reading and stored metadata; no recomputation under changed semantics |

Every internal `compile_columns` invocation is in scope. The first implementation does not promise global reuse across distinct invocations; avoid a new compilation-session abstraction without evidence it is necessary.

## Retained execution and compatibility

Retire additive prefix planning, task production, Arrow spooling/restoration and compiler substitution together. Remove obsolete native/exact prefix lowering and associated counters that no remaining calculation consumes. Keep exact arithmetic helpers needed by local aggregation. Audit layer registries, graph task kinds, saved fixtures, planner descriptions, estimated work and execution refusal registries in the same cutover; no empty prefix tasks or no-op replacement stages.

The retained pipeline continues to capture immutable inputs, resolve anchors/selectors, execute the shared local aggregation, validate, publish atomically and reopen receipts. Inline daily summaries execute inside that snapshot; do not introduce a new retained intermediate ABI just to replace the old one.

Separate three contracts:

- **Old final generations:** remain readable under their stored metadata/provenance; do not relabel them as newly calculated results.
- **Old executable plans/intermediate substitutions:** reject execution with a coded rebuild route rather than silently interpreting cumulative layers as local summaries. New saved plans use `SavedPlan.schema_version = 2`. Bind an internal aggregation-semantics revision into execution identity, and enforce it at every execution boundary, including bare `RefreshPlan` documents and direct `run_plan` callers—not only the saved-plan wrapper. Reconstruct a fresh plan from the ordinary dataset definition when explicitly refreshing.
- **No-change refresh:** the semantics revision invalidates computational reuse when moving to the new compiler. An old generation may still be opened deliberately, but a refresh under the new policy cannot take the no-task reuse shortcut and masquerade as a new computation.

Keep declaration identity separate from execution identity: `_bind_saved_plan` currently requires `plan.definition_hash == saved_plan.design_hash`; do not invalidate that equality by mixing a physical-plan revision into one side. The execution revision must also reach computational reuse decisions and artifact provenance. Old plans remain checksum-verified and inspectable, and their snapshot pins remain releasable through existing administrative operations; decoding an old plan does not authorize executing it. This preserves cleanup without retaining the prefix executor. No data deletion or automatic destructive migration is required. Generic/unverified backends remain read-only and statement-scoped; retained effects remain DuckDB-only.

## Verification and acceptance

This is one integrated compiler/runtime cutover, not a fixed-anchor-only release. Before declaring implementation complete:

1. **Local reducer, unit/integration:** independent known answers for half-open endpoints, duplicate anchors, zero/all-null/no-observation outcomes, count versus nonnull denominators, source/observation incompleteness and unavailable filters. Extend the existing owning modules rather than duplicate each case per recipe.
2. **Numerics, temporal/backend integration:** excluded large history does not erase a small native window; selected nonfinite values cannot publish; exact per-input quantization and `below_resolution` survive summaries; wide integers/decimals preserve their supported domains. No universal float bit assertions or arbitrary history-inflated tolerances.
3. **Batch planner, compiler integration:** mixed windows/predicates, two-source ratios without fan-out, selector dependencies, generated LTV/state leaves and eligibility checks. Compare raw and eligible-summary results at matched inputs. No summary use for partial buckets.
4. **Pathway coverage, existing corpus:** exercise each row of the rollout matrix through its owning route. Reuse recipe/artifact fixtures and focused modules (`tests/compile`, `integration/compile`, `integration/state_panels`, `integration/conformance`); the matrix is coverage, not a requirement for a large benchmark per API.
5. **Retained integration:** ordinary versus staged results, correction/refresh, old-generation immutability, old-plan rejection/rebuild and semantics-revision invalidation of no-change reuse. Cover both native and exact modes. Existing anchors include `tests/compile/test_persisted_layers.py`, `tests/runtime/test_layers.py`, `integration/warehouse_execution/test_ml_staging_exports.py`, and `test_incremental_equivalence.py`.
6. **Backend portability:** direct consistent execution on DuckDB/PostgreSQL; live existing conformance/parity cases on BigQuery/Snowflake when authorized. Compile-only evidence is not execution or warehouse-cost evidence. Keep exact/timestamp witnesses where operations require them; do not retain them merely to support retired native prefixes.
7. **Performance evidence:** rerun the representative fixed, overlapping and poor-compression workloads against the integrated implementation, including summary construction and full materialization/retained handling. Keep timing informational, not CI wall-clock gates. Existing SQL-size checks must no longer require per-output prefix expansion. Do not claim the standalone prototype's server ratio as an end-to-end Frame ratio.
8. **Complete surface migration:** update backend/numerical guides, cost documentation, strategy descriptions, saved-plan schema fixtures, artifact corpus, refusal documentation and changelog. Delete obsolete implementation-driven tests rather than repinning their source/wiring or preserving irrelevant refusal behavior.

New permanent tests must each catch a plausible consumer-visible regression and demonstrate failing-before/passing-after behavior. Tests run at owning boundaries and affected integration selection; do not benchmark or run the entire suite once per recipe/lane.

## Decision boundaries

This design chooses local aggregation rather than maintaining general cumulative and direct engines side by side. It preserves exact declarations, not historical computational machinery. It introduces no general optimizer, persistent summary cache, new public mode, new warehouse support or partial-bucket algorithm.

The experiment does not warrant an exact-prefix exception. Truly uncompressible, irregular repeated-anchor workloads remain unmeasured; this is a stated performance limit of the evidence, not a reason to retain a second general engine. New contrary evidence can motivate a separate, narrow optimization proposal. Removing `exact=True` or changing Catalog defaults remains a separate decision and is not part of this cutover.
