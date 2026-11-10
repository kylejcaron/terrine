# Compact generated SQL: measured results and remaining gaps

## Final combined continuation — 2026-11-09

This section supersedes the first checkpoint's failed-budget status and timing
summary below; those original results and artifacts remain preserved as history.
The final measured production source is
`24e057377953b11828035a20bfe44e1b3805f058`. The unchanged consumer ceilings now
**pass**: README **19,520 <= 20,000**, state-panel **18,525 <= 30,000**, and
metadata-only BigQuery **514,334 <= 1,000,000** formatted characters. No threshold,
fixture, comparator, refusal or expected-failure waiver was used. The two existing
`ltv`/`life_table` strict xfails remain; they are not newly passing claims.

### Final matched measurements

The already-captured five-repeat, unprofiled measurements at this exact source
were reused, not rerun. Their 155 source hashes per case, literal SQL bytes,
input schemas/content hashes, compared Arrow schemas, all five typed trials and
four corruption controls were checked against the current source and original
v2 references. Python 3.12.11, DuckDB 1.5.6, Ibis 12.0.0 and PyArrow 23.0.1;
one DuckDB thread, serial cases with no concurrent test/benchmark execution.
Medians [minimum–maximum], milliseconds:

| Case | Literal SQL → Arrow | Precompiled Terrine → Arrow | Public materialization | Query/literal |
|---|---:|---:|---:|---:|
| README | 2.563 [2.005–2.744] | 8.866 [7.894–9.092] | 303.908 [289.638–321.464] | 3.459× |
| Scaled | 26.216 [25.840–26.934] | 115.655 [114.189–122.051] | 505.280 [498.211–523.177] | 4.412× |
| Wide | 72.828 [69.581–78.765] | 133.905 [129.428–141.131] | 2,185.785 [2,148.409–2,217.830] | 1.839× |

Only wide meets the existing 3× query waypoint. It remains 83.9% slower than the
literal query: reaching 1× would require another 1.839× speedup (45.6% less
generated-query time). README/scaled also remain above 1× and 3×. Public calls
still do more work and return richer metadata: public/literal ratios are
118.579×, 19.274× and 30.013×, not identical-payload performance.

| Case | Build observation, ms | SQL-compile observation, ms | v2 → final query median, ms | Final/v2 query time |
|---|---:|---:|---:|---:|
| README | 64.785 | 74.665 | 37.121 → 8.866 | 0.239× |
| Scaled | 58.388 | 63.946 | 118.496 → 115.655 | 0.976× |
| Wide | 604.662 | 265.961 | 3,512.698 → 133.905 | 0.03812× |

Wide is approximately 26.2× faster than this historical v2 query baseline, not
50× and not handwritten parity. Scaled's historical query improvement is
negligible; independent runs and their ranges do not establish a meaningful
scaled runtime gain. The build/compile observations are single observations,
not repeated medians or disjoint additions to the public call, which recompiles.

| Case | Projected submitted bytes | Projected formatted bytes | Full-expression submitted bytes | Captured public statements/bytes |
|---|---:|---:|---:|---:|
| README | 13,649 | 19,793 | 13,970 | 3 / 37,299 |
| Scaled | 13,485 | 19,483 | 13,806 | 3 / 36,919 |
| Wide | 99,738 | 147,210 | 104,039 | 3 / 301,556 |

The exact private README consumer counts `len(ibis.to_sql(frame.expr))`:
**19,520**, not the comparator's 19,522-byte reformatted full expression or
19,793-byte matched projection. Public statement capture still excludes
metadata/admission probes, transaction control and catalog introspection.
It is neither all round trips nor a measurement of zero admission cost.

### State-query construction tradeoff

The separate state-specific experiment compares the original union fallback and
compact query **at one pinned source revision**, not two historical commits:
`966c7ffbe00c9916ca295fd6c98d9c5c0c131aab`. Five alternating paired samples use the
unchanged 10,000-unit/300,000-event cost fixture, one thread and the same literal
state reference. Full 20,000-row/20-column public Arrow schema, values,
completeness/reasons, observation support, ColumnMeta and invariant provenance
match, and all four corruption controls reject. Dynamic snapshot identity fields
are recorded separately rather than compared across calls.

| State phase, ms | Original union fallback median [range] | Compact query median [range] |
|---|---:|---:|
| Expression build | 96.730 [86.055–119.610] | 124.925 [108.896–158.428] |
| SQL compile | 106.520 [80.154–134.583] | 56.734 [49.136–109.282] |
| Precompiled full query | 142.312 [136.670–144.963] | 140.248 [128.001–146.604] |
| Full public build + materialize | 1,161.940 [1,085.018–1,213.325] | 1,255.171 [1,193.530–1,304.031] |

Consumer SQL falls from 32,387 to 22,767 characters in this isolated comparison.
Compilation decreases, but **construction and public-call costs increase**; the
overlapping query ranges establish no query-runtime gain. These phase medians
must not be added: the public timer includes fresh construction and compilation.
The final combined source subsequently reduces the actual state consumer to
18,525 characters; no extra state-specific timing experiment was run for that
combined source, so its lower text size does not imply a new runtime speedup.

The original failed diagnostic is preserved separately. Raw precompiled SQL
returns `gap: int64`; the existing public normalization returns `duration[us]`.
Normalization occurs outside the precompiled query timer, with no type-gate
relaxation. The executed literal digest excludes the artifact writer's appended
line feed; the saved SQL file has its own distinct file digest.

### Preserved semantics and verification

Daily envelopes reuse native-eligible guarded keys or integer rank/seconds/signed
remainder parts. Conservative declaration proof can normalize only UTC
microsecond point bounds for a naive microsecond source; source clocks are
untouched. Nanoseconds, unproven origins and declined policies retain exact
integer fallback. Same-source support sharing, full-membership count reuse and
dead-field pruning preserve each output's own availability, reasons and checks.
Unshifted portable comparisons retain native preference and integer fallback,
without introducing a decimal witness.

State-query compaction requires one untransformed sum yielding `DECIMAL(38,0)`
under an active unbounded-coefficient policy, plus whole-day endpoint/count/
host-clock proof. The original scheduled and endpoint validation studies remain
unchanged and separate. Native floats, bounded policies, other metric shapes and
declined clocks retain the original union. There is no validation/data cache,
public mode, new dependency, engine-name shortcut or new refusal.

At the final integrated source, the prescribed expanded affected `make check`
ran once: **907 passed, one justified native-domain skip, 71 deselected** and all
static gates passed. It includes the retained old-revision refresh, retained
state composition and ordinary README full-membership regression. Consumer
budgets give **eight passed, two existing strict xfails**, with thresholds
unchanged; forced generic DuckDB conformance gives **16 passed**.

PostgreSQL 16.15 scratch native selections give **18 passed/one native-domain
skip**, portable selections **37 passed**. Since those selected public windows
use one fixed origin, a separate actual public two-day daily count/sum proof
exercised naive microseconds, pre-epoch/adjacent lower/upper boundaries and an
America/New_York caller session. Its four rows and support agree with DuckDB;
caller timezone is unchanged, and scratch teardown exits 0.

The changed daily/state/README selectors also pass **11 cases** on the actual
DuckDB **1.5.0** disposable overlay. In that same process, actual public daily
microsecond/nanosecond materializations and the 20,000-row state materialization
pass. Installed 1.5.6 is separately asserted unchanged. Both versions' combined
state output matches **both** saved pre-daily-change public Arrow variants,
including exact serialized schema metadata, values, support, ColumnMeta,
invariant provenance, reference fields and all four controls.

The state comparison initially exposed a diagnostic boundary mistake:
`result.table` has no materialized-frame schema metadata, whereas the preserved
artifact used `result.to_pyarrow()`. Comparing that identical public exporter
fixes the diagnostic, without stripping metadata or weakening a dtype assertion.
The failure/probe/source are retained. Two earlier diagnostic connection/session
syntax mistakes were likewise corrected only in the throwaway evidence script;
no production/test source changed during final verification.

No fresh profile was run. The inclusive nested phase evidence below remains
at its original earlier source, not attributed to this final source. Live cloud
execution remains unauthorized/unrun; the BigQuery size pass is metadata-only.
The separate fixed-offset public state proof and native-float cancellation/
separate-overflow-domain regressions are retained, not generalized into cloud
parity or bitwise floating guarantees.

New [aggregate manifest](../../benchmarks/results/compact-sql-continuation-final-20261109.json)
binds the reused final directories, state-specific A/B evidence, prior milestones,
source/input/literal/schema hashes and actual final verification logs.
[Runtime evidence](../../benchmarks/results/compact-sql-continuation-verification-20261109/duckdb-1.5.6-public-smokes.json)
records the public outputs; the same directory preserves smoke source, floor,
PostgreSQL and exact consumer receipts. Prior manifests, original failed diagnostic, binary pstats,
literal references and all old/new result directories are preserved.

## Historical first checkpoint

2026-11-09. Production source `801369269df302a409988933bfd4ada54624f442`,
following `6b0ff5d14696648a10cd3f76170b78755e52fe79`. DuckDB-only performance
comparison; focused local PostgreSQL correctness evidence is reported separately.

## Finding

The wide five-metric × ten-window query is materially smaller and faster than the
[preserved literal-SQL baseline](2026-11-09-handwritten-sql-comparison.md), without
changing its source rows, literal reference, ordered support, Arrow types, values,
completeness or reasons. Its matched query median is now **1.903×** the literal
query, meeting the existing **3× waypoint for this case only**. README and scaled
monthly remain **14.708×** and **4.435×**; neither meets it. The wide result is
still 90.3% slower than 1×; reaching 1× would require a further 1.903× speedup of
that generated query (47.5% less elapsed time). This is not a 50× promise, a
public-API parity result, or an all-recipe/warehouse performance guarantee.

Two inherited consumer SQL budgets remain **failed**, not waived: README
84,821 > 20,000 formatted characters and state-panel 32,387 > 30,000. They also
failed on unchanged production (89,267 and 196,349). BigQuery's metadata-only
five-metric × ten-window compilation passes 515,634 <= 1,000,000; its strict
expected-failure mark was removed only after an observed real XPASS/pass. No
ceiling was raised and no new xfail was added. Acceptance of the residual budget
gaps is unresolved; documenting them does not turn them into passing checks.

## Matched work and correctness

The [frozen comparator](../../benchmarks/compare_sql.py) executes the original
literal SQL and a precompiled Terrine projection through the same native
DuckDB SQL → Arrow boundary. The public `Frame.materialize().table` call includes
execution-bound recompilation, admission, validation and a richer payload; its
ratio is not identical-guarantee or identical-payload performance.

| Case | Total input rows | Output rows | Compared columns | Public columns |
|---|---:|---:|---:|---:|
| readme | 8 | 6 | 11 | 21 |
| scaled | 310,000 | 20,000 | 11 | 21 |
| wide | 310,000 | 10,000 | 152 | 256 |

README is three customers/five orders with UTC-naive microsecond timestamps;
scaled is 10,000 customers/300,000 orders with UTC-aware microseconds; wide is
10,000 users/300,000 native-float events with UTC-aware microseconds. The larger
cases are not simply a row-count scaling experiment on identical timestamp types.
Wide compares 50 metric values plus every completeness/reason field (152 fields
including unit/origin), against 256 fields in the public payload. Counts and
Boolean activity retain exact types; fixture-bounded floating comparisons use
`rel_tol=1e-12, abs_tol=1e-12`, not a universal floating accuracy claim.

Every timing trial rechecks ordered unit/origin support, strict schema, values,
completeness and reasons against the independent Python oracle. Before trials,
the control checker rejects missing/duplicate origins, changed values and changed
types. All three cases passed every check. Input schemas and Arrow-stream hashes,
compared schema, output sizes and control outcomes are retained in each result.
All inputs, compared schemas and literal SQL bytes match the v2 baseline and all
Task2–5 milestones. The prior v2 manifest's file hashes were checked unchanged.

## Final unprofiled timings

Python 3.12.11, DuckDB 1.5.6, Ibis 12.0.0, PyArrow 23.0.1, one engine thread on
an Apple Silicon macOS workstation. Each case used a fresh process/directory and
five warm repeats, serially under `agent-slots -j 1`; no tests or other benchmarks
ran concurrently. The ordinary pytest plugin remained installed. Loading, model
binding, oracle checks and warmup are outside the sample timers. Query order
alternates; public materialization follows each pair, not a randomized cold run.
Medians [minimum–maximum], milliseconds:

| Case | Literal SQL → Arrow | Terrine precompiled → Arrow | Public materialization | Query/literal | ≤3×? |
|---|---:|---:|---:|---:|---|
| readme | 2.861 [2.566–3.853] | 42.081 [40.497–44.315] | 505.478 [471.442–568.677] | 14.708× | no |
| scaled | 31.213 [29.609–32.779] | 138.429 [135.852–144.718] | 658.570 [635.745–763.273] | 4.435× | no |
| wide | 80.800 [76.257–90.960] | 153.791 [145.934–166.890] | 2,732.132 [2,661.359–2,952.774] | 1.903× | yes |

The prior v2 baseline is a historical same-fixture comparison, not a concurrent
controlled run. Literal queries themselves are slower in this final run; README
and scaled absolute Terrine timings did not improve. Report the within-run ratios
and ranges rather than attributing workstation variation to a compiler change.

| Case | Baseline → final query median, ms | Baseline/final query | Baseline → final public median, ms | Baseline/final public |
|---|---:|---:|---:|---:|
| readme | 37.121 → 42.081 | 0.882× | 475.056 → 505.478 | 0.940× |
| scaled | 118.496 → 138.429 | 0.856× | 615.134 → 658.570 | 0.934× |
| wide | 3,512.698 → 153.791 | 22.841× | 17,565.748 → 2,732.132 | 6.429× |

Standalone construction and SQL translation are single observations, not repeated
medians or disjoint components to add to public materialization (which recompiles):

| Case | Design/expression build, ms | Projected SQL compile, ms | Matched Arrow bytes, each query | Public Arrow bytes |
|---|---:|---:|---:|---:|
| readme | 82.337 | 105.940 | 385 | 923 |
| scaled | 75.187 | 89.111 | 1,405,000 | 3,200,000 |
| wide | 730.151 | 346.389 | 5,901,646 | 14,170,396 |

For wide, the baseline build/compile observations were 4,913.383/1,539.944 ms,
versus 730.151/346.389 ms now. These single observations support separation of
construction/translation from execution, not a precise phase speedup guarantee.

## Structural SQL reduction and actual statements

Exact UTF-8 submitted bytes are distinct from formatter-expanded text. Each public
call's untimed capture observed three study statements (two validation, one output).

| Case | Literal submitted | Baseline → final projected submitted | Final projected formatted | Final full expression submitted | Captured public total submitted |
|---|---:|---:|---:|---:|---:|
| readme | 1,620 | 42,502 → 39,676 | 85,448 | 39,997 | 90,688 |
| scaled | 1,620 | 19,528 → 16,702 | 28,014 | 17,023 | 44,740 |
| wide | 10,856 | 1,063,594 → 99,958 | 148,750 | 104,259 | 301,996 |

Wide projected submitted SQL fell from 1,063,594 to 99,958 bytes (90.6% reduction),
while formatted SQL fell from 3,755,646 to 148,750. Original SQL files and every
actual public study statement remain available, not just digests/line counts.
Capture **excludes metadata/admission probes, transaction control and catalog
introspection**. These totals are not all database round trips and are not a
single-statement size comparison. Direct-driver admission/transaction costs are
unmeasured, not zero.

Prepared exact timestamp coordinates are reused across nonzero shifted
source/point comparisons. Declaration-proven UTC microsecond origins can use
native DuckDB shifts; naive/nanosecond and unproven origins retain exact paths.
Compatible contexts share expressions within an invocation, and each output's
finalizer carries only its own dependency union and original anchor fields.
No expression/data/snapshot/validation cache or new public strategy was added.

## What still dominates

The separate tiny-wide diagnostics/profile uses 10 units, 300 events and the same
50 outputs. It is **not a final timing sample**, and its source revisions differ
from the final zero-offset restoration. Original before/after inputs, schemas and
literal SQL are equal. Preserved JSON, source hashes and parsed pstats summaries
are in [the diagnostic record](../../benchmarks/results/compact-sql-narrow-diagnostics-20261109/profile-summary.json);
large binary profiles remain in the original local `/tmp` directories, not Git.

| Instrumented inclusive boundary, seconds | Before narrow branches | After narrow branches |
|---|---:|---:|
| Separate Design.build | 6.103 | 2.349 |
| Public materialize | 10.603 | 6.762 |
| Expression construction within public materialize | 6.092 | 2.195 |
| Execution-bound compile-design within public materialize | 6.348 | 2.457 |
| Public SQL rendering, all three statements | 3.666 | 3.719 |
| Study stream opening | 0.204 | 0.193 |
| Stream reads (deferred engine/Arrow conversion included) | 0.127 | 0.131 |

These timers are inclusive and nested: **do not sum them**. Profile total calls
fell from 67,196,658 to 36,894,945 and total profiled seconds from 16.632 to 9.044.
The before profile attributes 11.931 cumulative seconds to two expression builds,
including 6.308 in 100 finalizer calls. Afterward, two expression builds total
4.300 seconds, while the public SQL renderer totals 3.719; the latter is now the
largest measured public-path phase versus its 2.195-second expression rebuild.
Ibis graph replacement (3.790 cumulative) and SQL rewrite/translation remain
important overlapping costs, not additional independent durations. Output SQL
compilation alone takes 3.714 instrumented seconds after narrowing. Rebinding is
0.0073 seconds; validation scheduling is 0.0413 seconds inclusive of its nested
validation rendering. The later output rendering is outside that scheduling
boundary. These overlapping times must not be added or subtracted as disjoint work.
There is no evidence that matching SQL digests mean duplicate database work.

Session setup/close, some admission witnesses, remaining validation and metadata
assembly are not isolated. Stream reads combine deferred engine work with Arrow
conversion. Do not subtract unrelated medians or assign the entire public/query
gap to one subsystem.

The 6.83-second integration boolean-split case was independently profiled, not
moved or weakened: admission includes 10.363 cumulative profiled seconds and 18
SQL compilations total 9.916 seconds, overlapping Ibis rewrite work. Its profiled
call is 16.82 seconds; it already belongs to the integration tier, whose guideline
is roughly 30 seconds, not the unit tier's five-second reporting threshold.

### Remaining formatted consumer budgets

The exact README budget SQL contains a dominant 57,455-character daily-scope CTE:
its mixed naive/aware zero-offset envelope comparisons repeat rank/second/remainder
and timestamp reconstruction inside the masked local-day input. Across the full
statement there are 52 timestamp constructors and 239 CASE nodes. The state-panel
statement's largest CTE is 20,970 characters of state-transition/value/publication
composition. CTE body lengths are re-rendered subtree observations (nested work
can overlap), not disjoint byte accounting.

The reviewed nonzero-key/context/finalization changes do not remove that daily
mask or the state-shape compiler. No concrete missing approved dependency/key
requirement was found in this read-only inspection. Meeting 20,000/30,000 needs
further compiler work outside the accepted change, or explicit owner disposition;
there is no admission bypass, strategy change or xfail to conceal the gap.

## Correctness and portability evidence

The prescribed affected `make check` ran once: all static gates passed, then
868 tests passed, four existing generic as-of admission regressions failed, one
native-domain case skipped and 71 were deselected. The defect was traced to newly
introduced decimal coordinates for **unshifted** portable timestamp comparisons.
Restoring the original integer-parts seam fixed all four without suppressing
admission. The focused provider/timestamp/range `make check` passed all 44 tests
and static gates. The full selection was not blindly repeated. The retained
revision-one replay/empty-change refresh case was exercised in the original run.
Forced generic DuckDB conformance separately passed 16 (45 deselected).

Documented scratch lifecycle (`python -m tests.integration.postgres_scratch
start/stop`) ran PostgreSQL **16.15 Homebrew**, aarch64, under Ibis 12. Native
shifted timestamp parts/comparisons (all five operators; null/infinities/extrema),
counts/sums, averages/ratios, distinct strings/dates/Booleans and conformance gave
**18 passed, one skipped** (46 deselected). The skip says `The native coefficient
domain admits this sum.` Forced portable PostgreSQL timestamp/numeric/conformance
gave **37 passed** (68 deselected). Scratch teardown completed in `finally` with
exit 0; each probe owned its schema. The initial direct-client provenance attempt
failed before tests and was replaced by the repository connection fixture; both
scratch instances were stopped.

The supported DuckDB **1.5.0** floor was exercised earlier by the timestamp change
in a disposable uv overlay: nine focused probes passed, then the identical nine
passed on installed 1.5.6; no lock/dependency change. That is prior milestone
provenance, not a new final full-suite floor run. Commands/output are preserved
in [floor provenance](../../benchmarks/results/compact-sql-final-verification-20261109/floor-provenance.txt).

BigQuery/Snowflake live parity modules are preserved byte-for-byte from the task
base, including timestamp/numeric matrices, but **live cloud execution is unrun:
not authorized**. BigQuery's passing size check uses relation metadata only and
makes no warehouse call. Local PostgreSQL correctness is not PostgreSQL performance
or cloud parity evidence. Warnings are existing Ibis Arrow deprecations and the
intentionally exercised unknown-source-scan warning; no warning was suppressed.

## Reproduce and inspect

```sh
agent-slots -j 1 -- uv run python -m benchmarks.compare_sql --case readme --repeats 5 --output benchmarks/results/compact-sql-final-20261109-readme
agent-slots -j 1 -- uv run python -m benchmarks.compare_sql --case scaled --units 10000 --events-per-unit 30 --repeats 5 --output benchmarks/results/compact-sql-final-20261109-scaled
agent-slots -j 1 -- uv run python -m benchmarks.compare_sql --case wide --units 10000 --events-per-unit 30 --repeats 5 --output benchmarks/results/compact-sql-final-20261109-wide
```

Use different fresh paths when reproducing; never overwrite these records.
[Aggregate manifest](../../benchmarks/results/compact-sql-final-20261109.json)
contains final/baseline medians/ranges, ratios, hashes, environment and fixture
identities; each case directory preserves emitted SQL, actual captured statements,
all samples, input and output schemas, control outcomes and source hashes.
Task2–5 milestones and the Task4 dense/day evidence remain unmodified and included.
The original v2 baseline and superseded experiments were not overwritten.

Comparator SHA256:
`6bf68e1d6ed5a296f086a6394dd9ec554ce78b54256666b37fbc08bdbff7c00f`;
`run_overhead.py` SHA256:
`7a64bb9547e5f645a432b858cfb301b964d592f15abd7f084120f7bd4c1302dd`.
All final/milestone helper hashes match these frozen revisions. The v2 baseline
used the prior comparator `c4f4d889…`; the later comparator added separate diagnostic
instrumentation. Its original literal/input/schema hashes remain identical, and
final measurements are unprofiled. Production/source identities are recorded
separately rather than claiming an unchanged compiler across milestones.
