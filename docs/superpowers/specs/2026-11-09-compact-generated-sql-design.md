# Compact generated SQL and lower compiler overhead

## Scope ledger

The owner originally asked, “can we make a plan to address these things?”,
selecting the four areas below, then separately authorized implementation.
The bounded consumer-budget continuation below was also approved; no publishing,
merging, deployment or relaxation of a contract is authorized.

| Capability | Owner-selected wording | Status |
|---|---|---|
| Compact timestamp lowering | “Make timestamp lowering compact without weakening correctness. Use native comparisons where representability is provable. Otherwise compute exact timestamp coordinates once and reuse them” | Confirmed for planning |
| Shared aggregate inputs | “Share work by source and window, not output” | Confirmed for planning |
| Build/public execution costs | “Reduce duplicate expression construction and recompilation without caching stale validation results” | Confirmed for planning |
| Comparative evidence | “Judge every change against the saved handwritten benchmark” and preserve result/schema, timestamp, numerical and refusal contracts | Confirmed for planning |

No new study shape, arithmetic mode, user flag, warehouse capability, persistent
intermediate store, or public prepared-query API is proposed. The broader cost
backlog is not implicitly included. The previously mentioned 50× improvement is
an aspiration for the wide query, not a guaranteed acceptance threshold.

## Goal and evidence

Make ordinary generated study queries approach the handwritten reference in
query shape, SQL size and execution cost, while reducing the construction cost
paid by both `Design.build` and execution-bound recompilation. Keep the ordinary
Model → Design → Frame → Dataset workflow and every current safety boundary.

Authoritative production baseline: `64d7caf4f7925a8bd744b9ceafd4731d1eb67015`.
Benchmark and corrected typed evidence are committed at
`08bbba69e4432a205aa68c3d5b9c720480271f05`:

- `benchmarks/compare_sql.py`;
- `benchmarks/results/direct-sql-comparison-20261109-v2.json` and its three case
  directories;
- `docs/research/2026-11-09-handwritten-sql-comparison.md`.

The wide baseline has 10,000 units, 300,000 events, 50 values, and 152 matched
Arrow columns. Median query-to-Arrow is 66.957 ms handwritten versus 3,512.698 ms
compiled; submitted SQL is 10,856 versus 1,063,594 bytes. Full public
materialization is 17,565.748 ms and returns additional metadata. Its time is not
an identical-payload or identical-guarantees comparison.

Two read-only diagnostics inform this design, not production performance claims:

1. The saved generated AST has one grouped aggregation, 8,190 CASE expressions,
   a 451,372-byte join predicate and a 454,563-byte contribution projection.
   Repeated shifted timestamp membership dominates its text. A fixture-only
   native-shift substitution produced 167,072 bytes and a 271.026 ms median,
   versus 3,639.418 ms current and 68.473 ms handwritten in the same five-sample
   run. Input hashes and all typed outputs matched. It deliberately lacked a
   general timestamp-range proof and is not the implementation.
2. A separate cProfile run on 10 units/300 events with the same 50 outputs
   verified values and recorded 616 Ibis `mutate` calls, 50 `_finalize_window`
   calls, and three `_batch_projections` calls in a build. Those finalization
   calls dominated construction; public execution repeated that construction.
   Public execution rendered three distinct statements (108, 269 and 2,261,303
   bytes), not repeated copies of one statement. Instrumented times are not
   comparable to unprofiled benchmark timings and must not be added together.

## Current boundaries

- `aggregate_windows` in `terrine/temporal/aggregate.py` already shares
  `_Primitive(measure, window, counting)` reductions. It still constructs
  `_window_context` and finalization branches per output.
- `_reduce_source` repeats `_interval` for its bounded join and each primitive's
  contribution mask. `compare_timestamps` disables native keys for every nonzero
  offset and reconstructs exact timestamp parts inline.
- `_batch_projections` in `terrine/temporal/daily.py` preserves named column
  dependency layers. Selection must remain before numerical conversion.
- `Frame.__init__` compiles against the direct model for `.expr` and metadata.
  `Frame._materialize` opens an execution context, rebinds the model and compiles
  again against that context. These bindings are not interchangeable.
- `ExecutionContext._check_query` renders a statement, revalidates retained
  ownership as applicable, and admits native functions before execution. The
  session receives that same SQL. Removing this check is not an optimization.

## Selected architecture

### 1. Preserve an exact, reusable timestamp representation

Factor the existing shifted-comparison arithmetic into private helpers in
`terrine/backends/timestamps.py`:

```python
@dataclass(frozen=True, slots=True)
class _TimestampComparisonKey:
    rank: ir.IntegerValue
    nanoseconds: ir.DecimalValue

def _timestamp_comparison_key(
    value: ir.TimestampValue, *, backend: str | None = None
) -> _TimestampComparisonKey: ...

def _compare_timestamp_keys(
    left: _TimestampComparisonKey,
    right: _TimestampComparisonKey,
    op: Literal["lt", "le", "gt", "ge", "eq"],
    *, right_offset_seconds: int = 0,
) -> ir.BooleanValue: ...
```

The existing dialect `timestamp_parts` owns conversion. For parts `(r, s, n)`,
use `C = DECIMAL(38,0)(s) * 10**9 + n`. Only rank zero is shifted by
`offset_seconds * 10**9`. Compare rank first, then coordinate; infinities retain
their ranks and NULL predicates stay NULL. Signed remainders before the epoch
must not be floored. Admitted int64 seconds and int64 offsets fit in 38 decimal
digits, including their sum. No timestamp is constructed for a virtual endpoint.

Attach named key fields once to each source and point relation in
`_reduce_source`; use collision-safe builder names. Rebind those fields through
the joined relation before building contribution masks. Do not carry expression
objects from a different relation across a join. Preserve the current native
unshifted branch. Keep `compare_timestamps`'s signature and exports unchanged;
private helpers do not become public inputs or new package exports.

Stage the fallback keys only for windows that need them. The later native tier
must not leave unused conversions in an otherwise native statement. Verify the
emitted SQL actually references prepared fields: naming Python expressions alone
does not prove that Ibis has preserved sharing.

### 2. Certify a narrow native shifted path from declarations

Add a separate private origin-bounds proof. **Do not reinterpret
`CompileContext.fixed_anchors`: `_fixed_anchors` returns only the first two
Schedule instants for alignment, not its extrema.**

`_declared_origin_bounds(design, model)` in `terrine/compile/numeric.py` returns
`tuple[datetime, datetime] | None` in UTC:

- FixedDate: the resolved instant twice;
- nonempty Schedule: `instant(0)` and `instant(count - 1)` under the existing
  fixed-offset day boundary; no enumeration or source query;
- all event-relative, overridden `_anchor_table`, persisted-anchor or otherwise
  unproved origins: no proof. `anchor_range` filtering may narrow but never
  broaden the conservative declared bounds.

Carry these bounds separately through `CompileContext` and an internal-only
keyword of `aggregate_windows`; `relative=True` suppresses them. No public knob,
new schema metadata requirement or runtime extrema scan.

Add an operation-specific dialect seam:

```python
def native_shifted_comparison_keys(
    self, left: ir.TimestampValue, right: ir.TimestampValue,
    *, offset_seconds: int, right_bounds: tuple[datetime, datetime],
) -> tuple[ir.Value, ir.Value] | None: ...
```

Base returns `None`; the initial override is DuckDB only. It may return native
operands only when both types are UTC-aware microseconds (`scale None or 6`),
both declared bounds and their exact elapsed shifts lie in the conservative
UTC year-1 through year-9999 domain, and the interval can be represented exactly.
Check bounds using integer/host declaration arithmetic before constructing any
Ibis interval; overflow or missing proof chooses compact exact keys, not refusal.
An admitted shift is an explicit SECOND interval, never calendar-day arithmetic.
The span of that conservative domain is under `3.156e11` seconds or `3.156e17`
microseconds, within DuckDB's signed-int64 microsecond interval storage. A real
probe on DuckDB 1.5.6 verified both directions across years 1–9999 and an elapsed
86400-second shift across a New York DST transition; implementation must retain
live boundary tests, including the minimum supported DuckDB version.

Do not convert source timestamps merely to enter this path: naive/aware mixes,
nanosecond sources, unknown origins, unproved ranges and other dialects use the
exact representation. The proof applies to the actual point relation, not a
cached design from another compilation. Native predicates and point-side shifted
bounds must be staged once per distinct admitted offset.

### 3. Share window context and membership, not error identity

Keep sharing local to one `aggregate_windows` invocation and candidate grain.
Introduce a private semantic `_WindowContextKey` consisting of:

- window start/end seconds;
- effective source-coverage intersection (start/end);
- day boundary and event/day resolution;
- whether calendar alignment is required.

The anchor relation, observation columns and declaration-derived origin proof
are fixed for that invocation; never share across compilations, anchor origins,
sources with different coverage, or execution contexts. Construct bounds,
coverage/observation expressions and unavailable reasons once for equal keys.

Split shared context fields from request-specific `RowCheck` records. Reuse an
alignment-failure Boolean, but construct its refusal/context per metric/output
in original declaration order. Do the same for any shared numerical flags:
sharing a value cannot merge output attribution or change refusal precedence.

Deduplicate membership by `(source, window, ordered unique availability fields)`.
Keep measure filters, source identity, type/scale, counting mode and arithmetic
mode in primitive identity. Deduplicate the exact union of available windows;
do not replace disjoint or differently available windows with an unmasked broad
range. Apply equivalent reuse to event and scoped-daily reductions. Counts must
not become nonnull mean denominators; count-only requests must not evaluate
unused float payloads.

### 4. Reduce expression construction width; preserve execution rebinding

Before `_local_component` and `_finalize_window`, project a branch containing
only its required original anchor columns, shared context fields, relevant
primitive payloads and check columns. Finalization returns original anchors plus
that output's value/support/check columns, not every sibling's intermediate.
Merge branches through the existing `_batch_projections` mechanism rooted at the
retained relation. This cuts repeated high-level Ibis binding/dereference work
without changing the calculation, final schema, ordering or evaluation layers.

Use a private helper with one owner:

```python
def _finalization_fields(
    anchors: ibis.Table,
    context: _WindowContext,
    keys: Sequence[_Primitive],
    reduced_fields: Mapping[_Primitive, Mapping[str, str]],
) -> tuple[str, ...]: ...
```

It returns an insertion-ordered unique dependency list, including every context
check. If field requirements change, the owning finalization task updates this
helper and its consumer parity cases together. Do not add a general compiler IR
or global common-subexpression optimizer.

Both initial and execution-bound compilations benefit. Keep the latter inside
`open_execution`: current schema/source/function admission, snapshot ownership,
check scheduling, gated streaming, budgets and provenance still run each time.
No result cache, validated-study cache, global SQL cache, prepared-statement API,
physical materialization of validation intermediates, or lazy-validation change.
The measured three distinct SQL translations do not justify a translation cache.
If post-change attribution still shows a separate dominant lifecycle cost,
report its exact phase and evidence; do not remove guards to hit a ratio.

### 5. Preserve the benchmark as the decision instrument

Extend `benchmarks/compare_sql.py`, not a second benchmark implementation. Keep
all three literal-SQL fixtures, source schema distinctions, Arrow boundary,
strict type/schema/value/support/quality checks and four checker controls.
Reuse the instrumentation convention in `benchmarks/run_overhead.py`: one
separate diagnostic run, then uninstrumented timed samples. Optional
`--diagnostics` and `--profile` outputs record actual compile/build/validation/
stream phases, SQL digest/count/bytes and source identities. Label nested timing
and unknown phases; do not sum overlapping timers or call SQL-digest duplicates
proof of duplicate database work.

For every completed optimization task, the integration owner runs the same
frozen comparator on README, scaled monthly and wide cases, five warm samples,
one engine thread, into new directories. Report median/range, literal ratio,
baseline ratio, build, SQL compile and public API separately. Preserve submitted
and consistently formatted bytes, actual study statements and capture exclusions.
A diagnostic/profile run is never a timing sample. No concurrent benchmarks.

Correctness and representability are hard gates. Performance is measured evidence,
not a flaky elapsed-time test. The existing documented DuckDB 3× handwritten
query target is a useful waypoint; approximately 1× remains the goal, not a
promised 50× improvement. Every task must show its intended structural reduction
and measured effect. If noise obscures an effect, do not invent one. The final
report must quantify and explain the remaining gap rather than declare parity
because Terrine improved relative to itself. Keep existing consumer SQL-length
budgets; do not pin alias spelling, source text or private graph structure.

## Alternatives and decision

- **Unchecked native timestamps everywhere:** rejects currently exact virtual
  endpoints, extreme dates, mixed zones or nanoseconds. Rejected.
- **Only add a native fast path:** helps known declarations but leaves arbitrary
  origins with duplicated huge expressions. Insufficient alone.
- **Prepared exact keys plus a proved native tier, shared context and narrow
  finalization:** selected. Uses existing Ibis/dialect boundaries and improves
  both ordinary and general cases without a second execution strategy.
- **Global SQL/cache/validation materialization:** not supported by the measured
  duplicate-work evidence and changes snapshot/effect lifetime. Excluded.

## Required behavioral coverage

Extend existing tests first; add a test only for an independently nameable bug.

1. Timestamp comparison: pre-epoch signed fractions, ns/us neighbors, NULL,
   infinities, mixed naive/aware UTC under a non-UTC session, finite engine extrema
   and offsets outside timestamp range. Compare to independent integer/rank
   arithmetic, not the old SQL text.
2. Native certificate: full Schedule extrema, empty and overridden/relative
   origins, UTC elapsed seconds across DST, positive/negative shifts and declines
   outside the conservative range. Declining a fast path preserves exact results.
3. Sharing: same windows with different source coverage; duplicate anchor times
   with distinct identities; null means versus counts; exact and native values;
   unavailable/count-only requests cannot expose invalid input to arithmetic.
4. Finalization: all original public/support schemas and metadata survive narrow
   branches; exact reason priority and per-output refusal context/order remain.
5. Lifecycle: after source mutation the same Frame reads the new ordinary
   snapshot; an old retained snapshot remains immutable; budgets/native ownership
   still refuse on later calls. `.expr` execution is not substituted for public
   materialization.

Preserve existing event/day, portable dialect, retained refresh, PostgreSQL and
cloud parity boundaries. New remote spending or publishing requires separate
owner authority. No cloud runtime improvement is inferred from compiled SQL.

## Rollout, completion and non-goals

One integration PR is recommended. Benchmark support can be developed alongside
the exact-key task in isolated worktrees; edits to `aggregate.py` then run in one
ordered lane. Shared changelog, guides and final evidence belong to integration.
No shared-file parallel PR stack or compatibility shim is needed.

Completion requires reconciled behavioral gates, fresh comparative evidence and
owned reviews. The original implementation reached `02acf0b` with two inherited
consumer SQL ceilings still failed. Those failures remain acceptance gates;
they are not waived by improvement relative to the historical baseline.
The bounded continuation below does not adopt the broad cost backlog, other
recipes, cloud performance or an exactly 50× improvement target.

## Bounded consumer-budget continuation

### Authority and acceptance

After the original implementation, the recommendation was “continue with a
bounded optimization pass targeting those two remaining SQL constructions—not
waive the limits.” The owner replied “sounds good to me.” This continuation
targets only daily-envelope masking and binned state-panel construction.

- README: retain `len(ibis.to_sql(frame.expr)) <= 20_000`.
- State panel: retain 30,000 formatted characters per feature.
- Preserve all earlier evidence, the frozen comparator/helper, exact behavior,
  source eligibility, numerical/refusal policies and execution rebinding.
- No new public input, engine capability, cache, dependency or general optimizer.

The continuation base is `02acf0b9203786b02ac59502c5147526ffc8cfd6`.
Observed full-expression sizes there are 84,821 and 32,387 characters,
respectively. Both results agree with their value oracles.

### Daily envelope: reusable integer parts

The daily reducer groups active per-anchor bounds into a per-unit envelope, masks
unused instants, and only then computes `local_day`. Preserve that order.

Factor the public unshifted fallback into private
`_compare_timestamp_parts(left, right, op)`, consuming two exact integer triples
`(rank, seconds, signed_nanoseconds)` from existing `timestamp_parts`.
It implements the same five rank/seconds/remainder lexicographic comparisons.
The public comparator still prefers exact native operands; nonzero shifts still
use the existing decimal-coordinate representation.

In `_masked_day_events`, stage integer parts once on the event relation and once
per grouped envelope endpoint only where native comparison declines. Allocate
collision-safe names through the existing builder, and reconstruct fields from
the actual joined relation. Stage dependent membership/masking projections when
needed to retain real SQL reuse. All-native masks must not retain unused parts.
After masking, retain the original event fields needed by filters/contributions
and the typed masked timestamp, not unrelated envelope intermediates.

Stage a shared nullable timestamp key as a whole before extracting its fields,
not arbitrary descendants of its conditional branches. Portable policies must
retain their existing scalar construction rather than acquiring a new native
struct requirement. Reuse the real required unit/day-to-anchor membership
relation for both day scoping and final fanout, preserving full anchor-instance
identity and each context's original availability/membership/check fields.

For a naive microsecond source, a UTC-aware microsecond envelope bound may be
converted to an exact naive UTC clock **on the bound side only**. This requires
the existing declaration-derived `_origin_bounds`, representable host bounds for
every requested endpoint, and proof that active envelope bounds are clipped
inside those finite requested ranges. Unknown/persisted origins, overflow,
nanoseconds or other zones retain integer comparison. Check existing optional
`Dialect.comparison_keys(source, source)` admission before constructing any
wall-clock expression; a base-policy `None` keeps the old path entirely.
After staging `utc_wall_clock(bound)`, use the admitted equal-zone microsecond
operands. Never attach UTC to the naive source. A dtype-only shortcut is unsafe:
a valid TIMESTAMPTZ near year 294247 can fail local timestamp conversion.

An unfiltered count may reuse only the `event_count` term of an unfiltered
non-count daily payload from the same source when its complete `_MembershipKey`
set is a subset of the donor's. That term counts selected rows before value
binding, NULL handling or numeric guards. Extra donor days cannot enter the
count's independent final membership mask. Keep the count's public aliases,
checks, coverage and refusal attribution; filtered, unmatched and count-only
primitives retain their own branch. Unfiltered means no source or measure
predicate requiring provenance. Do not borrow non-null counts, numeric flags
or unknown-filter provenance. Identical masked/reduced private counter terms
may share an alias only when both original field and full membership match.

Observed-from/until support data are window-independent. Within one daily-route
compilation branch they may share two field names when source identity and typed source-span inputs,
anchor observation inputs and anchor/fixed branch are identical. Preserve public
support schema/metadata, keep names available before use, and do not introduce a
cross-source read. Window endpoints, availability, completeness and request
checks remain independent. Use an existing guarded exemplar or a local data
index, not a global cache or quadratic search.

When a bucket's full membership set equals every key whose OR filters the
required relation, that OR is TRUE on every retained row. Its per-unit/day
`ANY` mask is redundant: omit that field and pass TRUE only after the unchanged
unique unit/day scope join. If all bucket masks are redundant, the scope is the
real distinct unit/day projection. Other masks and all predicates, numerical
guards, alignment checks and request-specific final masks remain unchanged.

The timestamp mask selects the instant only when `inside` is TRUE; FALSE or NULL
select the same typed null. An immediate `coalesce(inside, FALSE)` is redundant
for that conditional alone. Keep normalization on separately consumed flags.
FALSE-OR identity terms may be omitted without changing SQL three-valued logic.
NULLs, infinity ranks, signed pre-epoch remainders, ns/us neighbors, mixed
naive/aware UTC semantics and half-open boundaries are unchanged.
No decimal operation is added to the unshifted portable path, and no naive
timestamp is attached to a zone merely to gain native eligibility.

### State panel: compact query, unchanged validation domains

Do not merge scheduled-state and endpoint validation. `_binned` must retain the
original `compiled`, scheduled-period and fixed-endpoint checks and anchor checks
in the same order, with the same failure domains and failed counts.

The compact relation is eligible only for a plain, single-measure sum:
resolved metric kind `mean`, one measure with aggregation `sum`, no transforms,
and an original period result of `DECIMAL(38, 0)`. The active expression policy
must explicitly guarantee unbounded exact **value-coefficient** accumulation.
Add private policy metadata `_unbounded_coefficients = False` to the base dialect;
the concrete DuckDB coefficient implementation overrides it to `True`. This is
not a claim about count accumulators, nor a public capability/API. Do not infer
it from `bounds_accumulations`: BigQuery disables that guard but its BIGNUMERIC
accumulator remains bounded.

For this eligible scale-zero exact sum, regrouping identical selected
contributions cannot change the mathematical coefficient. The existing count
payload invariant bounds counts by source rows below `2**63`; their HUGEINT
accumulation remains safe. Final checked `DECIMAL(38, 0)` narrowing is unchanged.
Native floats, exact floats with scale nine, transformed/composite metrics,
counts, other scales and bounded/unknown policies retain the original union.
No extra arithmetic flags may be discarded for an unproven route.

A private `_compact_state_schedule(schedule, model)` may extend a whole-day
elapsed schedule by one period only after proving its count increases by one
and `start_utc + old_count * every == model.day_boundary(old_end_exclusive)`.
Use the current fixed-offset boundary contract; do not assume whole-day spacing
alone proves clock equivalence. Host overflow, an unrepresentable new exclusive
end or a failed proof declines the optimization, not the study.

Compile the eligible query through ordinary `compile_columns` with the same
metric/window, zero gap and `include_membership=False`. Keep both original
compiled studies/checks/anchor checks unchanged and ordered. Their exact result
and checked narrowing now certify the same quantities as the compact relation.
Canonicalize consumed support fields to the original metadata if necessary.
Public candidates, persisted anchors, both state joins, classifiers, support,
observation bounds and provenance remain on their original paths.

This pays for one extra internal expression construction rather than a new
validation-partition subsystem. The integer-sum fixture's throwaway probe gave
32,387 to 22,767 characters with full Arrow/metadata/handwritten parity, but
roughly 92 to 109 ms build time. Arithmetic-preserving LEAD and classifier-only
probes gave 38,953 and 32,888 characters, respectively, missing the ceiling.
These are feasibility observations, not final performance evidence.

### Continuation behavioral gates

Extend existing owning cases rather than adding parallel suites:

1. Daily naive us/ns events immediately inside/outside a pre-epoch half-open day
   preserve selected count 2, sum 5 and mean 2.5; outside/unused invalid payloads
   remain masked. A deliberately omitted staged remainder must fail.
2. All five unshifted operators retain the existing integer/rank oracle and the
   generic provider's consumed-operation admission contract.
3. Binned states retain successor continuity, membership-exit availability,
   null reasons, exact cutpoints, original anchors and retained-snapshot behavior.
   Exercise whole-day, sub-day and date-overflow decline variants through actual
   consumer results.
4. Exact output overflow in both scheduled periods and the endpoint still
   refuses with scheduled failed count 2; endpoint-only overflow refuses with
   failed count 1. A deliberately merged or omitted validation domain must fail.
   A multi-day native-float cancellation case remains on its original direct
   endpoint arithmetic; it must not become an unchecked nonfinite state label.
5. Both unchanged consumer SQL ceilings pass with independent value agreement.
   Measure the frozen three-case comparator after each integrated optimization,
   then repeat the affected integrated selection once at the new final head.
