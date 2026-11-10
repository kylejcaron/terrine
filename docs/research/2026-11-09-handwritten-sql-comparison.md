# Handwritten SQL versus current Terrine

2026-11-09. Fresh measurements against merged production
`64d7caf4f7925a8bd744b9ceafd4731d1eb67015` (PR #25). No production changes.

## Finding

The original handwritten-SQL overhead problem remains. The aggregation cutover
improved Terrine relative to its older implementation; that is not evidence that
it approaches a straightforward handwritten query. On these fixtures the
compiled Terrine query is both larger and slower even before the public
materialization path's additional work.

The wide comparison is not merely three value columns against a richer dataset:
both precompiled queries return the same ordered 10,000 rows, Arrow field types
and 152 columns (unit/time, 50 values, and every value's completeness and reason).

## What was actually compared

`benchmarks/compare_sql.py` contains literal SQL, not an Ibis-generated substitute
or the previous local-aggregation prototype. The README query uses a roster × two
monthly snapshots, one bounded left join and conditional count/sum aggregates.
It preserves Carol's empty rows and zero values, Bob's empty March history, and
all March outcomes' null/`incomplete_coverage` results. The wide query uses one
range join, 30 filtered aggregates shared across 50 metric projections, and their
quality columns. Neither query calls Terrine to calculate its results.

This is a fresh literal query, not a byte-for-byte reproduction of an earlier
nine-line example. The README statement has 32 source lines because it also
spells out the matched completeness/reason and empty/all-null handling.

Three paths use the same loaded DuckDB tables:

1. **Handwritten SQL → Arrow:** execute the literal statement and collect Arrow.
2. **Terrine precompiled SQL → Arrow:** compile a projection to the exact same
   columns before timing; execute and collect through the identical native
   DuckDB `sql(...).to_arrow_table()` path. Compilation is excluded on both sides.
3. **Public `Frame.materialize().table`:** time the real public call, with its
   recompilation, admission, validation, output and richer metadata intact.
   Compare its modeling/quality projection afterward, outside the timer.

The first two are matched query-result timings. The third does more work and
returns extra fields and provenance; its ratio to handwritten SQL must not be
called a measurement of identical guarantees or payloads. `.expr` execution
bypasses public validation/export safeguards (`terrine/frame.py:466-481`);
materialization performs the full execution sequence (`frame.py:708-818,903-983`).

The queries implement the declared fixtures, not a second general compiler.
They do not reproduce Terrine's source-integrity checks, arbitrary observation
coverage, exact-float mode, snapshot/admission machinery, export protections or
Python/schema provenance. Matching quality values on these data does not prove
those wider safety contracts.

## Data and correctness

| Case | Units | Events | Output rows | Compared columns | Public Frame columns |
|---|---:|---:|---:|---:|---:|
| README | 3 | 5 | 6 | 11 | 21 |
| Scaled monthly study | 10,000 | 300,000 | 20,000 | 11 | 21 |
| Wide fixed study | 10,000 | 300,000 | 10,000 | 152 | 256 |

The README fixture/design are `tests/fixtures/readme.py:17-76`, matching the
current README's sample. The scaled study uses the same three metrics, 30-day
past/future windows, February/March origins and declared completeness, with
seeded events over 75 days. The wide case uses the existing seeded native-float
five-metric × ten-window fixture over 30 days.

The tiny README source timestamps are UTC-naive, as in the README; the two
larger sources are UTC-aware warehouse timestamps. Every SQL/Terrine pair uses
identical schemas and data, and the DuckDB session is explicitly UTC. This is
not a controlled experiment attributing differences between cases solely to row
count: timestamp representation can itself change the generated SQL shape.

An independent Python oracle reads raw source rows, applies half-open membership,
uses `math.fsum`/independent counts, and derives empty/all-null/unavailable reasons.
All paths match its complete ordered origin support, typed values, completeness
and reasons before timing and after **every** sample. Their compared Arrow schemas
must also match, including Boolean activity columns. Floating comparisons use
fixture-bounded `rel_tol=1e-12, abs_tol=1e-12`, not a universal accuracy promise.
Before trials, the checker demonstrably rejects a missing origin, duplicate
origin, corrupted metric and changed column type. No permanent application tests
were added.

## Timings

DuckDB 1.5.6, Ibis 12.0.0, Arrow 23.0.1, Python 3.12.11; one engine thread on a
shared Apple Silicon workstation. Five warm samples per path; handwritten and
precompiled Terrine query order alternates. Public materialization follows each
pair, so these are not isolated cold-start or order-unbiased measurements.
Loading, model binding, independent-oracle work and warmup are outside the timed
calls. Results are median `[minimum–maximum]`, in milliseconds.

| Case | Handwritten SQL → Arrow | Terrine precompiled SQL → Arrow | Public Terrine materialization |
|---|---:|---:|---:|
| README | 2.404 [2.180–2.686] | 37.121 [36.516–40.058] | 475.056 [461.542–500.146] |
| Scaled monthly | 26.479 [25.507–29.325] | 118.496 [117.440–127.171] | 615.134 [587.875–658.065] |
| Wide fixed | 66.957 [66.798–75.358] | 3,512.698 [3,358.044–4,327.362] | 17,565.748 [17,405.825–18,618.665] |

The precompiled wide query's gap establishes a query-level cost, not just an
API-validation overhead. The extra public-call time includes more compilation,
checks, transactions and richer output. Subtracting the two medians would not
cleanly attribute it to one subsystem.

Standalone design/expression construction and projected SQL compilation were
also observed; these are not disjoint phases to add to public materialization,
which recompiles internally.

| Case | Design + expression build, ms | Projected SQL compilation, ms | Handwritten Arrow bytes | Terrine projected Arrow bytes | Public Arrow bytes |
|---|---:|---:|---:|---:|---:|
| README | 104.641 | 69.415 | 385 | 385 | 923 |
| Scaled monthly | 85.554 | 74.052 | 1,405,000 | 1,405,000 | 3,200,000 |
| Wide fixed | 4,913.383 | 1,539.944 | 5,901,646 | 5,901,646 | 14,170,396 |

## SQL text size

These are exact submitted UTF-8 bytes for the matched projections, not formatted
line counts. Both statements are additionally normalized with the same SQLGlot
DuckDB formatter. A minified one-line generated statement is not shorter merely
because it has fewer lines.

| Case | Handwritten submitted bytes | Terrine projected submitted bytes | Handwritten formatted bytes | Terrine projected formatted bytes |
|---|---:|---:|---:|---:|
| README | 1,620 | 42,502 | 1,777 | 90,060 |
| Scaled monthly | 1,620 | 19,528 | 1,777 | 32,626 |
| Wide fixed | 10,856 | 1,063,594 | 11,509 | 3,755,646 |

Public execution has additional study statements. An untimed observation wraps
only the real `_DuckDBSession.execute`/`batches` entry points, records each actual
SQL string, delegates unchanged and restores both methods before any trial.
There are no fake results or bypassed checks. The capture boundary is
`terrine/execution/inputs.py:603-630`.

| Case | Full `.expr` submitted bytes | Captured public study statements | Their total submitted bytes |
|---|---:|---:|---:|
| README | 42,823 | 3 | 97,005 |
| Scaled monthly | 19,849 | 3 | 51,057 |
| Wide fixed | 1,067,895 | 3 | 2,261,680 |

These totals **exclude metadata/admission probes, catalog introspection and
transaction-control SQL**. They are study validation/output statements, not a
claim to capture every database round trip. They also cannot be compared with a
single query's size without labeling that difference.

## Interpretation

- The concern about generated SQL is supported by fresh matched measurements.
  Removing prefixes/per-output joins did not remove the wide query's large
  expression expansion or its remaining query cost.
- Terrine still buys declaration-driven completeness, validation, metadata,
  general shape handling and retained execution. Those features are real, but
  they do not explain away the matched precompiled-query gap.
- This experiment measures that gap; it does not yet attribute every cost to a
  compiler pass or establish which simplification would preserve all contracts.
  No compiler optimization, arithmetic-policy change or guard removal was made.
- The prior recipe cost harness used fastest-of-repeats server CTAS
  (`benchmarks/run_cost.py:21-59`); its numerical ratios are not directly
  interchangeable with these median query-to-Arrow measurements. This is a fresh
  current-version comparison, not a reproduced historical speedup or CI target.
- Only DuckDB was executed. These numbers establish no PostgreSQL/cloud runtime,
  warehouse scan-cost, peak-memory or all-recipe performance guarantee.

## Reproduce and inspect

```sh
agent-slots -j 1 -- uv run python -m benchmarks.compare_sql --case readme --repeats 5 --output /path/to/new-readme
agent-slots -j 1 -- uv run python -m benchmarks.compare_sql --case scaled --units 10000 --events-per-unit 30 --repeats 5 --output /path/to/new-scaled
agent-slots -j 1 -- uv run python -m benchmarks.compare_sql --case wide --units 10000 --events-per-unit 30 --repeats 5 --output /path/to/new-wide
```

Fresh evidence is under `benchmarks/results/direct-sql-{readme,scaled,wide}-20261109-v2/`.
Each directory contains the literal `handwritten.sql`, matched
`terrine-projected.sql`, full expression SQL, actual captured public study SQL,
all samples in `result.json`, and source/engine/argument identities in
`environment.json`. Input Arrow-stream hashes and schemas are in `result.json`.
Every recorded production/helper source hash was checked unchanged after its run,
and all three cases used the same benchmark-source hash. Prior experiments were
not overwritten. The benchmark helper imports existing fixture/model builders;
its handwritten results and Python oracle do not call the production aggregator.

The original run is preserved in the directories without `-v2`; its manifest is
explicitly superseded. Review found that the first handwritten wide query returned
decimal `0.0/1.0` activity columns while Terrine returned Boolean. The original
value-only tolerance check did not reject that type mismatch. The added schema
gate failed on the original SQL before correction; Boolean SQL/oracle values and
strict per-sample schema checks now protect the comparison. All three cases were
rerun from the same corrected helper, rather than mixing original and corrected
measurements. Only the corrected measurements appear above.
