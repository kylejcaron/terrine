# Task 9 — Quantized floats on the declared-decimal path

Status: **DONE_WITH_CONCERNS**. The complete cutover is ready in one commit, with the required focused checks passing. The measured materialization timing increased in this run despite substantially smaller SQL. Live warehouse execution was unavailable, as specified in the assignment.

Commit subject: `perf: quantize floats once and accumulate them on the exact decimal path`.
Base revision: `13314bb`, branch `perf/quantized-floats`.

## Numerical model and derivation

The input domain is nullable, finite binary32/binary64, widened to binary64, with exact magnitude strictly below `10**29`. The literal binary64 `1e29` is below that exact integer boundary; its next representable neighbor toward positive infinity is outside. Null input remains null.

The mathematical quantizer is `Q9(x) = sign(x) * floor(abs(x) * 10**9 + 1/2) / 10**9`, with ties away from zero. The implementation does not implement that formula by multiplying the full input in binary64: that multiplication can overflow the decimal cast's coefficient domain through rounding at the admitted boundary. Instead it separates the integral part and floating remainder, casts the integral part separately, and quantizes the remainder at scale 9. A scale-0 cast in the portable policy can round; it is corrected toward zero before forming the remainder, so the remainder has the input's sign. DuckDB uses its native floating `trunc` for that decomposition. The positive and negative `1023/1024` ties additionally exercise the integral correction in the portable policy.

The resulting decimal's integer coefficient is `c = Q9(x) * 10**9`. Grouping, accumulation, and endpoint subtraction use the existing declared-decimal exact machinery, not binary64 accumulation. The published sum is the conversion of the exact difference at scale 9.

The error model is the binding contract, against the exact sum of the original binary64 inputs, not their shortest text:

- `e_S = 1e-9 * n_W + 1e-12 * fsum(abs(x) for x in W)`.
- Means divide `e_S` by their actual denominator. The per-day regression uses two values over three calendar days, so confusing the event count with the calendar denominator cannot pass.
- Each exact ratio operand converts once at its own scale; an already floating quotient passes through. For estimates `n_hat`, `d_hat` and operand bounds `e_n`, `e_d`, the denominator's exact zero is checked before conversion. When `abs(d_hat) > e_d`, the bound is `(abs(n_hat)*e_d + abs(d_hat)*e_n) / (abs(d_hat)*(abs(d_hat)-e_d)) + 2**-51*abs(n_hat/d_hat)`. This follows by bounding the denominator below by `abs(d_hat)-e_d` in the quotient difference, with the final term allocated to conversions and division. A nonzero denominator inside its bound of zero is still returned, without an accuracy claim.

These are implemented/tested contracts, not a new claim that every native warehouse cast has been measured. The assigned later live-probe work remains the source of that evidence.

## Changes by file group

### Numeric interfaces and engine policies

- `terrine/backends/numeric.py`: adds `quantized_float` and scale-aware `float_ratio`; keeps exact coefficients, sum/subtract, count literals, accumulation guards, and output conversion. Removes the old float scale, exact float entry, normalized-significand quotient, and their unused parsing/certification helpers. Float zero operands use a floating zero rather than a BIGNUM literal, allowing ratios of already floating means to compose.
- `terrine/backends/dialect.py`: floats now share the scale-0 coefficient domain with declared decimals. Portable quantization separates integral and fractional pieces to admit the exact boundary without a rounded scaled cast. `exact_to_float` handles nonzero scales and passes floating quantities through. All registered policies advertise the shared quantization operation.
- `terrine/backends/engines/duckdb.py`: removes partition-scale float coefficients and normalized quotient support; preserves BIGNUM coefficients, HUGEINT counts and integer division. Its quantization override uses native floating `trunc` plus separate decimal casts.
- `terrine/backends/engines/postgres.py`: deletes the unscaled float-text state and native exact quotient route. Preserves unbounded NUMERIC coefficients, sum/subtract, and integer division. The binary64 parser retains explicit overflow/underflow guards so scale-aware coefficient text can be converted without an ordinary PostgreSQL cast raising first.
- `terrine/backends/engines/bigquery.py`: deletes unscaled float-text state and native exact quotient support; retains BIGNUMERIC coefficient translation and exact arithmetic.
- `terrine/backends/engines/snowflake.py`: deletes DECFLOAT entry, partition-scaled float state, and normalized quotient support. NUMBER(38, 0) sums retain their type and now use the existing conservative accumulation and subtraction guards, so unsafe float coefficients refuse as exact overflow.

### Temporal state, publication, and refusal consumers

- `terrine/temporal/daily.py`: quantizes floats once, extracts their scale-9 decimal coefficients, retains invalid-input counters, and feeds those coefficients through the same guards as declared decimals. Original values still drive predicates and filters.
- `terrine/temporal/prefix.py`: removes the float scale column requirement; retains exact running invalid-input and withheld-state counters. The float refusal now uses immutable `context["reason"]` and includes `out_of_range`.
- `terrine/temporal/aggregate.py`: removes `_Quantity.exponent`, data-derived scale handling, and normalized quotient composition. Float sums use scale 9; ratios receive both scales. Withheld state and overflowing exact differences use `terrine.temporal.exact_overflow`, including when a float state feeds a denominator. Non-finite and input-range failures keep the float refusal; output conversion/division failures use `bound_overflow`.
- `terrine/compile/shapes/cohorts.py`, `terrine/compile/shapes/ltv.py`: migrate their uses of the shared float refusal to the same `reason` context key. Their reductions of already published floats remain native; they do not quantize again.

### Tests, benchmark, and documentation

- `tests/_float_bound.py`: the specified sum and propagated ratio bounds.
- Backend, temporal, generic-dialect, quotient-composition, and conformance tests move to original-input Fraction oracles and stated bounds; integer/declared-decimal assertions remain exact.
- Required float cases cover large history cancellation, six rounding examples, quantized zero denominators, propagated ratios, mixed scales, calendar denominators, means not rescaled by `1e9`, the exact input range boundary, unbounded DuckDB history, and bounded denominator refusal before returning output.
- Existing compile/runtime/export tests consume the new refusal context. The predicate range test now admits `1e29` and refuses the next float, even behind a false eligibility conjunct.
- Existing warehouse test expectations were migrated without contacting warehouses: input range failures use `out_of_range`, subnano inputs vanish, sums use the quantized bound, and the Snowflake bounded-coefficient case uses two `9e28` inputs. These live tests were not executed here.
- `integration/agent/test_dataset_tool_runtime.py`: fixes the existing `ModelResolver` fixture's parameter-name mismatch exposed by the required type check. This is a fixture-only protocol conformance fix, not an agent API change.
- `benchmarks/run_overhead.py`: its float-mean oracle now uses exact original floats and the quantized mean bound, rather than shortest-text arithmetic and the removed quotient accuracy promise. Declared-decimal/count reference arithmetic stays unchanged.
- `docs/guides/backend-compatibility.md`, `docs/guides/limitations.md`, `docs/guides/temporal-engine.md`, and `AGENTS.md`: document the shared quantization contract, scale-aware quotient conversions, refusal reasons, bounded-state behavior, sub-`5e-10` disappearance, no bit/order promise, and decimal/micro-unit alternatives. Removed both temporal-engine passages promising retention of every finite binary64 and unrounded quotient composition.
- `docs/reference/refusals.md`: regenerated after changing the RefusalSpec. `terrine/_refusal_codes.py` was regenerated by the same command and is unchanged because the code set did not change.

## Spec B2 mapping

| B2 requirement | Implementation and evidence |
| --- | --- |
| Exact domains unchanged | `terrine/backends/numeric.py:70` count literals and `:75` decimal coefficients; `terrine/temporal/daily.py:219` retains declared-decimal scales. DuckDB BIGNUM/HUGEINT paths remain in `terrine/backends/engines/duckdb.py:172`. Exact integer, decimal, timestamp and boundary tests are included in the 440-test focused run. |
| Quantize once, nearest/ties away; then exact declared-decimal state; predicates original and thresholds published | `terrine/backends/numeric.py:157`, `terrine/backends/dialect.py:176`, `terrine/backends/engines/duckdb.py:153`; `terrine/temporal/daily.py:206-209` sends the quantized decimal through `decimal_coefficient(..., 9)`. Daily predicates precede that conversion (`daily.py:167-175`); aggregate publication converts only after endpoint subtraction (`aggregate.py:663-669`). Rounding tests: `tests/temporal/test_window_aggregates.py:536`; original-value predicates and published-value thresholds are exercised by the temporal and compile selections. |
| Window-local sum bound; actual denominator for means | `tests/_float_bound.py:7`; Fraction oracles in `tests/temporal/test_window_aggregates.py:512`, `:561`, and `:568`. Large history never appears in the sum-bound helper's window argument. |
| Ratio bound, exact quantized zero, nonzero near-zero behavior, no bit/order promise | `terrine/backends/numeric.py:179` converts both operands at their own scales and checks exact zero; `terrine/temporal/aggregate.py:256` composes means/ratios without normalized intermediate state. `tests/_float_bound.py:11` returns no bound when `abs(d_hat) <= e_d`; `test_window_aggregates.py:543`, `:552`, and `:573` cover zero, propagated, and mixed-scale cases. Documentation: `docs/guides/backend-compatibility.md:195-209` and `docs/guides/temporal-engine.md:670-688`. |
| Stable float failure reasons; declared-decimal accumulation limits; unbounded DuckDB | `terrine/temporal/prefix.py:21-31`; `aggregate.py:213`, `:358-396`, `:663-669`; exact accumulation guard at `terrine/backends/numeric.py:215`, running guard at `terrine/temporal/prefix.py:118-138`. Input boundary: `tests/temporal/test_window_aggregates.py:585`. DuckDB wide history: `:598`; bounded denominator: `:605`; public generic materialization overflow cases: `integration/backends/test_generic.py::test_accumulated_state_overflow_is_a_coded_refusal`. |
| Subnano limitation and near-zero ratios documented, with practical alternative | `docs/guides/limitations.md:53-66`, `docs/guides/temporal-engine.md:682-688`, `AGENTS.md:249-264`. The guide gives both a `DECIMAL(18, 12)` example and recording micro-units. |
| Remove float-specific machinery and move every consumer together | Production/test consumer search described below has no remaining removed numeric callsites. Native state and integer division remain; no aliases or compatibility path were added. |

## TDD evidence and numerical probes

All commands below ran with cwd `/Users/kylejcaron/orca/workspaces/terrine/test-cpu-stable-1.0`. Every pytest command was single-process (`-n 0`); no whole tier or live warehouse command was run.

### Required initial RED

Before implementation:

```sh
uv run pytest -n 0 tests/temporal/test_window_aggregates.py tests/backends/test_numeric.py
```

Observed output:

```text
FAILED test_quantization_rounds_to_nearest_nano_unit[4e-10-0.0]: obtained 4e-10, expected 0.0
FAILED test_quantization_rounds_to_nearest_nano_unit[-4e-10-0.0]: obtained -4e-10, expected 0.0
FAILED test_quantization_rounds_to_nearest_nano_unit[6e-10-1e-09]: obtained 6e-10, expected 1e-09
FAILED test_quantization_rounds_to_nearest_nano_unit[-6e-10--1e-09]: obtained -6e-10, expected -1e-09
FAILED test_quantization_rounds_to_nearest_nano_unit[0.0009765625-0.000976563]
FAILED test_quantization_rounds_to_nearest_nano_unit[-0.0009765625--0.000976563]
FAILED test_ratio_with_zero_quantized_denominator_is_null: obtained 3333333333.3333335
FAILED test_float_range_boundary_is_exact: DID NOT RAISE TerrineError
FAILED test_quantized_float_rounds_to_nine_places: cannot import name quantized_float
pytest: 9 failed, 64 passed, 178 warnings in 17.86s
```

The other added cases were regression protection for behavior the old exact implementation already satisfied; they were not falsely reported as initial failures.

### DuckDB cast probe and root fix

Executed:

```sh
uv run python -c 'import duckdb; c=duckdb.connect(); print(c.execute("SELECT x, TRY_CAST(x AS DECIMAL(38,9)), TRY_CAST(CAST(x AS VARCHAR) AS DECIMAL(38,9)), CAST(x AS BIGNUM), CAST(x AS DECIMAL(38,0)) FROM (VALUES (6e-10::DOUBLE),(-6e-10::DOUBLE),(0.0009765625::DOUBLE),(-0.0009765625::DOUBLE),(4e-10::DOUBLE),(-4e-10::DOUBLE),(1e29::DOUBLE)) v(x)").fetchall())'
```

Observed direct scale-9 casts: `6e-10 -> 1E-9`, `-6e-10 -> -1E-9`, `1/1024 -> 0.000976563`, `-1/1024 -> -0.000976563`, `±4e-10 -> 0E-9`. Thus the brief's rounding cases already rounded correctly. However both direct and shortest-text scale-9 casts of `1e29` returned NULL. BIGNUM and scale-0 decimal casts retained `99999999999999991433150857216`. Earlier ordinary CAST probes raised `ConversionException` at that same admitted input. The override is therefore required by the exact input boundary, not by a failed tie example.

The portable-policy boundary test was then strengthened to include `±1e29` and integral-correction ties. Before its integral/fractional cast decomposition:

```sh
uv run pytest -n 0 integration/backends/test_generic.py -k fixed_scale_entry
```

```text
Conversion Error: Could not cast value 99999999999999991433150857216.000000 to DECIMAL(38,9)
pytest: 1 failed, 41 deselected, 1 warning in 0.51s
```

It passes in the final focused selection, including the positive/negative `1023/1024 -> ±0.999023438` assertions.

### Bounded denominator RED and GREEN

The added denominator regression was also executed against the original `13314bb` numeric/temporal modules through a read-only import loader, without reverting files or changing checkouts. The loader source is recorded verbatim in `task-9-baseline-sql.json` under `command`. To reproduce that RED from the committed evidence:

```sh
uv run python -c 'import json; from pathlib import Path; s=json.loads(Path(".superpowers/sdd/2026-11-01-test-cpu-stable-release/task-9-baseline-sql.json").read_text())["command"]; exec(s.split("from benchmarks import run_overhead as b")[0] + "import pytest\nraise SystemExit(pytest.main([\"-n\", \"0\", \"tests/temporal/test_window_aggregates.py\", \"-k\", \"bounded_float_denominator_overflow\"]))\n")'
```

Observed:

```text
FAILED test_bounded_float_denominator_overflow_refuses_instead_of_publishing
assert 'terrine.temporal.float64_precision' == 'terrine.temporal.exact_overflow'
1 failed, 50 deselected, 5 warnings in 2.34s
```

The final focused GREEN below includes this test. It uses the same two `9e28` values as the DuckDB wide-history test, puts them in a generic-policy denominator, and verifies exact overflow before `_rows` can return output. Public generic materialization tests additionally exercise both daily and running-prefix withholding, and exact-overflow propagation through sum, mean and ratio consumers.

### Final GREEN

```sh
uv run pytest -n 0 tests/backends tests/temporal tests/compile/test_columns.py integration/backends/test_generic.py integration/temporal/test_window_quotient_composition.py integration/backends/test_numeric.py integration/conformance
```

```text
pytest: 440 passed, 1 skipped, 54 deselected, 886 warnings in 123.96s
SKIPPED tests/temporal/test_sketches.py: datasketches is not installed
```

```sh
uv run pytest -n 0 integration/conformance --generic-dialect
```

```text
pytest: 12 passed, 33 deselected, 43 warnings in 6.22s
```

Additional changed-consumer coverage:

```sh
uv run pytest -n 0 tests/compile/test_cohorts.py tests/compile/test_ltv.py integration/compile/test_column_runtime_contracts.py integration/compile/test_frame_runtime_window_export.py integration/agent/test_dataset_tool_runtime.py
```

```text
pytest: 70 passed, 1 skipped, 392 warnings in 83.90s
SKIPPED integration/compile/test_column_runtime_contracts.py: datasketches is not installed
```

Intermediate failures were resolved: stale `failure` assertions; BIGNUM zero compared with a nested floating quotient; treating registry dialect classes as instances; portable boundary casting; type annotations; and an incorrect temporary expectation that a cohort fixture quantized its already-published floats. That fixture directly constructs published period values, so its native huge-float reduction tests were retained with `bound_overflow`, not changed to an input-domain test.

### Lint, type checking, generated documentation and build

```sh
make lint
```

Passed: ruff, vulture, isolated pydoclint. Final output includes `All checks passed!`; command exit 0.

```sh
uv run ty check terrine tests integration
```

Final output: `Found 24 diagnostics`, exit 1. **All 24 are `unresolved-import` for unavailable optional packages**: Google Cloud/OAuth, cryptography (the Snowflake fixture dependency), psycopg, airflow, dagster, and datasketches. There are no remaining argument-type, operator, resolver-protocol, or other nonenvironmental diagnostics. The initial check's additional nonenvironmental errors were fixed rather than suppressed.

```sh
uv run python -m tests.documentation
uv run --group docs python -m tests.documentation --build
```

Final output: `Documented 176 codes from 276 declarations`; both commands exit 0. The strict docs build is quiet on success.

Warnings in the exercised tests are Ibis/DuckDB Arrow API deprecations; they are not numerical failures. No live PostgreSQL, BigQuery, or Snowflake execution was attempted.

## Deleted and replaced tests

Deleted tests of removed interfaces, with their now-unused fixtures/helpers:

- `test_float_scale_is_per_partition`: partition-derived float scales no longer exist.
- `test_normalized_truncates_to_nineteen_significant_digits`: normalized-significand state no longer exists.
- `test_quotient_of_exact_operands_meets_bound`.
- `test_quotient_of_rounded_operands_meets_bound`.
- `test_nested_quotients_keep_their_precision`.
- `test_quotient_overflow_is_flagged`.
- `test_quotient_without_a_finite_ratio_is_null`.

The last five tested the removed normalized quotient representation and its stronger accuracy/underflow behavior. Their user-facing concerns are covered by scale-aware means/ratios, propagated bounds, zero denominators and output conversion tests. The exact-to-float overflow test remains, rebuilt directly from exact coefficients rather than removed float-entry APIs.

The registry test for refusing removed normalized/exact-ratio operations was removed; the registry now verifies the shared quantization operation on every registered engine. Sparse-state equality to shortest-decimal sums became a window-bound check over original floats. Tiny/subnormal retention and unrounded-average composition expectations became quantized-zero expectations. Obsolete live refusal cases for subnano quotient overflow and data-derived-scale widening were removed because those states no longer exist; their replacement semantics are covered by zero-quantized-denominator and fixed-scale tests.

## Benchmark and compiled SQL evidence

Executed once, with one DuckDB thread and the benchmark's default five warm samples:

```sh
uv run python benchmarks/run_overhead.py --output /Users/kylejcaron/orca/workspaces/terrine/test-cpu-stable-1.0/.superpowers/sdd/2026-11-01-test-cpu-stable-release/task-9-benchmark.json
```

Exit 0; all ten cases report `oracle_match: true`. The console prints total build-plus-materialization times; the table below deliberately uses `warm_summary.materialize_seconds.median` from the JSON, matching the requested Task 8 comparison.

Baseline materialization medians are the previously reported Task 8 values. Baseline SQL sizes were independently measured from the unchanged `13314bb` numeric/temporal modules using the read-only loader recorded in `task-9-baseline-sql.json`; all baseline oracles also matched. The loader ran the same benchmark workloads in one process with one DuckDB thread. It changed no file in either checkout.

| Case | Task 8 materialization median (s) | Quantized materialization median (s) | Baseline output SQL characters | Quantized output SQL characters |
| --- | ---: | ---: | ---: | ---: |
| one_row_count | 0.193 | 0.502655 | 13,422 | 13,422 |
| one_row_decimal_sum | 0.268 | 0.828324 | 23,543 | 23,543 |
| one_row_float_average | 0.347 | 1.471321 | 62,705 | 31,438 |
| one_row_ratio | 0.386 | 1.281590 | 50,803 | 36,355 |
| one_row_four_columns | 0.867 | 0.985604 | 142,390 | 96,343 |
| few_rows_four_columns | 0.866 | 0.684050 | 142,390 | 96,343 |
| events_10k_count | 0.185 | 0.182693 | 13,424 | 13,424 |
| events_10k_four_columns | 0.981 | 0.928112 | 142,392 | 96,345 |
| events_100k_count | 0.226 | 0.221347 | 13,424 | 13,424 |
| events_100k_four_columns | 1.417 | 0.898148 | 142,392 | 96,345 |

Executed aggregation of these JSON records printed:

```text
Mean materialization median: 0.798384583s; baseline 0.574s;
delta +0.224384583s (+39.09%)
Output SQL total: 746885 -> 516982;
delta -229903 (-30.78%)
```

This is **not** evidence of a materialization speedup. Timing dispersion was substantial even for unchanged SQL: `one_row_count` materialization samples ranged from `0.364485` to `0.749344` seconds, while the later `events_10k_count` case ranged from `0.178700` to `0.218535`. [INFERENCE] Shared-machine contention may contribute, but it was not measured and is not an explanation established by this run. No favorable rerun was substituted. The SQL reduction is measured directly and the count/declared-decimal SQL sizes are unchanged.

Environment recorded by the benchmark: Python 3.12.11, Ibis 12.0.0, DuckDB 1.5.6, PyArrow 23.0.1, macOS arm64, `duckdb_threads=1`.

## Final consumer search and self-review

Searched `terrine`, `tests`, `integration`, `docs`, `benchmarks`, and `examples`, including ignored files, for each removed numeric name and `normalized(`. No executable removed numeric consumer remains. The only matches are immutable historical specification/plan text and old test-duration inventory entries; those are not imports/calls and were intentionally retained. Unrelated SQL/identifier normalization is not part of this numeric cutover.

Self-review findings addressed:

1. Native DuckDB scale-9 casting rejected the admitted boundary, although the required tie examples passed. Fixed at quantization, not by widening the allowed range or suppressing an error.
2. The portable policy needed the same boundary protection; integral correction must be toward zero, otherwise a positive fractional tie after rounding the integer part could round in the wrong direction. Positive/negative `1023/1024` cases cover that correction.
3. Ratios of means require a floating zero and pass-through conversion of floating operands. A BIGNUM zero was not a valid Ibis comparison against an already floating quotient.
4. Bounded float accumulation must use `exact_overflow`, including denominators. The final regression observes the correct refusal with two `9e28` values and cannot return output; DuckDB accepts that same history under the stated bound.
5. Shared float refusal producers outside temporal aggregation had to move to `reason` together. Cohort and cumulative-trajectory producers and tests were migrated in the same commit.
6. The required type check exposed an existing resolver fixture parameter-name mismatch; it was corrected and its complete focused file passed.

The protected main checkout was checked before committing:

```sh
git -C /Users/kylejcaron/terrine status --short
```

Observed output was only `?? .coverage`. No branch switch, rebase, push, other-checkout edit, warehouse connection, or review dispatch was performed.

## Files changed

Production:

- `terrine/backends/numeric.py`
- `terrine/backends/dialect.py`
- `terrine/backends/engines/duckdb.py`
- `terrine/backends/engines/postgres.py`
- `terrine/backends/engines/bigquery.py`
- `terrine/backends/engines/snowflake.py`
- `terrine/temporal/daily.py`
- `terrine/temporal/prefix.py`
- `terrine/temporal/aggregate.py`
- `terrine/compile/shapes/cohorts.py`
- `terrine/compile/shapes/ltv.py`

Tests and benchmark:

- `tests/_float_bound.py`
- `tests/backends/test_numeric.py`
- `tests/backends/test_generic.py`
- `tests/backends/test_engine_registry.py`
- `tests/temporal/test_sparse_state.py`
- `tests/temporal/test_window_aggregates.py`
- `tests/compile/test_cohorts.py`
- `tests/compile/test_ltv.py`
- `integration/backends/test_generic.py`
- `integration/backends/test_numeric.py`
- `integration/conformance/test_engines.py`
- `integration/temporal/test_window_quotient_composition.py`
- `integration/compile/test_column_runtime_contracts.py`
- `integration/compile/test_frame_runtime_window_export.py`
- `integration/warehouse_execution/test_bigquery_parity.py`
- `integration/warehouse_execution/test_postgres_windows.py`
- `integration/warehouse_execution/test_snowflake_parity.py`
- `integration/agent/test_dataset_tool_runtime.py`
- `benchmarks/run_overhead.py`

Documentation and evidence:

- `AGENTS.md`
- `docs/guides/backend-compatibility.md`
- `docs/guides/limitations.md`
- `docs/guides/temporal-engine.md`
- `docs/reference/refusals.md`
- `.superpowers/sdd/2026-11-01-test-cpu-stable-release/task-9-report.md`
- `.superpowers/sdd/2026-11-01-test-cpu-stable-release/task-9-benchmark.json`
- `.superpowers/sdd/2026-11-01-test-cpu-stable-release/task-9-baseline-sql.json`

## Concerns and intentionally unexercised scope

- The recorded timing is slower than the supplied baseline; only SQL size improved demonstrably. A performance claim needs controlled follow-on evidence, not this timing run.
- Live warehouse cast behavior and the migrated warehouse tests were not exercised. The assignment explicitly delegates live cast probes, generic witness expansion, and any additional warehouse quantization overrides to Task 10. Generic conformance and registered-policy unit tests passed here; they are not represented as live warehouse evidence.
- The required type command exits nonzero solely for optional-package imports, and datasketches tests skip in this environment. No nonenvironmental type diagnostic remains.
