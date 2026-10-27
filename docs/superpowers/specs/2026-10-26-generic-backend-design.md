# Generic Ibis Backend with Explicit Opt-In

## Purpose and boundary

Offer portable, read-only Frame materialization on an unregistered **Ibis** backend only after `allow_unverified=True`. This is not verification of that backend, permission to use a rejected registered server, or a promise of snapshot consistency. The existing engine-registry plan supplies `Engine`, `SessionStrategy`, `ENGINES`, dialect selection and provider admission; the existing seven cases in `integration/conformance/test_engines.py` supply the shared known answers. Implementation starts after those prerequisites land. This document specifies required behavior, not measured numerical or backend evidence.

## Global constraints

- Public API changes update production callers, exports, docstrings, guides, examples, fixtures and CHANGELOG in the same change; clean cutover, no aliases.
- Refusals keep stable package-wide codes; renamed codes get a breaking CHANGELOG entry; every new refusal survives pickle and deepcopy.
- Null versus zero and per-cell null reasons survive Arrow, manifests and exports unchanged.
- Package version stays `0.1.0.dev0`; no tag, push, publish or GitHub mutation.
- Tests belong in the cheapest tier; record red-first evidence for every new test, assert behavior rather than wording/wiring, and run `make check TESTS="<affected paths>"` before implementation commits.
- Study/compiler/temporal code remains Ibis expressions; native bindings belong in engine modules. No engine-name selection outside the registry and engine-specific modules.
- Verification for this change is local DuckDB plus local PostgreSQL 16, including both with the generic dialect forced. No new CI jobs, services, paid-warehouse runs or dependency extras are introduced.

## Admission and public API

Add keyword-only `allow_unverified: bool = False` to **all** Model binding constructors: `from_catalog`, `from_tables` and `from_increment`. Existing parameters and return types stay unchanged. The latter two forward the choice into the same admission path. Reject non-Boolean choices with `terrine.model.invalid_binding`; do not treat a truthy string as consent. Add `UnverifiedBackendWarning(UserWarning)` in `terrine.errors`, exported from `terrine` as a filterable warning category; no public engine/dialect configuration object is needed.

Extend `resolve_engine(con: Any, *, allow_unverified: bool = False) -> Engine`:

1. A registered exact Ibis module takes its existing `identify(con)` path, regardless of the flag. A failed identity check, unsupported PostgreSQL major, incompatible build or wire-compatible look-alike keeps its current refusal; **never** retry it as generic.
2. A genuine unregistered `ibis.backends.BaseBackend` instance refuses by default, before `table`, `sql`, schema access or source queries, using existing `terrine.backend.capability_unavailable` and immutable context `{backend: <actual casefolded backend name>, operation: "query", status: "unverified", route: ...}`. The route names `allow_unverified=True` and every registered verified query engine, derived in registry order, not a second list.
3. With the flag, that same unregistered Ibis instance emits `UnverifiedBackendWarning` at admission and returns the singleton `GENERIC`. The warning names the actual backend and explains unverified, per-statement reads and lack of retained/publication support. Suppressing warnings does not remove provenance.
4. Raw drivers and objects that merely claim an engine name remain refused, even with the flag. Import Ibis lazily on this admission path; metadata-only registry imports and capability inspection remain driver-free and query-free.

`Model` retains the admitted `Engine` internally. Binding, execution rebinding, preview/cost estimation, cohort summaries, batch materialization and capability gates consume that admission rather than attempting a second default-deny lookup. The authorization is not connection-global: binding another Model without the flag must still refuse. Reusing an admission must validate that the execution connection belongs to that engine; registered private connections retain identity checks. A new explicit admission may warn again; individual statement reads do not re-emit it.

## Generic record, SQL dialect and capabilities

`GENERIC` is an immutable `Engine` beside, **not inside**, the verified `ENGINES` map. Its `name` is `"generic"`, status `"unverified"`, dialect class the base `Dialect`, session strategy `SessionStrategy.NO_SNAPSHOT`, provider module `terrine.execution._generic`, identity callback the genuine-Ibis check, and no registered Ibis module match. Its portable feature set is `naive_anchor_utc`, `first_occurrence`, `event_resolution`; quantile, approximate distinct and warehouse preflight keep their existing capability boundaries.

`get_engine("generic")` returns that metadata record without adding it to the verified map; unknown actual names still return `None`. `engine_supports("generic", feature)` reads its feature set. Derived `BoundSource.dialect` records the expression policy (`"generic"` after generic admission), so pure semantic feature rules can use that metadata without Ibis imports; SQL admission continues to use `Catalog.dialect`.

Keep three concepts distinct:

- **Engine name:** the actual connection identity, for example `"sqlite"`, not `"generic"`.
- **Expression dialect:** `"generic"` for the portable base policy, or the registered policy name.
- **SQL dialect:** the real Ibis/compiler dialect used to parse/admit SQL and check Catalog dialect agreement. `Catalog.dialect` remains that real dialect; users must not put `"generic"` there.

`dialect_for_name("generic")` returns the immutable base policy. Compiler scopes derive expression policy from the admitted Engine, not from `Catalog.dialect`; the same selection must survive literal-only expressions, nested scopes, explicit timestamp selection and out-of-scope cohort summaries. Standalone expression helpers on unknown bound backends may select the base policy, but that never authorizes execution or replaces Model admission.

Generic capabilities explicitly set `snapshot`, `transaction_view`, `retained_snapshot`, `table_publication`, `atomic_manifest_publish`, `runtime_reader` and `native_parquet_unload` to `False`; `change_evidence` is empty. `dialect_admission`, `exact_timestamps`, `exact_decimals` and `arrow_batches` remain `None` (unverified), as do scan estimates, scan limits and cancellation. Extend `BackendCapabilities.require` with `allow_unverified: bool = False`, permitting `None` only for those four named read operations after generic admission; `False` never becomes allowed. The single capability gate owns this policy. Do not mark unverified operations `True`, bypass gates at callers, or admit unknown scan costs under a strict budget. Staging, retained input capture, Dataset reopening/publication and runtime paths refuse with the existing capability code **before writes**.

## Portable expression contract

Every registry-plan `Dialect` method gets either a portable-correct base implementation or an operation-specific coded refusal when the executing backend cannot represent its required primitive. No base method may leak `NotImplementedError`, silently switch to binary64 state, or invoke a registered engine override. Registered subclasses retain current native overrides and their numerical contracts.

| Operation family | Required base policy |
| --- | --- |
| Extrema | Coalesce each argument with the common first non-NULL argument before Ibis LEAST/GREATEST; all-NULL stays NULL. |
| Sequences/schedules | Portable integer sequence with exact cardinality; multiply integer seconds first, then `.as_interval("s")`. Never multiply an interval by a column. |
| ASOF | Range-join fallback: distinct `(unit, request_key)` requests; equality by unit and `<`/`<=` eligibility; group to the maximum eligible right key; join that mapping back to **every original left row**, then to the right payload. Right keys remain unique per unit; unmatched and NULL request keys retain one left row with NULL payload. Use fresh case-insensitive aliases. `has_native_asof=False`. |
| Timestamp ordering | Preserve NULL, precision, strict/inclusive boundaries and pre-epoch signed remainders using Ibis operations. Use native timestamp order rather than assuming comparable structs; reverse-time keys use lossless integer parts. Declared scale/type or actual values not representable losslessly refuse with the existing timestamp-order code, never truncate. |
| UTC | Explicit Ibis UTC timestamp casts/constructions, preserving the instant and interpreting naive source time as UTC. A session-sensitive bare aware/naive cast is **not** sufficient. Use typed UTC epoch/delta reconstruction where necessary; do not alter the caller's session timezone or add native bindings. The unchanged non-UTC conformance case is an acceptance gate. |
| Exact decimals/integers | Preserve source decimal scale and integer coefficients, using a separate `Decimal(38, 0)` coefficient/wide domain. No conversion through binary64. Narrowing checks original bounds before casting. Source/intermediate values outside the supported domain refuse, rather than silently lose digits. |
| Float facts | `has_scaled_float_state=False`; `exact_type()` is `Decimal(38, 9)`. Quantize once on entry; accumulate/subtract in fixed-scale decimal; convert only the completed quantity/quotient to binary64. DuckDB keeps its per-unit-scale BIGNUM override. |
| Quotients | Signed integer division truncates toward zero, not uncorrected Ibis floating `//`. Normalize the exact decimal remainder to the dividend's sign and remove it before division. Where Ibis division lowers to binary64, use a rounded seed plus a rounded residual correction **in `Decimal(38, 0)`**, then require the exact multiplication residual to be zero before int64 narrowing. Guard zero and signed-int64 overflow. Exact-state ratios use a NULL divisor for zero. |
| Finite/overflow flags | NULL is not nonfinite; nonfinite input, unrepresentable input and output overflow remain distinct flags/refusals owned by existing consumers. Unsupported Ibis/type/driver operations become the existing capability refusal with the actual backend and operation, chained from the original exception. Cancellation and process-control exceptions propagate. |

### Domain and error model (implementation obligations)

For float input `x`, let `t(x)` be its finite, widened binary64-to-decimal-text interpretation through Ibis. Define `Q9(t) = sign(t) * floor(abs(t) * 10**9) / 10**9`: **truncate toward zero at nine fractional digits**, implemented by decimal-text decomposition, not floating multiplication. Require `abs(Q9(t)) <= (10**38 - 1) / 10**9`; reject out-of-range input. NULL stays NULL; NaN/infinity use the existing nonfinite-input refusal. The decimal entry perturbation must satisfy `abs(Q9(t) - t) < 10**-9`. This is not a claim that arbitrary backend text is correctly rounded: probes must separately measure `abs(t(x) - x)`.

For a window `W`, the specified exact-state result is `S9 = sum(Q9(t(x_i)))`, provided **every** prefix/intermediate and difference fits the decimal domain. Prefix subtraction must cancel older quantized history exactly. Required error accounting before final binary64 conversion is `abs(S9 - sum(x_i)) <= sum(abs(t(x_i) - x_i)) + len(W) * 10**-9`; final conversion adds its separately measured conversion error. Ratios add the backend decimal-division rounding error before that final conversion. Do not claim DuckDB's native near-ulp guarantee, use a purely result-relative tolerance under cancellation, or relax the existing seven conformance assertions. Record probes for quantization, text conversion, division, cancellation and domain edges before publishing numerical documentation.

The decimal coefficient domain is independent: `abs(c) <= 10**38 - 1`, with source scale carried separately. Thus a scale-2 source such as the existing `9007199254740993.01` cancellation case is not first rounded to float or mistaken for scale 9. `exact_ratio`, wide arithmetic and nested normalization must not accidentally shrink that coefficient domain to 29 integer digits.

For integer quotient, set `n=a-r` after the exact signed remainder, `h=round(float(n)/float(b))`, `e=n-h*b` in the wide decimal domain, `c=round(float(e)/float(b))`, and `q=h+c` in that domain; certify `q*b=n` and the final int64 bounds. The seed is not the answer: retaining `h` alone loses large integer quotients. The implementation must derive the bounded-correction argument and test the actual Ibis lowering, including `INT64_MAX/1` and `INT64_MIN/1`; the design's Python arithmetic probe is not backend evidence.

A backend that cannot provide usable fixed-precision decimal must refuse **the operation needing decimal**, using `terrine.backend.capability_unavailable`, not refuse an otherwise usable count-only Model. Generic execution lazily checks decimal use in the Ibis graph, then admits the type/operations with tiny Ibis scalar known answers before reading user data; cache only within that execution session. Compilation acceptance alone is not evidence of decimal arithmetic (an affinity-only type is insufficient). No new verified-engine list or backend-name branch is permitted.

## NoSnapshot execution and failure boundary

The generic provider borrows the caller's Ibis connection. Its `NoSnapshotSession` implements the existing `ExecutionSession` protocol without BEGIN/COMMIT/ROLLBACK, timezone changes, temporary tables, connection cloning or disconnecting the caller. Each `execute`/batch statement reads independently; metadata and validation statements are not a shared snapshot. `ExecutionSnapshot` has `snapshot_id=None`, actual backend name, unknown source-generation values, no retained binding ID and `replay_equivalence="unverified"`.

Table references use Ibis resolution; SQL references use existing read-only admission in the **real** SQL dialect. Fail closed if that dialect cannot establish read-only safety. Rebind schemas at execution as today, without inventing identity/retention evidence. Lazy batch failures are translated inside iterator consumption, not just reader construction; readers release their resources on completion, error or early close. Context use after close retains the existing coded closed-context refusal.

Translate failures only around actual Ibis/backend boundary calls. Preserve existing `TerrineError`; map unsupported primitive/type/decimal admission failures to capability code and the owning operation, and representable-domain overflow/nonfinite failures to existing numerical codes. Other backend read failures use the capability code with `operation="query"`, `status="unverified"` and the original cause. Never return empty output, retry another dialect, suppress an exception, or classify a programming error elsewhere in Terrine as an unsupported backend.

## Provenance and serialization

Add required fields to existing frozen `Provenance`: `engine_name: str`, `dialect: str`, `engine_status: Literal["verified", "unverified"]`. Every producer (`Frame.materialize`, batch/staged runtime metadata and fixture builders) supplies them explicitly. A generic result records the actual backend, `"generic"`, `"unverified"`; verified producers record their registered identity/policy/status. Copy/replace-based views, cohorts, life-table outputs, exports and manifests retain the stamp.

Keep Arrow metadata version **2**, key `terrine.materialized_frame.v2`, JSON metadata version **1**, outer manifest versions and package version unchanged. New writers always emit the complete stamp. Existing-version payloads with **all three fields absent** decode as `engine_name=dialect=execution_snapshot.backend`, `engine_status="verified"` for a historically verified engine. Before this change official writers had no unverified path, so that recorded execution dialect is verified evidence, not an inference about an unknown backend. This preserves early published datasets without another rematerialization cutover.

Partial or inconsistent stamps, including any `"unverified"` payload missing one of the three fields, refuse with new registered `terrine.metadata.invalid_engine_stamp`, immutable context `{reason: "partial" | "inconsistent" | "unknown_legacy_engine", fields: <sorted tuple of present stamp fields>}` and a rematerialization route. A full verified stamp must name its registered policy; a generic stamp must have status `"unverified"`; an absent stamp with an unknown historical engine cannot acquire verified status. Register/document the refusal and prove pickle/deepcopy preservation. Do not introduce defaults at new producer callsites or an alternative metadata model. Arrow/IPC and JSON round trips preserve the stamp, rows, roles, NULL/zero distinction and exact per-cell null reasons.

## Acceptance, documentation and cost

1. Cheap admission tests cover default refusal before schema, genuine-Ibis opt-in, warning category, immutable/pickle/deepcopy context, raw/fake refusal, registered-identity non-bypass and Model-local permission.
2. Existing seven conformance cases pass unchanged on DuckDB and PostgreSQL with `Dialect` forced at the central selector: whole-day schedules, NULL extrema, float/decimal cancellation, non-UTC conversions, deterministic event ties, NULL observation censoring and strict ASOF. Native provider/lifetime remains intact for these dialect-only runs; separate generic-provider cases prove NoSnapshot and read-only gates.
3. A real SQLite integration attempts a supported count-only dataset; any unavailable timestamp/decimal operation gives its coded, operation-specific refusal. At least one real unregistered-backend read succeeds through the generic provider, even if that backend cannot materialize the complete timestamp workflow. No uncoded compile, execution or late-batch exception is accepted.
4. Decimal capability, exact quotient signs/large integers, NULLs, finite/overflow flags, range-join cardinality and metadata round trips have owner-level tests and independent oracles. Record commands, exit status and actual outputs; planned checks are not evidence.
5. Update `AGENTS.md` backend portability and `docs/guides/backend-compatibility.md` with verified, opted-in unverified and refused tiers; distinguish generic per-statement reads from verified consistent-view providers. Also update capabilities, public API, refusal reference, CHANGELOG and one runnable opt-in example.
6. Add `--generic-dialect` only to the existing conformance module's test configuration, not the public API. Required acceptance is local and serial. Default native coverage stays unchanged. No new CI job is authorized; only if measured cheap, extend existing integration/PostgreSQL commands to include the forced variant. Otherwise document the local commands and their evidence without representing future CI as completed.
