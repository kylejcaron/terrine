# Lazy study Frames and native execution authority

## Goal and scope

Complete the ordinary Catalog → Model → Design → Frame → Arrow workflow for scientists, engineers and agents. Preserve exact anchors, sparse window arithmetic, strict as-of values, eligibility accounting and honest observation/knowledge metadata. Task 4.2 of the study-API plan remains the acceptance contract; this document resolves implementation ownership, not a scope reduction.

The user authorized autonomous implementation and review, not changes to increment, remote publication, deployment, paid resources or release. Native DuckDB is the first exercised execution backend. Other backends remain usable only for capabilities actually established; accepting an Ibis connection does not certify its snapshot semantics. No estimator or dataset staging/runtime work is added to this milestone.

Assumptions: ordinary callers do not deliberately modify private runtime state or database bytes outside DuckDB. A runtime lease is not a security sandbox against hostile same-process Python. Catalog/source completeness declarations remain owner assertions. Effective-time history is not evidence of historical recording time.

## Authority decision

Choose explicit owned immutable publication plus ordinary no-copy transaction execution. A borrowed read-only-file lease can be a future optimization, but does not eliminate the required SQL-result publisher and introduces another issuer/lifecycle. A transaction is execution authority, not an immutable source-version handle.

Normal materialization opens a private native connection and a real transaction over a supported coherent native closure. Every exact check, eligibility audit and output batch uses that connection/view. No input CTAS, Arrow round-trip or whole-database copy is implicit. Caller-owned active transactions must finish before either private execution or publication; the runtime never commits or rolls them back. TEMP/local or opaque input variants may use explicit owned whole-result publication, not silently lose their state on a duplicate connection.

Before duplicating, serialize Terrine's access to the origin handle and call the verified native `system.main.current_transaction_id()` twice as separate statements. Equal IDs mean an active owner transaction and refuse; different IDs clear only this gate. Never BEGIN/COMMIT/ROLLBACK the caller's transaction. These reads allocate implicit transactions when none exists; they do not read source rows or mutate source data/catalog. An error or unverified builtin namespace refuses. Exclusive caller-side origin use during setup/capture is an API precondition, not something a Python mutex can enforce against arbitrary direct native calls; detected violations refuse. After private-view establishment, concurrent data writes through other native handles remain supported.

Duplicate cursors do not clone `USE`, search paths or timezone, and Ibis.from_connection resets timezone. Create the private Ibis wrapper first, then reproduce and verify differing origin semantic settings/current catalog/schema on the final runtime; progress-display settings are not semantic. Refuse if reproduction cannot be established. Disable Python replacement scans, retain original source SQL unchanged and establish the private transaction before final closure/schema binding. Other native handles may write data after this establishment; callers must not concurrently mutate the owned resolution environment.

Immutable execution is an explicit data-reading operation. The publisher captures native roots under a controlled coherent origin view, or establishes an admitted whole-result boundary, seals an owned native artifact, relinquishes writers, opens read-only execution and issues live handles. It does not claim that unrelated opaque upstream reads shared a historical snapshot. Several mutable attached files are not one atomic source instant merely because BEGIN succeeds. Unsupported upstream coherence requires a verified provider capability or a single explicit stored-result boundary, not sequential mutable copies labeled coherent.

### Public focused interfaces

These live in `terrine.execution`, not the root package:

- `PhysicalRelationId`: frozen/hashable identity with separate `kind`, `catalog`, `schema`, `name`, and `definition_hash` fields. Kind is `table` or `sql_result`; a logical result includes the original root/definition hash. Discovery sorts identities deterministically by their structured components. Qualified identifiers are structured, never split on dots in user labels.
- `RelationHandle`: opaque runtime-issued handle; no public constructor turns strings, OIDs, paths or receipts into authority.
- `VerifiedInputBinding`: opaque exact verified binding, held by Model; no JSON/YAML/pickle reconstruction grants authority.
- `publish_native_inputs(catalog: Catalog, *, con, units: Mapping[str, UnitSource] | None = None) -> PublishedInputs`.
- `PublishedInputs` is an explicit context manager with `con`, immutable `relations`, and idempotent `close()`. It owns local scratch storage and all issued handles. Close prevents new execution leases, including on existing Models/Frames; an already-held execution lease keeps its immutable inputs until completion. Release storage after the last such lease ends. Never overwrite a caller path.

The existing required facades remain exact:

- `Catalog.required_relations(con, *, units=None) -> tuple[PhysicalRelationId, ...]`.
- `Catalog.verify_immutable_inputs(con, *, units=None, relations: Mapping[PhysicalRelationId, RelationHandle]) -> VerifiedInputBinding`.
- `Model.from_catalog(..., input_binding: VerifiedInputBinding | None = None)` and the optional `from_increment` facade forward this same contract.

Example owner action, not automatic collection:

```python
from terrine.execution import publish_native_inputs

with publish_native_inputs(catalog, con=con, units=units) as inputs:
    binding = catalog.verify_immutable_inputs(
        inputs.con, units=units, relations=inputs.relations,
    )
    model = tr.Model.from_catalog(
        catalog, con=inputs.con, units=units,
        readiness=readiness, input_binding=binding,
    )
    result = design.build(model).materialize(budget=budget)
```

### Exact frontier and issuance

Discovery covers every Catalog source, dimension and supplied roster, including SQL/view dependencies, not only metric fact tables. Deduplicate physical identities deterministically while retaining all root-to-leaf associations. Native names follow actual DuckDB identifier/resolution rules; Unicode case folding is an output-label portability rule, not native physical identity. Preserve literal dotted table names.

A published managed context establishes substitutions before verification. Direct native roots resolve to their canonical stored tables. Whole SQL or opaque-view roots resolve to registered logical result identities tied to original definitions and backed by actual stored tables. Dependencies used only inside a replaced result are no longer execution inputs. A table independently referenced elsewhere remains required. Extra original leaves beside a replaced result are extra membership, not harmless receipts. An unresolved root without an issued stored-result boundary refuses; a partial inventory is never called complete.

A live publication records issuer ownership, managed connection/backend, Catalog and roster hashes, original root definitions, backing relation identity/schema, actual runtime generation and lease state. A random receipt labels this verified event; it is not the event's proof. Verification checks exact key membership and each handle's registry membership and facts. Wrong backend/connection, changed definitions/schema, stale generation, mutable/foreign handles, missing/extra entries and revoked leases refuse. Revalidate under an execution lease immediately before checks/output and retain ownership through full consumption or abandonment.

READ_ONLY alone is insufficient: TEMP tables/macros can shadow names. Execution owns an isolated resolution environment and verifies supported functions/settings/closure. OIDs, unchanged SQL or row counts are not generation IDs. Build canonical identities from qualified native table/view metadata; resolve literal TableRef names without splitting dots. SQL factors retain parsed catalog/schema/name components. For a partial reference require one matching object across the complete native table/view metadata snapshot, including TEMP entries in ambiguity detection; fully qualified SQL matches its exact namespace. Reject ambiguous/off-path/temporary/internal variants, with routes to explicit qualified SQL or whole-result publication.

Recursively inspect stored view query ASTs using the existing read-query/function policy and lexical CTE scopes. Track view OIDs/definitions and cycles; retain all admitted physical dependencies even when empty or optimizer-eliminated. Disable replacement scans before binding. Require native built-in functions and reject shadowing custom/macro contenders for admitted or compiler-generated names; metadata inspection uses system.main-qualified built-ins. Bind unchanged original roots and crosscheck native `get_table_names` through the same resolver; the extractor is a crosscheck, not identity authority. All mutable tables and view definitions must belong to one native database. Revalidate the metadata under the held transaction. Bound-plan serialization is not required by this construction.

Unknown native snapshot IDs and upstream generations stay `None`; transaction IDs are not data-version IDs. Runtime-issued publication generations identify only the issued stored inputs. `replay_equivalence` remains `unverified` unless a separate portable replay contract is actually established. Serialized provenance contains safe receipts/generations, never handles, SQL text, credentials or live connections.

Published definitions are checked against the issued stored-input map, not live upstream objects after capture: later upstream table/view changes do not revoke an already sealed immutable result. A changed Catalog/roster declaration, altered backing relation, foreign execution connection or revoked publication does invalidate that binding.

Generation-map keys are canonical compact JSON arrays `['source', name]`, `['dimension', name]`, or `['roster', unit]`, using JSON double quotes in serialization. This keeps namespaces distinct even when user names contain punctuation or prefixes. Include every declared root; use its actual issued stored-input generation on publication and `None` when unknown. These are logical-root receipt keys, not portable physical-version authority.

## Components and actual root replacement

- `execution/inputs.py` owns the two execution refusal declarations, discovery, issuance, verification and binding lifetime. Split native metadata/connection mechanics into `execution/_native.py` and owned publication lifecycle into `execution/_publish.py` if needed; no generic plugin framework.
- Model owns one internal relation resolver. Native schema binding, event-ID checks, source reads, dimension reads and rosters all consume it. `semantic.bind.normalize_source` accepts an internal optional resolver callback while preserving original Catalog/BoundSource definitions. `compile.anchors` stops independently resolving original sources/rosters.
- `compile/columns.py` composes candidates, expression results and eligibility; focused helpers in `compile/_asof.py` and `compile/_predicates.py` own those distinct semantics.
- `compile/cost.py` owns metadata-only preview, scan-policy enforcement and shared budget refusals.
- `frame.py` owns the safe materialization boundary, bounded output, conversions and provenance assembly. It rebuilds the graph against the actual execution context; attaching a receipt to a graph that still reads original mutable roots is invalid.

Model hashes include the Catalog plus semantic unit/roster bindings, event-ID bindings and bound readiness intervals, canonically ordered and UTC-normalized. They remain separate from design hash, source generations and execution snapshot. Readiness is an explicit observation declaration, not an invented source version. Preserve declaration identity when only internal locators change.

Bind only the readiness intervals supplied to Model, without requiring every Catalog metric's sources. Selected Design/view validation already resolves required sources and must require those intervals. Unused label-only coverage is not required to construct a feature-only Design from the original Catalog. This does not prune or weaken a caller-supplied immutable binding's complete Catalog/dimension/roster verification.

## Lazy compilation and public names

`Design.build(model) -> Frame` validates and compiles without source-row queries. Model/schema binding is metadata-only. `Frame.expr` is a low-level Ibis escape hatch: direct execution bypasses safety/integrity/budget gates and is not a verified export.

Ordinary Model binding admits native view dependencies and actual function ownership before calling any Ibis table/sql schema method. Data-dependent schema inference (for example a view over read_csv with a custom path function) refuses before invoking that function, with a route to explicit owned publication. Native metadata-only discovery is allowed; hidden sampling/data queries are not. Cache admitted bound relation expressions for build/default preview, or re-admit changed native definitions before any schema rebind. A changed view must not execute a rejected function through a stale metadata resolver. Supplied immutable bindings bind only their stored-result schemas, never original opaque SQL. Unknown backend schema-inference behavior cannot inherit a metadata-only guarantee.

Validate one namespace before SQL compilation: user features, outcomes and named eligibility cannot collide with each other or reserved roles `unit`, `anchor_id`, `t0`, `treatment`, `_observed_from`, `_observed_until`, or the `__terrine_` prefix. Compare NFC-normalized casefolded labels and any additional executing-backend collision rule; preserve original output labels. Use the existing stable conflict/reserved refusal codes. Labels with quotes/spaces/SQL-looking text are identifiers, never interpolated expressions.

Compile source normalization/enrichment and each distinct expression once per graph. Reuse sparse states by resolved measure and resolution; never create unit×day calendars or anchor×event-window expansions. Use day states only when alignment is established; otherwise retain exact event-resolution states and existing runtime calendar guards. Repeated lookups use fresh relation identities. Keep original event/nonnull counts, exact coefficients, Float64 history flags and final certificates.

Named features/outcomes keep window, role, source/unit observation bounds, completeness, exact null reason and error-column metadata. Numeric checks remain snapshot-bound and happen before rows leave the boundary. All-null, zero-denominator and no-observation nulls remain distinct from unavailable data. Outcomes may be built on eligibility-retained candidates; invalid source/history structure remains a hard refusal, not an UNKNOWN cohort value.

## Attributes, treatment and eligibility

Dimension attributes use the existing strict `attribute_asof` and exact history checks. Inline event-time/pre-exposure properties are a change-point history at source timestamps; project distinct unit/time/value records so identical duplicated observations do not create artificial ambiguity, and reject conflicting values at the same unit/time. Inline static properties project distinct unit/value records and reject conflicting values per unit. Reuse the existing history validator rather than taking an arbitrary first value. Strengthen its aggregate-result parser to fail closed on malformed/empty/null/non-integer counts; this is a related execution-boundary defect, not an additional feature.

Use one lossless native timestamp ordering key for attributes, enrichment, history overlaps and both prefix lookup strategies. Represent an instant as a lexicographically ordered struct of infinity rank, signed whole-second quotient and signed nanosecond remainder. For finite NS coefficients n use native truncating integer divide(n,10^9) and mod(n,10^9); for finite us coefficients u use divide(u,10^6) and mod(u,10^6)*1000. Each remainder multiplication is bounded; never scale an entire wide us coefficient into int64 nanoseconds or narrow NS timestamps. Preserve source/output timestamps, SQL NULL keys and supported infinity ordering. Coarser native units use lossless checked coefficient conversion and a coded refusal if that construction cannot represent the input. No Float64 time keys. Both native ASOF and fallback union/MAX-marker/equality-join compare this same full key, so wide cutoffs and strict ±1ns boundaries retain identical meaning. Do not rely on BIGNUM time comparisons or native TRY_CAST-to-NS: direct probes found overflow failures in both.

An as-of row found with a NULL property is a known observed null. No matching history is unavailable (`missing_history`), not a known null. Completeness intersects declared source and unit observation at the requested cutoff: strict left-limit lookups require lower < cutoff <= upper; non-strict point lookups require lower <= cutoff < upper. Keep coverage versus observation precedence. Treatment uses its explicit `at` point with non-strict lookup and preserves `known_at_t0`; descriptive builds do not require binary treatment or causal eligibility.

For existing estimator interoperability, a fixed-date cross-sectional Boolean treatment at zero with explicit `known_at_t0=True` carries `treatment_design="baseline_binary"`. Other descriptive/nonbaseline treatment stays unclassified. This classification does not infer assignment or causal validity; existing independent-unit, common-time-zero, complete-outcome and measured-input gates remain unchanged.

Cutoff membership alone does not certify carried change-point state. For inline event-time/pre-exposure histories, retain the matched observation timestamp and require complete source and unit observation from that timestamp through the cutoff. A matched record before source coverage starts gives unavailable `missing_history`; one before a unit observation start gives `incomplete_observation`, with ordinary source-before-unit precedence. The same continuity rule applies to derived change-point histories without an authoritative validity assertion. An explicit named valid_from/valid_to record may cover that gap under its declared interval semantics; merely applying LEAD to incomplete observations does not create such authority. No new user-authored proof or baseline API is added. Test both lower-bound gaps, including eligibility/IsNull: stale values and stale observed NULLs must not become complete features or known predicates.

`Gt`, `Eq`, `IsNull` and `And` use three-valued logic. Any known false conjunct excludes; all true retain; otherwise unknown. NULL comparisons and incomplete/missing values are unknown. `IsNull` tests known observed nulls only, with negation preserving unknowns. Exact numeric comparisons reuse the existing coefficient/rational filter comparison semantics rather than narrow decimals/integers through Float64. Float literals follow the same lossless typed-literal admission as filters.

Checked Float64 comparisons use outward `[value-error, value+error]`: Gt is true when lower > threshold, false when upper <= threshold; Eq is true only for a singleton equal to the threshold, false when outside the interval; otherwise unknown. Required interval overflow/numerical invalidity retains its stable numerical refusal, not a fabricated observed null. Invalid input/filter/history contracts remain hard errors; false-overrides-unknown is not permission to hide malformed source integrity.

Audit candidate, false, unknown and retained counts under the same execution authority. Producer materialization with unresolved unknowns raises `terrine.study.eligibility_unknown` with aggregate counts only. A successful export has exactly retained rows. Named eligibility outputs and metadata remain inspectable; they do not become features/outcomes. No Design weights field is added.

Feature/eligibility cutoffs are per named expression/predicate, including attributes; conjunction cutoff is the latest contributing cutoff with strictness derived at that instant. Half-open aggregate ends and strict as-of cutoffs retain their strictness. Current effective-time declarations cannot certify recorded-at history, so provenance labels them `retrospective_effective_time`. Explicit treatment knowledge declarations do not silently relabel the historical source axis. Preserve every shared typed metadata record; no user-authored compiler certificates are required.

## Preview, budgets and conversion

`tr.preview(design, model, budget=None, *, bounded_count=0) -> Preview` is metadata/compile-only by default: no COUNT, EXPLAIN, integrity probes or output query. Unknown row/output/scan estimates stay `None`. DuckDB cardinality estimates or output bytes are not source-scan byte bounds. Include coverage, chosen sparse strategy and row-free warnings only.

A positive bounded count explicitly requests one capped candidate-status aggregate under the same authority and safety checks. Limit the candidate-status relation to cap+1 before aggregating candidate/false/unknown/retained counts; return no customer rows. Set `counted_rows=min(observed, cap)` and `count_capped=observed>cap`. Only an uncapped observation establishes complete eligibility totals; otherwise those totals stay `None`. Unresolved eligibility may appear in this diagnostic summary rather than being exported as data. Strict unknown scan cost with max_scan_bytes refuses before any validation/count/output query. Non-strict scan policy may proceed with an explicit unknown-cost warning, not an invented estimate. Exact integrity-probe scans belong to the same total scan policy.

`Frame.materialize(budget=None) -> MaterializedFrame` performs one bounded output execution after checks/audit. Apply SQL LIMIT max_rows+1 when configured; abort rather than return truncated success. Read Arrow record batches, sum row and data-byte counts while consuming, discard partial output on any limit/refusal/error and always close reader/authority. No guard query followed by an uncapped reexecution. Output limits bound delivered data/buffers, not upstream scan/work or peak RSS; a single large cell/batch is not a hard memory guarantee.

The budget refusals are declared once in `compile/cost.py`: `terrine.budget.rows_exceeded`, `terrine.budget.output_bytes_exceeded`, `terrine.budget.scan_unknown`, and `terrine.budget.scan_exceeded`. All conversions use these same declarations. Execution capability/binding errors remain the two required `terrine.execution` codes; optional dataframe dependency absence uses `terrine.convert.missing_dependency`.

`to_pyarrow(*, budget=None)` returns Arrow with the shared metadata serialization; `to_pandas` and `to_polars` import optional dependencies only when called and before reading rows. Preserve nullable/exact/UTC representations through the supported native conversion, using nullable Arrow-backed pandas types. Missing conversion dependencies share one coded refusal with install route. Root exports remain Model, Design, Frame, needed study constructors, Budget and preview; execution authority internals stay focused-module only.

## Verification and delivery gates

Use tiny deterministic fixtures, focused tests at useful behavior boundaries, and the existing explicit slow tier for composed native integrations. No full-suite target or source/wording/wiring assertions. Regression cases must fail for a plausible consumer-visible bug, not an intentional private implementation choice.

Acceptance includes: names/quoting, intraday feature/outcome boundaries and known-null attributes; independent coverage/observation bounds and empty windows; false-overrides-unknown versus unresolved eligibility; certified threshold uncertainty; decimal/large-integer predicate comparisons; ordinary concurrent native mutations isolated across integrity/eligibility/output; exact immutable frontier and every stale/foreign/missing/extra/revoked variant; actual pinned SQL-result substitution; row limit+1 and byte-abort behavior; query-free default preview and strict unknown-scan refusal; optional conversion isolation.

Run the real public native example and both transaction and verified-input routes without increment imports, then run the committed clean-checkout smoke and installed-wheel smoke outside the source checkout. Provenance/Arrow round-trips must retain exact nullable values, cutoff metadata and unverified replay status. Record executed evidence and all review dispositions before declaring Task 4.2 complete. No warehouse-scale, portable replay, external-provider or package-wide completion claim follows from this milestone.
