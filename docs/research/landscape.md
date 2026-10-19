# Prior art: observational study frames and temporal dataset builders

## TL;DR

- **Verified with a scope caveat:** no located open-source project combines arbitrary product-analytics warehouse semantics, declarative causal-study design, point-in-time feature roles/leakage validation, and warehouse-side frame compilation. This is a bounded search, not proof of nonexistence.
- **Verified:** OHDSI ATLAS is explicitly built around standardized OMOP CDM healthcare data; HADES supplies R analysis packages for the OMOP ecosystem, not a general warehouse compiler.
- **Verified:** TrialEmulation and SEQTaRget implement sequential target-trial methods, but expect prepared longitudinal data and focus on estimation/analysis, not semantic-layer-to-frame compilation.
- **Verified:** Statsig does geotesting for non-randomized product/marketing settings, and holdouts are designed for randomized persistent controls. Neither is evidence of general observation-study frame construction.
- **Verified:** Amplitude Impact Analysis and Mixpanel Impact report non-randomized product-impact comparisons; their analysis/reporting does not expose a reusable, role-typed, leakage-checked dataset compiler.
- **Refuted as written:** semantic layers do support various rolling/cumulative windows (dbt MetricFlow, Cube), but documentation found no event-anchored, per-unit relative-window study grammar or leakage type system. Malloy/LookML are not independently assessed deeply here.
- **Closest conceptual neighbors:** Temporian (open-source, indexed entities, temporal windows sampled at chosen times, leak-aware feature operations) and Kaskada (open-source declarative event timelines, entity joins, time travel); neither is target-trial design or a warehouse-native causal frame compiler.
- **Gap is real, with these neighbors:** borrow their time/window primitives and TrialEmulation's sequential trial vocabulary, but retain a distinct layer for target-population eligibility, anchor/trial rows, treatment/outcome roles, censoring, and static temporal validation.

## Scope and status notation

“Verified” means the cited primary source explicitly supports the statement; “refuted” means the categorical claim is contradicted by a primary source; “unverified” means the available primary evidence does not settle the claim. Negative claims below are carefully bounded to the sources inspected and are not proof that no product or feature exists. Product documentation establishes advertised functionality, not a complete internal implementation. Terrine's comparison target is the spec's Anchor `(unit_id, anchor_id, t0)`, relative Window `[a,b)`, Aggregate/As-of/Expand, observed-through censoring, temporal role validator, sequential-trial default, and warehouse/ibis compilation [S1].

## Table of tools and fit

| Tool | What it covers | Gap vs terrine | Source URL |
| --- | --- | --- | --- |
| **Cross-market claim: no OSS compiler** — **verified within bounded search; universal negative unverified** | Sources inspected reveal distinct pieces: OMOP clinical study tools (ATLAS/HADES), statistical TTE packages (TrialEmulation/SEQTaRget), temporal feature engines (Temporian/Kaskada), and warehouse semantic layers (MetricFlow/Cube). | No source located joins declarative observational design + arbitrary product event/metric/attribute semantics + leakage-typed columns + output frame construction in a general warehouse. A stronger “none exists anywhere” claim is unverified. | [S1], [S2]–[S7], [S12]–[S16] |
| **OHDSI ATLAS / HADES — verified** | ATLAS is open source for scientific analyses on standardized observational data converted to OMOP CDM V5; cohort definitions and population-level analyses are healthcare-oriented. HADES provides open-source R packages for observational analyses in OHDSI. | Requires OMOP standardized vocabularies/data model and healthcare concepts. It offers cohort/design/analysis components, but not arbitrary product-analytics entities/events/metrics or terrine's general event-anchored frame API. | [S2] [S3] |
| **Aetion Evidence Platform / Panalgo IHD — commercial healthcare RWE — verified as commercial/proprietary; target-trial-specific wording partly unverified** | Aetion describes AEP as a platform transforming real-world data into transparent evidence and as a longitudinal RWD-to-RWE engine. Panalgo describes IHD as healthcare analytics software for RWE. Both products are marketed commercial healthcare analytics platforms. | Healthcare / RWE market and proprietary platform, not open-source product-analytics package; public materials inspected do not establish an arbitrary-schema point-in-time frame compiler or expose implementation details. “Target-trial emulation platform” specifically was not verified from primary product docs in this pass. | [S4] [S5] |
| **Statsig Geotesting / holdouts — partially refutes no quasi-experimental features** | Statsig docs explicitly describe geotesting as supporting product causal-inference techniques when a traditional A/B test is unavailable. Holdouts keep a group of users back from features for measurement. Warehouse Native supports use over warehouse data. | Geotesting is a bounded experiment-method feature, not a declarative longitudinal-study compiler. Holdouts are persistent randomized comparison groups, not observational designs. Public docs do not describe compiling user-level relative-time windows, as-of covariates, censoring, or role validation. | [S6] [S7] [S8] |
| **Eppo — randomized experimentation; GeoLift/quasi-experiment unverified** | Eppo's docs describe SDK-based feature rollouts/randomized experiments and warehouse-native analysis. Metrics map to warehouse entity/fact/dimension models; the platform analyzes experiment assignment data and supports diagnostics. | Not evidence of observational causal frame construction. The searched official docs did not verify a GeoLift or general quasi-experiment product; do not claim it categorically lacks one. | [S9] |
| **GrowthBook — experiment analysis; no verified quasi-experimental feature** | Supports running randomized experiments and analysis with custom assignment or third-party experiment exposure data already in the warehouse. | Requires assignment/exposure information; it analyzes experiment results rather than constructing counterfactual observational cohorts or pre-treatment feature frames. Official pages inspected did not establish a causal quasi-experiment module. | [S10] |
| **Amplitude Impact Analysis — partially refutes “experiments only”** | Official docs describe understanding how first-time engagement with one feature affects another behavior and provide guidance on causal interpretation. | A product impact report is not a reusable study spec or dataset compiler; docs inspected do not provide arbitrary anchors, covariate windows, treatment/outcome roles, censoring, static leakage checks, or exportable study frames. | [S11] |
| **Mixpanel Impact — partially refutes “experiments only”** | Impact report measures the effects of a product or marketing launch on KPIs. | Report-level analysis, not a general observation-study compiler. No evidence in the cited report documentation for terrine's row-grain, temporal metadata, or leakage-validation contract. | [S12] |
| **dbt MetricFlow — semantic metrics and cumulative windows — refutes broad “no windows,” not event-anchored claim** | MetricFlow supports simple/derived/ratio/cumulative metrics, sliding cumulative windows and grain-to-date aggregation; uses a time spine. | Windows are metric-time aggregation semantics over modeled data, not per-unit relative offsets from a per-unit event or scheduled trial anchor; no causal column roles/leakage validator. | [S13] [S14] |
| **Cube — semantic measures and rolling windows — refutes broad “no windows,” not event-anchored claim** | Cube measures include aggregated values and docs cover rolling windows, time shifts, and multi-stage calculations. | BI/analytics time calculations are not documented as each unit's event-relative `[a,b)` frame operators; no study-trial design, treatment/outcome semantics, or leakage rejection. | [S15] [S16] |
| **LookML / Malloy — event-window absence unverified** | Not investigated deeply enough from first-party references to make a categorical absence claim. Both are query/modeling languages, but that characterization alone does not establish their temporal feature set. | No evidence gathered here either way on precise per-unit event anchors and relative windows. Keep the claim unverified rather than infer absence from product category. | [S17] [S18] |
| **TrialEmulation (R) — nearest statistical TTE neighbor — verified** | Implements target trial emulation with marginal structural models for intention-to-treat/per-protocol effects on observational time-to-event data. Its API represents a `trial_sequence`, prepared data, trial expansion, censoring/switch weights, and case-control sampling. DuckDB is among its package imports. | Works on supplied/prepared longitudinal data and focuses on TTE processing and estimation; no general warehouse semantic model, declarative metric/event/attribute source mapping, ibis compiler, or spec-level temporal role validator. | [S19] [S20] |
| **SEQTaRget (R) — verified** | CRAN describes sequential trial emulation for observational databases with time-varying treatments/confounders and binary/failure-time outcomes. | A sequential-trial analytical implementation, not a general data-semantic / frame-spec compiler; no product-analytics warehouse abstraction established by cited docs. | [S21] |
| **zEpid — epidemiologic analysis toolkit — verified scope; compilation not found** | Open-source Python epidemiology package; documentation covers epidemiology analyses and methods (e.g. marginal structural models and causal methods). | Primarily analytical methods on caller-provided data, not a declarative target-trial-to-warehouse-frame compiler. No direct evidence of terrine primitives. | [S22] |
| **DoWhy / EconML — causal inference, not study-frame compilation — verified** | DoWhy provides causal model/identification/estimation/refutation workflow; it can use EconML methods. EconML focuses on heterogeneous treatment-effect estimation. | Estimator/causal-model APIs do not define the warehouse event model, trial anchors, time-relative features, censoring, or leakage-safe frame construction. | [S23] [S24] |
| **Temporian — nearest temporal feature-engineering neighbor — verified** | Open-source Python temporal feature engineering; EventSets indexed by entity; moving aggregations over time windows; sampling controls output timestamps. Its design includes explicit time shifts and `leak()` for future leakage. | It is an event/time-series preprocessing engine, not a causal study specification: no target-trial eligibility/assignment model, feature/treatment/outcome roles, censoring/observed-through policy, warehouse semantic metric compiler, or causal frame protocol. | [S25] [S26] |
| **Kaskada — nearest declarative event-query neighbor — verified, archived** | Source describes a declarative event-processing engine with per-entity automatic joins, event-based windowing, row generators, continuous values at arbitrary times, and native forward time travel (not backward), intended to avoid temporal leakage. GitHub owner is now `datastax-archive`. | Excellent temporal query precedent, but not an observational study design / target-trial framework or semantic model for metrics and as-of business attributes. It is a standalone event engine, not terrine's ibis-on-customer-warehouse compiler. Archived status suggests it is not a current maintained product (archive ownership is directly observable; activity status beyond that is not asserted). | [S27] [S28] |
| **tsfresh — time-series feature extraction — verified** | Python package extracts features from time series, including calculators over the given series. | Feature extraction after data is assembled, not point-in-time study construction, anchor-relative query planning, or causal validation. | [S29] |
| **Narrator Activity Schema — semantic-model neighbor — verified** | Activity Schema is an open warehouse-native standard for modeling entities doing activities over time; Narrator maintains a dbt package. | Provides a reusable activity/event model, but not a target-trial design grammar, windows relative to a study anchor, leakage roles, or estimation-ready censored frames. | [S30] [S31] |
| **Cohort-query / leakage literature — methods, not compiler — verified** | Target-trial papers formalize aligning eligibility, treatment assignment, and start of follow-up; leakage work identifies information leakage as an ML evaluation failure mode. | Supports terrine's correctness problem, but does not itself provide a product-analytics warehouse compiler. | [S32] [S33] |

## Findings by claim and search area

### 1. Is the gap real?

**Status: verified as a strong, bounded conclusion; universal absence unverified.** The closest source-level matches split along the exact boundary terrine targets. TrialEmulation and SEQTaRget understand sequential emulated trials but operate on supplied longitudinal analysis data; Temporian/Kaskada understand entity-indexed time-series/event operations but not target-trial causal semantics; MetricFlow/Cube define warehouse metrics/time calculations but not causal-study row construction. No reviewed primary source documents the complete combination. Terrine's differentiator should therefore be framed as **the intersection and contract**, not the novelty of any individual primitive.

A particularly important nuance: this is not “nobody does causal inference in product analytics.” Statsig Geotesting, Amplitude Impact Analysis, and Mixpanel Impact explicitly offer nontraditional/product impact analysis. The narrower gap is a reusable, auditable **dataset compiler** which takes declarative observational design and business semantics and emits analysis rows with temporal roles and censoring metadata.

### 2. OHDSI ATLAS/HADES

**Status: verified, with HADES wording qualified.** ATLAS's own README says OMOP CDM V5 and healthcare claims / drug and diagnosis cohorting. HADES is the OHDSI open-source R package ecosystem for large-scale observational analysis; its relationship to OMOP is central to that ecosystem. This is a genuine analogue for study design and cohort analysis, but not a product analytics/general schema analog. Do not reduce HADES to “just a UI”: distinguish ATLAS (UI/application) from HADES (analysis packages).

### 3. Aetion / Panalgo

**Status: commercial healthcare products verified; specific “target-trial-emulation platform” claim only partly verified.** First-party product pages market Aetion Evidence Platform as RWD-to-RWE evidence software and Panalgo IHD as healthcare RWE analytics. The reviewed material did not establish their exact internal study-design language or public availability of a target-trial emulation module. Present them as commercial RWE platform neighbors, not as verified direct technical equivalents. Neither is source-available open-source based on the cited product pages; to make a formal licensing/closed-source claim, inspect current terms or a specific source-code/license page rather than infer from marketing copy alone.

### 4. Warehouse experimentation and product causal analysis

- **Statsig:** yes, geotesting explicitly covers causal inference when A/B testing is unavailable; this refutes an overly broad claim that experimentation platforms only do randomized tests. Its holdouts are deliberate feature/experiment holdback groups, still randomized product experimentation. Available docs center experiment inputs, geographies, and KPI measurement; they do not show a general unit-anchor-window frame compiler.
- **Eppo:** official docs establish warehouse-native randomized experimentation, SDK assignments, warehouse entity/fact/dimension models, and diagnostics. No official evidence found for “GeoLift” or a general quasi-experiment module. Status: **unverified**, not false.
- **GrowthBook:** custom assignment/3rd-party exposure is supported if assignment data is in the warehouse; documentation describes experiment result analysis. No verified observational design/frame feature found.
- **Amplitude/Mixpanel:** both explicitly have impact reports beyond standard A/B test results. These should be acknowledged as quasi-experimental/product-impact neighbors; don't describe every warehouse analytics product as experiment-only. Their public reports are analytical surfaces, not documented general-purpose causal data compilers.

### 5. Semantic layers and relative windows

**Status: broad claim refuted; terrine-specific absence supported but not exhaustively proven.** MetricFlow defines trailing cumulative windows (e.g. 7 days) and uses a time spine. Cube documents rolling windows, shifts, and period-to-date calculations. Therefore “semantic layers lack windows” is wrong. The unfilled feature is narrower: *for each unit and anchor record*, evaluate arbitrary relative intervals around event/calendar time-zero, materialize one per-anchor result, propagate interval bounds through re-anchoring, handle observation end/censoring, and enforce pre-treatment feature roles. The searched LookML/Malloy primary docs were insufficient to assert those languages lack this behavior; compare only after a separate focused investigation.

### 6. Statistical TTE libraries vs causal estimators

- **TrialEmulation** is the strongest target-trial neighbor and is relevant for sequential trials, artificial censoring, weights, and sampling. Its API expects data and has analysis-specific trial expansion; the gap is semantic frame compilation and point-in-time guarantees before estimation.
- **SEQTaRget** also explicitly implements sequential emulation, with time-varying treatment/confounding. It is another methodology implementation, not the terrine spec/compiler boundary.
- **zEpid** contributes epidemiologic analysis methods; it is not the missing compiler.
- **DoWhy/EconML** are more distant: causal graph/identification/estimation and heterogeneous effect estimation respectively. They consume data, not build the data's temporal design from product semantics.
- “TargetTrialEmulation” and “emulate” names are ambiguous across papers and repositories; I did not identify a clearly maintained, general-purpose Python package under those exact names that beats TrialEmulation as a direct comparison. Status: **unverified** for the existence/absence of all similarly named projects; avoid naming them as a single established package without repo/version evidence.

### 7. Adjacent temporal and cohort tooling

Temporian and Kaskada are the strongest overlooked technical analogues. Temporian's indexed EventSet + moving window + sampling APIs align particularly well with Anchor/Window/Aggregate and leak-awareness; Kaskada's declarative timeline model explicitly discusses entity joins, time travel, event windows, and row generation. They lack the causal design types, and Kaskada is now archived under DataStax. tsfresh is a downstream feature-extraction toolkit and not an anchor-query system. Narrator Activity Schema provides an open warehouse-native activity model, but no study grammar. No source found for a direct causal/product-study compiler in Motif Analytics, Houseware, or PostHog; this is **unverified** because those product families were not exhaustively searched. Marketing attribution is excluded as instructed.

## Implications for terrine

- **Adopt the boundary, not a competitor's whole product:** terrine should own `Design → validate → build(frame)` between an existing semantic layer and causal estimators. Distinguish study specification/compiler from estimator APIs (TrialEmulation/DoWhy/EconML).
- **Borrow names carefully:** retain **Anchor**, **Window**, **Aggregate**, **As-of**, **Expand**, and **observed_until** from the handoff vocabulary; consider TrialEmulation's **trial sequence**, **ITT/per-protocol**, **artificial censoring**, and **switch/censor weights** as precise method terms, not necessarily top-level primitives. Temporian's `sampling` is a useful name/concept for explicit output-time anchors; Kaskada's “timeline” is evocative but may imply its event-query model.
- **Treat time windows as a semantic-layer capability, not a novelty claim:** MetricFlow/Cube already cover metric-time rolling calculations. Terrine's differentiating API is anchor-relative, per-unit windows with study roles, observation cutoffs, and a query plan that can use warehouse prefix tables.
- **Enforce leakage statically:** use the handoff's role-window bounds, including interval propagation through re-anchoring. Temporian's explicit distinction between backward-safe shifts and forward leakage is a precedent; it does not replace treatment/outcome validation.
- **Use sequential trials as first-class frame grain:** TrialEmulation confirms this is a well-established design pattern, not a terrine invention. Terrine's contribution is compiling the repeated eligibility/treatment/covariate/outcome rows directly from semantic models in customer warehouses.
- **Keep censoring/missingness distinct from zero:** model source completeness and unit exit time explicitly; the tool landscape does not provide a reason to collapse missing follow-up into zero. This is central to interpretable outcomes, not merely output formatting.
- **Preserve warehouse-native construction:** Eppo/Statsig illustrate that product analysis can remain close to warehouse tables; terrine can differentiate by compiling to ibis and returning lazy, role-rich frames rather than taking over experimentation/feature-flag operations.
- **Avoid claims to avoid:** do not say semantic layers have no windows; do not say experimentation platforms have no quasi-experiment/impact functions; do not describe TrialEmulation as a warehouse compiler; do not claim all commercial RWE vendors implement target-trial emulation absent explicit first-party source; do not say that no tool exists absolutely.

### Closing assessment

**The gap is real, but it is a compositional gap.** The nearest to terrine's temporal/data-construction half is Temporian, followed by Kaskada; the nearest to its observational design half is R TrialEmulation (with SEQTaRget nearby). None of the primary sources reviewed combines these into a declarative, warehouse-native, product-analytics study-frame compiler with static leakage validation. Terrine should position the novelty at that seam, and its first design review should benchmark against Temporian's sampling/window semantics and TrialEmulation's sequential trial/censoring workflow.

## Sources

All sources below are first-party docs, source repositories, or papers. Accessed for this research on 2026-10-18.

- **[S1]** Terrine handoff: [docs/superpowers/specs/2026-10-16-terrine-handoff.md](../superpowers/specs/2026-10-16-terrine-handoff.md) (§§2–6).
- **[S2]** ATLAS source repository and README: https://github.com/OHDSI/Atlas
- **[S3]** HADES source repository: https://github.com/OHDSI/Hades
- **[S4]** Aetion Evidence Platform: https://aetion.com/technology/platform
- **[S5]** Panalgo IHD Analytics: https://panalgo.com/ihd-analytics
- **[S6]** Statsig, Types of Experiments / Geotesting: https://docs.statsig.com/statsig-warehouse-native/features/types-of-experiments
- **[S7]** Statsig, Holdouts: https://docs.statsig.com/experiments/holdouts-introduction
- **[S8]** Statsig Warehouse Native overview: https://docs.statsig.com/
- **[S9]** Eppo architecture / warehouse analysis docs: https://docs.geteppo.com/
- **[S10]** GrowthBook, Running Experiments: https://docs.growthbook.io/experiments
- **[S11]** Amplitude, Impact Analysis: https://amplitude.com/docs/analytics/charts/impact-analysis
- **[S12]** Mixpanel, Impact: https://docs.mixpanel.com/docs/reports/apps/impact
- **[S13]** dbt MetricFlow, Cumulative metrics: https://docs.getdbt.com/docs/build/cumulative
- **[S14]** dbt MetricFlow time spine: https://docs.getdbt.com/docs/build/metricflow-time-spine
- **[S15]** Cube, Measures: https://docs.cube.dev/docs/data-modeling/measures
- **[S16]** Cube, Multi-stage calculations: https://cube.dev/docs/product/data-modeling/concepts/multi-stage-calculations
- **[S17]** Looker LookML reference (focused per-event-anchor search not completed): https://cloud.google.com/looker/docs/reference/param-explore-join-sql-on
- **[S18]** Malloy language documentation: https://docs.malloydata.dev/documentation
- **[S19]** CRAN TrialEmulation manual: https://cran.r-project.org/web/packages/TrialEmulation/refman/TrialEmulation.html
- **[S20]** TrialEmulation source: https://github.com/Causal-LDA/TrialEmulation
- **[S21]** CRAN SEQTaRget: https://cran.r-project.org/package=SEQTaRget
- **[S22]** zEpid source / documentation: https://github.com/pzivich/zEpid and https://zepid.readthedocs.io/en/latest/
- **[S23]** DoWhy documentation: https://www.pywhy.org/dowhy/
- **[S24]** EconML documentation: https://econml.azurewebsites.net/
- **[S25]** Temporian source: https://github.com/google/temporian
- **[S26]** Temporian moving window operators: https://temporian.readthedocs.io/en/stable/reference/temporian/operators/window/moving_sum/
- **[S27]** Kaskada source repository (now DataStax archive): https://github.com/kaskada-ai/kaskada
- **[S28]** Kaskada time-centric example: https://github.com/kaskada-ai/kaskada/blob/main/python/docs/examples/time_centric.qmd
- **[S29]** tsfresh source: https://github.com/blue-yonder/tsfresh
- **[S30]** Activity Schema specification: https://github.com/ActivitySchema/ActivitySchema
- **[S31]** Narrator dbt Activity Schema package: https://github.com/narratorai/dbt-activity-schema
- **[S32]** Hernán MA, Robins JM. “Using Big Data to Emulate a Target Trial When a Randomized Trial Is Not Available.” *American Journal of Epidemiology* 2016;183(8):758–764. https://doi.org/10.1093/aje/kwv254
- **[S33]** Kaufman S, Rosset S, Perlich C, Stitelman O. “Leakage in data mining: formulation, detection, and avoidance.” *ACM Transactions on Knowledge Discovery from Data* 2012;6(4). https://doi.org/10.1145/2382577.2382579
