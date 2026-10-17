# Design validation

`Design.validate(model)` checks a design against already-bound model metadata. It does not compile a query, read rows, estimate observed completeness, or materialize results. Invalid or unresolved design references raise `TerrineError` with a stable `terrine.*` code.

Metric and property references, analysis-unit compatibility, metric window declarations, readiness declarations, temporal boundaries, and semantic capability are checked statically. A metric window extending beyond declared source coverage is not rejected: execution may yield null values with incompleteness reasons. Missing a coverage declaration is a static refusal.

Feature and eligibility windows must end no later than the negative gap; strict as-of cutoffs obey the same rule. Outcome windows begin at or after anchor time. All windows are half-open and must have positive duration. Calendar-day averages require aligned local-day endpoints. When alignment depends on a runtime anchor, the report records an alignment obligation; anchors are never rounded. Event resolution is capability-gated by the bound backend profile.

Treatment is a separate descriptive classification: a baseline treatment recorded at time zero is not rejected solely because the design has a positive gap. Date-only fixed anchors and date bounds resolve through the model's configured day boundary; timestamp bounds must be timezone-aware. Intraday metric windows require exact event-time resolution, admitted only for the bound DuckDB profile; unsupported profiles must use genuinely aligned anchors and endpoints or a supported backend.

Static inline properties and non-versioned dimensions do not require event-time resolution. Inline event-time/pre-exposure properties and versioned dimensions are classified using the exact local clock of the anchor plus the signed cutoff; aligned cutoffs remain valid on profiles without event resolution. An unknown event-anchor clock remains a runtime alignment obligation.
Dimension validity controls this requirement even when an individual property's
value is annotated `static`: a constant value does not remove version boundaries.

The internal `validate_design` helper returns a row-free `ValidationReport` containing sorted required source names and runtime obligations. It is an internal summary, not user-authored compiler input.
