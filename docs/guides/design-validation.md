# Design validation

`Design.validate(model)` checks a design against already-bound model metadata. It does not compile a query, read rows, estimate observed completeness, or materialize results. Invalid or unresolved design references raise `TerrineError` with a stable `terrine.*` code.

Metric and property references, analysis-unit compatibility, metric window declarations, readiness declarations, temporal boundaries, and semantic capability are checked statically. A metric window extending beyond declared source coverage is not rejected: execution may yield null values with incompleteness reasons. Missing a coverage declaration is a static refusal.

Feature and eligibility windows must end no later than the negative gap; strict as-of cutoffs obey the same rule. Outcome windows begin at or after anchor time. All windows are half-open and must have positive duration. Calendar-day averages require aligned local-day endpoints. When alignment depends on a runtime anchor, the report records an alignment obligation; anchors are never rounded. Event resolution is capability-gated by the bound backend profile.

Treatment is a separate descriptive classification: a baseline treatment recorded at time zero is not rejected solely because the design has a positive gap. Date-only fixed anchors and date bounds resolve through the model's configured day boundary; timestamp bounds must be timezone-aware. Intraday metric windows require exact event-time resolution, admitted only for the bound DuckDB profile; unsupported profiles must use genuinely aligned anchors and endpoints or a supported backend.

Static inline properties and non-versioned dimensions do not require event-time resolution. Inline event-time/pre-exposure properties and versioned dimensions are classified using the exact local clock of the anchor plus the signed cutoff; aligned cutoffs remain valid on profiles without event resolution. An unknown event-anchor clock remains a runtime alignment obligation.
Dimension validity controls this requirement even when an individual property's
value is annotated `static`: a constant value does not remove version boundaries.

The internal `validate_design` helper returns a row-free `ValidationReport` containing sorted required source names and runtime obligations. It is an internal summary, not user-authored compiler input.

## Describing a design

Before building the [first dataset](first-dataset.md), print the same design's
summary:

```python
print(design.describe(model))
```

For the README's in-memory customer and orders tables, this prints:

```text
One row per customer (table customer, key customer_id) at each snapshot.
Snapshots: every 1 month from 2025-02-01 through 2025-03-01 (2 dates: 2025-02-01, 2025-03-01), at
  00:00 UTC.
Features (before t0):
  orders_30d: count of orders in [t0 - 30d, t0)
  spend_30d: sum of orders.amount in [t0 - 30d, t0)
Outcomes (after t0):
  spend_next_30d: sum of orders.amount in [t0, t0 + 30d)
Completeness: orders is declared complete for 2025-01-01 through 2025-03-20 (UTC days).
  A metric window outside declared source coverage is null with reason incomplete_coverage; unit
    observation bounds use incomplete_observation.
Empty complete metric windows (no matching events):
  orders_30d: 0 (no reason)
  spend_30d: 0 (no reason)
  spend_next_30d: 0 (no reason)
Columns: unit, anchor_id, t0, orders_30d, orders_30d__complete, orders_30d__reason, spend_30d,
  spend_30d__complete, spend_30d__reason, spend_next_30d, spend_next_30d__complete,
  spend_next_30d__reason.
```

The table name is `customer` because in-memory inputs are registered under their
mapping keys. A connection-backed roster bound to `"customers"` instead reports
that table name.

`describe` resolves an inferred unit and validates the design before reading its
bound metadata. It neither compiles a query nor reads source rows, and invalid
references raise the same coded refusals as `validate`. Coverage comes only from
the required sources' declarations, not observed data. Long schedules list their
first twelve dates and the remaining count. Shape summaries include their own
events, periods, state levels or sequence limits; non-cross-sectional structural
columns are described by reference to the roles available after building.

Empty-window values are listed per metric output, from its bound aggregation:
calendar-day averages return zero on an empty complete window, while event
averages return null with `no_observations`. Shape-specific completeness is
separate: a sequence uses `source_coverage` or `unit_observation`, while
time-to-event follow-up can end in complete censoring rather than a missing
metric window. Positional sequences have an `is_event=false` header plus
`is_event=true` child rows; array sequences keep their events in `event_values`
on a single row. Cross-sectional and panel column lists include eligibility
values and their support columns.

The [dataset agent tools](dataset-agent-tools.md) return the same text as
`summary` in both design validation and proposal outputs, after source
authorization.

## Building and exporting

`Design.build(model)` adds output-namespace and compilation checks without reading
rows. Feature, outcome and eligibility labels share one NFC/casefolded namespace;
labels retain their spelling, but collisions and reserved support names refuse.
Only sources selected by the design need readiness declarations; unused label
definitions do not prevent building a feature-only design.

Safe Frame execution runs all deferred producer checks before export, including
checks for candidates later excluded by eligibility. Known false excludes a
candidate even when another conjunct is unknown. Otherwise unresolved eligibility
refuses with aggregate counts, not customer rows. Numeric comparisons use exact
integer/decimal predicates, or the rounded binary64 value of a float window,
against the declared threshold.

A fixed-date cross-sectional Boolean treatment at zero is classified as baseline
binary only when `known_at_t0=True` was explicitly declared. Other treatments stay
descriptive/unclassified. This does not infer assignment, independence or causal
validity; estimator admission still checks its stronger data contracts.
