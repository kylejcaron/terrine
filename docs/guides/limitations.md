# Inherent limits

These are limits of the available source information, dataset construction or
statistical validity. Current implementation/backend support belongs in the
[capability matrix](capabilities.md) and [compatibility guide](backend-compatibility.md),
not here.

## Source declarations are assumptions

Completeness cannot be inferred from the last event: inactivity and missing
history can look identical. Declare source coverage and per-unit observation
bounds from an authoritative source. A complete empty window may be zero; an
unobserved window remains a numeric null with its exact reason.

A first or nth event is selected within declared available history. It is not
proof of a lifetime-first occurrence. Roster membership is not follow-up
completeness, and an absent observation bound does not establish lifetime
observability.

## Effective time is not knowledge time

SCD2 history describes when a value was effective. It cannot establish when an
analyst could have known a later correction unless the source retains a
recorded-at axis. Point-in-time feature windows and split purging prevent the
specified temporal leakage; they cannot reconstruct missing historical
knowledge. Timestamp precision discarded before ingestion cannot be recovered.

## A watermark is not a revision log

Readiness describes completeness, not whether old records changed. Hashes of
definitions and a newer watermark cannot detect authoritative restatements or
deletes. Supply change/revision evidence or request a conservative rebuild.
An ordinary transaction view is consistent for that execution but not a durable
replay certificate. Retain immutable inputs when reproducible rebuilding is
required. A Dataset pins an output generation; it does not freeze unrelated
mutable sources.

## Counts do not establish a statistical model

Treatment, repeated-row cluster, trial and weight roles describe data, not
identification. Grace-defined initiation is not automatically baseline treatment.
Shared export preflight protects known dataset hazards but cannot prove causal
identification, model fit, independent censoring or valid inference.
Risk-set counts and plain rates do not imply a survival curve. Aggregate
risk-set counts without entry history cannot reconstruct per-unit delayed-entry
records; use per-unit records when the downstream method needs that history.
Terrine's descriptive reports make no p-value, interval or causal pass/fail claim.

## Exactness and resource bounds have domains

Finite timestamp and numeric representations cannot encode every requested
endpoint or exact result. A coded refusal is preferable to silently narrowing a
timestamp or overflowing an amount. On DuckDB and PostgreSQL, float facts enter
exact state as their shortest decimal text, so a window sum is that exact sum
rounded once to binary64; BigQuery first rounds each value to 38 fractional
digits and Snowflake refuses a coefficient or sum beyond 38 digits
([backend compatibility](backend-compatibility.md#bigquery)). Averages
and ratios carry a stated relative bound ([temporal engine](temporal-engine.md)).
Eligibility predicates and state bins compare the rounded value. Approximate
sketches have only their documented error properties.

Output cardinality and bytes cannot certify the work needed to produce them.
A tiny result can require a large scan, join or sort. Bounded client batches do
not bound backend memory. See [cost and data residency](cost-and-residency.md)
for the separate controls and explicit unknowns.
