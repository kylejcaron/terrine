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

## Day boundaries are fixed offsets

`day_boundary` accepts `UTC` or a fixed UTC offset, not an IANA daylight-saving timezone. Daily and monthly snapshots in a daylight-saving zone therefore shift by an hour relative to local midnight at each transition. Choose the standard-time or daylight-time offset for the reporting period that matters most, or use UTC when one fixed boundary across the year is required.

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
timestamp or overflowing an amount. Float measures sum in native binary64
unless declared `exact=True`; counts, integers, declared decimals, keys and
timestamps are always exact.

Native binary64 reduction has finite range and precision. Ordinary source-window
sums consume selected contributions only, not lifetime running totals, but
rounding and cancellation can still erase small terms among the selected values.
Physical reduction order, including daily regrouping, can change results;
neither arithmetic mode promises bitwise floating parity. Ill-conditioned sums
and ratios have no universal relative-error bound. Underflow and arithmetic
errors follow the backend. Selected nonfinite inputs and returned nonfinite
results are checked, but an engine arithmetic exception can occur first.

The separate cumulative-trajectory and cohort reductions retain their
magnitude, quotient, product and underflow safeguards. These can refuse finite
results on PostgreSQL and portable policies that DuckDB may publish; they are
not an ordinary source-window magnitude certificate. The
[specialized arithmetic contract](backend-compatibility.md#specialized-cumulative-and-cohort-arithmetic)
describes those limits.

An exact float measure quantizes each selected input once to
`DECIMAL(38, 9)`-scale values by the engine's float-to-decimal conversion
(nearest, ties away from zero, with about $10^{-16}$ relative binary64 conversion
error), so a value within that error of a halfway point may round either way;
only the stated bound is promised.
Quantized values share the declared-decimal exact coefficient path, so the
window's error is bounded by $10^{-9}n_W + 10^{-12}\sum_{i\in W}|x_i|$
whatever the history; a mean divides that by its actual denominator. Nonzero
float values below $5\cdot10^{-10}$ vanish from exact sums: the window
publishes the retained sum with `__reason = below_resolution` (and
`__complete` unchanged) whenever it discarded one. Declare decimals or
rescale small measures (for example into micro-units) when those
contributions matter. Exact-derived ratios with a nonzero denominator within
its propagated error bound of zero have no stated accuracy; native ratios have
no general accuracy promise. Exactly zero denominators return NULL with
`zero_denominator`.
See the [shared numerical contract](backend-compatibility.md#shared-numerical-contract)
for the bounds, when to choose exact, and the input and accumulation limits,
and [temporal engine](temporal-engine.md) for error propagation. Row filters
compare original values; eligibility thresholds and state bins compare
published results. Approximate sketches have only their documented error
properties.

Output cardinality and bytes cannot certify the work needed to produce them.
A tiny result can require a large scan, join or sort. Bounded client batches do
not bound backend memory. See [cost and data residency](cost-and-residency.md)
for the separate controls and explicit unknowns.
