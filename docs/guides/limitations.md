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

A native sum's error is bounded against the unit's whole history prefix
through the window end, $2\,n_P\,2^{-53}\sum_{i\in P}|x_i|$, not against the
window alone: a small window after a large history is only as accurate as
that history total allows (three small events after $10^6$ events of
$10^3$ are answered within $0.22$). A mean divides that bound by its actual
denominator; ratios propagate both operands' bounds. Non-finite inputs and
results outside the binary64 range refuse. On PostgreSQL and the portable
policy, whose float arithmetic raises instead of rounding to an infinity or
zero, `bound_overflow` also refuses finite results the guards cannot admit:
a unit whose summed history magnitudes reach about half the range
($8.99\cdot10^{307}$, even where values cancel), quotients that could round
to zero (below $2^{-1020}$ with a divisor of magnitude at least one) or
overflow (above $2^{1020}$ with a divisor below one), and cohort reductions
whose scaled quotients or products leave that range; DuckDB publishes those
finite results.

An exact float measure quantizes once to `DECIMAL(38, 9)`-scale values by the
engine's float-to-decimal conversion (nearest, ties away from zero, with about
$10^{-16}$ relative binary64 conversion error), so a value within that error
of a halfway point may round either way; only the stated bound is promised.
Quantized values share the declared-decimal exact coefficient path, so the
window's error is bounded by $10^{-9}n_W + 10^{-12}\sum_{i\in W}|x_i|$
whatever the history; a mean divides that by its actual denominator. Nonzero
float values below $5\cdot10^{-10}$ vanish from exact sums: the window
publishes the retained sum with `__reason = below_resolution` (and
`__complete` unchanged) whenever it discarded one. Declare decimals or
rescale small measures (for example into micro-units) when those
contributions matter, in either mode. Ratios with a nonzero denominator within
its error bound of zero have no stated accuracy; exactly zero quantized
denominators return NULL with `zero_denominator`.
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
