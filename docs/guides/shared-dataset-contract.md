# Shared-dataset contract: increment and Terrine

This pilot measures a fixed-cohort value shape through increment's public
per-unit panel API and Terrine's native `Model`/`Design`/`Frame` workflow. It
does not propose a shared runtime, dependency reversal, or changes to either
library's temporal rules. The optional increment dependency is pinned to
`increment==0.1.0a2`.

## Executable comparison

The deterministic fixture is `tests/parity/test_increment_frames.py`; the
DuckDB example is `examples/increment_parity.py`. Run:

```sh
uv sync --extra increment --extra duckdb
uv run pytest tests/parity/test_increment_frames.py -q
uv run python examples/increment_parity.py
```

Increment receives one row per `(unit, day)` made by bucketing and aggregating
the same raw event and order records used to populate Terrine's native DuckDB
tables. It enters through `increment.frame.from_unit_panel(...)`; the per-unit
exports use the returned public `FramePanelSource.unit_frame(...)`. Terrine
builds from real DuckDB tables, an explicit `UnitSource` roster, distinct
`events`/`orders` source coverage, and a fixed-date Design. Four units are kept,
including a no-activity unit. The values are small hand-checkable integers.

The day-aligned comparison anchors at midnight. Increment's public panel
`unit_frame` call is unwindowed and sums the post-exposure day rows available in
the input (the only post-exposure date here is day zero); Terrine independently
applies its exact half-open `[0d, 1d)` window over that same day support. This
compares sorted unit keys, per-unit amount and event-count totals, a pre-period
amount feature, and ratio numerator/denominator separately. A longer Terrine
label extends past the declared source coverage. The corresponding increment
panel spec declares `window_days=3` and `observation_end`; its aggregate total
moments censor every unit at that maturity, while its public per-unit
`unit_frame` refuses to export a windowed metric. The test checks both outcomes.

A separate noon-anchor fixture contains (a) an event exactly at noon and (b)
activity earlier on that same calendar day. Terrine includes the equality event
under its `>= t0` window start and excludes the earlier event. Increment's
public panel has only a date/day grain: its day-zero bucket includes both
records, and cannot decide whether either timestamp was before, equal to, or
after an intraday anchor. The measured difference is therefore an intentional
day-grain-versus-instant-grain expressibility difference, not an arithmetic bug.
It is not evidence that this public panel path implements increment's separate
raw-query strict boundary.

The installed increment source documents that raw query path explicitly:
`increment/query/builders.py:1109-1120` filters ordinary post-exposure raw events
with `ts > first_exposure_ts` before day aggregation, while
`increment/query/builders.py:1252-1264` chooses `>` or `>=` according to
`include_exposure`. The frame-panel API is a different seam: its public
constructor requires a day-grain date (`increment/frame.py:1742-1775`), day
elapsed time is computed in whole calendar days (`increment/_frame_panel.py:76-107`),
and post-exposure admission includes day index zero
(`increment/_frame_panel.py:282-307`; `increment/_frame_moments.py:713-724`).
Thus an instant-equality comparison cannot be inferred from frame-panel rows;
the example reports the observed panel-versus-instant difference instead of
rewriting either path.

## Capability matrix

| Capability | Evidence path | Observed result and classification |
|---|---|---|
| Fixed cohort, sorted per-unit keys, no-event unit | Increment `from_unit_panel`/`unit_frame` versus Terrine's explicit roster | Match: both return `u1`–`u4`; `u4` has zero amount and count on both sides. |
| Amounts and event counts | Day-aligned common-support fixture | Match: post-period revenue 23/17/19/0 and order counts 3/1/1/0. Terrine returns exact decimals; increment returns floats. |
| Pre-period covariate | Constant per-unit covariate export versus a Terrine pre-period window | Match: 5/13/0/0. |
| Ratio components | `unit_frame` `y`/`y_den` versus Terrine's separate numerator/denominator measures | Match: (23, 6), (17, 4), (19, 5), (0, 0). |
| Intraday boundary | Noon t0 with an earlier same-day event and an equality event | Intentional information loss: increment's panel API buckets to date (`u1` = 23); Terrine's instant window gives 18, including the equality event and excluding the earlier same-day amount 5. |
| Strict raw timestamp boundary | Increment raw query source uses `>` for ordinary post-exposure stats and conditionally `>=` when `include_exposure` is true | Intentional semantic difference from Terrine's `>=` start, classified from installed source only. The builder is not exported publicly, so this public-API pilot does not execute it. |
| Fixed-window per-unit export | Windowed increment panel metric passed to `unit_frame` | Intentional public shape limitation: refusal `source.frame.unit_frame_panel` observed (`increment/frame.py:1294-1325`). `moments(..., grain="asof")` is aggregate, not a per-unit replacement. |
| Incomplete follow-up and source coverage | Terrine readiness on two sources, plus increment `observation_end` | Consistent: Terrine's 3-day outcome is null for every unit; increment total moments are empty because all four units are censored at `observation_end`. Increment's panel input has no per-source coverage contract. |

## Migration implications

The exercised compatible value shape is a fixed cohort of per-unit amounts,
counts, covariates, and ratio components when input has already been reduced to
a shared day support. It does not establish that increment's public panel
per-unit exporter executes an arbitrary fixed-duration window: the API rejects
windowed `unit_frame` exports, although the panel's aggregate moments path has
window/censoring support (`increment/frame.py:1287-1325, 1153-1279`).

A future shared engine would need to express temporal grain and boundary
inclusivity rather than implicitly bucket instants to days. It would also need
per-source coverage/completeness and incomplete-label reasons as first-class
state. Those are requirements for a future interface proposal, not
implementation work authorized by this pilot. The test/example do not measure
panel materialization at scale, dimensions, filters, winsorization, calendar
averages, DST, clustering, or causal estimands.

### Executed results

Python 3.13.5, increment 0.1.0a2, ibis-framework 12.0.0, DuckDB 1.5.6 and
PyArrow 23.0.1. `uv run pytest tests/parity/test_increment_frames.py -q`: 3 passed.
`uv run python examples/increment_parity.py` printed:

```text
increment 3-day total moments: [] (all units censored at the supplied observation_end)
u1: pre=Decimal('5'); revenue=Decimal('23'); orders=3; ratio=(23.0, 6.0); long_outcome=None
u2: pre=Decimal('13'); revenue=Decimal('17'); orders=1; ratio=(17.0, 4.0); long_outcome=None
u3: pre=Decimal('0'); revenue=Decimal('19'); orders=1; ratio=(19.0, 5.0); long_outcome=None
u4: pre=Decimal('0'); revenue=Decimal('0'); orders=0; ratio=(0.0, 0.0); long_outcome=None
day-grain boundary: increment u1 day-0 revenue=23.0; Terrine noon-to-next-day revenue=Decimal('18'); difference=Decimal('5.0')
```

Increment also warned that censoring dropped 4 of 4 enrolled units for the
3-day metric. No mismatch on matched support was classified as a bug.
