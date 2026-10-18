# Bounded state panels

`StatePanel` builds repeated unit-period rows from a roster-backed `Schedule`. It is a dataset shape, not an estimator: ordered states remain labels, and persistence or occupancy summaries are choices for downstream analysis. The builder does not fit a MOST estimator, infer causal identification, or produce expected future engaged periods.

## Binned metric states

A numeric metric can be mapped to explicit right-closed bins. With cutpoints `(0, 3, 9)`, the states are `(-inf, 0]`, `(0, 3]`, `(3, 9]`, and `(9, inf)`. Keep the level order declared; it is not lexical order and does not imply equal spacing.

```python
import terrine as tr
from terrine.spec import BinnedMetricState, Design, Schedule, StatePanel, agg

model = tr.Model.from_catalog(
    "semantic.yaml",
    con=con,
    readiness={"product_events": tr.Coverage("2025-01-01", "2025-04-01")},
    units={"user": tr.UnitSource("users", "user_id")},
)
design = Design(
    unit="user",
    anchor=Schedule("1w", "2025-01-06", "2025-03-31"),
    features={"past_sessions": agg("weekly_sessions", window=("-4w", "0d"))},
    shape=StatePanel(
        BinnedMetricState(
            "weekly_sessions",
            cutpoints=(0, 3, 9),
            levels=("inactive", "occasional", "regular", "power"),
        ),
        max_periods=12,
        baseline_features={"baseline_sessions": agg("weekly_sessions", window=("-1w", "0d"))},
    ),
)
materialized = design.build(model).materialize(
    tr.Budget(max_rows=100_000, max_output_bytes=128_000_000)
)
```

Each period value is reused as one transition's response and its successor's prior state. Missing or ambiguous values remain null with per-side completeness and reason columns; a certified Float64 interval that crosses a cutpoint is reported as `ambiguous_state_bin`. Cutpoints keep their exact integer, Decimal, or Float64 representation in the safe design format. Binned state windows preserve the metric's aggregation and readiness semantics.

## Point-observed states

`StateObservations` reads the declared string property only at its exact source timestamp; it never carries a prior value forward. Missing assessments remain missing on both adjacent transitions. A known-null assessment is complete but has no usable state label. Readiness and unit observation bounds are half-open, so the exclusive endpoint is unavailable.

```python
from terrine.spec import Design, Schedule, StateObservations, StatePanel

status_panel = Design(
    unit="user",
    anchor=Schedule("1w", "2025-01-06", "2025-03-31"),
    shape=StatePanel(
        StateObservations(
            "status",
            levels=("active", "closed"),
            absorbing=("closed",),
        ),
        max_periods=12,
    ),
)
```

The first usable `closed` point ends one incoming transition at its actual time and removes all later rows for that unit. An off-grid absorbing assessment is allowed; other off-grid assessments within the requested panel refuse. Repeated terminal padding is redundant, while a later usable different state refuses. The terminal scan uses the declared history and runs before anchor-range/eligibility filtering. `from_state` is a conditioning state observed at the origin, not a strictly pre-origin feature. Ordered levels and point-time references are retained in Arrow metadata.

State panels expand only the declared schedule and require positive `max_rows` and `max_output_bytes` limits for materialization. These limits bound output, not upstream scan work; inspect strict scan-budget policy separately. A panel with twelve weekly periods requests twelve transitions per eligible roster member, not a hidden daily calendar.

Arrow metadata preserves ordered levels and prior-state timing. Export consumers should construct ordered categorical targets explicitly; do not infer order from strings or dictionary codes. Future terminal flags and realized timing columns are structural outputs, not automatic predictors.

State-panel datasets are motivated by transition and ordinal analysis, but assumptions remain external: see [Harrell's 2024 talk](https://www.fharrell.com/talk/icsa/index.html), the [transition and occupancy explanation](https://hbiostat.org/talks/ordmarkov2.html), and the [worked modeling chapter](https://hbiostat.org/rmsc/markov). These references are not Terrine dependencies or guarantees about an analysis.
