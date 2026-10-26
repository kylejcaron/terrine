# Bounded state panels

Start with `recipes.state_panel` for repeated unit-period transitions from a
roster-backed schedule. It returns an ordinary Design, not an estimator: ordered
states remain labels, and persistence or occupancy summaries are choices for
downstream analysis. The builder does not fit a MOST estimator, infer causal
identification, or produce expected future engaged periods.

Required arguments are `unit`, `state_source` (`StateObservations` or
`BinnedMetricState`), `every`, `start`, exclusive `end`, and positive `periods`.
The schedule defines candidate origins; `periods` bounds transitions. Dates are
calendar dates or ISO date strings, never timestamps. Supported options are
`features`, `eligibility`, `anchor_range`, and `gap="0d"`; other optional inputs
default to `None`. Arbitrary `outcomes`, treatment, and administrative cutoffs
are not supported. Run `uv run python examples/state_panel.py` for binned and
point-observed recipe datasets with Arrow round-trip and terminal-row assertions.

## Binned metric states

A numeric metric can be mapped to explicit right-closed bins. With cutpoints `(0, 3, 9)`, the states are `(-inf, 0]`, `(0, 3]`, `(3, 9]`, and `(9, inf)`. Keep the level order declared; it is not lexical order and does not imply equal spacing.

```python
import terrine as tr
from terrine import recipes
from terrine.spec import BinnedMetricState, agg

model = tr.Model.from_catalog(
    "semantic.yaml",
    con=con,
    readiness={"product_events": tr.Coverage("2025-01-01", "2025-04-01")},
    units={"user": tr.UnitSource("users", "user_id")},
)
state = BinnedMetricState(
    "weekly_sessions",
    cutpoints=(0, 3, 9),
    levels=("inactive", "occasional", "regular", "power"),
)
features = {"past_sessions": agg("weekly_sessions", window=("-4w", "0d"))}
design = recipes.state_panel(
    "user", state_source=state, every="1w", start="2025-01-06",
    end="2025-03-31", periods=12, features=features,
)
materialized = design.build(model).materialize(
    tr.Budget(max_rows=100_000, max_output_bytes=128_000_000)
)
```

### Under the hood

```python
from terrine.spec import Design, Schedule, StatePanel

equivalent = Design(
    "user", Schedule("1w", "2025-01-06", "2025-03-31"),
    features=features, shape=StatePanel(state, max_periods=12),
)
assert design == equivalent
```

For additional shape-specific controls, use the low-level `StatePanel` directly;
for example, its `baseline_features` evaluates separate baseline expressions.
The recipe exposes only the supported common row and feature options.

Each period value is reused as one transition's response and its successor's prior state. Missing values remain null with per-side completeness and reason columns. A Float64 metric is binned by its rounded value, so a value equal to a cutpoint belongs to the lower bin. Cutpoints keep their exact integer, Decimal, or Float64 representation in the safe design format. Binned state windows preserve the metric's aggregation and readiness semantics.

## Point-observed states

`StateObservations` reads the declared string property only at its exact source timestamp; it never carries a prior value forward. Missing assessments remain missing on both adjacent transitions. A known-null assessment is complete but has no usable state label. Readiness and unit observation bounds are half-open, so the exclusive endpoint is unavailable.

```python
from terrine import StateObservations, recipes

status = StateObservations("status", levels=("active", "closed"), absorbing=("closed",))
status_panel = recipes.state_panel(
    "user", state_source=status, every="1w", start="2025-01-06",
    end="2025-03-31", periods=12,
)
```

### Under the hood

```python
equivalent_status = Design(
    "user", Schedule("1w", "2025-01-06", "2025-03-31"),
    shape=StatePanel(status, max_periods=12),
)
assert status_panel == equivalent_status
```

The first usable `closed` point ends one incoming transition at its actual time and removes all later rows for that unit. An off-grid absorbing assessment is allowed; other off-grid assessments within the requested panel refuse. Repeated terminal padding is redundant, while a later usable different state refuses. The terminal scan uses the declared history and runs before anchor-range/eligibility filtering. `from_state` is a conditioning state observed at the origin, not a strictly pre-origin feature. Ordered levels and point-time references are retained in Arrow metadata.

State panels expand only the declared schedule and require positive `max_rows` and `max_output_bytes` limits for materialization. These limits bound output, not upstream scan work; inspect strict scan-budget policy separately. A panel with twelve weekly periods requests twelve transitions per eligible roster member, not a hidden daily calendar.

Arrow metadata preserves ordered levels and prior-state timing. Export consumers should construct ordered categorical targets explicitly; do not infer order from strings or dictionary codes. Future terminal flags and realized timing columns are structural outputs, not automatic predictors.

State-panel datasets are motivated by transition and ordinal analysis, but assumptions remain external: see [Harrell's 2024 talk](https://www.fharrell.com/talk/icsa/index.html), the [transition and occupancy explanation](https://hbiostat.org/talks/ordmarkov2.html), and the [worked modeling chapter](https://hbiostat.org/rmsc/markov). These references are not Terrine dependencies or guarantees about an analysis.
