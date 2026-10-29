# Library hand-offs

Terrine constructs datasets, not causal estimates. Hand-offs preserve declared
column roles, observation completeness and provenance. A successful preflight
is not evidence of identification, independent censoring, model fit or valid
inference: those assumptions and methods belong to the downstream analyst/library.

## Installation and shared preflight

Each library has its own optional extra so a consumer does not install unrelated
methods. Base Terrine imports none of them. For the tested examples:

```sh
uv sync --group dev --extra duckdb --extra doubleml --extra dowhy --extra duckreg --extra lifelines --extra diff-diff
uv run pytest -q integration/handoffs
```

DoWhy 0.14 supports Python 3.13 but not 3.14; its extra is inactive on 3.14.
The other extras remain independently selectable. CI runs all five on 3.13.
Increment remains an independent optional [semantic adapter](increment-compatibility.md),
never a requirement for these workflows. sklearn/Arrow keeps the existing ML
staging boundary: these workflows are not a replacement for labeled training exports.

For local consumers, start with a verified `MaterializedFrame`:

```python
from terrine.export import preflight_export

# materialized = frame.materialize(budget)
checked = preflight_export(materialized, drop_incomplete=True)
print(dict(checked.excluded_counts))
```

Default behavior refuses incomplete outcomes. Explicit drop mode removes the
union of incomplete rows, never imputes or zero-fills, and reports excluded rows
per exact reason. A row is counted once per reason, even when outcomes share
completeness evidence; two distinct reasons may both count that row. Unknown
eligibility is always refused. Repeated rows per independent unit require an
existing cluster column. Grace-defined treatment cannot stand in for a baseline
treatment. These hazards use stable `terrine.export.*` codes across all hand-offs.
Select a complete analysis population or change the design rather than deleting
metadata to evade a refusal.

Local examples explicitly collect the checked rows into pandas. Retain the original
`MaterializedFrame` alongside the downstream object for role/provenance inspection;
consumer dataframes are not portable Terrine metadata containers.

## DoubleML: retain repeated-unit clusters

```python
from doubleml import DoubleMLData
from terrine.export import preflight_export

checked = preflight_export(materialized)
r = materialized.roles
consumer = DoubleMLData(
    checked.table.to_pandas(),
    y_col=r.outcomes[0],       # explicitly choose one outcome
    d_cols=r.treatment,
    x_cols=list(r.features),  # never infer predictors from support/identity columns
    cluster_cols=r.cluster_unit,
)
```

A repeated-row frame must declare the independent-unit cluster; pass that role to
`cluster_cols`, not an anchor/trial identifier. This example constructs the data
backend, not an estimator or a claim about valid nuisance learning. Declared weights
are not consumed by `DoubleMLData`; choosing an appropriately weighted downstream
method is a separate analyst decision, not a reason to silently discard the role.
The executable example checks repeated units and cluster identity.

## Catalog → a grid of trial panels → DoubleML

Reuse one catalog to build a dataset for each treatment/outcome pair. The pairs
and covariates are the analyst's choices, not causal relationships inferred by
Terrine. This example assumes `warehouse.duckdb` contains an authoritative
`customers(customer_id)` roster, `orders(customer_id, ordered_at, amount)`,
`sessions(customer_id, started_at)` and
`account_events(customer_id, occurred_at, upgrade, referral)`.

The account-event columns are **nullable labels**: set the corresponding label
to a non-null value when that event occurs and leave it null otherwise.
`Initiation` tests whether a label has ever been non-null, not whether a Boolean
is true; `False` is also non-null. Separate labels are essential: declaring two
row events with `column: null` on the same source would make both names select
every account-event row and produce identical treatments.

Save this as `semantic.yaml`:

```yaml
schema_version: 2
day_boundary: UTC
dialect: duckdb
sources:
  orders:
    relation: {kind: table, name: orders}
    time: ordered_at
    units: {customer: customer_id}
    events: [order, order_amount]
    properties: []
    dimensions: []
  sessions:
    relation: {kind: table, name: sessions}
    time: started_at
    units: {customer: customer_id}
    events: [session]
    properties: []
    dimensions: []
  account_events:
    relation: {kind: table, name: account_events}
    time: occurred_at
    units: {customer: customer_id}
    events: []
    properties: [upgrade, referral]
    dimensions: []
events:
  order: {source: orders, column: null}
  order_amount: {source: orders, column: amount}
  session: {source: sessions, column: null}
properties:
  upgrade: {source: account_events, column: upgrade, dtype: bool, as_of: event_time}
  referral: {source: account_events, column: referral, dtype: bool, as_of: event_time}
dimensions: {}
measures:
  order_count: {event: order, aggregation: count}
  net_revenue: {event: order_amount, aggregation: sum}
  session_count: {event: session, aggregation: count}
metrics:
  order_count: {kind: mean, unit: customer, measure: order_count}
  net_revenue: {kind: mean, unit: customer, measure: net_revenue}
  session_count: {kind: mean, unit: customer, measure: session_count}
```

Declare completeness from your loading guarantees, not the observed first/last
event. The single customer unit is inferred. The end date is inclusive, so the
weekly schedule includes March 31; covariates use the preceding 90 elapsed days
and each outcome the next 28 elapsed days.

```python
from datetime import UTC, datetime

import ibis
import terrine as tr
from doubleml import DoubleMLData
from terrine.export import preflight_export
from terrine.spec import Initiation

con = ibis.duckdb.connect("warehouse.duckdb")
try:
    model = tr.Model.from_catalog(
        "semantic.yaml", con=con,
        units={"customer": tr.UnitSource("customers", "customer_id")},
        readiness={name: tr.Coverage("2024-01-01", "2025-06-30")
                   for name in ("orders", "sessions", "account_events")},
    )
    covariates = {
        "orders_90d": tr.agg("order_count", window=("-90d", "0s")),
        "sessions_90d": tr.agg("session_count", window=("-90d", "0s")),
    }
    history_start = datetime(2024, 1, 1, tzinfo=UTC)
    grid = [
        ("upgrade", "net_revenue"), ("upgrade", "session_count"),
        ("referral", "net_revenue"), ("referral", "session_count"),
    ]
    budget = tr.Budget(max_rows=2_000_000, max_output_bytes=2_000_000_000)
    datasets = {}
    materialized_frames = {}
    for event, metric in grid:
        design = tr.recipes.trial_panel(
            every="7d", start="2025-01-06", end="2025-03-31",
            treatment=Initiation(event, history_start=history_start),
            features=covariates,
            outcomes={"y": tr.agg(metric, window=("0s", "28d"))},
        )
        materialized = design.build(model).materialize(budget)
        checked = preflight_export(materialized)
        r = materialized.roles
        materialized_frames[event, metric] = materialized
        datasets[event, metric] = DoubleMLData(
            checked.table.to_pandas(), y_col="y", d_cols=r.treatment,
            x_cols=list(r.features), cluster_cols=r.cluster_unit,
        )
finally:
    con.disconnect()
```

Choose event history and eligibility that leave both treated and control units
in the analysis population. A panel repeats units across trials, so
`cluster_cols` is the independent customer identity, not `trial_id`.
The executable counterpart,
`integration/handoffs/test_causal_grid.py`, builds all four pairs on tiny tables
and checks distinct treated populations, complete outcomes and cluster identity.
The loop is ordinary Python, not a Terrine helper: change the pairs to match
your study. DoWhy's preflight refuses repeated units even with a cluster role,
and every weight role; use a one-row-per-unit unweighted design for that library.
The `dowhy` extra is Python 3.13 only; `doubleml` has no Python 3.14 cap.
Neither preflight nor constructing `DoubleMLData` establishes identification or
valid inference, and `x_cols`/`common_causes` remain modeling choices.

## DoWhy: one row per unit, unweighted only

```python
from dowhy import CausalModel
from terrine.export import preflight_export

checked = preflight_export(materialized, consumer="dowhy")
r = materialized.roles
model = CausalModel(
    data=checked.table.select([r.treatment, *r.features, r.outcomes[0]]).to_pandas(),
    treatment=r.treatment,
    outcome=r.outcomes[0],
    common_causes=list(r.features),  # analyst-declared graph assumptions
)
identified = model.identify_effect()
```

The consumer-specific check refuses repeated units even with a cluster column
(`terrine.export.repeated_units_unsupported`) and every declared weight role
(`terrine.export.unsupported_weights`), including target/trial weights. Supply a
one-row-per-unit unweighted design, or choose a library which applies the required
weights/clustering. Do not pass them as extra `CausalModel` keywords: DoWhy stores
extra kwargs but does not apply them. `common_causes` is a modeling choice, not a
causal graph inferred by Terrine from a feature role. The tiny example exercises
identification, without estimating or comparing effects.

## duckreg: keep the rows in DuckDB

Use an already verified materialized frame table on its original Ibis connection,
with matching `roles`, `columns` and `provenance` retained by the producer. Do not
execute `frame.expr` directly: that bypasses source-integrity and budget checks.
`preflight_warehouse` checks export hazards, not the producer's materialization
obligations. It is a context manager that opens its own DuckDB snapshot transaction;
enter outside an existing transaction. Evidence and the downstream fit share that
snapshot, even if another connection changes the source after preflight.

```python
from duckreg import DuckRegression
from terrine.export import preflight_warehouse

with preflight_warehouse(
    con.table("materialized_frame"), roles=roles, columns=columns,
    provenance=provenance, drop_incomplete=True,
) as checked:
    con.create_view("checked_frame", checked.table)
    regression = DuckRegression(
        db_name=None, connection=con, table_name="checked_frame",
        formula="y ~ treatment + x",  # use reviewed identifiers from the roles
        cluster_col=roles.cluster_unit, seed=42, n_bootstraps=0,
    )
    regression.fit()
    print(dict(checked.excluded_counts))
```

Only aggregate reason/count evidence and duckreg's compressed sufficient statistics
leave the warehouse; no individual dataset rows are collected. The relation/view
must be consumed **inside** the context; exit rolls back temporary views and any
other writes inside it. Do not perform unrelated writes in the context. duckreg
receives the same caller-owned connection, not a reopened database or exported
pandas table. Compression groups by the right-hand-side values: high-cardinality
predictors can yield many aggregate groups; no bound on compression is implied.
The example disables bootstrap, so no unit-ID lists are collected. It does not apply
declared weight roles; weighted method selection is explicitly downstream.

## lifelines: distinguish KM arrays from Cox predictors

Start from `TimeToEvent` per-unit records, not a general outcome dataframe:

```python
from lifelines import KaplanMeierFitter, CoxPHFitter
from terrine.export import to_lifelines_km, to_lifelines_cox

km = to_lifelines_km(survival_records, drop_incomplete=True)
curve = KaplanMeierFitter().fit(
    km.durations, event_observed=km.event_observed, entry=km.entry, weights=km.weights,
)

cox = to_lifelines_cox(survival_records)
model = CoxPHFitter().fit(
    cox.df, duration_col=cox.duration_col, event_col=cox.event_col,
    entry_col=cox.entry_col, weights_col=cox.weights_col, cluster_col=cox.cluster_col,
)
```

Terrine's `duration` field is an **elapsed Arrow duration from `t0`**. The
lifelines helper consumes it directly and computes `(entry_time - t0)` in
elapsed seconds, subtracting integer timestamps before float conversion. Valid
records satisfy `0 <= entry <= duration`; Cox additionally requires strict
`entry < duration`. Conversion rounds elapsed seconds, not epoch timestamps; it
introduces no statistical error model. The unit test pins nanosecond-scale
elapsed differences at modern calendar dates.

The KM result carries only durations, event flags, entry and declared row weights,
plus immutable exclusion counts. KM's fit interface has no cluster argument:
retaining repeated-unit cluster metadata in the source does not give this fit
cluster-adjusted inference. Weight meaning and censoring assumptions remain the
analyst's responsibility. Cox maps the declared weight and cluster roles to
`weights_col` and `cluster_col`. Its dataframe contains **only declared features**
plus assigned duration/event/entry/weight/cluster fields. Treatment is not
automatically a predictor; neither are unit IDs, other outcomes, event timestamps,
or completeness/support columns. Choose covariates deliberately in the design.

For a single-group `LifeTable` **without delayed entry**, `to_lifelines_km` instead
returns compressed event and censor observations at `time_bin`, with frequency
weights; `entry` is `None`. This is coarsened bin-index time, not the original
event time or seconds. It has the same event/censor count mapping as lifelines'
`survival_events_from_table`; no individual rows are reconstructed. Apply the
helper separately to a selected group, retaining its metadata. Multiple pooled
groups or any delayed entries are refused with a route to per-unit records
(`terrine.lifelines.unsupported_shape` / `terrine.lifelines.delayed_entry`).

Risk-set counts carry completeness evidence and exact reason lists for their
group's trajectory. Group IDs repeated across bins are aggregate grain, not
repeated independent subjects, and do not require a fictitious cluster role.
Incomplete risk trajectories are refused even with `drop_incomplete=True`:
dropping bins cannot repair unknown history. Drop incomplete **per-unit records**
first and reconstruct the risk set instead. Ordinary right-censoring with known
follow-up is not an incomplete record and is retained. Cox always requires the
per-unit records, never the count table.

## diff-diff: encode observed first adoption, not initiation flags

```python
from diff_diff import CallawaySantAnna, SunAbraham
from terrine.export import to_diff_diff

panel = to_diff_diff(panel_records, period_col="t0", period_seconds=86400)
arguments = dict(
    outcome=panel_records.roles.outcomes[0],
    unit=panel.unit_col, time=panel.time_col, first_treat=panel.first_treat_col,
)
cs = CallawaySantAnna(cluster=panel_records.roles.cluster_unit).fit(panel.df, **arguments)
event_study = cs.aggregate("event_study").to_dataframe()
sa = SunAbraham(cluster=panel_records.roles.cluster_unit).fit(panel.df, **arguments)
sa_event_study = sa.event_study_effects
```

Only Callaway–Sant'Anna, Sun–Abraham and their event studies are covered here.
The helper prepares data, not an estimator or causal identification gate.

`first_treat` is constant within unit: first **observed** persistent-adoption
period, or `0` for units untreated throughout the supplied history (diff-diff's
never-treated encoding). This is not a lifetime never-treated claim. Validate
sufficient history for your study. An initiation indicator that turns back off is
not persistent adoption; treatment reversals, nonbinary/null treatment and duplicate
unit-period rows are refused rather than repaired.

The generated `__terrine_period` is positive so treatment in original period zero
does not collide with the never-treated sentinel. Integer periods shift by a
constant, preserving gaps. Timestamp/date periods require `period_seconds` or a
retained panel cadence, and must align exactly to it. For example, source ticks
`[-1, 0, 2]` encode as `[1, 2, 4]`, not `[1, 2, 3]`. This remains true after
outcome exclusions. The immutable `period_mapping` records source values and codes.
Keep calendar spacing, reference periods and anticipation choices consistent with
the downstream method; a missing calendar period is not silently compressed away.

Shared preflight runs first. The helper then validates treatment history and derives
cohorts from the **original** supplied records, before applying its checked outcome
projection. Dropping the first-adoption row or every treated row cannot relabel a
unit as never-treated, nor hide a reversal. It preserves emitted row order and
refuses pre-existing output-name collisions. `excluded_counts` retains exact reasons.
The helper returns the other source columns unchanged; explicitly select outcomes
and covariates for the downstream method. Weight roles are retained in the source,
not silently interpreted as diff-diff survey weights by this example.

## API sources and executed examples

- [DoubleMLData arguments](https://docs.doubleml.org/stable/api/generated/doubleml.data.DoubleMLData.html)
- [DoWhy CausalModel source](https://github.com/py-why/dowhy/blob/main/dowhy/causal_model.py)
- [duckreg connection and compression contracts](https://github.com/py-econometrics/duckreg)
- [lifelines survival table conversion](https://lifelines.readthedocs.io/en/latest/lifelines.utils.html#lifelines.utils.survival_events_from_table)
- [lifelines Cox fit arguments](https://lifelines.readthedocs.io/en/latest/fitters/regression/CoxPHFitter.html)
- [diff-diff staggered treatment and never-treated convention](https://diff-diff.readthedocs.io/en/stable/tutorials/02_staggered_did.html)
- [Callaway–Sant'Anna and event-study aggregation](https://diff-diff.readthedocs.io/en/stable/api/_autosummary/diff_diff.CallawaySantAnna.html)
- Executable counterparts: `integration/handoffs/`; these exercise real libraries,
  skip only if that library is absent, and make no estimate-parity claims.
