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
staging boundary: these recipes are not a replacement for labeled training exports.

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

Local recipes explicitly collect the checked rows into pandas. Retain the original
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
`cluster_cols`, not an anchor/trial identifier. This recipe constructs the data
backend, not an estimator or a claim about valid nuisance learning. Declared weights
are not consumed by `DoubleMLData`; choosing an appropriately weighted downstream
method is a separate analyst decision, not a reason to silently discard the role.
The executable example checks repeated units and cluster identity.

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
The recipe disables bootstrap, so no unit-ID lists are collected. It does not apply
declared weight roles; weighted method selection is explicitly downstream.

## API sources and executed examples

- [DoubleMLData arguments](https://docs.doubleml.org/stable/api/generated/doubleml.data.DoubleMLData.html)
- [DoWhy CausalModel source](https://github.com/py-why/dowhy/blob/main/dowhy/causal_model.py)
- [duckreg connection and compression contracts](https://github.com/py-econometrics/duckreg)
- Executable counterparts: `integration/handoffs/`; these exercise real libraries,
  skip only if that library is absent, and make no estimate-parity claims.
