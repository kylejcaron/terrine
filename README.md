# terrine

> [!WARNING]
> Terrine is an experimental idea for simplifying dataset curation. It is pre-alpha: the API will change without notice, and it is not ready for production use.

**Build datasets directly from event logs.**

You have event tables: orders, sessions, payments. You want a modeling table, for example one row per customer per snapshot date, with features computed from the past and labels from the future. Terrine builds that table in your warehouse (DuckDB, PostgreSQL, BigQuery, Snowflake, and more) from a few lines of Python.

## Why Terrine?

Most data science datasets come in a few shapes:

- snapshots with features and labels;
- cohort curves;
- time-to-event tables;
- before-and-after windows around an event.

Each time, the SQL is the same pile of date joins and window boundaries, rewritten by hand. Terrine takes the shape as a starting point. You name the rows you want and the windows to aggregate over; Terrine writes and runs the queries.

Because Terrine knows each row's snapshot time, features can only look backwards and targets only forwards. Each value also says whether its window was fully covered by your data, so you decide how to treat gaps for your problem.

## Example: a customer spend forecasting table

Requires Python 3.13 or 3.14.

```sh
pip install --pre "terrine[duckdb,pandas]"
```

<!-- --8<-- [start:first-dataset] -->
```python
import ibis
import pandas as pd
import terrine as tr

customers = pd.DataFrame({"customer_id": ["alice", "bob", "carol"]})
orders = pd.DataFrame({
    "customer_id": ["alice", "alice", "alice", "alice", "bob"],
    "ordered_at": pd.to_datetime(
        ["2025-01-03", "2025-01-20", "2025-02-10", "2025-03-05", "2025-01-05"]
    ),
    "amount": [30.0, 45.0, 25.0, 60.0, 80.0],
})

con = ibis.duckdb.connect()  # or your warehouse: ibis.bigquery.connect(...), ...
con.create_table("customers", customers)
con.create_table("orders", orders)

# Tell Terrine who the rows are about and which dates the orders table covers.
model = tr.Model.from_tables(
    con,
    units={"customer": tr.UnitSource("customers", "customer_id")},
    events={"orders": tr.Events(
        "orders", "ordered_at", {"customer": "customer_id"},
        values=("amount",), complete=("2025-01-01", "2025-03-20"),
    )},
)

# One row per customer every 28 days: the last 30 days as features,
# spend in the next 30 days as the target.
design = tr.recipes.time_series(
    "customer", every="28d", start="2025-02-01", end="2025-03-01",
    features={
        "orders_30d": tr.agg("orders_count", window=("-30d", "0s")),
        "spend_30d": tr.agg("orders_amount_sum", window=("-30d", "0s")),
    },
    outcomes={
        "spend_next_30d": tr.agg("orders_amount_sum", window=("0s", "30d")),
    },
)

data = design.build(model).to_pandas()
columns = ["unit", "t0", "orders_30d", "spend_30d", "spend_next_30d", "spend_next_30d__reason"]
print(data[columns].assign(t0=data["t0"].dt.date).sort_values(["t0", "unit"]).to_string(index=False))
```
<!-- --8<-- [end:first-dataset] -->

```text
 unit         t0  orders_30d  spend_30d  spend_next_30d spend_next_30d__reason
alice 2025-02-01           2       75.0            25.0                   <NA>
  bob 2025-02-01           1       80.0             0.0                   <NA>
carol 2025-02-01           0        0.0             0.0                   <NA>
alice 2025-03-01           1       25.0            <NA>    incomplete_coverage
  bob 2025-03-01           0        0.0            <NA>    incomplete_coverage
carol 2025-03-01           0        0.0            <NA>    incomplete_coverage
```

(Only some columns are printed; `t0` is shown as a date.)

Reading the output:

- **Carol has no orders.** She still has a row at each snapshot, with zero counts.
- **Alice's February 10 order** is in her February 1 target and her March 1 features. The same order is the future for one row and the past for the next.
- **The March targets are unknown.** Their 30-day window runs past March 20, where the orders data ends. Drop them, keep them for scoring, or handle them however suits your model.

Terrine names metrics after your table and columns: `orders_count`, `orders_amount_sum` and `orders_amount_mean`. Every value has a `<name>__complete` flag and a `<name>__reason` column, so you can filter or impute deliberately.

## The four ideas

| Idea | In the example |
| --- | --- |
| **Unit:** who each row is about, read from a table so nobody silently drops out | `customers.customer_id` |
| **t0:** the snapshot time of each row | every 28 days from 2025-02-01 through 2025-03-01, both days included |
| **Windows:** relative to t0, start included, end excluded. Features must end at or before t0; outcomes must start at or after t0 | `("-30d", "0s")` is the 30 days before t0; `("0s", "30d")` is the 30 days after |
| **Completeness:** the calendar days, inclusive, that you declare fully loaded for each event table (not just the first and last event you happen to have). Windows outside them are unknown, not zero | `complete=("2025-01-01", "2025-03-20")` |

## Other datasets

`tr.recipes` covers the usual shapes. Each one returns a normal `Design` you can edit.

| You want | Recipe | Guide |
| --- | --- | --- |
| Forecasting, propensity and other features-plus-target tables | `time_series` | [Scheduled time series](https://github.com/kylejcaron/terrine/blob/main/docs/guides/temporal-engine.md#scheduled-time-series) |
| Customer lifetime value curves by cohort | `ltv` | [Cumulative LTV](https://github.com/kylejcaron/terrine/blob/main/docs/guides/trajectories-survival.md#cumulative-ltv) |
| Time-to-event / survival analysis | `survival`, `life_table` | [Survival records](https://github.com/kylejcaron/terrine/blob/main/docs/guides/trajectories-survival.md#survival-records) |
| Behavior before and after an event (event study) | `event_panel` | [Event panels](https://github.com/kylejcaron/terrine/blob/main/docs/guides/sequential-panels.md#event-panels) |
| Repeated eligibility for observational "trials" | `trial_panel` | [Trial panels](https://github.com/kylejcaron/terrine/blob/main/docs/guides/sequential-panels.md#scheduled-trial-panels) |
| State changes such as plan upgrades and downgrades | `state_panel` | [State panels](https://github.com/kylejcaron/terrine/blob/main/docs/guides/state-panels.md) |
| Ordered events in a window after an event, up to a set count | `event_sequence` | [Event sequences](https://github.com/kylejcaron/terrine/blob/main/docs/guides/temporal-engine.md#bounded-event-sequences-and-event-time-origins) |

More feature recipes are in [common feature patterns](https://github.com/kylejcaron/terrine/blob/main/docs/guides/common-feature-patterns.md): recency, "active in the last N days" filters, attributes as of t0, conversions after a purchase, and true/false labels.

## Where it runs

Queries run where your data lives; nothing is uploaded. Results come back as pandas, Polars or Arrow.

| Backend | Status |
| --- | --- |
| DuckDB | Reference backend; can also save versioned datasets |
| PostgreSQL 16, BigQuery, Snowflake | Verified against DuckDB; floating-point results match within documented tolerances |
| Other Ibis backends | Read-only, unverified; opt in with `allow_unverified=True` |

See [backend compatibility](https://github.com/kylejcaron/terrine/blob/main/docs/guides/backend-compatibility.md) for extras and details.

## Learn more

- [Your first dataset](https://github.com/kylejcaron/terrine/blob/main/docs/guides/first-dataset.md): this example step by step, with output limits and saved datasets.
- [Training and scoring sets](https://github.com/kylejcaron/terrine/blob/main/docs/guides/ml-staging.md): reproducible train/score tables and saved definitions.
- [Library hand-offs](https://github.com/kylejcaron/terrine/blob/main/docs/guides/library-handoffs.md): pass datasets to modeling libraries. Terrine builds data and makes no causal claims.
- [All guides](https://github.com/kylejcaron/terrine/blob/main/docs/guides/index.md), [limitations](https://github.com/kylejcaron/terrine/blob/main/docs/guides/limitations.md) and [error codes](https://github.com/kylejcaron/terrine/blob/main/docs/reference/refusals.md).

Contributing? See [CONTRIBUTING.md](https://github.com/kylejcaron/terrine/blob/main/CONTRIBUTING.md).

## License

[Apache License 2.0](https://github.com/kylejcaron/terrine/blob/main/LICENSE); see [NOTICE](https://github.com/kylejcaron/terrine/blob/main/NOTICE).
