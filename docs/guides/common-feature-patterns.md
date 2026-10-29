# Common feature patterns

Start with the [first dataset](first-dataset.md) for `Model.from_tables` and
`tr.recipes`. These examples use `Model.from_catalog` because event labels,
filtered measures and as-of attributes need explicit source semantics. Each
snippet below runs in order after this local setup; no warehouse is required.

## Local setup

Install `terrine[duckdb,pandas]`. The roster includes `c`, who has no events.
Completeness is declared independently of those events; its dates are inclusive.

```python
from tempfile import TemporaryDirectory

import ibis
import terrine as tr

scratch = TemporaryDirectory(prefix="terrine-patterns-")
con = ibis.duckdb.connect(f"{scratch.name}/patterns.duckdb")
con.raw_sql("CREATE TABLE users AS SELECT * FROM (VALUES ('a'), ('b'), ('c')) t(user_id)")
con.raw_sql("""
    CREATE TABLE events AS SELECT * FROM (VALUES
        ('a', TIMESTAMP '2025-01-15 12:00:00', 'visit', 'bronze'),
        ('a', TIMESTAMP '2025-01-30 12:00:00', 'visit', 'gold'),
        ('a', TIMESTAMP '2025-02-02 00:00:00', 'purchase', 'gold'),
        ('a', TIMESTAMP '2025-02-02 00:30:00', 'visit', 'gold'),
        ('b', TIMESTAMP '2025-01-25 12:00:00', 'visit', 'silver')
    ) t(user_id, occurred_at, kind, tier)
""")
catalog = {
    "schema_version": 2, "dialect": "duckdb", "day_boundary": "UTC",
    "sources": {"events": {
        "relation": {"kind": "table", "name": "events"},
        "time": "occurred_at", "units": {"user": "user_id"},
        "events": ["activity"], "properties": ["kind", "tier"], "dimensions": [],
    }},
    "events": {"activity": {"source": "events", "column": None}},
    "properties": {
        name: {"source": "events", "column": name, "dtype": "string", "as_of": "event_time"}
        for name in ("kind", "tier")
    },
    "dimensions": {},
    "measures": {
        "activity_count": {"event": "activity", "aggregation": "count"},
        "purchase_count": {
            "event": "activity", "aggregation": "count",
            "filters": [{"property": "kind", "op": "equals", "values": ["purchase"]}],
        },
    },
    "metrics": {
        name: {"kind": "mean", "unit": "user", "measure": name}
        for name in ("activity_count", "purchase_count")
    },
}
model = tr.Model.from_catalog(
    catalog, con=con, units={"user": tr.UnitSource("users", "user_id")},
    readiness={"events": tr.Coverage("2025-01-01", "2025-03-31")},
)
```

All following rows use t0 = `2025-02-01 00:00:00 UTC`. Names in `features`,
`outcomes` and `eligibility` become output columns, each with adjacent
`__complete` and `__reason` columns. `to_pandas()` is a compact view;
[Arrow is the lossless hand-off](common-stack-interop.md#bounded-frames).
For large data, pass an explicit [output budget](cost-and-residency.md).

## Rolling windows

A dictionary comprehension creates independent trailing windows. The complete
empty count for `c` is zero, not a missing row.

```python
rolling = {
    f"activity_{days}d": tr.agg("activity_count", window=(f"-{days}d", "0s"))
    for days in (7, 14, 30)
}
rolling_rows = tr.recipes.time_series(
    "user", every="1d", start="2025-02-01", end="2025-02-01", features=rolling,
).build(model).to_pandas()
```

## Conditional events

Select a purchase-labeled event during follow-up, then measure activity relative
to its timestamp. This is an expression pattern, not another dataset constructor.
The interval includes the purchase event itself. No purchase means a null result
with reason `missing_selector_origin`, not zero activity after an invented origin.

```python
purchase = tr.event_time(
    "activity", window=("0s", "7d"), label_property="kind", equals="purchase",
)
conditional_rows = tr.recipes.time_series(
    "user", every="1d", start="2025-02-01", end="2025-02-01",
    outcomes={"activity_after_purchase": tr.agg(
        "activity_count", window=("0s", "1h"), relative_to=purchase,
    )},
).build(model).to_pandas()
```

See [event-time origins](temporal-engine.md#bounded-event-sequences-and-event-time-origins)
for nested selectors and timing validation.

## Active-before eligibility

Require activity strictly before t0. A complete zero is ineligible; unknown
eligibility refuses export rather than being treated as false.

```python
eligible_rows = tr.recipes.time_series(
    "user", every="1d", start="2025-02-01", end="2025-02-01",
    features=rolling,
    eligibility={"active_before": tr.gt(tr.agg("activity_count", window=("-30d", "0s")), 0)},
).build(model).to_pandas()
```

## Attributes at t0

`asof(..., before="0s")` selects the last known attribute strictly before t0,
not an arbitrary current value. The setup explicitly declares `tier` as an
event-time property through `Model.from_catalog`; `Model.from_tables` does not
infer attribute meaning. For authoritative validity intervals, declare
[SCD2 dimension history](temporal-attributes.md) in the Catalog instead.

```python
attribute_rows = tr.recipes.time_series(
    "user", every="1d", start="2025-02-01", end="2025-02-01",
    features={"tier_at_t0": tr.asof("tier", before="0s")},
).build(model).to_pandas()
```

## Recency from the last event

Use `occurrence="last"` for the most recent event in a bounded lookback.
Subtract its timestamp from t0 in the compact pandas view to obtain an elapsed
duration. Preserve the selector's support columns: no event is not zero recency.
This derived pandas column is not a new Terrine expression or saved definition.

```python
recency_rows = tr.recipes.time_series(
    "user", every="1d", start="2025-02-01", end="2025-02-01",
    features={"last_seen": tr.event_time(
        "activity", window=("-30d", "0s"), occurrence="last",
    )},
).build(model).to_pandas()
recency_rows["recency"] = recency_rows["t0"] - recency_rows["last_seen"]
```

## True/false labels

Predicates work as ordinary named outputs. Here conversion means at least one
purchase in `[t0, t0 + 7d)`; churn means no activity in `[t0, t0 + 30d)`.
These definitions are explicit rules, not inferred business meanings.
Incomplete follow-up remains null with its completeness reason, not false.

```python
label_rows = tr.recipes.time_series(
    "user", every="1d", start="2025-02-01", end="2025-02-01",
    outcomes={
        "converted_7d": tr.gt(tr.agg("purchase_count", window=("0s", "7d")), 0),
        "churned_30d": tr.eq(tr.agg("activity_count", window=("0s", "30d")), 0),
    },
).build(model).to_pandas()
```

## Close local resources

After running the snippets:

```python
con.disconnect()
scratch.cleanup()
```
