# terrine

Terrine turns event tables and an explicit unit roster into analytical and ML datasets with point-in-time features and future outcomes.
You declare what each row means, its time zero and source completeness; Terrine compiles the dataset and preserves the difference between observed zero and unknown.

The example below produces these value columns (`anchor_id` and `t0` omitted here):

| unit | before | before__complete | before__reason | after | after__complete | after__reason |
| --- | ---: | --- | --- | ---: | --- | --- |
| a | 1 | true | null | 2 | true | null |
| b | 0 | true | null | 0 | true | null |

## Install

Requires CPython 3.13 or 3.14. Install the DuckDB backend and pandas for this example:

```sh
uv pip install 'terrine[duckdb,pandas]'
```

Use `terrine[duckdb]` for Arrow-only workflows, or add the `polars` extra for Polars.

## Build a study dataset

Run this locally: the temporary DuckDB database contains two users, including `b` with no events. Nothing is uploaded.

<!-- --8<-- [start:first-dataset] -->
```python
from tempfile import TemporaryDirectory
import ibis
import terrine as tr

with TemporaryDirectory(prefix="terrine-first-") as directory:
    con = ibis.duckdb.connect(f"{directory}/analytics.duckdb")
    try:
        con.raw_sql("CREATE TABLE users AS SELECT * FROM (VALUES ('a'), ('b')) t(user_id)")
        con.raw_sql("CREATE TABLE events AS SELECT 'a' AS user_id, unnest([TIMESTAMP '2025-01-01 12:00:00', TIMESTAMP '2025-01-02 00:00:00', TIMESTAMP '2025-01-02 12:00:00']) AS occurred_at")
        model = tr.Model.from_tables(
            con, units={"user": tr.UnitSource("users", "user_id")},
            events={"sessions": tr.Events("events", "occurred_at", {"user": "user_id"}, complete=("2025-01-01", "2025-01-03"))},
        )
        design = tr.recipes.time_series(
            "user", every="1d", start="2025-01-02", end="2025-01-03",
            features={"before": tr.agg("sessions_count", window=("-1d", "0s"))},
            outcomes={"after": tr.agg("sessions_count", window=("0s", "1d"))},
        )
        data = design.build(model).to_pandas()
        print(data.sort_values("unit").to_string(index=False))
    finally:
        con.disconnect()
```
<!-- --8<-- [end:first-dataset] -->

**Model → Design → Frame → Dataset:** bind tables, describe rows, build lazily, then collect or optionally stage a pinned generation.
The event at midnight belongs to `after`, not `before`. The compact pandas view keeps each value's `__complete` / `__reason` columns; [Arrow retains all observation metadata](https://github.com/kylejcaron/terrine/blob/main/docs/guides/common-stack-interop.md#bounded-frames).
See [your first dataset](https://github.com/kylejcaron/terrine/blob/main/docs/guides/first-dataset.md) for budgets, refusals and staging.

## Datasets you can build

Each `tr.recipes` constructor returns an ordinary, editable `Design`; the [low-level API](https://github.com/kylejcaron/terrine/blob/main/docs/guides/public-api.md) remains available.

| Dataset | What a row represents |
| --- | --- |
| [Time series](https://github.com/kylejcaron/terrine/blob/main/docs/guides/temporal-engine.md#scheduled-time-series) | A roster unit at each scheduled anchor. |
| [Trial panels](https://github.com/kylejcaron/terrine/blob/main/docs/guides/sequential-panels.md#scheduled-trial-panels) | An eligible unit at each trial time zero. |
| [Event panels](https://github.com/kylejcaron/terrine/blob/main/docs/guides/sequential-panels.md#event-panels) | A selected event and relative period. |
| [Conditional events](https://github.com/kylejcaron/terrine/blob/main/docs/guides/common-feature-patterns.md#conditional-events) | An expression evaluated relative to a label-filtered event, not a separate constructor. |
| [LTV / cohorts](https://github.com/kylejcaron/terrine/blob/main/docs/guides/trajectories-survival.md#cumulative-ltv) | A unit's cumulative value at an age since cohort entry. |
| [Survival / life tables](https://github.com/kylejcaron/terrine/blob/main/docs/guides/trajectories-survival.md#survival-records) | A unit's elapsed event/censor duration, or a group's risk interval. |
| [State panels](https://github.com/kylejcaron/terrine/blob/main/docs/guides/state-panels.md) | A unit-period transition from prior to response state. |
| [Event sequences](https://github.com/kylejcaron/terrine/blob/main/docs/guides/temporal-engine.md#bounded-event-sequences-and-event-time-origins) | An event anchor with a bounded, ordered sequence. |

[Common feature patterns](https://github.com/kylejcaron/terrine/blob/main/docs/guides/common-feature-patterns.md): rolling windows, active-before eligibility, attributes at t0, recency and true/false labels.

## Five rules

1. **Choose t0.** Every row has an explicit time zero: fixed, scheduled or event-based.
2. **Use half-open windows.** `[start, end)` includes the start, excludes the end; offsets are elapsed durations, not calendar dates.
3. **Features before, outcomes after.** Features and eligibility cannot look beyond the pre-t0 cutoff; outcomes describe follow-up.
4. **Declare completeness.** `Model.from_tables` requires `complete=` on every `Events` descriptor; with `Model.from_catalog`, only sources selected by the Design need `Coverage`.
   Both declare complete local days, never inferred from the latest event.
5. **Null is not zero.** A complete empty count is zero; unavailable values stay null with `__reason`. Unknown eligibility refuses export rather than silently dropping a row.

## Supported warehouses

| Backend | Dataset execution | Staging / publication |
| --- | --- | --- |
| DuckDB | Reference backend; local tables | Supported |
| PostgreSQL 16 | Direct Frame materialization | DuckDB only |
| BigQuery | Direct Frame materialization | DuckDB only |
| Snowflake | Direct Frame materialization | DuckDB only |
| Other Ibis backends | Read-only, unverified; needs `allow_unverified=True` per Model | Refused |

See [backend compatibility](https://github.com/kylejcaron/terrine/blob/main/docs/guides/backend-compatibility.md) for extras, consistent-read semantics, supported operations, precision, the unverified opt-in tier and executable warehouse examples.

## Guides

- [All guides](https://github.com/kylejcaron/terrine/blob/main/docs/guides/index.md) · [Capabilities](https://github.com/kylejcaron/terrine/blob/main/docs/guides/capabilities.md) · [Inherent limits](https://github.com/kylejcaron/terrine/blob/main/docs/guides/limitations.md) · [Refusal codes](https://github.com/kylejcaron/terrine/blob/main/docs/reference/refusals.md)
- [Training/scoring and saved dataset definitions](https://github.com/kylejcaron/terrine/blob/main/docs/guides/ml-staging.md) · [Library hand-offs](https://github.com/kylejcaron/terrine/blob/main/docs/guides/library-handoffs.md) (Terrine makes no causal estimates or identification claims)
- [Durable runtime and saved plans](https://github.com/kylejcaron/terrine/blob/main/docs/guides/materialization-runtime.md) · [Owned inputs](https://github.com/kylejcaron/terrine/blob/main/docs/guides/materialization-runtime.md#owned-inputs-for-repeated-builds)
- [Restricted dataset tools](https://github.com/kylejcaron/terrine/blob/main/docs/guides/dataset-agent-tools.md) · [Airflow, Dagster and dbt](https://github.com/kylejcaron/terrine/blob/main/docs/guides/orchestrators.md)
- [Cost and data residency](https://github.com/kylejcaron/terrine/blob/main/docs/guides/cost-and-residency.md) · [Synthetic worlds with independent expected rows](https://github.com/kylejcaron/terrine/blob/main/docs/guides/synthetic-worlds.md)

Contributing? See [CONTRIBUTING.md](https://github.com/kylejcaron/terrine/blob/main/CONTRIBUTING.md) for setup, test tiers and the Contributor License Agreement.

## License

[Apache License 2.0](https://github.com/kylejcaron/terrine/blob/main/LICENSE); see [NOTICE](https://github.com/kylejcaron/terrine/blob/main/NOTICE).
