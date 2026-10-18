"""Local DuckDB walkthrough for cohort collection and survival backtests."""

from datetime import UTC, datetime

import ibis
import pyarrow as pa

from terrine.data import (
    Budget,
    ColumnMeta,
    Coverage,
    ExecutionSnapshot,
    FeatureCutoff,
    MaterializedFrame,
    Provenance,
    Roles,
    UnitSource,
)
from terrine.errors import TerrineError
from terrine.estimate.survival import backtest_survival, to_surrogate_arrow
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import CohortSpec, Design, EventAnchor
from terrine.spec.trajectory import CohortGrouping, Trajectory


def _model():
    con = ibis.duckdb.connect()
    con.raw_sql("""
        CREATE TABLE events AS SELECT * FROM (VALUES
            ('u1', TIMESTAMPTZ '2025-01-01 00:00:00+00', 'purchase', 0::BIGINT),
            ('u1', TIMESTAMPTZ '2025-01-03 00:00:00+00', 'revenue', 10::BIGINT),
            ('u1', TIMESTAMPTZ '2025-01-10 00:00:00+00', 'revenue', 5::BIGINT),
            ('u2', TIMESTAMPTZ '2025-01-01 00:00:00+00', 'purchase', 0::BIGINT),
            ('u3', TIMESTAMPTZ '2025-01-15 00:00:00+00', 'purchase', 0::BIGINT)
        ) AS e(user_id, ts, kind, amount)
    """)
    con.raw_sql("CREATE TABLE users AS SELECT * FROM (VALUES ('u1'), ('u2'), ('u3')) AS u(id)")
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["purchase", "revenue"],
                    "properties": ["kind"],
                    "dimensions": [],
                }
            },
            "facts": {
                "purchase": {"source": "events", "column": None},
                "revenue": {"source": "events", "column": "amount"},
            },
            "properties": {
                "kind": {
                    "source": "events",
                    "column": "kind",
                    "dtype": "string",
                    "as_of": "event_time",
                }
            },
            "dimensions": {},
            "measures": {"net_revenue_sum": {"fact": "revenue", "aggregation": "sum"}},
            "metrics": {
                "net_revenue": {
                    "kind": "mean",
                    "entity": "user",
                    "measure": "net_revenue_sum",
                }
            },
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )
    return con, model


def _survival_data() -> MaterializedFrame:
    table = pa.table(
        {
            "user": pa.array(["u1", "u2"]),
            "t0": pa.array(
                [datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "entry": pa.array(
                [datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "duration": pa.array(
                [datetime(2025, 1, 3, tzinfo=UTC), datetime(2025, 1, 8, tzinfo=UTC)],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "event": pa.array([True, True]),
            "late_corrected_feature": pa.array([7, 9], type=pa.int64()),
            "feature_complete": pa.array([True, True]),
            "feature_reason": pa.array([None, None], type=pa.string()),
        }
    )
    return MaterializedFrame(
        table,
        Roles(
            "user", "user", "t0", None, ("late_corrected_feature",), ("duration", "event", "entry")
        ),
        {
            "late_corrected_feature": ColumnMeta(
                None, "feature", None, None, "feature_complete", "feature_reason"
            )
        },
        Provenance(
            {"late_corrected_feature": FeatureCutoff(0, True, "retrospective_effective_time")},
            {},
            "local-backtest-design",
            "local-backtest-model",
            {"events": "fixture-v1"},
            ExecutionSnapshot("local-fixture", "arrow", {}, None, "verified"),
            "time_to_event",
            None,
        ),
    )


def main() -> None:
    con, model = _model()
    try:
        design = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                "purchase", Trajectory("net_revenue", "7d", periods=3, cumulative=True)
            ),
            administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
        )
        cohort = design.build(model).cohorts(
            grouping=CohortGrouping("7d", datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"),
            value_column="cumulative_value",
        )
        budget = Budget(max_rows=100, max_output_bytes=1_000_000)
        cohort_arrow = cohort.to_pyarrow(budget=budget)
        print("Cohort rows:", cohort_arrow.to_pylist())
        readiness = model.readiness
        later_design = Design(
            "user",
            design.anchor,
            shape=design.shape,
            administrative_cutoff=datetime(2025, 1, 25, tzinfo=UTC),
        )
        later_rows = later_design.build(model).to_pyarrow(budget=budget).to_pylist()
        print(
            "Later cutoff completeness (readiness unchanged):",
            readiness == model.readiness,
            sum(not row["cumulative_value_complete"] for row in later_rows),
        )

        source = _survival_data()
        cutoffs = (datetime(2025, 1, 4, tzinfo=UTC), datetime(2025, 1, 6, tzinfo=UTC))
        fits = backtest_survival(
            source,
            duration="duration",
            event="event",
            entry="entry",
            prediction_cutoffs=cutoffs,
            fitter=lambda frame: frame.table.select(
                ["user", "late_corrected_feature", "duration", "event"]
            ).to_pylist(),
        )
        print("Earlier-only measurement-time fits:", fits)
        print(
            "The pre-cutoff effective-time feature includes a late correction; it is labeled "
            "retrospective_effective_time, not historically recorded/known."
        )
        try:
            backtest_survival(
                source,
                duration="duration",
                event="event",
                entry="entry",
                prediction_cutoffs=cutoffs,
                fitter=lambda frame: frame,
                knowledge="recorded_at",
            )
        except TerrineError as error:
            print("Recorded-at request refused:", error.code)

        surrogate = to_surrogate_arrow(
            source, features=("late_corrected_feature",), outcomes=("duration", "event")
        )
        restored = type(surrogate).from_arrow(surrogate.to_arrow())
        print("Surrogate metadata:", dict(restored.metadata))
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
