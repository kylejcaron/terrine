"""Estimate stacked cohort-time effects under both admissible control modes."""

from datetime import UTC, datetime

import ibis
import pyarrow as pa

from terrine.data import Budget, Coverage, UnitSource
from terrine.estimate import naive_staggered_twfe_warning, stacked_event_study
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import CalendarComparisonPanel, Design, Initiation, Schedule, agg


def main() -> None:
    con = ibis.duckdb.connect()
    con.create_table("users", pa.table({"user_id": ["a", "b", "c", "d"]}))
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["a", "b", "d"],
                "ts": [
                    datetime(2025, 1, 4, tzinfo=UTC),
                    datetime(2025, 1, 7, tzinfo=UTC),
                    datetime(2025, 1, 10, tzinfo=UTC),
                ],
                "event_id": ["a1", "b1", "d1"],
            }
        ),
    )
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
                    "facts": ["session"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {"session": {"source": "events", "column": None}},
            "properties": {},
            "dimensions": {},
            "measures": {"sessions_count": {"fact": "session", "aggregation": "count"}},
            "metrics": {
                "sessions": {"kind": "mean", "entity": "user", "measure": "sessions_count"}
            },
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-20")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    try:
        base = Design(
            "user",
            Schedule("1d", "2025-01-01", "2025-01-15"),
            outcomes={"y": agg("sessions", window=("0d", "1d"))},
        )
        cohort = Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC))
        for mode in ("never_treated", "not_yet_treated"):
            frame = CalendarComparisonPanel(
                base,
                model,
                cohort=cohort,
                control=mode,
                reference_period=-1,
                comparison_periods=(0, 1),
            ).materialize(Budget(max_rows=500, max_output_bytes=2_000_000))
            result = stacked_event_study(
                frame, outcome="y", treatment="treatment", cohort="cohort", period="period"
            )
            print(mode, dict(result.aggregate_effects), dict(result.aggregate_standard_errors))
        print(naive_staggered_twfe_warning().to_dict())
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
