from datetime import UTC, datetime

from terrine.data import Budget, Coverage, UnitSource
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import CalendarComparisonPanel, Design, Initiation, Schedule, quantile


def test_calendar_comparison_compiles_quantile_outcome_horizon(native_inputs):
    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE events ADD COLUMN metric_value INTEGER")
    con.raw_sql("UPDATE events SET metric_value = 1")
    definition = catalog.model_dump(mode="python")
    definition["sources"]["events"]["facts"] += ("session_value",)
    definition["facts"]["session_value"] = {
        "source": "events",
        "column": "metric_value",
    }
    definition["measures"]["session_value_sum"] = {
        "fact": "session_value",
        "aggregation": "sum",
    }
    definition["metrics"]["session_value"] = {
        "kind": "mean",
        "entity": "user",
        "measure": "session_value_sum",
    }
    catalog = Catalog.model_validate(definition)
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-12")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    base = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-12"),
        outcomes={"p50": quantile("session_value", "0.5", window=("0d", "1d"))},
    )
    frame = CalendarComparisonPanel(
        base,
        model,
        cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
        control="never_treated",
        reference_period=-1,
        comparison_periods=(0,),
    ).materialize(Budget(max_rows=200, max_output_bytes=1_000_000))

    assert frame.table.num_rows > 0
    assert "p50" in frame.roles.outcomes
