from datetime import UTC, datetime

import pyarrow as pa

from terrine.data import Budget, Coverage, MaterializedFrame, UnitSource
from terrine.model import Model
from terrine.spec import (
    CalendarComparisonPanel,
    Design,
    Initiation,
    Schedule,
    agg,
)


def test_calendar_comparison_materializes_roster_controls_for_both_modes(native_inputs):
    catalog, con = native_inputs
    con.insert("users", pa.table({"user_id": ["c", "d"], "alt_id": ["cc", "dd"]}))
    con.insert(
        "events",
        pa.table(
            {
                "user_id": ["d"],
                "ts": [datetime(2025, 1, 8, tzinfo=UTC)],
                "event_id": ["d1"],
                "alternate_event_id": ["d2"],
            }
        ),
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    base = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-12"),
        outcomes={"y": agg("sessions", window=("0d", "1d"))},
    )
    cohort = Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC))
    frames = {
        mode: CalendarComparisonPanel(
            base,
            model,
            cohort=cohort,
            control=mode,
            reference_period=-1,
            comparison_periods=(0, 1),
        ).materialize(Budget(max_rows=200, max_output_bytes=1_000_000))
        for mode in ("never_treated", "not_yet_treated")
    }

    never = frames["never_treated"]
    not_yet = frames["not_yet_treated"]
    assert never.provenance.shape == not_yet.provenance.shape == "calendar_comparison"
    assert never.provenance.calendar_comparison is not None
    assert not_yet.provenance.calendar_comparison is not None
    assert never.provenance.calendar_comparison.control == "never_treated"
    assert not_yet.provenance.calendar_comparison.control == "not_yet_treated"
    assert never.provenance.design_hash != not_yet.provenance.design_hash
    never_rows = never.table.to_pylist()
    not_yet_rows = not_yet.table.to_pylist()
    early_never = [row for row in never_rows if row["cohort"] == datetime(2025, 1, 3, tzinfo=UTC)]
    early_not_yet = [
        row for row in not_yet_rows if row["cohort"] == datetime(2025, 1, 3, tzinfo=UTC)
    ]
    assert {row["unit"] for row in early_never if not row["treatment"]} == {"c"}
    assert {row["unit"] for row in early_not_yet if not row["treatment"]} == {"c", "d"}
    assert all(row["event_period"] in (-1, 0, 1) for row in not_yet_rows)
    restored = MaterializedFrame.from_arrow(not_yet.to_arrow())
    assert restored.provenance.calendar_comparison == not_yet.provenance.calendar_comparison
    assert restored.table.equals(not_yet.table)
