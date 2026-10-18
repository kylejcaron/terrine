from datetime import UTC, datetime

from terrine.data import Budget, Coverage, MaterializedFrame, UnitSource
from terrine.model import Model
from terrine.spec import (
    Design,
    EventAnchor,
    EventPanel,
    Initiation,
    Schedule,
    Sequential,
    from_json,
    to_json,
)


def test_all_shapes_build_materialize_and_arrow_round_trip(native_inputs):
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    treatment = Initiation("events", "session", history_start=datetime(2024, 1, 1, tzinfo=UTC))
    sequential = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-05")),
        treatment=treatment,
    )
    event_panel = EventPanel(
        Design("user", EventAnchor("session", "first")),
        periods=(-1, 0),
        period_seconds=86_400,
    )

    assert from_json(to_json(sequential)) == sequential
    for design in (sequential, event_panel):
        materialized = design.build(model).materialize(
            Budget(max_rows=20, max_output_bytes=100_000)
        )
        restored = MaterializedFrame.from_arrow(materialized.to_arrow())
        assert restored.table.equals(materialized.table)
        assert restored.provenance.shape == (
            "sequential" if design is sequential else "event_panel"
        )
        assert restored.provenance.panel is not None
