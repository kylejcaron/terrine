from datetime import UTC, datetime

import ibis
import pyarrow as pa
import pytest

from terrine.data import Coverage
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Design, EventAnchor, EventSequence
from terrine.windows import Window


@pytest.fixture
def sequence_model():
    con = ibis.duckdb.connect()
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["u", "u", "u"],
                "ts": [
                    datetime(2025, 1, 1, 12, tzinfo=UTC),
                    datetime(2025, 1, 1, 12, tzinfo=UTC),
                    datetime(2025, 1, 1, 13, tzinfo=UTC),
                ],
                "event_id": ["z", "a", "end"],
                "kind": ["click", "view", "purchase"],
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
                    "facts": ["signup"],
                    "properties": ["kind"],
                    "dimensions": [],
                }
            },
            "facts": {"signup": {"source": "events", "column": None}},
            "properties": {
                "kind": {
                    "source": "events",
                    "column": "kind",
                    "dtype": "string",
                    "as_of": "event_time",
                }
            },
            "dimensions": {},
            "measures": {},
            "metrics": {},
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-02")},
        event_ids={"events": "event_id"},
    )
    try:
        yield model
    finally:
        con.disconnect()


def test_sequence_compiler_uses_configured_event_id_and_preserves_tied_order(sequence_model):
    from terrine.compile.dispatch import compile_design

    design = Design(
        "user",
        EventAnchor("signup"),
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5, properties=("kind",)),
    )
    compiled = compile_design(design, sequence_model)
    compiled.validate(execute=sequence_model.con.to_pyarrow)
    rows = sequence_model.con.to_pyarrow(compiled.table).to_pylist()
    children = [row for row in rows if row["is_event"]]
    assert [row["event_id"] for row in children] == ["a", "z", "end"]


def test_timestamp_identity_refuses_ties_without_configured_id(sequence_model):
    from terrine.errors import TerrineError
    from terrine.spec import Design, EventAnchor, EventSequence
    from terrine.windows import Window

    model_without_id = Model.from_catalog(
        sequence_model.catalog,
        con=sequence_model.con,
        readiness=sequence_model.readiness,
        event_ids={},
    )
    design = Design(
        "user",
        EventAnchor("signup"),
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5),
    )
    compiled = design.build(model_without_id)._compiled
    with pytest.raises(TerrineError) as error:
        compiled.validate(execute=model_without_id.con.to_pyarrow)
    assert error.value.code == "terrine.compile.anchor_integrity"
    assert error.value.context["check_key"] == "tied_event_timestamps"
