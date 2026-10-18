from datetime import UTC, datetime

import ibis
import pyarrow as pa
import pytest

from terrine.compile.anchors import AnchorTable
from terrine.compile.shapes.event_sequence import _identity_checks
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


def test_sequence_compiler_publishes_funnel_attempt_schema(sequence_model):
    from terrine.spec import FunnelSpec

    design = Design(
        "user",
        EventAnchor("signup"),
        shape=EventSequence(
            "signup",
            Window("0s", "2h"),
            max_events=5,
            properties=("kind",),
            funnel=FunnelSpec("kind", ("view", "purchase")),
        ),
    )
    compiled = design.build(sequence_model)._compiled
    compiled.validate(execute=sequence_model.con.to_pyarrow)
    rows = sequence_model.con.to_pyarrow(compiled.table).to_pylist()
    header = next(row for row in rows if not row["is_event"])
    assert header["funnel_attempts"] == [
        {
            "step0_event_id": "a",
            "deadline": datetime(2025, 1, 1, 14, tzinfo=UTC),
            "progress": 2,
            "status": "complete",
            "reason": None,
        }
    ]

    children = [row for row in rows if row["is_event"]]
    assert all(row["funnel_attempts"] is None for row in children)
@pytest.mark.parametrize(
    ("property_name", "with_sessions", "selected_property"),
    [
        ("unit", False, None),
        ("previous_event_id", True, None),
        ("unit", False, "__terrine_funnel_event_property"),
        ("previous_event_id", False, "previous_event_id"),
    ],
)
def test_funnel_dependency_generated_name_does_not_overwrite_event_values(
    sequence_model, property_name, with_sessions, selected_property
):
    from terrine.spec import FunnelSpec, SessionSpec

    definition = sequence_model.catalog.model_dump(mode="python")
    properties = ["kind", property_name]
    if selected_property is not None:
        properties.append(selected_property)
    definition["sources"]["events"]["properties"] = tuple(dict.fromkeys(properties))
    for name in dict.fromkeys((property_name, selected_property)):
        if name is not None:
            definition["properties"][name] = {
                "source": "events",
                "column": "kind",
                "dtype": "string",
                "as_of": "event_time",
            }
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=sequence_model.con,
        readiness=sequence_model.readiness,
        event_ids=sequence_model.event_ids,
    )
    design = Design(
        "user",
        EventAnchor("signup"),
        shape=EventSequence(
            "signup",
            Window("0s", "2h"),
            max_events=5,
            properties=(selected_property,) if selected_property is not None else (),
            sessions=SessionSpec(3600) if with_sessions else None,
            funnel=FunnelSpec(property_name, ("view", "purchase")),
        ),
    )
    rows = model.con.to_pyarrow(design.build(model)._compiled.table).to_pylist()
    header = next(row for row in rows if not row["is_event"])
    assert header["funnel_attempts"][0]["status"] == "complete"
    if selected_property is not None:
        child = next(row for row in rows if row["is_event"])
        assert child[selected_property] == "view"


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


def test_sequence_arrays_support_default_empty_property_list(sequence_model):
    from terrine.spec import Design, EventAnchor, EventSequence
    from terrine.windows import Window

    design = Design(
        "user",
        EventAnchor("signup"),
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5, representation="arrays"),
    )
    compiled = design.build(sequence_model)._compiled
    compiled.validate(execute=sequence_model.con.to_pyarrow)
    rows = compiled.table.to_pyarrow().to_pylist()
    assert "event_values" in compiled.table.columns
    assert rows[0]["event_values"] == [
        {
            "event_id": event_id,
            "event_time": datetime(2025, 1, 1, hour, tzinfo=UTC),
        }
        for event_id, hour in (("a", 12), ("z", 12), ("end", 13))
    ]


def test_sequence_sessions_are_compiled_per_anchor(sequence_model):
    from terrine.spec import Design, EventAnchor, EventSequence, SessionSpec
    from terrine.windows import Window

    design = Design(
        "user",
        EventAnchor("signup", "each"),
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5, sessions=SessionSpec("1h")),
    )
    rows = design.build(sequence_model)._compiled.table.to_pyarrow().to_pylist()
    events = [row for row in rows if row["is_event"]]
    assert events
    assert {row["session_index"] for row in events} == {0}
    assert sum(row["transition_reason"] == "missing_predecessor" for row in events) == len(
        {row["anchor_id"] for row in events}
    )


@pytest.mark.parametrize(
    ("unit", "timestamp", "check_key"),
    [
        (None, datetime(2025, 1, 1, 12, tzinfo=UTC), "null_event_unit"),
        ("u", None, "null_event_timestamp"),
    ],
)
def test_configured_identity_rejects_null_unit_or_timestamp(
    sequence_model, unit, timestamp, check_key
):
    from terrine.errors import TerrineError

    events = ibis.memtable(
        pa.table(
            {
                "_unit": [unit],
                "_timestamp": pa.array([timestamp], type=pa.timestamp("us", tz="UTC")),
                "identity": ["event-1"],
            }
        )
    )
    anchors = AnchorTable(
        events,
        tuple(_identity_checks(events, "_unit", "identity", configured=True)),
    )
    with pytest.raises(TerrineError) as error:
        anchors.validate(execute=sequence_model.con.to_pyarrow)
    assert error.value.code == "terrine.compile.anchor_integrity"
    assert error.value.context["check_key"] == check_key


def test_sequence_compiler_projects_dimension_backed_property(sequence_model):
    from terrine.semantic.models import Dimension, Property, TableRef
    from terrine.spec import Design, EventAnchor, EventSequence
    from terrine.windows import Window

    sequence_model.con.create_table(
        "regions", ibis.memtable({"user_id": ["u"], "region": ["west"]})
    )
    source = sequence_model.catalog.sources["events"].model_copy(
        update={"properties": (), "dimensions": ("regions",)}
    )
    dimension = Dimension.model_validate(
        {
            "relation": TableRef(kind="table", name="regions"),
            "source_entity": "user",
            "entity_column": "user_id",
            "properties": ("region",),
        }
    )
    region = Property(
        source="events",
        column="region",
        dtype="string",
        as_of="event_time",
        dimension="regions",
    )
    catalog = sequence_model.catalog.model_copy(
        update={
            "sources": {"events": source},
            "dimensions": {"regions": dimension},
            "properties": {"region": region},
        }
    )
    model = Model.from_catalog(
        catalog,
        con=sequence_model.con,
        readiness=sequence_model.readiness,
        event_ids=sequence_model.event_ids,
    )
    design = Design(
        "user",
        EventAnchor("signup"),
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5, properties=("region",)),
    )
    compiled = design.build(model)._compiled
    compiled.validate(execute=model.con.to_pyarrow)
    rows = model.con.to_pyarrow(compiled.table).to_pylist()
    assert [row["region"] for row in rows if row["is_event"]] == ["west", "west", "west"]


def test_sequence_compiler_publishes_naive_source_times_as_utc(sequence_model):
    from terrine.semantic.models import TableRef
    from terrine.spec import Design, EventAnchor, EventSequence
    from terrine.windows import Window

    sequence_model.con.create_table(
        "naive_events",
        ibis.memtable(
            {
                "user_id": ["u", "u"],
                "ts": [datetime(2025, 1, 1, 12), datetime(2025, 1, 1, 13)],
                "event_id": ["a", "b"],
                "kind": ["click", "purchase"],
            }
        ),
    )
    source = sequence_model.catalog.sources["events"].model_copy(
        update={"relation": TableRef(kind="table", name="naive_events")}
    )
    catalog = sequence_model.catalog.model_copy(update={"sources": {"events": source}})
    model = Model.from_catalog(
        catalog,
        con=sequence_model.con,
        readiness=sequence_model.readiness,
        event_ids=sequence_model.event_ids,
    )
    design = Design(
        "user",
        EventAnchor("signup"),
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5),
    )
    compiled = design.build(model)._compiled
    compiled.validate(execute=model.con.to_pyarrow)
    rows = model.con.to_pyarrow(compiled.table).to_pylist()
    event = next(row for row in rows if row["is_event"])
    assert event["event_time"] == datetime(2025, 1, 1, 12, tzinfo=UTC)


def test_sequence_compiler_validates_original_design_treatment(sequence_model):
    from terrine.errors import TerrineError
    from terrine.spec import Design, EventAnchor, EventSequence
    from terrine.spec.design import Treatment
    from terrine.windows import Window

    design = Design(
        "user",
        EventAnchor("signup"),
        treatment=Treatment("kind"),
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5),
    )
    with pytest.raises(TerrineError) as error:
        design.build(sequence_model)
    assert error.value.code == "terrine.sequence.invalid_spec"


def test_sequence_compiler_projects_multiple_rebound_properties_from_original_row(
    sequence_model,
):
    from terrine.semantic.models import Property, TableRef
    from terrine.spec import Design, EventAnchor, EventSequence
    from terrine.windows import Window

    sequence_model.con.create_table(
        "alias_events",
        ibis.memtable(
            {
                "user_id": ["u"],
                "ts": [datetime(2025, 1, 1, 12, tzinfo=UTC)],
                "event_id": ["one"],
                "physical_b": ["B"],
                "physical_c": ["C"],
            }
        ),
    )
    source = sequence_model.catalog.sources["events"].model_copy(
        update={
            "relation": TableRef(kind="table", name="alias_events"),
            "properties": ("a", "b"),
        }
    )
    catalog = sequence_model.catalog.model_copy(
        update={
            "sources": {"events": source},
            "properties": {
                "a": Property(
                    source="events", column="physical_b", dtype="string", as_of="event_time"
                ),
                "b": Property(
                    source="events", column="physical_c", dtype="string", as_of="event_time"
                ),
            },
        }
    )
    model = Model.from_catalog(
        catalog,
        con=sequence_model.con,
        readiness=sequence_model.readiness,
        event_ids=sequence_model.event_ids,
    )
    design = Design(
        "user",
        EventAnchor("signup"),
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5, properties=("b", "a")),
    )
    compiled = design.build(model)._compiled
    compiled.validate(execute=model.con.to_pyarrow)
    child = next(row for row in model.con.to_pyarrow(compiled.table).to_pylist() if row["is_event"])
    assert (child["b"], child["a"]) == ("C", "B")


def test_sequence_timestamp_temporary_alias_avoids_physical_property(sequence_model):
    from terrine.semantic.models import Property, TableRef
    from terrine.spec import Design, EventAnchor, EventSequence
    from terrine.windows import Window

    sequence_model.con.create_table(
        "alias_events",
        ibis.memtable(
            {
                "user_id": ["u"],
                "ts": [datetime(2025, 1, 1, 12, tzinfo=UTC)],
                "event_id": ["one"],
                "_sequence_raw_timestamp": ["preserve-me"],
            }
        ),
    )
    source = sequence_model.catalog.sources["events"].model_copy(
        update={"relation": TableRef(kind="table", name="alias_events"), "properties": ("raw",)}
    )
    catalog = sequence_model.catalog.model_copy(
        update={
            "sources": {"events": source},
            "properties": {
                "raw": Property(
                    source="events",
                    column="_sequence_raw_timestamp",
                    dtype="string",
                    as_of="event_time",
                )
            },
        }
    )
    model = Model.from_catalog(
        catalog,
        con=sequence_model.con,
        readiness=sequence_model.readiness,
        event_ids=sequence_model.event_ids,
    )
    design = Design(
        "user",
        EventAnchor("signup"),
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5, properties=("raw",)),
    )
    compiled = design.build(model)._compiled
    compiled.validate(execute=model.con.to_pyarrow)
    child = next(row for row in model.con.to_pyarrow(compiled.table).to_pylist() if row["is_event"])
    assert child["raw"] == "preserve-me"
