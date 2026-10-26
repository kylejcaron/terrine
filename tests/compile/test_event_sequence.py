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


def test_event_sequence_composes_dimension_label_selector(sequence_model):
    con = sequence_model.con
    con.raw_sql("""
        CREATE TABLE tier_history AS SELECT 'u' AS user_id, 'gold' AS tier,
            TIMESTAMPTZ '2025-01-01' AS starts, TIMESTAMPTZ '2025-01-02' AS ends
    """)
    definition = sequence_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["dimensions"] = ("history",)
    definition["properties"]["tier"] = {
        "source": "events",
        "column": "tier",
        "dtype": "string",
        "as_of": "event_time",
        "dimension": "history",
    }
    definition["dimensions"]["history"] = {
        "relation": {"kind": "table", "name": "tier_history"},
        "source_entity": "user",
        "entity_column": "user_id",
        "validity": {"valid_from": "starts", "valid_to": "ends"},
        "properties": ("tier",),
    }
    catalog = type(sequence_model.catalog).model_validate(definition)
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-02")},
        event_ids={"events": "event_id"},
    )
    from terrine.compile.dispatch import compile_design
    from terrine.spec import EventTime

    design = Design(
        "user",
        EventAnchor("signup"),
        outcomes={"selected": EventTime("signup", Window("0s", "2h"), "tier", "gold")},
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5),
    )
    compiled = compile_design(design, model)
    rows = con.to_pyarrow(compiled.table).to_pylist()
    assert next(row for row in rows if not row["is_event"])["selected"] == datetime(
        2025, 1, 1, 12, tzinfo=UTC
    )


def test_sequence_compiler_excludes_known_false_eligibility_anchors(sequence_model):
    from terrine.spec import asof, eq

    design = Design(
        "user",
        EventAnchor("signup"),
        eligibility={"not_purchase": eq(asof("kind", before="-1s"), "purchase")},
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5),
    )
    compiled = design.build(sequence_model)._compiled
    rows = sequence_model.con.to_pyarrow(compiled.table).to_pylist()
    audit = sequence_model.con.to_pyarrow(compiled.audit_table).to_pylist()
    assert rows == []
    assert audit
    assert all(row["__terrine_eligible"] is not True for row in audit)


def test_transition_header_uses_resolved_label_column_type(sequence_model):
    from terrine.semantic.models import Property, TableRef
    from terrine.spec import SessionSpec

    sequence_model.con.create_table(
        "typed_events",
        pa.table(
            {
                "user_id": pa.array([1, 1], type=pa.int64()),
                "ts": pa.array(
                    [
                        datetime(2025, 1, 1, 12, tzinfo=UTC),
                        datetime(2025, 1, 1, 13, tzinfo=UTC),
                    ],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "event_id": ["a", "b"],
                "kind": ["first", "second"],
            }
        ),
    )
    source = sequence_model.catalog.sources["events"].model_copy(
        update={
            "relation": TableRef(kind="table", name="typed_events"),
            "properties": ("kind", "user_id"),
        }
    )
    catalog = sequence_model.catalog.model_copy(
        update={
            "sources": {"events": source},
            "properties": {
                **sequence_model.catalog.properties,
                "user_id": Property(
                    source="events", column="kind", dtype="string", as_of="event_time"
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
        shape=EventSequence(
            "signup",
            Window("0s", "2h"),
            max_events=5,
            label_property="user_id",
            sessions=SessionSpec("1h"),
        ),
    )
    rows = model.con.to_pyarrow(design.build(model)._compiled.table).to_pylist()
    header = next(row for row in rows if not row["is_event"])
    children = [row for row in rows if row["is_event"]]
    assert header["transition_from"] is None
    assert [row["transition_to"] for row in children] == ["first", "second"]


def test_sequence_excludes_rows_without_selected_fact_before_event_cap():
    con = ibis.duckdb.connect()
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["u", "u", "u", "u"],
                "ts": [
                    datetime(2025, 1, 1, 12, tzinfo=UTC),
                    datetime(2025, 1, 1, 13, tzinfo=UTC),
                    datetime(2025, 1, 1, 14, tzinfo=UTC),
                    datetime(2025, 1, 1, 15, tzinfo=UTC),
                ],
                "event_id": ["anchor", "view", "purchase", "not-an-activity"],
                "signup": ["yes", None, None, None],
                "activity": [None, "view", "purchase", None],
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
                    "facts": ["signup", "activity"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {
                "signup": {"source": "events", "column": "signup"},
                "activity": {"source": "events", "column": "activity"},
            },
            "properties": {},
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
        from terrine.compile.dispatch import compile_design

        design = Design(
            "user",
            EventAnchor("signup"),
            shape=EventSequence("activity", Window("0s", "4h"), max_events=2),
        )
        compiled = compile_design(design, model)
        compiled.validate(execute=con.to_pyarrow)
        rows = con.to_pyarrow(compiled.table).to_pylist()
        assert [row["event_id"] for row in rows if row["is_event"]] == ["view", "purchase"]
        assert [row["event_count"] for row in rows if not row["is_event"]] == [2]
    finally:
        con.disconnect()


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
            "start_time": datetime(2025, 1, 1, 12, tzinfo=UTC),
            "matched_event_ids": ["a", "end"],
            "matched_event_times": [
                datetime(2025, 1, 1, 12, tzinfo=UTC),
                datetime(2025, 1, 1, 13, tzinfo=UTC),
            ],
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
        ("kind", False, "_sequence_unit"),
        ("kind", False, "unit_right"),
        ("kind", False, "_funnel_has_missing_history"),
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


def test_funnel_rejects_non_string_event_property(sequence_model):
    from terrine.errors import TerrineError
    from terrine.semantic.models import Property, TableRef
    from terrine.spec import FunnelSpec

    sequence_model.con.create_table(
        "numeric_events",
        ibis.memtable(
            {
                "user_id": ["u", "u"],
                "ts": [
                    datetime(2025, 1, 1, 12, tzinfo=UTC),
                    datetime(2025, 1, 1, 13, tzinfo=UTC),
                ],
                "event_id": ["a", "b"],
                "kind": ["view", "purchase"],
                "amount": [1, 2],
            }
        ).to_pyarrow(),
    )
    source = sequence_model.catalog.sources["events"].model_copy(
        update={
            "relation": TableRef(kind="table", name="numeric_events"),
            "properties": ("kind", "amount"),
        }
    )
    catalog = sequence_model.catalog.model_copy(
        update={
            "sources": {"events": source},
            "properties": {
                **sequence_model.catalog.properties,
                "amount": Property(
                    source="events", column="amount", dtype="int", as_of="event_time"
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
        shape=EventSequence(
            "signup",
            Window("0s", "2h"),
            max_events=5,
            funnel=FunnelSpec("amount", ("1", "2")),
        ),
    )
    with pytest.raises(TerrineError) as error:
        design.validate(model)
    assert error.value.code == "terrine.sequence.invalid_spec"


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
    assert sum(row["transition__reason"] == "missing_predecessor" for row in events) == len(
        {row["anchor_id"] for row in events}
    )
    headers = [row for row in rows if not row["is_event"]]
    for header in headers:
        anchor_events = [row for row in events if row["anchor_id"] == header["anchor_id"]]
        assert header["sessions__complete"] is True
        assert header["sessions__reason"] is None
        assert header["sessions"] == [
            {
                "session_index": 0,
                "session_event_count": len(anchor_events),
                "session_start_time": min(row["event_time"] for row in anchor_events),
                "session_end_time": max(row["event_time"] for row in anchor_events),
            }
        ]


def test_sessions_preserve_properties_matching_summary_helpers(sequence_model):
    from terrine.spec import SessionSpec

    definition = sequence_model.catalog.model_dump(mode="python")
    properties = ("kind", "_sequence_unit", "_sessions")
    definition["sources"]["events"]["properties"] = properties
    for name in properties[1:]:
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
            properties=("_sequence_unit", "_sessions"),
            sessions=SessionSpec("1h"),
        ),
    )
    rows = model.con.to_pyarrow(design.build(model)._compiled.table).to_pylist()
    header = next(row for row in rows if not row["is_event"])
    children = [row for row in rows if row["is_event"]]
    assert [row["_sequence_unit"] for row in children] == ["view", "click", "purchase"]
    assert [row["_sessions"] for row in children] == ["view", "click", "purchase"]
    assert header["_sequence_unit"] is None
    assert header["_sessions"] is None
    assert len(header["sessions"]) == 1


def test_sequence_sessions_emit_requested_label_transitions(sequence_model):
    from terrine.spec import Design, EventAnchor, EventSequence, FunnelSpec, SessionSpec

    design = Design(
        "user",
        EventAnchor("signup", "each"),
        shape=EventSequence(
            "signup",
            Window("0s", "2h"),
            max_events=5,
            label_property="kind",
            sessions=SessionSpec("1h"),
            funnel=FunnelSpec("kind", ("view", "purchase")),
        ),
    )
    rows = design.build(sequence_model)._compiled.table.to_pyarrow().to_pylist()
    events = [row for row in rows if row["is_event"]]
    assert next(row for row in events if row["event_id"] == "end")["transition_from"] == "click"
    assert next(row for row in events if row["event_id"] == "end")["transition_to"] == "purchase"


def test_transition_label_named_unit_uses_property_value(sequence_model):
    from terrine.semantic.models import Property
    from terrine.spec import Design, EventAnchor, EventSequence, SessionSpec

    source = sequence_model.catalog.sources["events"].model_copy(update={"properties": ("unit",)})
    catalog = sequence_model.catalog.model_copy(
        update={
            "sources": {"events": source},
            "properties": {
                **sequence_model.catalog.properties,
                "unit": Property(
                    source="events", column="kind", dtype="string", as_of="event_time"
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
        shape=EventSequence(
            "signup",
            Window("0s", "2h"),
            max_events=5,
            label_property="unit",
            sessions=SessionSpec("1h"),
        ),
    )
    rows = model.con.to_pyarrow(design.build(model)._compiled.table).to_pylist()
    end = next(row for row in rows if row.get("event_id") == "end")
    assert end["transition_from"] == "click"
    assert end["transition_to"] == "purchase"


def test_session_output_names_are_reserved_from_event_properties():
    from terrine.errors import TerrineError
    from terrine.spec import EventSequence, SessionSpec

    with pytest.raises(TerrineError) as error:
        EventSequence(
            "signup",
            Window("0s", "1d"),
            max_events=5,
            properties=("sessions",),
            sessions=SessionSpec(3600),
        )
    assert error.value.code == "terrine.sequence.invalid_spec"


def test_design_output_names_cannot_collide_with_sequence_columns(sequence_model):
    from terrine.errors import TerrineError
    from terrine.spec import EventTime

    design = Design(
        "user",
        EventAnchor("signup"),
        features={"event_count": EventTime("signup", Window("0s", "2h"))},
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5),
    )
    with pytest.raises(TerrineError):
        design.build(sequence_model)


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


@pytest.mark.parametrize("has_history_match", [True, False])
def test_sequence_compiler_projects_dimension_backed_property(sequence_model, has_history_match):
    from terrine.semantic.models import Dimension, Property, TableRef
    from terrine.spec import Design, EventAnchor, EventSequence, FunnelSpec
    from terrine.windows import Window

    history = (
        pa.table({"user_id": ["u"], "region": ["west"]})
        if has_history_match
        else pa.table(
            {
                "user_id": pa.array([], type=pa.string()),
                "region": pa.array([], type=pa.string()),
            }
        )
    )
    sequence_model.con.create_table("regions", history)
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
        shape=EventSequence(
            "signup",
            Window("0s", "2h"),
            max_events=5,
            properties=("region",),
            funnel=FunnelSpec("region", ("west", "east")) if not has_history_match else None,
        ),
    )
    compiled = design.build(model)._compiled
    compiled.validate(execute=model.con.to_pyarrow)
    rows = model.con.to_pyarrow(compiled.table).to_pylist()
    if not has_history_match:
        header = next(row for row in rows if not row["is_event"])
        assert header["funnel_attempts__complete"] is False
        assert header["funnel_attempts__reason"] == "missing_history"
    assert [row["region"] for row in rows if row["is_event"]] == (
        ["west", "west", "west"] if has_history_match else [None, None, None]
    )
    assert [row["region__reason"] for row in rows if row["is_event"]] == (
        [None, None, None] if has_history_match else ["missing_history"] * 3
    )


def test_overlapping_dimension_history_refuses_before_event_identity_check(sequence_model):
    from terrine.errors import TerrineError
    from terrine.semantic.models import Dimension, Property, TableRef

    sequence_model.con.create_table(
        "overlapping_regions",
        ibis.memtable(
            {
                "user_id": ["u", "u"],
                "region": ["west", "east"],
                "starts": [
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, tzinfo=UTC),
                ],
                "ends": [
                    datetime(2025, 1, 2, tzinfo=UTC),
                    datetime(2025, 1, 2, tzinfo=UTC),
                ],
            }
        ),
    )
    source = sequence_model.catalog.sources["events"].model_copy(
        update={"properties": (), "dimensions": ("regions",)}
    )
    dimension = Dimension.model_validate(
        {
            "relation": TableRef(kind="table", name="overlapping_regions"),
            "source_entity": "user",
            "entity_column": "user_id",
            "validity": {"valid_from": "starts", "valid_to": "ends"},
            "properties": ("region",),
        }
    )
    catalog = sequence_model.catalog.model_copy(
        update={
            "sources": {"events": source},
            "dimensions": {"regions": dimension},
            "properties": {
                "region": Property(
                    source="events",
                    column="region",
                    dtype="string",
                    as_of="event_time",
                    dimension="regions",
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
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5, properties=("region",)),
    )
    compiled = design.build(model)._compiled
    with pytest.raises(TerrineError) as error:
        compiled.validate(execute=model.con.to_pyarrow)
    assert error.value.code == "terrine.temporal.duplicate_history"


def test_selected_property_named_normalized_unit_preserves_event_identity(sequence_model):
    from terrine.semantic.models import Property

    source = sequence_model.catalog.sources["events"].model_copy(update={"properties": ("_unit",)})
    catalog = sequence_model.catalog.model_copy(
        update={
            "sources": {"events": source},
            "properties": {
                **sequence_model.catalog.properties,
                "_unit": Property(
                    source="events", column="kind", dtype="string", as_of="event_time"
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
        shape=EventSequence("signup", Window("0s", "2h"), max_events=5, properties=("_unit",)),
    )
    compiled = design.build(model)._compiled
    rows = model.con.to_pyarrow(compiled.table).to_pylist()
    header = next(row for row in rows if not row["is_event"])
    assert header["event_count"] == 3
    assert [row["_unit"] for row in rows if row["is_event"]] == ["view", "click", "purchase"]


def test_funnel_dependency_preserves_entity_key_for_dimension_enrichment(sequence_model):
    from terrine.semantic.models import Dimension, Property, TableRef
    from terrine.spec import EventTime, FunnelSpec

    sequence_model.con.create_table(
        "regions",
        ibis.memtable(
            {
                "user_id": ["u", "u"],
                "region": ["west", "east"],
                "starts": [
                    datetime(2025, 1, 1, 12, tzinfo=UTC),
                    datetime(2025, 1, 1, 12, 30, tzinfo=UTC),
                ],
                "ends": [
                    datetime(2025, 1, 1, 12, 30, tzinfo=UTC),
                    datetime(2025, 1, 2, tzinfo=UTC),
                ],
            }
        ),
    )
    source = sequence_model.catalog.sources["events"].model_copy(
        update={"properties": ("user_id", "kind"), "dimensions": ("regions",)}
    )
    dimension = Dimension.model_validate(
        {
            "relation": TableRef(kind="table", name="regions"),
            "source_entity": "user",
            "entity_column": "user_id",
            "properties": ("region",),
            "validity": {"valid_from": "starts", "valid_to": "ends"},
        }
    )
    catalog = sequence_model.catalog.model_copy(
        update={
            "sources": {"events": source},
            "dimensions": {"regions": dimension},
            "properties": {
                **sequence_model.catalog.properties,
                "user_id": Property(
                    source="events", column="kind", dtype="string", as_of="event_time"
                ),
                "region": Property(
                    source="events",
                    column="region",
                    dtype="string",
                    as_of="event_time",
                    dimension="regions",
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
        outcomes={"_sequence_unit": EventTime("signup", Window("0s", "2h"), "kind", "purchase")},
        shape=EventSequence(
            "signup",
            Window("0s", "2h"),
            max_events=5,
            properties=("region",),
            funnel=FunnelSpec("region", ("west", "east")),
        ),
    )
    rows = model.con.to_pyarrow(design.build(model)._compiled.table).to_pylist()
    header = next(row for row in rows if not row["is_event"])
    children = [row for row in rows if row["is_event"]]
    assert header["funnel_attempts"][0]["status"] == "complete"
    assert header["_sequence_unit"] == datetime(2025, 1, 1, 13, tzinfo=UTC)
    assert [row["region"] for row in children] == ["west", "west", "east"]


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
                "__terrine_sequence_event_id": ["collision"],
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
