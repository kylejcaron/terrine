from datetime import UTC, date, datetime

import ibis
import pyarrow as pa
import pytest

from terrine.compile.anchors import compile_anchors
from terrine.data import Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import AnchorRange, Design, EventAnchor, FixedDate, Schedule


@pytest.fixture
def native_model():
    con = ibis.duckdb.connect()
    con.create_table(
        "events",
        pa.table(
            {
                "ts": [
                    datetime(2025, 1, 2, tzinfo=UTC),
                    datetime(2025, 1, 2, tzinfo=UTC),
                    datetime(2025, 1, 3, 12, tzinfo=UTC),
                    datetime(2025, 1, 4, tzinfo=UTC),
                ],
                "user_id": ["a", "a", "a", "b"],
                "occurred": [None, 1, 1, 1],
                "event_id": ["z", "a", "c", "d"],
            }
        ),
    )
    con.create_table(
        "users",
        pa.table(
            {
                "id": ["a", "b", "c"],
                "entered": [
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, tzinfo=UTC),
                ],
                "exited": [None, datetime(2025, 1, 4, tzinfo=UTC), None],
                "observed_from": [None, datetime(2024, 12, 1, tzinfo=UTC), None],
                "observed_until": [datetime(2025, 2, 1, tzinfo=UTC), None, None],
            }
        ),
    )
    catalog = {
        "schema_version": 1,
        "day_boundary": "UTC-05:00",
        "dialect": "duckdb",
        "sources": {
            "events": {
                "relation": {"kind": "table", "name": "events"},
                "timestamp": "ts",
                "entities": {"user": "user_id"},
                "facts": ["occurred"],
                "properties": [],
                "dimensions": [],
            }
        },
        "facts": {"occurred": {"source": "events", "column": "occurred"}},
        "properties": {},
        "dimensions": {},
        "measures": {},
        "metrics": {},
    }
    model = Model.from_catalog(
        Catalog.model_validate(catalog),
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={
            "user": UnitSource(
                "users", "id", "entered", "exited", "observed_from", "observed_until"
            )
        },
        event_ids={"events": "event_id"},
    )
    try:
        yield model
    finally:
        con.disconnect()


def rows(table):
    return table.to_pyarrow().to_pylist()


def candidate_rows(design, model):
    anchors = compile_anchors(design, model)
    anchors.validate(execute=lambda query: query.to_pyarrow())
    return rows(anchors.table)


def test_fixed_date_uses_declared_roster_not_event_population(native_model):
    result = candidate_rows(Design(unit="user", anchor=FixedDate(date(2025, 1, 3))), native_model)
    assert sorted(row["unit"] for row in result) == ["a", "b", "c"]
    assert {row["t0"] for row in result} == {datetime(2025, 1, 3, 5, tzinfo=UTC)}
    assert {row["anchor_id"] for row in result} == {"fixed"}


def test_fixed_date_keeps_exact_aware_time_zero(native_model):
    instant = datetime(2025, 1, 3, 12, 34, 56, 123456, tzinfo=UTC)
    result = candidate_rows(Design(unit="user", anchor=FixedDate(instant)), native_model)
    assert {row["t0"] for row in result} == {instant}


@pytest.mark.parametrize(
    ("unit", "anchor", "reference"),
    [
        ("user", FixedDate(date(2025, 1, 3)), "roster"),
        ("user", Schedule("1d", "2025-01-02", "2025-01-04"), "roster"),
        ("account", EventAnchor("occurred"), "occurred"),
    ],
)
def test_candidate_unit_refusal_matches_static_validation(native_model, unit, anchor, reference):
    model = Model.from_catalog(
        native_model.catalog,
        con=native_model.con,
        readiness=native_model.readiness,
    )
    design = Design(unit=unit, anchor=anchor)
    with pytest.raises(TerrineError) as static_error:
        design.validate(model)
    with pytest.raises(TerrineError) as compile_error:
        compile_anchors(design, model)
    for error in (static_error.value, compile_error.value):
        assert error.code == "terrine.design.incompatible_unit"
        assert error.context["unit"] == unit
        assert error.context["name"] == reference


def test_event_anchor_requires_fact_declared_by_bound_source(native_model):
    definition = native_model.catalog.model_dump(mode="python")
    definition["facts"]["ghost"] = {"source": "events", "column": "occurred"}
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=native_model.con,
        readiness=native_model.readiness,
    )
    design = Design(unit="user", anchor=EventAnchor("ghost"))
    with pytest.raises(TerrineError) as static_error:
        design.validate(model)
    with pytest.raises(TerrineError) as compile_error:
        compile_anchors(design, model)
    assert static_error.value.code == compile_error.value.code == "terrine.design.missing_reference"
    assert compile_error.value.context["kind"] == "bound fact"


def test_roster_observation_bounds_are_independent_of_exit(native_model):
    result = candidate_rows(Design(unit="user", anchor=FixedDate(date(2025, 1, 3))), native_model)
    user_b = next(row for row in result if row["unit"] == "b")
    assert user_b["_observed_from"] == datetime(2024, 12, 1, tzinfo=UTC)
    assert user_b["_observed_until"] is None


def test_schedule_end_exclusive_and_roster_lifetime_half_open(native_model):
    design = Design(unit="user", anchor=Schedule("1d", "2025-01-02", "2025-01-04"))
    result = candidate_rows(design, native_model)
    assert {(row["unit"], row["t0"]) for row in result} == {
        ("a", datetime(2025, 1, 2, 5, tzinfo=UTC)),
        ("a", datetime(2025, 1, 3, 5, tzinfo=UTC)),
        ("b", datetime(2025, 1, 2, 5, tzinfo=UTC)),
        ("b", datetime(2025, 1, 3, 5, tzinfo=UTC)),
        ("c", datetime(2025, 1, 2, 5, tzinfo=UTC)),
        ("c", datetime(2025, 1, 3, 5, tzinfo=UTC)),
    }


def test_event_first_uses_exact_t0_and_stable_id_for_ties(native_model):
    design = Design(unit="user", anchor=EventAnchor("occurred", "first"))
    result = candidate_rows(design, native_model)
    user_a = next(row for row in result if row["unit"] == "a")
    assert user_a["t0"] == datetime(2025, 1, 2, tzinfo=UTC)
    assert user_a["anchor_id"].endswith(":1:a")
    assert user_a["_observed_until"] == datetime(2025, 2, 1, tzinfo=UTC)


def test_nullable_fact_values_still_represent_each_events(native_model):
    result = candidate_rows(
        Design(unit="user", anchor=EventAnchor("occurred", "each")), native_model
    )
    assert len(result) == 3
    assert any(row["anchor_id"].endswith(":1:z") for row in result)


@pytest.mark.parametrize("offset", ["0s", "1d"])
def test_first_event_is_selected_before_range_filter(native_model, offset):
    design = Design(
        unit="user",
        anchor=EventAnchor("occurred", "first", offset=offset),
        anchor_range=AnchorRange(start="2025-01-03"),
    )
    result = candidate_rows(design, native_model)
    assert all(row["unit"] != "a" for row in result)


def test_nth_event_is_selected_before_range_filter(native_model):
    design = Design(
        unit="user",
        anchor=EventAnchor("occurred", "nth", n=3),
        anchor_range=AnchorRange(start="2025-01-03"),
    )
    result = candidate_rows(design, native_model)
    user_a = next(row for row in result if row["unit"] == "a")
    assert user_a["t0"] == datetime(2025, 1, 3, 12, tzinfo=UTC)


@pytest.mark.parametrize("which,n", [("first", None), ("nth", 3), ("each", None)])
def test_offset_shifts_selected_events_after_original_time_range(native_model, which, n):
    bounds = AnchorRange(
        start=datetime(2025, 1, 2, tzinfo=UTC),
        end=datetime(2025, 1, 4, tzinfo=UTC),
    )
    original = candidate_rows(
        Design("user", EventAnchor("occurred", which, n), anchor_range=bounds), native_model
    )
    shifted = candidate_rows(
        Design("user", EventAnchor("occurred", which, n, offset="1d"), anchor_range=bounds),
        native_model,
    )
    times = (
        [datetime(2025, 1, 3, tzinfo=UTC)]
        if which == "first"
        else [datetime(2025, 1, 4, 12, tzinfo=UTC)]
        if which == "nth"
        else [
            datetime(2025, 1, 3, tzinfo=UTC),
            datetime(2025, 1, 3, tzinfo=UTC),
            datetime(2025, 1, 4, 12, tzinfo=UTC),
        ]
    )
    assert [(row["unit"], row["t0"]) for row in sorted(shifted, key=lambda row: row["t0"])] == [
        ("a", instant) for instant in times
    ]
    assert {row["anchor_id"] for row in original}.isdisjoint(row["anchor_id"] for row in shifted)
    assert {row["anchor_provenance"] for row in shifted} == {f"event:occurred:{which}:offset:86400"}


def test_each_event_range_filters_selected_events(native_model):
    design = Design(
        unit="user",
        anchor=EventAnchor("occurred", "each"),
        anchor_range=AnchorRange(start=datetime(2025, 1, 3, tzinfo=UTC)),
    )
    result = candidate_rows(design, native_model)
    assert [(row["unit"], row["t0"]) for row in result] == [
        ("a", datetime(2025, 1, 3, 12, tzinfo=UTC))
    ]


def test_event_unit_missing_from_roster_has_null_observation_bounds(native_model):
    native_model.con.insert(
        "events",
        pa.table(
            {
                "ts": [datetime(2025, 1, 3, tzinfo=UTC)],
                "user_id": ["unlisted"],
                "occurred": [None],
                "event_id": ["unlisted-event"],
            }
        ),
    )
    result = candidate_rows(
        Design(unit="user", anchor=EventAnchor("occurred", "each")), native_model
    )
    unlisted = next(row for row in result if row["unit"] == "unlisted")
    assert unlisted["_observed_from"] is None
    assert unlisted["_observed_until"] is None


def test_duplicate_event_id_is_deferred_integrity_error(native_model):
    native_model.con.raw_sql("UPDATE events SET event_id = 'a' WHERE event_id = 'c'")
    anchors = compile_anchors(
        Design(unit="user", anchor=EventAnchor("occurred", "each")), native_model
    )
    with pytest.raises(TerrineError) as error:
        anchors.validate(execute=lambda query: query.to_pyarrow())
    assert error.value.code == "terrine.compile.anchor_integrity"


def test_event_ids_may_be_reused_across_units(native_model):
    native_model.con.raw_sql("DELETE FROM events")
    instant = datetime(2025, 1, 2, tzinfo=UTC)
    native_model.con.insert(
        "events",
        pa.table(
            {
                "ts": [instant, instant],
                "user_id": ["a", "b"],
                "occurred": [1, 1],
                "event_id": ["shared", "shared"],
            }
        ),
    )
    result = candidate_rows(
        Design(unit="user", anchor=EventAnchor("occurred", "each")), native_model
    )
    assert sorted((row["unit"], row["t0"]) for row in result) == [
        ("a", instant),
        ("b", instant),
    ]


def test_duplicate_candidate_identity_refuses_without_configured_event_ids(native_model):
    model = Model.from_catalog(
        native_model.catalog,
        con=native_model.con,
        readiness=native_model.readiness,
        units=native_model.units,
    )
    anchors = compile_anchors(Design(unit="user", anchor=EventAnchor("occurred", "each")), model)
    with pytest.raises(TerrineError) as error:
        anchors.validate(execute=lambda query: query.to_pyarrow())
    assert error.value.context["check_key"] == "ambiguous_event_identity"


def test_anchor_validation_rejects_malformed_arrow_results(native_model):
    anchors = compile_anchors(Design(unit="user", anchor=FixedDate(date(2025, 1, 3))), native_model)
    malformed = pa.table({"invalid_count": pa.array([], type=pa.int64())})
    with pytest.raises(TerrineError) as error:
        anchors.validate(execute=lambda query: malformed)
    assert error.value.context["check_key"] == "invalid_executor_result"


def test_null_roster_key_refuses(native_model):
    native_model.con.insert(
        "users",
        pa.table(
            {
                "id": pa.array([None], type=pa.string()),
                "entered": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "exited": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "observed_from": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "observed_until": pa.array([None], type=pa.timestamp("us", tz="UTC")),
            }
        ),
    )
    anchors = compile_anchors(Design(unit="user", anchor=FixedDate(date(2025, 1, 3))), native_model)
    with pytest.raises(TerrineError) as error:
        anchors.validate(execute=lambda query: query.to_pyarrow())
    assert error.value.context["check_key"] == "null_roster_key"


def test_roster_key_named_like_aggregate_alias_is_checked(native_model):
    native_model.con.create_table(
        "alias_roster",
        pa.table({"invalid_count": ["a", "a"], "n": [1, 2]}),
    )
    model = Model.from_catalog(
        native_model.catalog,
        con=native_model.con,
        readiness=native_model.readiness,
        units={"user": UnitSource("alias_roster", "invalid_count", None, None, None, None)},
    )
    anchors = compile_anchors(Design(unit="user", anchor=FixedDate(date(2025, 1, 3))), model)
    with pytest.raises(TerrineError) as error:
        anchors.validate(execute=lambda query: query.to_pyarrow())
    assert error.value.context["check_key"] == "duplicate_roster_key"


@pytest.mark.parametrize("column", ["user_id", "ts"])
def test_event_tie_check_accepts_physical_column_named_n(native_model, column):
    native_model.con.raw_sql("DELETE FROM events WHERE event_id = 'z'")
    native_model.con.raw_sql(f'ALTER TABLE events RENAME COLUMN "{column}" TO n')
    definition = native_model.catalog.model_dump(mode="python")
    source = definition["sources"]["events"]
    if column == "user_id":
        source["entities"]["user"] = "n"
    else:
        source["timestamp"] = "n"
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=native_model.con,
        readiness=native_model.readiness,
    )
    result = candidate_rows(Design(unit="user", anchor=EventAnchor("occurred", "first")), model)
    assert {row["unit"]: row["t0"] for row in result} == {
        "a": datetime(2025, 1, 2, tzinfo=UTC),
        "b": datetime(2025, 1, 4, tzinfo=UTC),
    }


def test_duplicate_roster_key_refuses(native_model):
    native_model.con.insert(
        "users",
        pa.table(
            {
                "id": ["a"],
                "entered": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "exited": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "observed_from": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "observed_until": pa.array([None], type=pa.timestamp("us", tz="UTC")),
            }
        ),
    )
    anchors = compile_anchors(Design(unit="user", anchor=FixedDate(date(2025, 1, 3))), native_model)
    with pytest.raises(TerrineError) as error:
        anchors.validate(execute=lambda query: query.to_pyarrow())
    assert error.value.code == "terrine.compile.anchor_integrity"


def _submicro_model(value: int):
    con = ibis.duckdb.connect()
    con.create_table(
        "events",
        pa.table(
            {
                "ts": pa.array([value], type=pa.timestamp("ns")),
                "user_id": ["u"],
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
                    "facts": ["occurred"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {"occurred": {"source": "events", "column": None}},
            "properties": {},
            "dimensions": {},
            "measures": {},
            "metrics": {},
        }
    )
    return Model.from_catalog(
        catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-01-31")}
    )


def test_submicrosecond_event_timestamp_refuses_during_explicit_validation():
    design = Design(unit="user", anchor=EventAnchor("occurred"))
    model = _submicro_model(1_700_000_000_000_000_123)
    try:
        anchors = compile_anchors(design, model)
        with pytest.raises(TerrineError) as error:
            anchors.validate(execute=lambda query: query.to_pyarrow())
        assert error.value.code == "terrine.compile.anchor_timestamp_precision"
        assert error.value.context["column"] == "ts"
    finally:
        model.con.disconnect()


def test_persisted_anchor_validation_preserves_source_precision_refusal():
    design = Design(unit="user", anchor=EventAnchor("occurred"))
    model = _submicro_model(1_700_000_000_000_000_123)
    try:
        ordinary = compile_anchors(design, model)
        persisted = ibis.memtable(model.con.to_pyarrow(ordinary.table))
        reused = compile_anchors(design, model, persisted=persisted)
        with pytest.raises(TerrineError) as error:
            reused.validate(execute=lambda query: query.to_pyarrow())
        assert error.value.code == "terrine.compile.anchor_timestamp_precision"
        assert error.value.context["column"] == "ts"
    finally:
        model.con.disconnect()


@pytest.mark.parametrize(
    ("anchor_range", "included"),
    [
        (None, True),
        (AnchorRange("1600-01-01", "2500-01-01"), True),
        (AnchorRange("1600-01-01", "1601-01-01"), False),
        (AnchorRange("2500-01-01", "2501-01-01"), False),
    ],
)
def test_microsecond_aligned_nanosecond_timestamp_remains_exact(anchor_range, included):
    design = Design(unit="user", anchor=EventAnchor("occurred"), anchor_range=anchor_range)
    model = _submicro_model(1_700_000_000_000_001_000)
    try:
        anchors = compile_anchors(design, model)
        anchors.validate(execute=lambda query: query.to_pyarrow())
        result = rows(anchors.table)
        assert [row["t0"] for row in result] == (
            [datetime(2023, 11, 14, 22, 13, 20, 1, tzinfo=UTC)] if included else []
        )
    finally:
        model.con.disconnect()


def test_aware_timestamp_identity_is_timezone_and_unrelated_column_independent(native_model):
    native_model.con.raw_sql("DELETE FROM events WHERE event_id <> 'c'")
    native_model.con.raw_sql("ALTER TABLE events ADD COLUMN event_id_ts TIMESTAMPTZ")
    native_model.con.raw_sql("UPDATE events SET event_id_ts = ts")
    design = Design(unit="user", anchor=EventAnchor("occurred"))
    for event_ids in ({}, {"events": "event_id_ts"}):
        model = Model.from_catalog(
            native_model.catalog,
            con=native_model.con,
            readiness=native_model.readiness,
            units=native_model.units,
            event_ids=event_ids,
        )
        identities = []
        for timezone in ("UTC", "America/Los_Angeles"):
            native_model.con.raw_sql(f"SET TimeZone = '{timezone}'")
            anchors = compile_anchors(design, model)
            anchors.validate(execute=lambda query: query.to_pyarrow())
            identities.append(rows(anchors.table)[0]["anchor_id"])
        assert identities[0] == identities[1]
    native_model.con.raw_sql("ALTER TABLE events ADD COLUMN _ANCHOR_IDENTITY_TEXT VARCHAR")
    native_model.con.raw_sql("UPDATE events SET _ANCHOR_IDENTITY_TEXT = 'unrelated'")
    with pytest.raises(TerrineError) as error:
        candidate_rows(design, model)
    assert error.value.code == "terrine.execution.invalid_input_binding"
    rebound = Model.from_catalog(
        model.catalog,
        con=model.con,
        readiness=model.readiness,
        units=model.units,
        event_ids=model.event_ids,
    )
    assert candidate_rows(design, rebound)[0]["anchor_id"] == identities[-1]
