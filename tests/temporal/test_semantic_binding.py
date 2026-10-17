from collections.abc import Mapping
from datetime import UTC, date, datetime

import ibis
import pytest

from terrine.data import Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.bind import bind_metric, normalize_events, normalize_source
from terrine.semantic.models import Catalog


@pytest.fixture
def event_catalog():
    return Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id", "workspace": "workspace_id"},
                    "facts": ["session"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {"session": {"source": "events", "column": None}},
            "properties": {},
            "dimensions": {},
            "measures": {"count": {"fact": "session", "aggregation": "count"}},
            "metrics": {"sessions": {"kind": "mean", "entity": "user", "measure": "count"}},
        }
    )


def test_selected_logical_entity_resolves_only_its_physical_key(event_catalog):
    catalog = event_catalog
    metric = bind_metric(catalog, "sessions")
    assert metric.entity_columns == {"events": "user_id"}
    assert metric.measures[0].aggregation == "count"

    undeclared = catalog.model_dump(mode="python")
    undeclared["sources"]["events"]["facts"] = []
    with pytest.raises(TerrineError) as error:
        bind_metric(Catalog.model_validate(undeclared), "sessions")
    assert error.value.code == "terrine.semantic.missing_reference"


def test_ratio_binds_different_physical_keys_and_retains_filters():
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "numerator": {
                    "relation": {"kind": "table", "name": "n"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["click"],
                    "properties": ["kind"],
                    "dimensions": [],
                },
                "denominator": {
                    "relation": {"kind": "table", "name": "d"},
                    "timestamp": "ts",
                    "entities": {"user": "customer_id"},
                    "facts": ["visit"],
                    "properties": [],
                    "dimensions": [],
                },
            },
            "facts": {
                "click": {"source": "numerator", "column": "click_id"},
                "visit": {"source": "denominator", "column": None},
            },
            "properties": {
                "kind": {
                    "source": "numerator",
                    "column": "kind",
                    "dtype": "string",
                    "as_of": "event_time",
                }
            },
            "dimensions": {},
            "measures": {
                "clicks": {
                    "fact": "click",
                    "aggregation": "sum",
                    "filters": [{"property": "kind", "op": "equals", "value": "revenue"}],
                },
                "visits": {"fact": "visit", "aggregation": "count"},
            },
            "metrics": {
                "rate": {
                    "kind": "ratio",
                    "entity": "user",
                    "numerator": "clicks",
                    "denominator": "visits",
                    "winsorization": {"method": "fixed", "limit": 5},
                }
            },
        }
    )
    metric = bind_metric(catalog, "rate")
    assert metric.entity_columns == {"numerator": "user_id", "denominator": "customer_id"}
    assert metric.measures[0].filters[0].values == ("revenue",)
    assert metric.measures[0].source_name == "numerator"
    transform = metric.transforms[0]
    assert isinstance(transform, Mapping)
    assert transform["method"] == "fixed"
    assert metric.measures[1].aggregation == "count"


def test_normalized_events_keep_other_entity_keys(event_catalog):
    table = ibis.memtable(
        {
            "ts": [datetime(2025, 3, 1, tzinfo=UTC)],
            "user_id": ["u"],
            "workspace_id": ["w"],
            "sessions": [2],
            "birthday": [date(2000, 1, 1)],
        },
        schema={
            "ts": "timestamp('UTC')",
            "user_id": "string",
            "workspace_id": "string",
            "sessions": "int64",
            "birthday": "!date",
        },
    )
    definition = event_catalog.model_dump(mode="python")
    definition["facts"]["session"]["column"] = "sessions"
    definition["sources"]["events"]["properties"] = ["birthday"]
    definition["properties"]["birthday"] = {
        "source": "events",
        "column": "birthday",
        "dtype": "date",
        "as_of": "event_time",
    }
    catalog = Catalog.model_validate(definition)
    bound = normalize_source(_SchemaConnection(table), catalog, "events")
    events = normalize_events(table, bound, "user_id")
    con = ibis.duckdb.connect()
    try:
        result = con.to_pyarrow(events.select("_unit", "workspace_id", "birthday"))
    finally:
        con.disconnect()
    assert result.to_pylist() == [{"_unit": "u", "workspace_id": "w", "birthday": date(2000, 1, 1)}]

    mismatch = ibis.memtable({"ts": [datetime(2025, 3, 1, tzinfo=UTC)], "user_id": ["u"]})
    with pytest.raises(TerrineError) as error:
        normalize_source(_SchemaConnection(mismatch), catalog, "events")
    assert error.value.code == "terrine.semantic.schema_mismatch"


@pytest.mark.parametrize(
    ("column", "dtype"),
    [("ts", "array<timestamp>"), ("bad", "array<int64>")],
)
def test_nested_physical_types_are_not_scalar_bindings(event_catalog, column, dtype):
    definition = event_catalog.model_dump(mode="python")
    schema = {"ts": "timestamp('UTC')", "user_id": "string", "workspace_id": "string"}
    schema[column] = dtype
    if column == "bad":
        definition["sources"]["events"]["properties"] = ["bad"]
        definition["properties"]["bad"] = {
            "source": "events",
            "column": "bad",
            "dtype": "int",
            "as_of": "event_time",
        }
    relation = ibis.table(schema, name="events")
    with pytest.raises(TerrineError) as error:
        normalize_source(_SchemaConnection(relation), Catalog.model_validate(definition), "events")
    assert error.value.code == "terrine.semantic.schema_mismatch"


def test_dimension_filter_requires_dimension_property_membership(event_catalog):
    definition = event_catalog.model_dump(mode="python")
    definition["sources"]["events"]["properties"] = ["tier"]
    definition["sources"]["events"]["dimensions"] = ["accounts"]
    definition["properties"]["tier"] = {
        "source": "events",
        "column": "missing_tier",
        "dtype": "string",
        "as_of": "event_time",
        "dimension": "accounts",
    }
    definition["dimensions"]["accounts"] = {
        "relation": {"kind": "table", "name": "accounts"},
        "source_entity": "user",
        "entity_column": "user_id",
        "properties": [],
    }
    definition["measures"]["count"]["filters"] = [
        {"property": "tier", "op": "equals", "value": "premium"}
    ]
    catalog = Catalog.model_validate(definition)
    relation = ibis.table(
        {"ts": "timestamp('UTC')", "user_id": "string", "workspace_id": "string"},
        name="events",
    )
    for bind in (
        lambda: normalize_source(_SchemaConnection(relation), catalog, "events"),
        lambda: bind_metric(catalog, "sessions"),
    ):
        with pytest.raises(TerrineError) as error:
            bind()
        assert error.value.code == "terrine.semantic.missing_reference"


class _SchemaConnection:
    name = "duckdb"

    def __init__(self, table):
        self._table = table

    def table(self, name):
        return self._table


@pytest.mark.parametrize(
    ("boundary", "expected_start", "expected_end"),
    [
        (
            "UTC-05:00",
            datetime(2025, 3, 1, 5, tzinfo=UTC),
            datetime(2025, 4, 1, 5, tzinfo=UTC),
        ),
        (
            "+05:30",
            datetime(2025, 2, 28, 18, 30, tzinfo=UTC),
            datetime(2025, 3, 31, 18, 30, tzinfo=UTC),
        ),
    ],
)
def test_coverage_uses_catalog_fixed_boundary_and_readiness_is_per_source(
    boundary, expected_start, expected_end
):
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": boundary,
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
            "measures": {"count": {"fact": "session", "aggregation": "count"}},
            "metrics": {"sessions": {"kind": "mean", "entity": "user", "measure": "count"}},
        }
    )
    table = ibis.memtable({"ts": [datetime(2025, 3, 1, tzinfo=UTC)], "user_id": ["u"]})
    model = Model.from_catalog(
        catalog,
        con=_SchemaConnection(table),
        readiness={"events": Coverage("2025-03-01", "2025-03-31")},
    )
    coverage = model.readiness.for_fact("events")
    assert coverage.start == expected_start
    assert coverage.end_exclusive == expected_end
    assert model.day_boundary(Coverage("2025-03-01", "2025-03-31").start) == coverage.start
    with pytest.raises(AttributeError):
        model.model_hash = "drift"  # ty: ignore[invalid-assignment]
