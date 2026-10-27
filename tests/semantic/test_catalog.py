import copy
import pickle
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from terrine.data import Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.io import dump_catalog, load_catalog
from terrine.semantic.models import Catalog

CATALOG = {
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
            "dimensions": ["accounts"],
        }
    },
    "facts": {"session": {"source": "events", "column": None}},
    "properties": {},
    "dimensions": {
        "accounts": {
            "relation": {"kind": "table", "name": "accounts"},
            "source_entity": "workspace",
            "entity_column": "workspace_id",
            "validity": {"changed_at": "changed_at"},
            "properties": [],
        }
    },
    "measures": {"count": {"fact": "session", "aggregation": "count"}},
    "metrics": {"sessions": {"kind": "mean", "entity": "user", "measure": "count"}},
}


def test_events_descriptor_preserves_declarations_and_detaches_mutable_inputs():
    from terrine.data import Events

    keys = {"customer": "customer_id"}
    values = ["amount"]
    complete = ["2025-01-01", "2025-01-31"]
    events = Events("orders", "occurred_at", keys, values, complete)  # ty: ignore[invalid-argument-type]
    keys["customer"] = "changed"
    values.append("changed")
    complete[0] = "2024-01-01"
    assert events.table == "orders"
    assert events.time == "occurred_at"
    assert events.keys == {"customer": "customer_id"}
    assert events.values == ("amount",)
    assert events.complete == ("2025-01-01", "2025-01-31")
    with pytest.raises(TypeError):
        events.keys["customer"] = "changed"  # ty: ignore[invalid-assignment]
    with pytest.raises(FrozenInstanceError):
        events.table = "changed"  # ty: ignore[invalid-assignment]


@pytest.mark.parametrize(
    "changes",
    [
        {"table": ""},
        {"time": ""},
        {"keys": {"customer": ""}},
        {"keys": {1: "customer_id"}},
        {"values": ("",)},
        {"values": "amount"},
        {"complete": ("2025-01-31", "2025-01-01")},
        {"complete": ("2025-01-01",)},
        {"complete": (datetime(2025, 1, 1), date(2025, 1, 31))},
    ],
)
def test_events_descriptor_rejects_invalid_fields(changes):
    from terrine.data import Events

    arguments = {"table": "orders", "time": "occurred_at", "keys": {"customer": "customer_id"}}
    with pytest.raises((TypeError, ValueError)):
        Events(**(arguments | changes))


def test_from_tables_binds_generated_names_roster_and_options():
    import ibis

    import terrine as tr

    con = ibis.duckdb.connect()
    try:
        con.create_table("customers", schema=ibis.schema({"id": "int64"}))
        con.create_table(
            "order_events",
            schema=ibis.schema(
                {
                    "occurred_at": "timestamp",
                    "customer_id": "int64",
                    "amount": "float64",
                    "event_id": "string",
                }
            ),
        )
        roster = {"customer": UnitSource("customers", "id")}
        model = Model.from_tables(
            con,
            units=roster,
            events={
                "orders": tr.Events(
                    "order_events",
                    "occurred_at",
                    {"customer": "customer_id"},
                    ("amount",),
                    ("2025-01-01", "2025-01-31"),
                )
            },
            day_boundary="UTC+05:30",
            readiness={
                "orders": Coverage("2024-01-01", "2026-01-01"),
                "other": Coverage("2025-01-01", "2025-01-31"),
            },
            event_ids={"orders": "event_id"},
        )
        assert model.units == roster
        assert model.catalog.dialect == "duckdb"
        assert model.catalog.day_boundary == "UTC+05:30"
        assert model.event_ids == {"orders": "event_id"}
        assert set(model.sources) == {"orders"}
        assert model.sources["orders"].facts == {"orders": None, "orders_amount": "amount"}
        assert model.sources["orders"].entities == {"customer": "customer_id"}
        assert set(model.metrics) == {"orders_count", "orders_amount_sum", "orders_amount_mean"}
        for metric, fact, aggregation in (
            ("orders_count", "orders", "count"),
            ("orders_amount_sum", "orders_amount", "sum"),
            ("orders_amount_mean", "orders_amount", "avg_event"),
        ):
            assert model.metrics[metric].entity == "customer"
            assert model.metrics[metric].measures[0].fact == fact
            assert model.metrics[metric].measures[0].aggregation == aggregation
        assert model.readiness.sources["orders"].start == datetime(2024, 12, 31, 18, 30, tzinfo=UTC)
        assert model.readiness.sources["orders"].end_exclusive == datetime(
            2025, 1, 31, 18, 30, tzinfo=UTC
        )
        assert "other" in model.readiness.sources
    finally:
        con.disconnect()


@pytest.mark.parametrize(
    ("changes", "code", "context"),
    [
        (
            {"complete": None},
            "terrine.model.missing_event_completeness",
            {"source": "orders"},
        ),
        (
            {"keys": {"customer": "customer_id", "shop": "shop_id"}},
            "terrine.model.multiple_event_units",
            {"source": "orders", "units": ("customer", "shop")},
        ),
        (
            {"keys": {}},
            "terrine.model.invalid_binding",
            {},
        ),
        (
            {"keys": {"shop": "shop_id"}},
            "terrine.model.invalid_binding",
            {},
        ),
    ],
)
def test_from_tables_refuses_incomplete_or_undeclared_event_units(changes, code, context):
    from terrine.data import Events

    arguments = {
        "table": "orders",
        "time": "occurred_at",
        "keys": {"customer": "customer_id"},
        "complete": ("2025-01-01", "2025-01-31"),
    }
    with pytest.raises(TerrineError) as caught:
        Model.from_tables(
            object(),
            units={"customer": UnitSource("customers", "id")},
            events={"orders": Events(**(arguments | changes))},
        )
    error = caught.value
    assert error.code == code
    for key, value in context.items():
        assert error.context[key] == value
    with pytest.raises(TypeError):
        error.context["source"] = "changed"  # ty: ignore[invalid-assignment]
    for restored in (copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
        assert restored.code == error.code
        assert restored.context == error.context


@pytest.mark.parametrize(
    ("declarations", "readiness", "name", "sources"),
    [
        (
            {"orders": ("amount",), "orders_amount": ()},
            {},
            "orders_amount",
            ("orders", "orders_amount"),
        ),
        (
            {"orders": ("amount",), "orders_count": ()},
            {},
            "orders_count",
            ("orders", "orders_count"),
        ),
        (
            {"orders": ("amount",), "orders_amount_sum": ()},
            {},
            "orders_amount_sum",
            ("orders", "orders_amount_sum"),
        ),
        ({"orders": ("count",)}, {}, "orders_count", ("orders",)),
        ({"orders": ("amount", "amount_sum")}, {}, "orders_amount_sum", ("orders",)),
        ({"orders": ("amount", "amount")}, {}, "orders_amount", ("orders",)),
        (
            {"orders": ("amount",)},
            {"orders_amount_sum": Coverage("2025-01-01", "2025-01-31")},
            "orders_amount_sum",
            ("orders",),
        ),
        (
            {"orders": ("amount",)},
            {"orders_amount": Coverage("2025-01-01", "2025-01-31")},
            "orders_amount",
            ("orders",),
        ),
    ],
)
def test_from_tables_refuses_generated_name_collisions(declarations, readiness, name, sources):
    from terrine.data import Events

    events = {
        source: Events(
            "orders",
            "occurred_at",
            {"customer": "customer_id"},
            values,
            ("2025-01-01", "2025-01-31"),
        )
        for source, values in declarations.items()
    }
    with pytest.raises(TerrineError) as caught:
        Model.from_tables(
            object(),
            units={"customer": UnitSource("customers", "id")},
            events=events,
            readiness=readiness,
        )
    error = caught.value
    assert error.code == "terrine.model.generated_metric_collision"
    assert error.context["name"] == name
    assert error.context["sources"] == sources
    with pytest.raises(TypeError):
        error.context["name"] = "changed"  # ty: ignore[invalid-assignment]
    for restored in (copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
        assert restored.code == error.code
        assert restored.context == error.context


def test_from_catalog_mapping_binds_same_model_as_catalog():
    import ibis

    con = ibis.duckdb.connect()
    con.create_table(
        "events",
        schema=ibis.schema({"ts": "timestamp", "user_id": "int64", "workspace_id": "int64"}),
    )
    con.create_table(
        "accounts", schema=ibis.schema({"workspace_id": "int64", "changed_at": "timestamp"})
    )
    catalog = Catalog.model_validate(CATALOG)
    readiness = {"events": Coverage("2025-01-01", "2025-02-01")}
    from_mapping = Model.from_catalog(CATALOG, con=con, readiness=readiness)
    from_catalog = Model.from_catalog(catalog, con=con, readiness=readiness)
    assert from_mapping.catalog == from_catalog.catalog

    with pytest.raises(TerrineError) as error:
        Model.from_catalog(
            {**CATALOG, "schema_version": 2}, con=con, readiness=readiness
        )
    assert error.value.code == "terrine.catalog.invalid_definition"


def test_safe_yaml_roundtrips_native_mapping(tmp_path: Path):
    catalog = Catalog.model_validate(CATALOG)
    path = tmp_path / "catalog.yaml"
    path.write_text(dump_catalog(catalog))
    assert load_catalog(path) == catalog
    with pytest.raises(TypeError):
        catalog.sources["events"].entities["user"] = "workspace_id"  # ty: ignore[invalid-assignment]
    with pytest.raises(TypeError):
        catalog.sources["events"] = catalog.sources["events"]  # ty: ignore[invalid-assignment]


def test_catalog_rejects_untrusted_callbacks():
    unsafe = {
        **CATALOG,
        "metrics": {
            "sessions": {
                "kind": "mean",
                "entity": "user",
                "measure": "count",
                "winsorization": lambda value: value,
            }
        },
    }
    with pytest.raises(ValueError):
        Catalog.model_validate(unsafe)


@pytest.mark.parametrize(
    ("definition", "location", "kind"),
    [
        ({**CATALOG, "surprise": True}, ("surprise",), "extra_forbidden"),
        (
            {key: value for key, value in CATALOG.items() if key != "dialect"},
            ("dialect",),
            "missing",
        ),
        ({**CATALOG, "day_boundary": "UTC+14:01"}, ("day_boundary",), "invalid_day_boundary"),
        (
            {
                **CATALOG,
                "dimensions": {
                    "accounts": {
                        **CATALOG["dimensions"]["accounts"],
                        "validity": {"valid_from": "start", "changed_at": "change"},
                    }
                },
            },
            ("dimensions", "accounts", "validity"),
            "invalid_validity_axis",
        ),
        (
            {
                **CATALOG,
                "measures": {
                    "count": {
                        "fact": "session",
                        "aggregation": "count",
                        "filters": [{"property": "active", "op": "equals", "values": [1, 2]}],
                    }
                },
            },
            ("measures", "count", "filters", 0),
            "invalid_filter_arity",
        ),
        (
            {
                **CATALOG,
                "measures": {
                    "count": {
                        "fact": "session",
                        "aggregation": "count",
                        "filters": [{"property": "active", "op": "in", "values": [[1]]}],
                    }
                },
            },
            ("measures", "count", "filters", 0),
            "invalid_filter_values",
        ),
        (
            {**CATALOG, "metrics": {"sessions": {"kind": "mean", "entity": "user"}}},
            ("metrics", "sessions"),
            "invalid_metric_shape",
        ),
        (
            {
                **CATALOG,
                "metrics": {
                    "sessions": {
                        "kind": "mean",
                        "entity": "user",
                        "measure": "count",
                        "winsorization": {"not_json": {1}},
                    }
                },
            },
            ("metrics", "sessions"),
            "unsafe_transform",
        ),
    ],
)
def test_loader_reports_coded_actionable_catalog_schema_errors(
    tmp_path: Path, definition, location, kind
):
    import yaml

    path = tmp_path / "invalid.yaml"
    path.write_text(yaml.safe_dump(definition), encoding="utf-8")
    with pytest.raises(TerrineError) as caught:
        load_catalog(path)

    error = caught.value
    assert error.code == "terrine.catalog.invalid_definition"
    assert any(
        issue["location"] == location and issue["kind"] == kind for issue in error.context["issues"]
    )
    assert all(
        isinstance(issue["hint"], str) and issue["hint"] for issue in error.context["issues"]
    )
    if kind == "invalid_day_boundary":
        assert "day_boundary" in str(error)
        assert "UTC" in str(error)
        assert "14:00" in str(error)
        # Loading fails before Model binding can inspect any backend schema.
        with pytest.raises(TerrineError) as model_error:
            Model.from_catalog(path, con=object(), readiness={})
        assert model_error.value.code == error.code
        definition["day_boundary"] = "UTC"
        path.write_text(yaml.safe_dump(definition), encoding="utf-8")
        assert load_catalog(path).day_boundary == "UTC"

    issues = error.context["issues"]
    with pytest.raises(TypeError):
        issues[0]["kind"] = "changed"
    for restored in (copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
        assert restored.code == error.code

        assert restored.context == error.context


def test_catalog_rejects_unknown_fields_and_unsupported_versions():
    with pytest.raises(ValueError):
        Catalog.model_validate({**CATALOG, "surprise": True})
    with pytest.raises(ValueError):
        Catalog.model_validate({**CATALOG, "schema_version": 2})


@pytest.mark.parametrize("validity", [{}, {"valid_from": "start", "changed_at": "change"}])
def test_history_validity_requires_one_unambiguous_time_axis(validity):
    definition = Catalog.model_validate(CATALOG).model_dump(mode="python")
    definition["dimensions"]["accounts"]["validity"] = validity
    with pytest.raises(ValueError):
        Catalog.model_validate(definition)


def test_safe_yaml_rejects_duplicate_keys_and_python_tags(tmp_path: Path):
    for name, text in (
        ("duplicate", "schema_version: 1\nschema_version: 1\n"),
        ("tag", "!!python/object/apply:os.system ['echo unsafe']"),
    ):
        path = tmp_path / f"{name}.yaml"
        path.write_text(text)
        with pytest.raises(ValueError):
            load_catalog(path)
