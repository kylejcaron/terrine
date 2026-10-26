import copy
import pickle
from pathlib import Path

import pytest

from terrine.data import Coverage
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
