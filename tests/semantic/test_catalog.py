from pathlib import Path

import pytest

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
            "dimensions": [],
        }
    },
    "facts": {"session": {"source": "events", "column": None}},
    "properties": {},
    "dimensions": {},
    "measures": {"count": {"fact": "session", "aggregation": "count"}},
    "metrics": {"sessions": {"kind": "mean", "entity": "user", "measure": "count"}},
}


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


def test_catalog_rejects_unknown_fields_and_unsupported_versions():
    with pytest.raises(ValueError):
        Catalog.model_validate({**CATALOG, "surprise": True})
    with pytest.raises(ValueError):
        Catalog.model_validate({**CATALOG, "schema_version": 2})


def test_safe_yaml_rejects_duplicate_keys_and_python_tags(tmp_path: Path):
    for name, text in (
        ("duplicate", "schema_version: 1\nschema_version: 1\n"),
        ("tag", "!!python/object/apply:os.system ['echo unsafe']"),
    ):
        path = tmp_path / f"{name}.yaml"
        path.write_text(text)
        with pytest.raises(ValueError):
            load_catalog(path)
