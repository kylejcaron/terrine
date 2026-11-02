import json
from typing import Any, cast

import pytest

import terrine as tr
from terrine.spec import DatasetDefinition

ROOT = {
    "Model",
    "UnitSource",
    "Events",
    "History",
    "Coverage",
    "count",
    "sum",
    "mean",
    "min",
    "max",
    "count_distinct",
    "ratio",
    "Design",
    "FixedDate",
    "EventAnchor",
    "Schedule",
    "AnchorRange",
    "agg",
    "asof",
    "event_time",
    "since",
    "eq",
    "gt",
    "is_null",
    "all_of",
    "recipes",
    "Dataset",
    "Budget",
    "preview",
    "to_yaml",
    "from_yaml",
    "TerrineError",
    "UnverifiedBackendWarning",
}


def test_root_exports_the_everyday_workflow():
    assert set(tr.__all__) == ROOT
    assert set(dir(tr)) == ROOT
    for name in ROOT:
        assert getattr(tr, name) is not None
    for name in ("Frame", "Window", "DatasetDefinition", "SketchRequest", "TimeToEvent"):
        with pytest.raises(AttributeError):
            getattr(tr, name)


def test_model_has_no_public_constructor():
    with pytest.raises(TypeError) as error:
        tr.Model(catalog=None)
    assert all(name in str(error.value) for name in ("from_tables", "from_catalog"))


def test_dataset_definition_takes_no_schema_version():
    design = tr.Design("user", tr.FixedDate("2025-01-03"))
    definition = DatasetDefinition(base_design=design)
    assert DatasetDefinition.from_json(definition.canonical_json()).base_design == design
    assert json.loads(definition.canonical_json())["schema_version"] == 4
    with pytest.raises(TypeError):
        cast(Any, DatasetDefinition)(schema_version=4, base_design=design)


def test_windows_exports_duration_values_and_the_parser():
    import terrine.windows as windows

    assert windows.__all__ == ["Window", "Duration", "Months", "duration"]


def test_recipes_dir_shows_only_recipes():
    import terrine.recipes as r

    public = {name for name in dir(r) if not name.startswith("_")}
    assert public == set(r.__all__)
