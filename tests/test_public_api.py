import importlib
import json
from pathlib import Path
from typing import Any, cast

import griffe
import pytest

import terrine as tr
from scripts.api_check import PROVISIONAL_NAMES, STABLE_FACADES, stable_surface
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


def test_provisional_names_are_still_exported():
    expected = {
        "terrine.Dataset",
        "terrine.frame.Frame.stage",
        "terrine.frame.Frame.expr",
        "terrine.spec.Sequential",
        "terrine.spec.EventPanel",
        "terrine.spec.CalendarPanel",
        "terrine.spec.CalendarComparisonPanel",
        "terrine.spec.matched_t0",
        "terrine.spec.CalendarComparisonSpec",
        "terrine.windows.duration",
        "terrine.data.Provenance",
        "terrine.data.ColumnMeta",
        "terrine.data.Roles",
        "terrine.data.Preview",
        "terrine.data.ExpressionTiming",
        "terrine.data.SamplingMeta",
        "terrine.data.ExecutionSnapshot",
        "terrine.data.FeatureCutoff",
        "terrine.data.MaterializedFrame",
        "terrine.data.ApproximationMeta",
        "terrine.data.CalendarComparisonMeta",
        "terrine.data.CohortOperationMeta",
        "terrine.data.DatasetViewMeta",
        "terrine.data.SequenceMeta",
        "terrine.data.StatePanelMeta",
        "terrine.data.TreatmentHistoryMeta",
        "terrine.data.CoverageSummary",
        "terrine.data.EligibilitySummary",
    }
    assert expected <= PROVISIONAL_NAMES
    assert "terrine.metrics" in STABLE_FACADES
    assert "terrine.reasons" in STABLE_FACADES
    for qualified in PROVISIONAL_NAMES:
        parts = qualified.split(".")
        for module_end in range(len(parts), 0, -1):
            try:
                module = importlib.import_module(".".join(parts[:module_end]))
            except ModuleNotFoundError:
                continue
            for name in parts[module_end:]:
                module = getattr(module, name)
            assert module is not None
            break
        else:
            raise AssertionError(f"{qualified} does not resolve")


def test_stable_surface_excludes_provisional_members():
    root = Path(__file__).resolve().parents[1]
    package = cast(griffe.Module, griffe.load("terrine", search_paths=[str(root)]))
    surface = stable_surface(package)
    assert "terrine.frame.Frame.stage" not in surface
