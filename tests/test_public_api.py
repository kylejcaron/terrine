import importlib
import json
from typing import Any, cast

import griffe
import pytest

import terrine as tr
from scripts.api_check import PROVISIONAL_NAMES, STABLE_FACADES, stable_breakages, stable_surface
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


def test_provisional_frame_member_does_not_hide_stable_member_breakages(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    release = {
        "__init__.py": "from terrine.frame import Frame\n__all__ = ['Frame']\n",
        "frame.py": (
            "__all__ = ['Frame']\n\n"
            "class Frame:\n"
            "    def stage(self, path, snapshot): pass\n"
            "    def to_pandas(self, schema): pass\n"
        ),
    }
    current = {
        **release,
        "frame.py": release["frame.py"].replace(
            "stage(self, path, snapshot)", "stage(self, path)"
        ).replace("to_pandas(self, schema)", "to_pandas(self)"),
    }
    with (
        griffe.temporary_visited_package("terrine", release) as old,
        griffe.temporary_visited_package("terrine", current) as new,
    ):
        assert "terrine.Frame" in stable_surface(old)
        breakages = stable_breakages(old, new)

    assert len(breakages) == 1
    assert "to_pandas" in breakages[0]
    assert "schema" in breakages[0]
