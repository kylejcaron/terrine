import json
from datetime import UTC, datetime
from importlib.metadata import version
from typing import Any, cast

import pyarrow as pa
import pytest

import terrine as tr
from terrine.spec import DatasetDefinition, approx_quantile

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


def test_approx_quantile_needs_no_sketch_settings():
    pytest.importorskip("datasketches")
    model = tr.Model.from_tables(
        units={"user": tr.UnitSource(pa.table({"id": ["a"]}), key="id")},
        events={
            "orders": tr.Events(
                pa.table(
                    {
                        "user_id": ["a", "a", "a"],
                        "at": [datetime(2025, 1, 2, tzinfo=UTC)] * 3,
                        "amount": [2, 2, 2],
                    }
                ),
                key="user_id",
                time="at",
                metrics={"spend": tr.sum("amount")},
                complete=("2025-01-01", "2025-01-03"),
            ),
        },
    )
    try:
        result = (
            tr.Design(
                None,
                tr.FixedDate("2025-01-03"),
                features={"q": approx_quantile("spend", "0.5", window=("-1d", "0s"))},
            )
            .build(model)
            .materialize()
        )
        assert result.table.column("q").to_pylist() == [pytest.approx(2.0)]
        metadata = result.columns["q"].approximation
        assert metadata is not None
        assert metadata.algorithm == "tdigest"
        assert metadata.version == version("datasketches")
        assert dict(metadata.parameters) == {"k": 200}
        assert metadata.error_description == "t-digest empirical, no guaranteed bound"
    finally:
        model.con.disconnect()


def test_dataset_definition_takes_no_schema_version():
    design = tr.Design("user", tr.FixedDate("2025-01-03"))
    definition = DatasetDefinition(base_design=design)
    assert DatasetDefinition.from_json(definition.canonical_json()).base_design == design
    assert json.loads(definition.canonical_json())["schema_version"] == 3
    with pytest.raises(TypeError):
        cast(Any, DatasetDefinition)(schema_version=3, base_design=design)


def test_recipes_dir_shows_only_recipes():
    import terrine.recipes as r

    public = {name for name in dir(r) if not name.startswith("_")}
    assert public == set(r.__all__)
