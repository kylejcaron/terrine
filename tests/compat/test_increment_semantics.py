from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

import ibis
import pytest

from terrine.data import Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.io import load_catalog
from terrine.semantic.models import SqlQuery

increment_semantics = pytest.importorskip("increment.semantics")

_FIXTURES = Path(__file__).parent / "fixtures"
_READINESS = {
    "events": Coverage("2025-01-01", "2025-01-31"),
    "other_events": Coverage("2025-01-01", "2025-01-31"),
}


def test_increment_and_native_catalogs_preserve_dataset_meaning():
    con = ibis.duckdb.connect()
    model = Model.from_increment(_FIXTURES / "increment.yaml", con=con, readiness=_READINESS)
    catalog = model.catalog
    native = load_catalog(_FIXTURES / "catalog.yaml")
    actual = catalog.model_dump(mode="python")
    expected = native.model_dump(mode="python")
    actual_transform = actual["metrics"]["revenue"].pop("winsorization")
    expected_transform = expected["metrics"]["revenue"].pop("winsorization")
    # Dataset parity concerns clipping, not upstream estimator-inference defaults.
    assert {
        key: value
        for key, value in actual_transform.items()
        if value is not None and key != "inference"
    } == expected_transform

    assert actual == expected
    actual_relation = catalog.sources["events"].relation
    native_relation = native.sources["events"].relation
    assert isinstance(actual_relation, SqlQuery) and isinstance(native_relation, SqlQuery)
    actual_rows = con.sql(actual_relation.sql).to_pyarrow().to_pylist()
    native_rows = con.sql(native_relation.sql).to_pyarrow().to_pylist()
    assert (
        actual_rows
        == native_rows
        == [
            {
                "user_id": 1,
                "account_id": 101,
                "occurred_at": datetime(2025, 1, 1),
                "amount": 4.0,
                "platform": "mobile",
            },
            {
                "user_id": 2,
                "account_id": 202,
                "occurred_at": datetime(2025, 1, 2),
                "amount": 8.0,
                "platform": "web",
            },
        ]
    )
    assert catalog.schema_version == 2
    assert catalog.sources["events"].units == {
        "user_id": "user_id",
        "account_id": "account_id",
    }
    assert catalog.sources["events"].events == ("spend", "purchase", "visit")
    assert catalog.events["visit"].source == "events"
    assert catalog.events["visit"].column is None
    assert catalog.dimensions["region"].unit == "user_id"
    assert catalog.dimensions["region"].key == "user_id"
    assert catalog.measures["revenue"].event == "spend"
    assert catalog.metrics["purchases"].unit == "account_id"
    assert catalog.properties["region"].source == "events"
    assert catalog.dimensions["lifecycle"].validity is None
    region_validity = catalog.dimensions["region"].validity
    changes_validity = catalog.dimensions["changes"].validity
    assert region_validity is not None and region_validity.valid_from == "valid_from"
    assert changes_validity is not None and changes_validity.changed_at == "changed_at"
    assert catalog.measures["revenue"].window_days == 30
    assert catalog.measures["purchases"].window_days == 7
    winsorization = catalog.metrics["revenue"].winsorization
    assert isinstance(winsorization, Mapping)
    assert winsorization["lower_value"] == 0.0
    assert winsorization["upper_percentile"] == 0.99
    assert catalog.measures["other_revenue"].filters[0].property == "other_events.platform"
    assert catalog.measures["other_revenue"].filters[3].property == "region"
    assert model.metrics["other_revenue"].measures[0].filters[3].property_name == "region"
    con.disconnect()


def test_published_definitions_object_and_yaml_directory_are_supported(tmp_path):
    from terrine.adapters.increment import to_catalog

    definitions_path = _FIXTURES / "increment.yaml"
    definitions = increment_semantics.load(definitions_path)
    object_catalog = to_catalog(definitions, dialect="duckdb")
    assert definitions.week_start == "sunday"
    assert [exposure.name for exposure in definitions.exposures] == ["assignment"]
    assert [experiment.name for experiment in definitions.experiments] == ["unprojected"]
    assert "unused" not in object_catalog.dimensions
    assert "unused" not in object_catalog.properties
    assert object_catalog.properties["region"].source == "events"
    assert object_catalog.properties["other_events.platform"].as_of == "pre_exposure"

    definitions_dir = tmp_path / "definitions"
    definitions_dir.mkdir()
    (definitions_dir / "catalog.yaml").write_text(
        definitions_path.read_text(encoding="utf-8"), encoding="utf-8"
    )
    con = ibis.duckdb.connect()
    model = Model.from_increment(definitions_dir, con=con, readiness=_READINESS)
    assert model.metrics["other_revenue"].measures[0].filters[3].property_name == "region"
    con.disconnect()


def test_scoped_names_preserve_same_named_source_and_dimension_properties():
    from terrine.adapters.increment import to_catalog

    definitions = increment_semantics.load(_FIXTURES / "same-name-collision.yaml")
    catalog = to_catalog(definitions, dialect="duckdb")

    assert set(catalog.properties) == {"source.shared.region", "dimension.shared.region"}
    assert catalog.sources["shared"].properties == ("source.shared.region",)
    assert catalog.dimensions["shared"].properties == ("dimension.shared.region",)
    assert catalog.properties["dimension.shared.region"].source == "consumer"


def test_duplicate_yaml_keys_are_refused(tmp_path):
    path = tmp_path / "duplicate.yaml"
    path.write_text("fact_sources: []\nfact_sources: []\n", encoding="utf-8")
    from terrine.adapters.increment import to_catalog

    with pytest.raises(TerrineError) as error:
        to_catalog(path, dialect="duckdb")
    assert error.value.code == "terrine.increment.duplicate_yaml_key"


def test_invalid_upstream_definition_is_wrapped_with_safe_native_context(tmp_path):
    from terrine.adapters.increment import to_catalog

    with pytest.raises(TerrineError) as error:
        to_catalog(tmp_path / "missing.yaml", dialect="duckdb")
    assert error.value.code == "terrine.increment.invalid_definitions"
    assert error.value.context["upstream_code"] == "definition.path.missing"


def test_unsupported_increment_metric_uses_native_temporal_refusal():
    from terrine.adapters.increment import to_catalog

    with pytest.raises(TerrineError) as error:
        to_catalog(_FIXTURES / "unsupported-metric.yaml", dialect="duckdb")
    assert error.value.code == "terrine.metric.unsupported_temporal_kind"
