from datetime import date
from importlib.resources import files
from typing import cast

import pytest

from terrine.errors import TerrineError
from terrine.spec import Design, FixedDate
from terrine.spec.dataset import (
    DatasetDefinition,
    DatasetDefinitionReference,
    DatasetDefinitionStore,
)


def test_definition_reference_resolves_only_from_configured_owner_store(tmp_path) -> None:
    definition = DatasetDefinition(schema_version=2, base_design=design())
    store = DatasetDefinitionStore(tmp_path / "owner-a", owner="owner-a")
    reference = store.put(definition)
    assert reference == DatasetDefinitionReference("owner-a", definition.digest)
    assert store.resolve(reference).canonical_json() == definition.canonical_json()
    other = DatasetDefinitionStore(tmp_path / "owner-b", owner="owner-b")
    with pytest.raises(TerrineError):
        other.resolve(reference)


def test_definition_reference_store_refuses_corrupt_content(tmp_path) -> None:
    definition = DatasetDefinition(schema_version=2, base_design=design())
    store = DatasetDefinitionStore(tmp_path / "owner-a", owner="owner-a")
    reference = store.put(definition)
    (tmp_path / "owner-a" / f"{definition.digest}.json").write_bytes(b"corrupt")
    with pytest.raises(TerrineError):
        store.resolve(reference)


def test_definition_reference_missing_object_uses_invalid_definition_refusal(tmp_path) -> None:
    definition = DatasetDefinition(schema_version=2, base_design=design())
    store = DatasetDefinitionStore(tmp_path / "owner-a", owner="owner-a")
    reference = store.put(definition)
    (store.root / f"{definition.digest}.json").unlink()
    with pytest.raises(TerrineError) as error:
        store.resolve(reference)
    assert error.value.code == "terrine.dataset.invalid_definition"


def test_definition_reference_rejects_non_string_fields() -> None:
    with pytest.raises(TerrineError) as error:
        DatasetDefinitionReference("owner", cast(str, None))
    assert error.value.code == "terrine.dataset.invalid_definition"


def test_concurrent_identical_definition_puts_publish_complete_content(tmp_path) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    definition = DatasetDefinition(schema_version=2, base_design=design())
    store = DatasetDefinitionStore(tmp_path / "owner-a", owner="owner-a")
    barrier = Barrier(2)

    def put():
        barrier.wait()
        return store.put(definition)

    with ThreadPoolExecutor(max_workers=2) as executor:
        references = list(executor.map(lambda _: put(), range(2)))
    assert references == [DatasetDefinitionReference("owner-a", definition.digest)] * 2
    assert store.resolve(references[0]).canonical_json() == definition.canonical_json()


def design() -> Design:
    return Design(unit="unit", anchor=FixedDate(date(2025, 1, 1)))


def test_dataset_definition_round_trip_reconstructs_base_design_and_view() -> None:
    definition = DatasetDefinition(
        schema_version=2,
        base_design=design(),
        view={"kind": "training", "features": ["x"], "labels": ["y"]},
    )
    decoded = DatasetDefinition.from_json(definition.canonical_json())
    assert decoded.base_design == definition.base_design
    assert decoded.view == definition.view
    assert decoded.digest == definition.digest


def test_definition_canonical_bytes_ignore_mapping_insertion_order() -> None:
    first = DatasetDefinition(
        schema_version=2,
        base_design=design(),
        view={
            "kind": "scoring",
            "features": ["x"],
            "eligibility": {"b": 2, "a": 1},
            "as_of": "2025-01-01T00:00:00+00:00",
        },
    )
    second = DatasetDefinition(
        schema_version=2,
        base_design=design(),
        view={
            "as_of": "2025-01-01T00:00:00+00:00",
            "eligibility": {"a": 1, "b": 2},
            "features": ["x"],
            "kind": "scoring",
        },
    )
    assert first.canonical_json() == second.canonical_json()
    assert first.digest == second.digest


def test_dataset_definition_rejects_unknown_fields_tags_and_runtime_values() -> None:
    with pytest.raises(TerrineError):
        DatasetDefinition.from_json(b'{"schema_version":2,"base_design":{},"view":null,"extra":1}')
    with pytest.raises(TerrineError):
        DatasetDefinition(
            schema_version=2, base_design=design(), view={"kind": "training", "unknown": "x"}
        )
    with pytest.raises(TerrineError):
        DatasetDefinition(
            schema_version=2,
            base_design=design(),
            view={"kind": "training", "features": [object()]},
        )


def test_definition_put_removes_temporary_file_after_io_failure(tmp_path, monkeypatch) -> None:
    import os

    definition = DatasetDefinition(schema_version=2, base_design=design())
    store = DatasetDefinitionStore(tmp_path / "owner-a", owner="owner-a")

    failure = OSError("injected fsync failure")

    def fail_fsync(_):
        raise failure

    monkeypatch.setattr(os, "fsync", fail_fsync)
    with pytest.raises(OSError) as error:
        store.put(definition)
    assert error.value is failure
    assert list(store.root.iterdir()) == []


def test_definition_bytes_load_with_same_digest() -> None:
    serialized = files("tests.fixtures").joinpath("definition_v2.json").read_bytes()
    definition = DatasetDefinition.from_json(serialized)
    assert definition.canonical_json() == serialized
    assert definition.digest == "6b8d4f43afa6587197f8b8b6a776532f969d42ae2e1c80339a0a924911fbb74b"


def test_v1_definition_refuses() -> None:
    serialized = files("tests.fixtures").joinpath("pre_rename_definition.json").read_bytes()
    with pytest.raises(TerrineError) as error:
        DatasetDefinition.from_json(serialized)
    assert error.value.code == "terrine.dataset.invalid_definition"


def test_manifest_json_with_renamed_fields_refuses() -> None:
    from terrine.dataset.manifest import DatasetManifest

    serialized = files("tests.fixtures").joinpath("pre_rename_manifest.json").read_bytes()
    with pytest.raises(TerrineError) as error:
        DatasetManifest.from_json(serialized)
    assert error.value.code == "terrine.dataset.invalid_manifest"


@pytest.mark.parametrize("artifact", ["definition", "transform"])
def test_pre_rename_fitted_identity_refuses_with_invalid_definition(artifact):
    from terrine.spec.transforms import Winsorization

    filename, decode = {
        "definition": ("pre_rename_fitted_definition.json", DatasetDefinition.from_json),
        "transform": ("pre_rename_fitted_transform.json", Winsorization.from_json),
    }[artifact]
    serialized = files("tests.fixtures").joinpath(filename).read_bytes()
    with pytest.raises(TerrineError) as error:
        decode(serialized)
    assert error.value.code == "terrine.dataset.invalid_definition"
