from datetime import date
from typing import cast

import pytest

from terrine.errors import TerrineError
from terrine.spec import Design, FixedDate
from terrine.spec.dataset import DatasetRecipe, DatasetRecipeReference, DatasetRecipeStore


def test_recipe_reference_resolves_only_from_configured_owner_store(tmp_path) -> None:
    recipe = DatasetRecipe(schema_version=1, base_design=design())
    store = DatasetRecipeStore(tmp_path / "owner-a", owner="owner-a")
    reference = store.put(recipe)
    assert reference == DatasetRecipeReference("owner-a", recipe.digest)
    assert store.resolve(reference).canonical_json() == recipe.canonical_json()
    other = DatasetRecipeStore(tmp_path / "owner-b", owner="owner-b")
    with pytest.raises(TerrineError):
        other.resolve(reference)


def test_recipe_reference_store_refuses_corrupt_content(tmp_path) -> None:
    recipe = DatasetRecipe(schema_version=1, base_design=design())
    store = DatasetRecipeStore(tmp_path / "owner-a", owner="owner-a")
    reference = store.put(recipe)
    (tmp_path / "owner-a" / f"{recipe.digest}.json").write_bytes(b"corrupt")
    with pytest.raises(TerrineError):
        store.resolve(reference)


def test_recipe_reference_missing_object_uses_invalid_recipe_refusal(tmp_path) -> None:
    recipe = DatasetRecipe(schema_version=1, base_design=design())
    store = DatasetRecipeStore(tmp_path / "owner-a", owner="owner-a")
    reference = store.put(recipe)
    (store.root / f"{recipe.digest}.json").unlink()
    with pytest.raises(TerrineError) as error:
        store.resolve(reference)
    assert error.value.code == "terrine.dataset.invalid_recipe"


def test_recipe_reference_rejects_non_string_fields() -> None:
    with pytest.raises(TerrineError) as error:
        DatasetRecipeReference("owner", cast(str, None))
    assert error.value.code == "terrine.dataset.invalid_recipe"


def test_concurrent_identical_recipe_puts_publish_complete_content(tmp_path) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    recipe = DatasetRecipe(schema_version=1, base_design=design())
    store = DatasetRecipeStore(tmp_path / "owner-a", owner="owner-a")
    barrier = Barrier(2)

    def put():
        barrier.wait()
        return store.put(recipe)

    with ThreadPoolExecutor(max_workers=2) as executor:
        references = list(executor.map(lambda _: put(), range(2)))
    assert references == [DatasetRecipeReference("owner-a", recipe.digest)] * 2
    assert store.resolve(references[0]).canonical_json() == recipe.canonical_json()


def design() -> Design:
    return Design(unit="unit", anchor=FixedDate(date(2025, 1, 1)))


def test_dataset_recipe_round_trip_reconstructs_base_design_and_view() -> None:
    recipe = DatasetRecipe(
        schema_version=1,
        base_design=design(),
        view={"kind": "training", "features": ["x"], "labels": ["y"]},
    )
    decoded = DatasetRecipe.from_json(recipe.canonical_json())
    assert decoded.base_design == recipe.base_design
    assert decoded.view == recipe.view
    assert decoded.digest == recipe.digest


def test_recipe_canonical_bytes_ignore_mapping_insertion_order() -> None:
    first = DatasetRecipe(
        schema_version=1,
        base_design=design(),
        view={
            "kind": "scoring",
            "features": ["x"],
            "eligibility": {"b": 2, "a": 1},
            "as_of": "2025-01-01T00:00:00+00:00",
        },
    )
    second = DatasetRecipe(
        schema_version=1,
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


def test_dataset_recipe_rejects_unknown_fields_tags_and_runtime_values() -> None:
    with pytest.raises(TerrineError):
        DatasetRecipe.from_json(b'{"schema_version":1,"base_design":{},"view":null,"extra":1}')
    with pytest.raises(TerrineError):
        DatasetRecipe(
            schema_version=1, base_design=design(), view={"kind": "training", "unknown": "x"}
        )
    with pytest.raises(TerrineError):
        DatasetRecipe(
            schema_version=1,
            base_design=design(),
            view={"kind": "training", "features": [object()]},
        )


def test_recipe_put_removes_temporary_file_after_io_failure(tmp_path, monkeypatch) -> None:
    import os

    recipe = DatasetRecipe(schema_version=1, base_design=design())
    store = DatasetRecipeStore(tmp_path / "owner-a", owner="owner-a")

    failure = OSError("injected fsync failure")

    def fail_fsync(_):
        raise failure

    monkeypatch.setattr(os, "fsync", fail_fsync)
    with pytest.raises(OSError) as error:
        store.put(recipe)
    assert error.value is failure
    assert list(store.root.iterdir()) == []
