import json
from dataclasses import replace
from datetime import date
from decimal import Decimal
from typing import cast

import pytest

from terrine.errors import TerrineError
from terrine.spec import Design, FixedDate
from terrine.spec.dataset import (
    DatasetDefinition,
    DatasetDefinitionReference,
    DatasetDefinitionStore,
)
from terrine.spec.io import from_yaml, to_yaml


def test_definition_reference_resolves_only_from_configured_owner_store(tmp_path) -> None:
    definition = DatasetDefinition(base_design=design())
    store = DatasetDefinitionStore(tmp_path / "owner-a", owner="owner-a")
    reference = store.put(definition)
    assert reference == DatasetDefinitionReference("owner-a", definition.digest)
    assert store.resolve(reference).canonical_json() == definition.canonical_json()
    other = DatasetDefinitionStore(tmp_path / "owner-b", owner="owner-b")
    with pytest.raises(TerrineError):
        other.resolve(reference)


def test_definition_reference_store_refuses_corrupt_content(tmp_path) -> None:
    definition = DatasetDefinition(base_design=design())
    store = DatasetDefinitionStore(tmp_path / "owner-a", owner="owner-a")
    reference = store.put(definition)
    (tmp_path / "owner-a" / f"{definition.digest}.json").write_bytes(b"corrupt")
    with pytest.raises(TerrineError):
        store.resolve(reference)


def test_definition_reference_missing_object_uses_invalid_definition_refusal(tmp_path) -> None:
    definition = DatasetDefinition(base_design=design())
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

    definition = DatasetDefinition(base_design=design())
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
        base_design=design(),
        view={"kind": "training", "features": ["x"], "labels": ["y"]},
    )
    decoded = DatasetDefinition.from_json(definition.canonical_json())
    assert decoded.base_design == definition.base_design
    assert decoded.view == definition.view
    assert decoded.digest == definition.digest


def test_definition_nested_records_omit_default_fields() -> None:
    from terrine.dataset.splits import GroupSplit, TimeSplit, policy_from_record
    from terrine.spec.transforms import Winsorization

    groups = {
        "kind": "group",
        "group": "unit",
        "train_fraction": "0.8",
        "validation_fraction": "0.1",
        "seed": 0,
        "codec": "terrine.group.v1",
    }
    split = {
        "kind": "time",
        "train_until": "2025-01-15",
        "validation_until": None,
        "embargo_microseconds": 0,
        "groups": groups,
    }
    definition = DatasetDefinition(
        base_design=design(),
        view={
            "kind": "training",
            "features": ["x"],
            "labels": ["y"],
            "incomplete_labels": "drop",
            "split": split,
        },
        transforms=(
            Winsorization("x", "0", "1", source_type="int64", output_precision=20, output_scale=0),
        ),
    )
    payload = json.loads(definition.canonical_json())
    assert payload["view"] == {
        "kind": "training",
        "features": ["x"],
        "labels": ["y"],
        "split": {
            "kind": "time",
            "train_until": "2025-01-15",
            "groups": {"kind": "group", "codec": "terrine.group.v1"},
        },
    }
    assert payload["transforms"] == [
        {
            "kind": "winsorization",
            "schema_version": 1,
            "column": "x",
            "lower": "0",
            "upper": "1",
            "source_type": "int64",
            "output_precision": 20,
            "output_scale": 0,
        }
    ]
    assert DatasetDefinition.from_json(definition.canonical_json()) == definition
    assert policy_from_record(payload["view"]["split"]) == TimeSplit(
        train_until="2025-01-15", groups=GroupSplit()
    )


@pytest.mark.parametrize("seed", [False, 0.0])
def test_wrongly_typed_split_default_is_kept_and_refused(native_inputs, seed) -> None:
    from tests.dataset.test_training_view import _frame

    frame = _frame(native_inputs)
    view = {
        "kind": "training",
        "features": ["past_sessions"],
        "labels": ["churn"],
        "split": {"kind": "group", "seed": seed, "codec": "terrine.group.v1"},
    }
    stored = DatasetDefinition(base_design=frame._design, view=view).canonical_json()
    with pytest.raises(TerrineError) as error:
        DatasetDefinition.from_json(stored).build(frame._model)
    assert error.value.code == "terrine.dataset.invalid_split"
    assert (error.value.context["reason"], error.value.context["field"]) == ("wrong_type", "seed")


def test_edited_definition_has_new_digest() -> None:
    loaded = from_yaml(to_yaml(design()))
    definition = DatasetDefinition.from_json(DatasetDefinition(base_design=loaded).canonical_json())
    edited = replace(definition, base_design=replace(loaded, unit="account"))
    assert edited.digest != definition.digest


def test_definition_canonical_bytes_ignore_mapping_insertion_order() -> None:
    first = DatasetDefinition(
        base_design=design(),
        view={
            "kind": "scoring",
            "features": ["x"],
            "eligibility": {"b": 2, "a": 1},
            "as_of": "2025-01-01T00:00:00+00:00",
        },
    )
    second = DatasetDefinition(
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
        DatasetDefinition.from_json(b'{"schema_version":4,"base_design":{},"view":null,"extra":1}')
    with pytest.raises(TerrineError):
        DatasetDefinition(base_design=design(), view={"kind": "training", "unknown": "x"})
    with pytest.raises(TerrineError):
        DatasetDefinition(
            base_design=design(),
            view={"kind": "training", "features": [object()]},
        )


def test_definition_put_removes_temporary_file_after_io_failure(tmp_path, monkeypatch) -> None:
    import os

    definition = DatasetDefinition(base_design=design())
    store = DatasetDefinitionStore(tmp_path / "owner-a", owner="owner-a")

    failure = OSError("injected fsync failure")

    def fail_fsync(_):
        raise failure

    monkeypatch.setattr(os, "fsync", fail_fsync)
    with pytest.raises(OSError) as error:
        store.put(definition)
    assert error.value is failure
    assert list(store.root.iterdir()) == []


def test_v3_definition_document_refuses_with_version() -> None:
    serialized = (
        b'{"base_design":{"administrative_cutoff":null,"anchor":{"date":{"type":"date",'
        b'"value":"2025-01-05"},"type":"FixedDate"},"anchor_range":null,"eligibility":{},'
        b'"features":{},"gap":0,"outcomes":{},"sample":null,"shape":"cross_section",'
        b'"treatment":null,"unit":"user"},"schema_version":3,"view":null}'
    )
    with pytest.raises(TerrineError) as error:
        DatasetDefinition.from_json(serialized)
    assert error.value.code == "terrine.dataset.invalid_definition"
    assert error.value.context["reason"] == "unsupported_schema_version"
    assert error.value.context["field"] == "schema_version"
    assert error.value.context["expected"] == 4
    assert error.value.context["schema_version"] == 3


def test_definition_version_gate_precedes_field_decoding() -> None:
    with pytest.raises(TerrineError) as error:
        DatasetDefinition.from_json(
            b'{"schema_version":4.0,"base_design":null,"view":{"kind":true}}'
        )
    assert error.value.code == "terrine.dataset.invalid_definition"
    assert error.value.context["reason"] == "unsupported_schema_version"
    assert error.value.context["schema_version"] == 4.0


def test_definition_with_v2_base_design_refuses_with_code() -> None:
    serialized = (
        b'{"schema_version":4,"base_design":{"unit":"user","anchor":'
        b'{"type":"EventAnchor","event":"signup","which":"first"}},"view":null}'
    )
    with pytest.raises(TerrineError) as error:
        DatasetDefinition.from_json(serialized)
    assert error.value.code == "terrine.dataset.invalid_definition"


def test_fitted_transform_with_unknown_fit_field_refuses() -> None:
    from terrine.spec.transforms import Winsorization, WinsorizationFit

    fit = WinsorizationFit(
        *(character * 64 for character in "abcdef"),
        snapshot={"snapshot_id": "snapshot"},
        split_policy={"kind": "time", "train_until": "2025-01-15"},
        probabilities=(Decimal("0.25"), Decimal("0.75")),
        row_count=3,
        non_null_count=3,
    )
    transform = Winsorization(
        "x", "0", "1", source_type="int64", output_precision=20, output_scale=0, fit=fit
    )
    record = json.loads(transform.to_json())
    record["fit"]["recipe_hash"] = record["fit"].pop("definition_hash")
    with pytest.raises(TerrineError) as error:
        Winsorization.from_json(json.dumps(record))
    assert error.value.code == "terrine.dataset.invalid_definition"
