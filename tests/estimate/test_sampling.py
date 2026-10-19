import pytest

from terrine.estimate.sampling import NuisanceSample, select_nuisance_units


def test_nuisance_sample_is_deterministic_and_records_inclusion_weights():
    policy = NuisanceSample(max_rows=2, seed=7)

    first = select_nuisance_units(["a", "b", "c", "d"], policy)
    second = select_nuisance_units(["d", "b", "a", "c"], policy)

    assert first.selected_units == second.selected_units
    assert first.inclusion_probability == {unit: 0.5 for unit in first.selected_units}
    assert first.inverse_inclusion_weight == {unit: 2.0 for unit in first.selected_units}


def test_nuisance_sample_respects_strata_without_using_outcomes():
    policy = NuisanceSample(max_rows=2, seed=1, strata=("arm",))

    selected = select_nuisance_units(
        ["a", "b", "c", "d"], policy, strata={"a": (0,), "b": (0,), "c": (1,), "d": (1,)}
    )

    assert {"a", "b"}.intersection(selected.selected_units)
    assert {"c", "d"}.intersection(selected.selected_units)
    assert all(selected.inverse_inclusion_weight[unit] == 2.0 for unit in selected.selected_units)


def test_nuisance_sample_refuses_missing_or_duplicate_sampling_units():
    policy = NuisanceSample(max_rows=1, seed=0)

    with pytest.raises(ValueError):
        select_nuisance_units(["a", "a"], policy)


def test_nuisance_sample_metadata_is_immutable():
    selected = select_nuisance_units(["a", "b"], NuisanceSample(max_rows=1, seed=0))

    with pytest.raises(TypeError):
        selected.inclusion_probability["a"] = 0.25  # ty: ignore[invalid-assignment]
