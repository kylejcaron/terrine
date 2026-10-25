from datetime import UTC, datetime, timedelta, timezone

import numpy as np
import pyarrow as pa
import pytest
from pydantic import ValidationError

from terrine.errors import TerrineError
from terrine.simulate import WorldConfig, simulate


def test_same_seed_reproduces_all_tables_and_metadata():
    config = WorldConfig(n_units=12)
    left, right = simulate(config, seed=2**64 - 1), simulate(config, seed=2**64 - 1)
    for name in ("roster", "events", "attributes", "analysis_table", "oracle"):
        assert getattr(left, name).equals(getattr(right, name))
    assert left.metadata == right.metadata
    assert left.oracle.equals(left.analysis_table)
    assert left.readiness == right.readiness
    assert left.units == right.units
    assert left.event_ids == right.event_ids == {"world_events": "event_id"}
    assert left.semantic_yaml == right.semantic_yaml
    assert len(set(left.events["event_id"].to_pylist())) == left.events.num_rows
    assert left.roster.schema.field("unit_id").type == pa.int64()
    assert left.events.schema.field("ts").type == pa.timestamp("us", tz="UTC")
    assert left.roster["observed_from"].null_count == config.n_units
    assert left.roster["observed_until"].null_count == config.n_units
    assert left.units["user"].key == "unit_id"
    coverage = left.readiness["world_events"]
    assert coverage.start <= (config.t0 - timedelta(seconds=config.lookback_seconds)).date()
    assert coverage.through >= (config.t0 + timedelta(seconds=config.horizon_seconds)).date()
    with pytest.raises(TypeError):
        left.metadata["seed"] = 4  # ty: ignore[invalid-assignment]
    with pytest.raises(TypeError):
        left.readiness["extra"] = coverage  # ty: ignore[invalid-assignment]
    with pytest.raises(ValidationError):
        config.n_units = 4  # ty: ignore[invalid-assignment]


def test_child_streams_keep_baselines_and_assignment_independent_of_outcome_noise_and_timing():
    config = WorldConfig(n_units=16, outcome_noise_sd=0)
    base = simulate(config, seed=31)
    noisy = simulate(config.model_copy(update={"outcome_noise_sd": 3}), seed=31)
    moved = simulate(config.model_copy(update={"horizon_seconds": 50}), seed=31)
    assert base.attributes.equals(noisy.attributes)
    assert base.attributes.equals(moved.attributes)
    for name in ("x", "plan_tier", "baseline_enabled"):
        assert base.analysis_table[name].equals(noisy.analysis_table[name])
        assert base.analysis_table[name].equals(moved.analysis_table[name])
    seeds = np.random.SeedSequence(31).spawn(4)
    baseline = np.random.Generator(np.random.PCG64(seeds[0]))
    x = baseline.uniform(-1, 1, config.n_units)
    tier = baseline.choice(["basic", "plus", "enterprise"], config.n_units, p=[0.5, 0.3, 0.2])
    np.testing.assert_array_equal(base.analysis_table["x"], x)
    np.testing.assert_array_equal(base.analysis_table["plan_tier"], tier)
    assert not base.oracle.equals(simulate(config, seed=32).oracle)


@pytest.mark.parametrize(
    "changes",
    [
        {"n_units": 0},
        {"n_units": True},
        {"lookback_seconds": 0},
        {"horizon_seconds": 1.5},
        {"outcome_noise_sd": -1},
        {"propensity_quadratic": float("nan")},
        {"target_weight_slope": -1},
        {"target_weight_slope": 1},
        {"t0": datetime(2025, 3, 3)},
        {"t0": "2025-03-03T12:34:56.123456789Z"},
        {"t0": datetime(9999, 12, 30, 12, tzinfo=UTC), "horizon_seconds": 86400},
        {"t0": datetime.min.replace(tzinfo=timezone(timedelta(hours=14)))},
    ],
)
def test_invalid_world_config_is_rejected(changes):
    with pytest.raises(ValidationError):
        WorldConfig(**changes)


def test_nonfinite_observed_outcomes_refuse():
    config = WorldConfig(n_units=256, outcome_noise_sd=1.7e308)
    with pytest.raises(TerrineError) as error:
        simulate(config, seed=31)
    assert error.value.code == "terrine.simulate.nonfinite"


@pytest.mark.parametrize("seed", [-1, 2**64, True, 1.5])
def test_seed_must_be_an_unsigned_64_bit_integer(seed):
    with pytest.raises(TerrineError) as exc:
        simulate(WorldConfig(n_units=1), seed=seed)
    assert exc.value.code == "terrine.simulate.invalid_seed"


def test_aware_time_zero_preserves_the_instant():
    config = WorldConfig(n_units=1, t0=datetime.fromisoformat("2025-03-03T14:34:56+02:00"))
    assert config.t0 == datetime(2025, 3, 3, 12, 34, 56, tzinfo=UTC)
    assert simulate(config, seed=0).analysis_table["t0"][0].as_py() == config.t0
