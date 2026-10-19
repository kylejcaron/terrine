"""Independent raw-source generators for baseline and repeated-trial worlds."""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from importlib.resources import files

import numpy as np
import pyarrow as pa

from terrine.data import Coverage, UnitSource
from terrine.errors import RefusalSpec
from terrine.simulate.world import World, WorldConfig

_INVALID_SEED = RefusalSpec("terrine.simulate.invalid_seed", "Use an integer seed in [0, 2**64).")
_NONFINITE = RefusalSpec(
    "terrine.simulate.nonfinite",
    "Generated {quantity} is nonfinite; reduce coefficient or noise magnitudes.",
)
_TIMESTAMP = pa.timestamp("us", tz="UTC")


def _baseline_values(config: WorldConfig, baseline) -> dict[str, np.ndarray]:
    x = baseline.uniform(-1, 1, config.n_units)
    tier = baseline.choice(["basic", "plus", "enterprise"], config.n_units, p=[0.5, 0.3, 0.2])
    plus, enterprise = tier == "plus", tier == "enterprise"
    h = x * x - 1 / 3
    with np.errstate(over="ignore", invalid="ignore"):
        eta = -0.2 + 0.8 * x + 0.5 * plus - 0.4 * enterprise + config.propensity_quadratic * h
        e = np.exp(-np.logaddexp(0, -eta))
        mean = 10 + 2 * x + 1.5 * plus - enterprise + config.outcome_quadratic * h
        q = 1 + config.target_weight_slope * x
    arrays = {"x": x, "e": e, "mean": mean, "target_weight": q}
    for name, array in arrays.items():
        if not np.isfinite(array).all():
            raise _NONFINITE.error(quantity=name)
    return {
        "unit_id": np.arange(1, config.n_units + 1, dtype=np.int64),
        "x": x,
        "plan_tier": tier,
        **{name: array for name, array in arrays.items() if name != "x"},
    }

def _raw_tables(config: WorldConfig, oracle, assigned, observed, timing):
    n = config.n_units
    ids = oracle["unit_id"]
    start = config.t0 - timedelta(seconds=config.lookback_seconds)
    roster = pa.table(
        {
            "unit_id": pa.array(ids, type=pa.int64()),
            "observed_from": pa.nulls(n, type=_TIMESTAMP),
            "observed_until": pa.nulls(n, type=_TIMESTAMP),
        }
    )
    attributes = pa.table(
        {
            "unit_id": pa.array(ids, type=pa.int64()),
            "valid_from": pa.array([start] * n, type=_TIMESTAMP),
            "valid_to": pa.nulls(n, type=_TIMESTAMP),
            "x": pa.array(oracle["x"], type=pa.float64()),
            "plan_tier": pa.array(oracle["plan_tier"], type=pa.string()),
            "baseline_enabled": pa.array(assigned, type=pa.bool_()),
        }
    )
    revenue_offsets = timing.integers(0, config.horizon_seconds * 1_000_000, size=n)
    event_rows = [
        {
            "event_id": f"revenue:{int(unit)}",
            "unit_id": int(unit),
            "ts": config.t0 + timedelta(microseconds=int(offset)),
            "kind": "revenue",
            "value": float(value),
        }
        for unit, offset, value in zip(ids, revenue_offsets, observed, strict=True)
    ]
    counts = np.maximum(0, np.floor(2 + 2 * oracle["x"])).astype(np.int64)
    for unit, count in zip(ids, counts, strict=True):
        offsets = timing.integers(1, config.lookback_seconds * 1_000_000 + 1, size=int(count))
        for index, offset in enumerate(offsets):
            event_rows.append(
                {
                    "event_id": f"engagement:{int(unit)}:{index}",
                    "unit_id": int(unit),
                    "ts": config.t0 - timedelta(microseconds=int(offset)),
                    "kind": "engagement",
                    "value": 1.0,
                }
            )
    events = pa.Table.from_pylist(
        event_rows,
        schema=pa.schema(
            [
                ("event_id", pa.string()),
                ("unit_id", pa.int64()),
                ("ts", _TIMESTAMP),
                ("kind", pa.string()),
                ("value", pa.float64()),
            ]
        ),
    )
    return roster, events, attributes


def _analysis_table(config: WorldConfig, oracle, assigned, observed, coverage: Coverage):
    n = config.n_units
    table = pa.table(
        {
            "unit_id": pa.array(oracle["unit_id"], type=pa.int64()),
            "anchor_id": pa.array([f"fixed:{config.t0.isoformat()}"] * n, type=pa.string()),
            "t0": pa.array([config.t0] * n, type=_TIMESTAMP),
            "x": pa.array(oracle["x"], type=pa.float64()),
            "plan_tier": pa.array(oracle["plan_tier"], type=pa.string()),
            "baseline_enabled": pa.array(assigned, type=pa.bool_()),
            "net_revenue": pa.array(observed, type=pa.float64()),
            "target_weight": pa.array(oracle["target_weight"], type=pa.float64()),
        }
    )
    first = datetime.combine(coverage.start, time.min, UTC)
    last = datetime.combine(coverage.through + timedelta(days=1), time.min, UTC)
    for name in ("x", "plan_tier", "baseline_enabled", "net_revenue"):
        for suffix, array in (
            ("observed_from", pa.array([first] * n, type=_TIMESTAMP)),
            ("observed_until", pa.array([last] * n, type=_TIMESTAMP)),
            ("complete", pa.array([True] * n, type=pa.bool_())),
            ("reason", pa.nulls(n, type=pa.string())),
        ):
            table = table.append_column(f"{name}__{suffix}", array)
    return table


def simulate(config: WorldConfig, seed: int) -> World:
    """Generate a fixed-cohort raw-source world from independent PCG64 streams."""
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed < 2**64:
        raise _INVALID_SEED.error()
    children = np.random.SeedSequence(seed).spawn(4)
    baseline, assignment, outcome, timing = [
        np.random.Generator(np.random.PCG64(child)) for child in children
    ]
    values = _baseline_values(config, baseline)
    assigned = assignment.binomial(1, values["e"], config.n_units).astype(bool)
    plus = values["plan_tier"] == "plus"
    observed = (
        values["mean"] + assigned * (2 + 0.75 * values["x"] + 0.5 * plus)
        + outcome.normal(0, config.outcome_noise_sd, config.n_units)
    )
    roster, events, attributes = _raw_tables(config, values, assigned, observed, timing)
    coverage = Coverage(
        (config.t0 - timedelta(seconds=config.lookback_seconds)).date(),
        (config.t0 + timedelta(seconds=config.horizon_seconds)).date(),
    )
    analysis_table = _analysis_table(config, values, assigned, observed, coverage)
    return World(
        config=config,
        metadata={
            "generator_version": 1,
            "seed": seed,
            "rng": "PCG64",
            "streams": {"baseline": 0, "assignment": 1, "outcomes": 2, "event_timing": 3},
        },
        roster=roster,
        events=events,
        attributes=attributes,
        analysis_table=analysis_table,
        oracle=analysis_table,
        semantic_yaml=files("terrine.simulate")
        .joinpath("baseline.yaml")
        .read_text(encoding="utf-8"),
        readiness={"world_events": coverage},
        units={
            "user": UnitSource(
                table="world_roster",
                key="unit_id",
                observed_from="observed_from",
                observed_until="observed_until",
            )
        },
        event_ids={"world_events": "event_id"},
    )


