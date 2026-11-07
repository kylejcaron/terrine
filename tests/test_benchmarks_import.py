"""Fast smoke coverage for benchmark workload construction."""

from benchmarks import workloads
from terrine.simulate.scale import ScaleConfig


def test_benchmark_workloads_construct() -> None:
    """Benchmark workload designs construct from the smallest valid configuration."""
    config = ScaleConfig(
        units=1,
        events_per_unit=1,
        batch_size=1,
        span_days=1,
        anchor_periods=1,
        window_days=1,
    )

    assert set(workloads.designs(config)) == {
        "frame",
        "schedule",
        "sequential",
        "event_panel",
        "sketch",
    }
