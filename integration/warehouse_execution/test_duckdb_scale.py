"""Explicit local DuckDB evidence, not a live-warehouse result; scheduled scale tier."""

import pytest

from benchmarks.run_duckdb import run
from terrine.simulate.scale import ScaleConfig

pytestmark = pytest.mark.scale


def test_retained_compiler_generations_and_bounded_ranges_match_independent_oracles():
    config = ScaleConfig(
        units=3,
        events_per_unit=20,
        batch_size=7,
        seed=20261018,
        span_days=9,
        anchor_periods=4,
        window_days=7,
    )
    record = run(config)
    assert record["backend"] == "duckdb" and record["status"] == "verified"
    assert record["input_events"] == config.units * config.events_per_unit
    assert record["oracle_match"]
    for case in record["cases"]:
        assert case["oracle_match"]
        assert case["emitted_rows"] <= case["budget"]["max_rows"]
        for bound in case["range_bounds"].values():
            assert bound["requests"] <= bound["request_bound"]
            assert bound["candidates"] <= bound["candidate_bound"]
    assert record["runtime"]["baseline"]["observed_cost"]["output_rows"] == config.units
    assert record["runtime"]["corrected"]["observed_cost"]["output_rows"] == config.units
    assert record["runtime"]["old_generation_unchanged"]
    assert record["runtime"]["corrected"]["estimated_scan_bytes"] is None
