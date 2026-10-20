"""Local compiler scale checks; run with the scheduled scale tier."""

import pytest

from benchmarks.run_duckdb import run
from terrine.simulate.scale import ScaleConfig

pytestmark = pytest.mark.scale


@pytest.mark.parametrize(("units", "events", "anchors", "batch"), [(2, 17, 4, 3), (3, 29, 5, 7)])
def test_real_compiler_metrics_shapes_bounds_and_runtime_generations(units, events, anchors, batch):
    config = ScaleConfig(
        units=units,
        events_per_unit=events,
        anchor_periods=anchors,
        batch_size=batch,
        seed=19,
        span_days=9,
        window_days=7,
    )
    record = run(config)
    assert record["status"] == "verified" and record["oracle_match"]
    assert record["input_events"] == config.units * config.events_per_unit
    assert record["declared_schedule_anchors"] == config.units * config.anchor_periods
    assert record["max_source_batch_rows"] <= config.batch_size
    cases = {case["case"]: case for case in record["cases"]}
    assert set(cases) == {"frame", "schedule", "sequential", "event_panel", "sketch"}
    expected_rows = {
        "frame": units,
        "sketch": units,
        "schedule": units * anchors,
        "sequential": units * anchors,
        "event_panel": 3 * units,
    }
    for name, case in cases.items():
        assert case["oracle_match"] and case["emitted_rows"] == expected_rows[name]
        assert case["physical_plan"]
        for bound in case["range_bounds"].values():
            assert bound["requests"] <= 2 * case["anchors"]
            assert bound["candidates"] <= bound["max_candidates_per_event"] * record["input_events"]
    assert cases["sketch"]["approximation"]["tdigest"]["version"] == "5.2.0"
    assert cases["sketch"]["approximate_errors"]["hll_max_absolute_error"] is not None
    runtime = record["runtime"]
    assert runtime["baseline"]["oracle_match"] and runtime["corrected"]["oracle_match"]
    assert runtime["baseline"]["snapshot_id"] != runtime["corrected"]["snapshot_id"]
    assert runtime["old_generation_unchanged"]
    assert record["source_digest"] != record["corrected_source_digest"]
    assert (
        runtime["corrected_input_events"]
        == record["input_events"] - runtime["corrections"]["deletes"]
    )
    assert set(runtime["strict_unknown_refusals"].values()) == {"terrine.budget.scan_unknown"}
    assert record["scan_bytes"] is None and record["scan_cost"] is None


def test_compiler_and_oracle_are_invariant_to_source_batch_partitioning():
    first = run(ScaleConfig(units=2, events_per_unit=15, batch_size=2, seed=11, span_days=9))
    second = run(ScaleConfig(units=2, events_per_unit=15, batch_size=11, seed=11, span_days=9))
    assert first["source_digest"] == second["source_digest"]
    assert {case["case"]: case["output_digest"] for case in first["cases"]} == {
        case["case"]: case["output_digest"] for case in second["cases"]
    }
    assert (
        first["runtime"]["corrected"]["output_digest"]
        == second["runtime"]["corrected"]["output_digest"]
    )
