from dataclasses import replace
from datetime import timedelta

import pyarrow as pa
import pytest

from terrine.data import FeatureCutoff, MaterializedFrame
from terrine.simulate import WorldConfig, simulate
from terrine.windows import Window
from tests.fixtures.baseline import boundary_world, materialized_baseline


@pytest.mark.parametrize("weighted", [False, True])
def test_materialized_baseline_has_typed_honest_causal_metadata(weighted):
    world = simulate(WorldConfig(n_units=8, target_weight_slope=0.25), seed=7)
    frame = materialized_baseline(world, weighted=weighted)
    assert frame.table.equals(world.analysis_table)
    assert frame.roles.unit == "unit_id"
    assert frame.roles.anchor == "anchor_id" and frame.roles.t0 == "t0"
    assert frame.roles.features == ("x", "plan_tier")
    assert frame.roles.outcomes == ("net_revenue",)
    assert frame.roles.treatment == "baseline_enabled"
    assert frame.roles.weight == ("target_weight" if weighted else None)
    provenance = frame.provenance
    cutoff = FeatureCutoff(0, True, "retrospective_effective_time")
    assert provenance.feature_cutoffs == {"x": cutoff, "plan_tier": cutoff}
    assert provenance.eligibility_cutoffs == {}
    assert provenance.eligibility_summary is not None
    assert provenance.eligibility_summary.candidate == provenance.eligibility_summary.retained == 8
    assert provenance.eligibility_summary.unknown == 0
    assert provenance.treatment_design == "baseline_binary"
    assert provenance.treatment_known_at_t0 is True
    assert provenance.treatment_knowledge == "retrospective_effective_time"
    assert provenance.weight_kind == ("baseline_function" if weighted else None)
    assert provenance.weight_cutoff == (cutoff if weighted else None)
    assert provenance.weight_depends_on_treatment == (False if weighted else None)
    assert provenance.execution_snapshot.backend == "arrow"
    assert provenance.execution_snapshot.snapshot_id is None
    assert provenance.execution_snapshot.replay_equivalence == "unverified"
    assert set(provenance.source_generations) == {
        "world_roster",
        "world_events",
        "world_attributes",
    }
    meta = frame.columns["net_revenue"]
    assert meta.window == Window(0, world.config.horizon_seconds)
    assert meta.numeric_policy is None and meta.absolute_error_column is None
    assert frame.table[meta.complete_column].to_pylist() == [True] * 8
    assert frame.table[meta.reason_column].to_pylist() == [None] * 8
    assert MaterializedFrame.from_arrow(frame.to_arrow()).table.equals(frame.table)


def test_fixture_preserves_incomplete_rows_and_null_reasons():
    world = boundary_world()
    until = world.config.t0 + timedelta(seconds=1)
    roster = world.roster.set_column(
        2, "observed_until", pa.array([until, None, None], type=pa.timestamp("us", tz="UTC"))
    )
    frame = materialized_baseline(replace(world, roster=roster))
    row = frame.table.to_pylist()[0]
    assert frame.table.num_rows == 3
    meta = frame.columns["net_revenue"]
    assert row["net_revenue"] is None
    assert row[meta.complete_column] is False
    assert row[meta.reason_column] == "incomplete_observation"
    assert row[meta.observed_until_column] == until
    restored = MaterializedFrame.from_arrow(frame.to_arrow())
    assert restored.table.to_pylist()[0][meta.reason_column] == "incomplete_observation"
