from dataclasses import replace
from datetime import timedelta

import pyarrow as pa
import pytest

from terrine.data import Coverage
from terrine.simulate import WorldConfig, simulate
from tests.fixtures.baseline import boundary_world
from tests.reference.baseline import reference_rows


def test_reference_reads_raw_half_open_events_and_strict_history():
    world = boundary_world()
    rows = reference_rows(world).to_pylist()
    assert [row["unit_id"] for row in rows] == [1, 2, 3]
    assert rows[0]["t0"] == world.config.t0
    assert rows[0]["x"] == 0.25
    assert rows[0]["plan_tier"] == "basic"
    assert rows[0]["baseline_enabled"] is False
    assert rows[0]["net_revenue"] == 5  # left endpoint and ties; not the right endpoint
    assert rows[1]["net_revenue"] == 0  # roster member with no events
    assert rows[1]["net_revenue__complete"] is True
    assert rows[1]["net_revenue__reason"] is None
    assert rows[2]["x"] is None
    assert rows[2]["x__complete"] is False
    assert rows[2]["x__reason"] == "missing_history"
    assert rows[2]["net_revenue"] == 7
    altered = replace(
        world, analysis_table=world.analysis_table.slice(0, 0), oracle=world.oracle.slice(0, 0)
    )
    assert reference_rows(altered).equals(reference_rows(world))


@pytest.mark.parametrize(
    "bound,offset,complete",
    [
        ("observed_from", 0, True),
        ("observed_from", 1, False),
        ("observed_until", 0, True),
        ("observed_until", -1, False),
    ],
)
def test_reference_intersects_unit_observation_coverage(bound, offset, complete):
    world = boundary_world()
    endpoint = world.config.t0
    if bound == "observed_until":
        endpoint += timedelta(seconds=world.config.horizon_seconds)
    value = endpoint + timedelta(microseconds=offset)
    roster = world.roster.set_column(
        world.roster.schema.get_field_index(bound),
        bound,
        pa.array([value] * 3, type=pa.timestamp("us", tz="UTC")),
    )
    rows = reference_rows(replace(world, roster=roster)).to_pylist()
    for row in rows:
        assert row["net_revenue__complete"] is complete
        if complete:
            assert row["net_revenue__reason"] is None
        else:
            assert row["net_revenue"] is None
            assert row["net_revenue__reason"] == "incomplete_observation"
        assert row[f"net_revenue__{bound}"] == value


@pytest.mark.parametrize("side", ["start", "through"])
def test_reference_intersects_declared_source_coverage(side):
    world = boundary_world()
    coverage = world.readiness["world_events"]
    short = (
        Coverage(world.config.t0.date() + timedelta(days=1), coverage.through)
        if side == "start"
        else Coverage(coverage.start, world.config.t0.date())
    )
    rows = reference_rows(replace(world, readiness={"world_events": short})).to_pylist()
    assert len(rows) == 3
    for row in rows:
        assert row["net_revenue"] is None
        assert row["net_revenue__complete"] is False
        assert row["net_revenue__reason"] == "incomplete_coverage"


def test_nonempty_all_null_revenue_is_not_an_observed_zero():
    world = boundary_world()
    events = world.events.to_pylist()
    events.append(
        {
            "event_id": "null-posting",
            "unit_id": 2,
            "ts": world.config.t0,
            "kind": "revenue",
            "value": None,
        }
    )
    table = pa.Table.from_pylist(events, schema=world.events.schema)
    rows = reference_rows(replace(world, events=table)).to_pylist()
    assert rows[1]["net_revenue"] is None
    assert rows[1]["net_revenue__complete"] is True
    assert rows[1]["net_revenue__reason"] == "all_null"


@pytest.mark.parametrize("malformation", ["overlap", "duplicate", "reversed"])
def test_malformed_history_is_rejected_instead_of_arbitrarily_selected(malformation):
    world = boundary_world()
    rows = world.attributes.to_pylist()
    if malformation == "reversed":
        rows[0]["valid_to"] = rows[0]["valid_from"]
    elif malformation == "overlap":
        rows[0]["valid_to"] = world.config.t0 + timedelta(seconds=1)
    else:
        rows.append(dict(rows[0]))
    table = pa.Table.from_pylist(rows, schema=world.attributes.schema)
    with pytest.raises(ValueError):
        reference_rows(replace(world, attributes=table))


def test_generated_analysis_matches_independent_raw_rows_and_expected_dataset_oracle():
    world = simulate(WorldConfig(n_units=16), seed=17)
    assert reference_rows(world).equals(world.analysis_table)
    assert world.oracle.equals(world.analysis_table)
