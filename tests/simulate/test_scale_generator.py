from datetime import datetime, timedelta

import pyarrow as pa
import pytest

from terrine.simulate.scale import (
    ScaleConfig,
    anchor_batches,
    correction_batches,
    event_batches,
    oracle_batches,
    unit_batches,
)
from tests.reference.scale_metrics import metric_rows


def test_scale_generation_is_seed_and_version_reproducible():
    config = ScaleConfig(units=3, events_per_unit=7, batch_size=4, seed=19)
    first = pa.concat_tables(list(event_batches(config)))
    second = pa.concat_tables(list(event_batches(config)))
    assert first.equals(second)
    changed = pa.concat_tables(list(event_batches(ScaleConfig(**{**config.to_dict(), "seed": 20}))))
    assert not first.equals(changed)


def test_scale_batches_concatenate_to_partition_independent_world_and_oracle():
    config = ScaleConfig(units=4, events_per_unit=5, batch_size=3, seed=11)
    events = pa.concat_tables(list(event_batches(config)))
    expected = pa.concat_tables(list(oracle_batches(config)))
    assert events.num_rows == config.units * config.events_per_unit
    assert expected.num_rows == config.units
    assert events.schema.field("event_time").type == pa.timestamp("us", tz="UTC")
    assert len(set(events.column("event_id").to_pylist())) == events.num_rows
    assert events.column("unit_id").to_pylist()[:5] == ["unit-000000"] * 5
    assert expected.column("target_event_count").to_pylist() == [
        sum(
            row["event_type"] == "target"
            for row in events.to_pylist()
            if row["unit_id"] == f"unit-{i:06d}"
        )
        for i in range(config.units)
    ]
    whole = pa.concat_tables(
        list(event_batches(ScaleConfig(units=4, events_per_unit=5, batch_size=100, seed=11)))
    )
    assert events.equals(whole)


def test_generator_respects_configured_batch_bound():
    config = ScaleConfig(units=10, events_per_unit=13, batch_size=17, seed=2)
    batches = list(event_batches(config))
    assert all(batch.num_rows <= config.batch_size for batch in batches)
    assert sum(batch.num_rows for batch in batches) == 130


def test_zero_target_event_oracle_preserves_observed_units():
    config = ScaleConfig(units=1, events_per_unit=1, batch_size=1, seed=1)
    expected = pa.concat_tables(list(oracle_batches(config)))
    assert expected.to_pylist() == [{"unit_id": "unit-000000", "target_event_count": 0}]


def test_partition_order_preserves_typed_identity_and_oracle():
    config = ScaleConfig(units=5, events_per_unit=19, batch_size=7, seed=23)
    full = pa.concat_tables(list(event_batches(config))).sort_by("event_id")
    reordered = pa.concat_tables(
        [
            *event_batches(config, unit_start=2, unit_stop=5),
            *event_batches(config, unit_start=0, unit_stop=2),
        ]
    ).sort_by("event_id")
    assert full.equals(reordered)
    oracle = pa.concat_tables(list(oracle_batches(config))).sort_by("unit_id")
    reordered_oracle = pa.concat_tables(
        [
            *oracle_batches(config, unit_start=2, unit_stop=5),
            *oracle_batches(config, unit_start=0, unit_stop=2),
        ]
    ).sort_by("unit_id")
    assert oracle.equals(reordered_oracle)


def test_sparse_null_duplicate_tied_observations_and_corrections_are_explicit():
    config = ScaleConfig(units=4, events_per_unit=40, batch_size=7, seed=11, span_days=9)
    batches = list(event_batches(config))
    rows = pa.concat_tables(batches).to_pylist()
    assert all(batch.num_rows <= config.batch_size for batch in batches)
    assert any(row["value"] is None for row in rows)
    assert all(row["value"] is None or -50 <= row["value"] <= 50 for row in rows)
    by_occurrence = {}
    for row in rows:
        by_occurrence.setdefault((row["unit_id"], row["occurrence_id"]), set()).add(
            row["event_time"]
        )
    assert any(len(times) > 1 for times in by_occurrence.values())
    assert len({(row["unit_id"], row["event_time"]) for row in rows}) < len(rows)
    sparse_days = {row["event_time"].day for row in rows if row["unit_id"] == "unit-000002"}
    assert len(sparse_days) < config.span_days
    changes = list(correction_batches(config))
    assert all(batch.num_rows <= config.batch_size for batch in changes)
    corrections = pa.concat_tables(changes).to_pylist()
    assert {row["change_kind"] for row in corrections} == {"update", "delete"}
    observed_ids = {row["event_id"] for row in rows}
    assert all(row["event_id"] in observed_ids and row["revision"] == 2 for row in corrections)
    assert "q25" not in batches[0].column_names
    assert "event_time" not in next(oracle_batches(config)).column_names


def test_anchor_and_roster_batches_are_bounded_and_match_declared_schedule():
    config = ScaleConfig(units=5, events_per_unit=3, batch_size=4, anchor_periods=6)
    anchors = list(anchor_batches(config))
    roster = list(unit_batches(config))
    assert all(batch.num_rows <= config.batch_size for batch in (*anchors, *roster))
    assert sum(batch.num_rows for batch in anchors) == config.units * config.anchor_periods
    assert sum(batch.num_rows for batch in roster) == config.units
    assert anchors[0].schema.field("anchor_time").type == pa.timestamp("us", tz="UTC")
    assert len({row["unit_id"] for batch in roster for row in batch.to_pylist()}) == config.units


@pytest.mark.parametrize("corrected", [False, True])
def test_histogram_reference_matches_independent_pandas_windows(corrected):
    import pandas as pd

    config = ScaleConfig(units=3, events_per_unit=23, batch_size=7, span_days=5, window_days=3)
    observed = {
        row["event_id"]: row for batch in event_batches(config) for row in batch.to_pylist()
    }
    if corrected:
        for batch in correction_batches(config):
            for change in batch.to_pylist():
                if change["change_kind"] == "delete":
                    del observed[change["event_id"]]
                else:
                    observed[change["event_id"]] = {
                        key: value for key, value in change.items() if key != "change_kind"
                    }
    frame = pd.DataFrame(observed.values())
    for row in metric_rows(config, "schedule", corrected=corrected):
        t0 = row["t0"]
        assert isinstance(t0, datetime)
        selected = frame[
            (frame.unit_id == row["unit"])
            & (frame.event_time >= t0 - timedelta(days=config.window_days))
            & (frame.event_time < t0)
        ]
        values = selected.value.dropna()
        assert row["distinct"] == values.nunique()
        assert row["minimum"] == (None if values.empty else values.min())
        assert row["maximum"] == (None if values.empty else values.max())
        assert row["q25"] == (None if values.empty else values.quantile(0.25))


@pytest.mark.parametrize(
    "options",
    [
        {"version": 1},
        {"version": True},
        {"span_days": 0},
        {"anchor_periods": -1},
        {"window_days": True},
    ],
)
def test_generator_rejects_unsupported_versions_and_invalid_dimensions(options):
    with pytest.raises((TypeError, ValueError)):
        ScaleConfig(units=2, events_per_unit=3, batch_size=2, **options)


def test_large_unit_identifiers_keep_reference_order_without_materializing_the_roster():
    config = ScaleConfig(units=1_000_001, events_per_unit=1, batch_size=2)
    start, stop = 999_999, 1_000_001
    events = pa.concat_tables(list(event_batches(config, unit_start=start, unit_stop=stop)))
    identifiers = events["unit_id"].to_pylist()
    assert identifiers == sorted(identifiers)
    assert identifiers == ["unit-0999999", "unit-1000000"]
    for stream in (anchor_batches, unit_batches, oracle_batches, correction_batches):
        emitted = pa.concat_tables(list(stream(config, unit_start=start, unit_stop=stop)))
        assert set(emitted["unit_id"].to_pylist()) == set(identifiers)
    assert next(metric_rows(config, "frame"))["unit"] == "unit-0000000"
