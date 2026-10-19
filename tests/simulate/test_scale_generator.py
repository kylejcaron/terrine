import pyarrow as pa

from terrine.simulate.scale import ScaleConfig, event_batches, oracle_batches


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
