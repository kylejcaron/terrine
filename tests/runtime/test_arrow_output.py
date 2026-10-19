from __future__ import annotations

import pyarrow as pa
import pytest


def test_durable_arrow_output_is_bounded_verified_and_metadata_preserving(tmp_path):
    from terrine.runtime.arrow import ArrowOutput

    schema = pa.schema([pa.field("value", pa.decimal128(38, 0))], metadata={b"contract": b"exact"})
    batches = [
        pa.record_batch([pa.array([i], type=schema.field(0).type)], schema=schema) for i in range(5)
    ]
    output = ArrowOutput.write(pa.RecordBatchReader.from_batches(schema, batches), root=tmp_path)
    restored = ArrowOutput.from_bytes(output.to_bytes())
    assert restored.num_rows == 5
    assert restored.schema.equals(schema, check_metadata=True)
    with restored.open() as reader:
        assert [batch.num_rows for batch in reader] == [1] * 5
    restored.path.write_bytes(b"corrupt")
    with pytest.raises(ValueError):
        restored.open()


def test_arrow_output_memory_tracks_batches_not_total_rows(tmp_path):
    from terrine.runtime.arrow import ArrowOutput

    schema = pa.schema([pa.field("value", pa.string())])
    baseline = pa.total_allocated_bytes()
    live = []

    def batches():
        for _ in range(200):
            batch = pa.record_batch([pa.array(["x" * 100] * 1024)], schema=schema)
            live.append(pa.total_allocated_bytes() - baseline)
            yield batch

    output = ArrowOutput.write(pa.RecordBatchReader.from_batches(schema, batches()), root=tmp_path)
    assert output.num_rows == 200 * 1024
    assert max(live) < output.nbytes // 10
