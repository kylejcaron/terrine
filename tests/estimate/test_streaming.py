import pyarrow as pa
import pytest

from terrine.estimate.streaming import MaterializedBatchSource


def test_batch_source_reopens_same_generation_with_bounded_batches(frame):
    source = MaterializedBatchSource(frame, snapshot_id="snapshot-1", generation_id="generation-4")

    first = tuple(source.iter_batches(17))
    reopened = source.reopen()
    second = tuple(reopened.iter_batches(17))

    assert source.snapshot_id == reopened.snapshot_id == "snapshot-1"
    assert source.generation_id == reopened.generation_id == "generation-4"
    assert [batch.num_rows for batch in first] == [17] * 7 + [1]
    assert pa.Table.from_batches(first).equals(pa.Table.from_batches(second))
    assert reopened.roles == frame.roles
    assert reopened.columns == frame.columns


def test_batch_source_refuses_invalid_batch_size_and_closed_reads(frame):
    source = MaterializedBatchSource(frame, snapshot_id="snapshot-1", generation_id="generation-4")

    with pytest.raises(ValueError):
        tuple(source.iter_batches(0))
    source.close()
    with pytest.raises(RuntimeError):
        tuple(source.iter_batches(10))


def test_batch_source_cancel_closes_and_reopen_is_independent(frame):
    source = MaterializedBatchSource(frame, snapshot_id="snapshot-1", generation_id="generation-4")
    reopened = source.reopen()

    source.cancel()

    assert source.closed
    assert not reopened.closed
    assert sum(batch.num_rows for batch in reopened.iter_batches(100)) == frame.table.num_rows
