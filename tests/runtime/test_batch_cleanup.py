import duckdb
import pyarrow as pa
import pytest

from terrine.runtime.backends.duckdb import DuckDBBackend, StagedGeneration
from tests.runtime.test_duckdb_publication import _lease


@pytest.mark.parametrize("started", [False, True])
def test_exact_generation_batch_close_releases_cursor(started):
    con = duckdb.connect()
    backend = DuckDBBackend(con, namespace="owner")
    receipt = backend.publish_generation(
        StagedGeneration("orders", "g1", {"frame": pa.table({"id": [1, 2]})}),
        expected_manifest=None,
        lease=_lease(con, "owner"),
    )
    cursors = []

    class Cursor:
        def __init__(self):
            self.raw = con.cursor()
            self.closed = False
            cursors.append(self)

        def execute(self, *args):
            self.raw.execute(*args)
            return self

        def to_arrow_reader(self, **kwargs):
            return self.raw.to_arrow_reader(**kwargs)

        def close(self):
            self.closed = True
            self.raw.close()

    class Connection:
        def execute(self, *args):
            return con.execute(*args)

        def cursor(self):
            return Cursor()

    backend.con = Connection()
    reader = backend.read_batches(receipt, "frame", batch_size=1)
    if started:
        assert next(reader).num_rows == 1
    reader.close()
    assert len(cursors) == int(started)
    assert all(cursor.closed for cursor in cursors)
    con.close()
