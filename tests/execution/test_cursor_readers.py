"""Cursor ownership at the PostgreSQL and Snowflake driver boundaries."""

from collections import deque
from datetime import UTC, datetime
from types import SimpleNamespace

import ibis
import pyarrow as pa
import pytest

from terrine.execution import _postgres, _snowflake


@pytest.mark.parametrize("engine", ["postgres", "snowflake"])
def test_retained_closed_reader_releases_driver_cursor(engine, monkeypatch):
    opened = set()
    failure = OSError("fetch failed")
    fail = False

    class Cursor:
        def __init__(self, **kwargs):
            opened.add(self)
            self.rows = deque([(1,), (2,)])

        def execute(self, sql):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

        def close(self):
            opened.remove(self)

        def fetchmany(self, size):
            if fail:
                raise failure
            return [self.rows.popleft() for _ in range(min(size, len(self.rows)))]

        def fetch_arrow_batches(self):
            while batch := self.fetchmany(1):
                yield pa.table({"x": [batch[0][0]]})

    raw = SimpleNamespace(cursor=Cursor, adapters=SimpleNamespace(register_loader=lambda *a: None))
    con = SimpleNamespace(_run_pre_execute_hooks=lambda _: None)
    if engine == "postgres":
        pytest.importorskip("psycopg")
        session = _postgres.PostgresSession(raw)
    else:
        monkeypatch.setattr(_snowflake, "raw_connection", lambda _: raw)
        session = _snowflake.SnowflakeSession(con, datetime(2025, 1, 1, tzinfo=UTC), "2025-01-01")
    query = ibis.table({"x": "int64"}, name="t")
    for mode in ("unread", "partial", "exhausted", "error"):
        reader = session.batches(con, query, "SELECT x FROM t", chunk_size=1)
        assert len(opened) == (engine == "snowflake")
        if mode != "unread":
            assert next(reader).to_pylist() == [{"x": 1}]
            assert len(opened) == 1
        if mode == "exhausted":
            assert reader.read_all().to_pylist() == [{"x": 2}]
            assert not opened
        if mode == "error":
            fail = True
            with pytest.raises(OSError):
                next(reader)
            assert not opened
        reader.close()
        reader.close()
        assert not opened
