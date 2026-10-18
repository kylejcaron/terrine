from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from terrine.runtime.leases import LeaseStore


def test_runtime_timestamps_are_naive_utc_timestamps_with_microsecond_precision():
    con = duckdb.connect(":memory:")
    store = LeaseStore(con, namespace="owner")
    store.initialize()
    instant = datetime(2025, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    lease = store.acquire("dataset:orders", "run-a", ttl=timedelta(microseconds=1), now=instant)
    type_row = con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema='owner' AND table_name='runtime_leases' "
        "AND column_name='expires_at'"
    ).fetchone()
    value_row = con.execute(
        'SELECT expires_at FROM "owner".runtime_leases WHERE resource=?',
        ["dataset:orders"],
    ).fetchone()
    assert type_row is not None and value_row is not None
    stored_type = type_row[0]
    stored = value_row[0]
    assert stored_type == "TIMESTAMP"
    assert stored == datetime(2025, 1, 1, 0, 0, 0, 123457)
    assert lease.expires_at == datetime(2025, 1, 1, 0, 0, 0, 123457, tzinfo=UTC)


def test_lease_heartbeat_renewal_and_fencing():
    store = LeaseStore(duckdb.connect(":memory:"), namespace="owner")
    store.initialize()
    now = datetime(2025, 1, 1, tzinfo=UTC)
    first = store.acquire("dataset:orders", "run-a", ttl=timedelta(seconds=5), now=now)
    renewed = store.renew(first, ttl=timedelta(seconds=5), now=now + timedelta(seconds=1))
    assert renewed.fence == first.fence
    assert renewed.expires_at == now + timedelta(seconds=6)
    with pytest.raises(ValueError):
        store.acquire(
            "dataset:orders", "run-b", ttl=timedelta(seconds=5), now=now + timedelta(seconds=2)
        )
    second = store.acquire(
        "dataset:orders", "run-b", ttl=timedelta(seconds=5), now=now + timedelta(seconds=7)
    )
    assert second.fence > first.fence
    with pytest.raises(ValueError):
        store.assert_current(first)
