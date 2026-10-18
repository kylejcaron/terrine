from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from terrine.runtime.leases import LeaseStore


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
