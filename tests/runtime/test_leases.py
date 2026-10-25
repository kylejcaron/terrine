from datetime import UTC, datetime, timedelta

import duckdb
import pytest

import terrine.runtime.leases as leases_module


def test_runtime_timestamps_are_naive_utc_timestamps_with_microsecond_precision():
    con = duckdb.connect(":memory:")
    store = leases_module.LeaseStore(con, namespace="owner")
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


def test_initialize_migrates_existing_lease_timestamp_values_to_naive_utc():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute(
        "CREATE TABLE owner.runtime_leases (resource VARCHAR PRIMARY KEY, "
        "owner VARCHAR NOT NULL, fence BIGINT NOT NULL, expires_at VARCHAR NOT NULL)"
    )
    con.execute(
        "INSERT INTO owner.runtime_leases VALUES (?, ?, ?, ?)",
        ["dataset:orders", "run-a", 1, "2025-01-01T02:00:00.123456+02:00"],
    )
    store = leases_module.LeaseStore(con, namespace="owner")
    store.initialize()
    row = con.execute("SELECT expires_at, typeof(expires_at) FROM owner.runtime_leases").fetchone()
    assert row == (datetime(2025, 1, 1, 0, 0, 0, 123456), "TIMESTAMP")
    with pytest.raises(ValueError):
        store.acquire(
            "dataset:orders",
            "run-b",
            ttl=timedelta(seconds=1),
            now=datetime(2025, 1, 1, tzinfo=UTC),
        )


def test_lease_initialization_migrates_timestamp_with_case_variant_namespace():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute(
        "CREATE TABLE owner.runtime_leases (resource VARCHAR PRIMARY KEY, "
        "owner VARCHAR NOT NULL, fence BIGINT NOT NULL, expires_at VARCHAR NOT NULL)"
    )
    con.execute(
        "INSERT INTO owner.runtime_leases VALUES (?, ?, ?, ?)",
        ["dataset:orders", "run", 1, "2025-01-01T00:00:01+00:00"],
    )
    leases_module.LeaseStore(con, namespace="OWNER").initialize()
    assert con.execute("SELECT typeof(expires_at) FROM owner.runtime_leases").fetchone() == (
        "TIMESTAMP",
    )


def test_lease_heartbeat_renewal_and_fencing():
    store = leases_module.LeaseStore(duckdb.connect(":memory:"), namespace="owner")
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


def test_disjoint_dataset_scopes_can_be_leased_concurrently(monkeypatch):
    con = duckdb.connect(":memory:")
    store = leases_module.LeaseStore(con, namespace="owner")
    store.initialize()
    now = datetime(2025, 1, 1, tzinfo=UTC)

    orders = store.acquire(
        "dataset:orders:[2025-01-01,2025-02-01)", "run-a", ttl=timedelta(minutes=1), now=now
    )
    users = store.acquire(
        "dataset:users:[2025-01-01,2025-02-01)", "run-b", ttl=timedelta(minutes=1), now=now
    )

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(leases_module, "datetime", FixedDateTime)

    assert orders.resource != users.resource
    store.assert_current(orders)
    store.assert_current(users)
