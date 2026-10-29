from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from terrine.errors import RefusalSpec

__all__ = ["Lease", "LeaseStore"]

_INVALID_NAMESPACE = RefusalSpec(
    "terrine.dataset.invalid_destination",
    "Runtime {field} is invalid: {reason}; use a simple SQL identifier.",
)


@dataclass(frozen=True, slots=True)
class Lease:
    resource: str
    owner: str
    fence: int
    expires_at: datetime


class LeaseStore:
    """DuckDB-backed, monotonically fenced resource leases."""

    def __init__(self, con: Any, *, namespace: str) -> None:
        if not isinstance(namespace, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
            raise _INVALID_NAMESPACE.error(reason="invalid_namespace", field="namespace")
        self.con = con
        self.namespace = namespace
        self._schema = f'"{namespace}"'

    def initialize(self) -> None:
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_leases ("
            "resource VARCHAR PRIMARY KEY, owner VARCHAR NOT NULL, fence BIGINT NOT NULL, "
            "expires_at TIMESTAMP NOT NULL)"
        )
        column_type = self.con.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE lower(table_schema)=lower(?) "
            "AND table_name='runtime_leases' AND column_name='expires_at'",
            [self.namespace],
        ).fetchone()[0]
        if column_type != "TIMESTAMP":
            self.con.execute(
                f"ALTER TABLE {self._schema}.runtime_leases "
                "ALTER COLUMN expires_at SET DATA TYPE TIMESTAMP USING "
                "CAST(CAST(expires_at AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP)"
            )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_lease_fences ("
            "resource VARCHAR PRIMARY KEY, last_fence BIGINT NOT NULL)"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_resource_guards ("
            "resource VARCHAR PRIMARY KEY, guard_version BIGINT NOT NULL)"
        )

    def acquire(
        self,
        resource: str,
        owner: str,
        *,
        ttl: timedelta,
        now: datetime,
        _in_transaction: bool = False,
    ) -> Lease:
        self._validate(resource, owner, ttl, now)
        expires = now + ttl
        if not _in_transaction:
            self.con.execute("BEGIN TRANSACTION")
        try:
            row = self.con.execute(
                f"SELECT owner, fence, expires_at "
                f"FROM {self._schema}.runtime_leases WHERE resource=?",
                [resource],
            ).fetchone()
            if row is not None and row[2].replace(tzinfo=UTC) > now:
                raise ValueError("resource is leased by an active owner")
            fence_row = self.con.execute(
                f"SELECT last_fence FROM {self._schema}.runtime_lease_fences WHERE resource=?",
                [resource],
            ).fetchone()
            fence = (fence_row[0] if fence_row else 0) + 1
            self.con.execute(
                f"INSERT INTO {self._schema}.runtime_lease_fences VALUES (?, ?) "
                "ON CONFLICT(resource) DO UPDATE SET last_fence=excluded.last_fence",
                [resource, fence],
            )
            self.con.execute(
                f"INSERT INTO {self._schema}.runtime_leases VALUES (?, ?, ?, ?) "
                "ON CONFLICT(resource) DO UPDATE SET owner=excluded.owner, fence=excluded.fence, "
                "expires_at=excluded.expires_at",
                [resource, owner, fence, _naive_utc(expires)],
            )
            self.con.execute(
                f"INSERT INTO {self._schema}.runtime_resource_guards VALUES (?, 0) "
                "ON CONFLICT DO NOTHING",
                [resource],
            )
            self.con.execute(
                f"UPDATE {self._schema}.runtime_resource_guards "
                "SET guard_version=guard_version+1 WHERE resource=?",
                [resource],
            )
            if not _in_transaction:
                self.con.execute("COMMIT")
        except Exception:
            if not _in_transaction:
                self.con.execute("ROLLBACK")
            raise
        return Lease(resource, owner, fence, expires)

    def renew(self, lease: Lease, *, ttl: timedelta, now: datetime) -> Lease:
        self._validate(lease.resource, lease.owner, ttl, now)
        self._assert_matches(lease, now)
        renewed = Lease(lease.resource, lease.owner, lease.fence, now + ttl)
        self.con.execute(
            f"UPDATE {self._schema}.runtime_leases SET expires_at=? "
            "WHERE resource=? AND owner=? AND fence=?",
            [_naive_utc(renewed.expires_at), lease.resource, lease.owner, lease.fence],
        )
        return renewed

    def release(self, lease: Lease) -> None:
        self.con.execute(
            f"DELETE FROM {self._schema}.runtime_leases WHERE resource=? AND owner=? AND fence=?",
            [lease.resource, lease.owner, lease.fence],
        )

    def assert_current(self, lease: Lease) -> None:
        self._assert_matches(lease, datetime.now(UTC))

    def guard_publication(self, lease: Lease) -> None:
        """Fence takeover against a publication transaction's final lease check."""
        self._assert_matches(lease, datetime.now(UTC))
        self.con.execute(
            f"UPDATE {self._schema}.runtime_resource_guards "
            "SET guard_version=guard_version+1 WHERE resource=?",
            [lease.resource],
        )

    def _assert_matches(self, lease: Lease, now: datetime) -> None:
        row = self.con.execute(
            f"SELECT owner, fence, expires_at FROM {self._schema}.runtime_leases WHERE resource=?",
            [lease.resource],
        ).fetchone()
        if (
            row is None
            or row[0] != lease.owner
            or row[1] != lease.fence
            or row[2].replace(tzinfo=UTC) <= now
        ):
            raise ValueError("lease is expired or fenced by a newer owner")

    @staticmethod
    def _validate(resource: str, owner: str, ttl: timedelta, now: datetime) -> None:
        if not resource or not owner:
            raise ValueError("resource and owner are required")
        if ttl <= timedelta(0):
            raise ValueError("lease ttl must be positive")
        if now.tzinfo is None:
            raise ValueError("lease clock must be timezone-aware")


def _naive_utc(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None)
