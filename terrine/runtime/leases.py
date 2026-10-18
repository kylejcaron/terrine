from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

__all__ = ["Lease", "LeaseStore"]


@dataclass(frozen=True, slots=True)
class Lease:
    resource: str
    owner: str
    fence: int
    expires_at: datetime


class LeaseStore:
    """DuckDB-backed, monotonically fenced resource leases."""

    def __init__(self, con: Any, *, namespace: str) -> None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
            raise ValueError("namespace must be a simple SQL identifier")
        self.con = con
        self.namespace = namespace
        self._schema = f'"{namespace}"'

    def initialize(self) -> None:
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_leases ("
            "resource VARCHAR PRIMARY KEY, owner VARCHAR NOT NULL, fence BIGINT NOT NULL, "
            "expires_at VARCHAR NOT NULL)"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_lease_fences ("
            "resource VARCHAR PRIMARY KEY, last_fence BIGINT NOT NULL)"
        )
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._schema}.runtime_resource_guards ("
            "resource VARCHAR PRIMARY KEY, guard_version BIGINT NOT NULL)"
        )

    def acquire(self, resource: str, owner: str, *, ttl: timedelta, now: datetime) -> Lease:
        self._validate(resource, owner, ttl, now)
        expires = now + ttl
        self.con.execute("BEGIN TRANSACTION")
        try:
            row = self.con.execute(
                f"SELECT owner, fence, expires_at "
                f"FROM {self._schema}.runtime_leases WHERE resource=?",
                [resource],
            ).fetchone()
            if row is not None and datetime.fromisoformat(row[2]) > now:
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
                [resource, owner, fence, expires.isoformat()],
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
            self.con.execute("COMMIT")
        except Exception:
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
            [renewed.expires_at.isoformat(), lease.resource, lease.owner, lease.fence],
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
            or datetime.fromisoformat(row[2]) <= now
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
