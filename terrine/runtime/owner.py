from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = ["RuntimeOwner", "SnapshotRetention"]


@dataclass(frozen=True, slots=True)
class SnapshotRetention:
    """Retention rules for execution snapshots owned by this runtime."""

    keep_unfinished: bool = True
    keep_published: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.keep_unfinished, bool) or not isinstance(self.keep_published, bool):
            raise TypeError("snapshot retention flags must be bool")


@dataclass(frozen=True, slots=True)
class RuntimeOwner:
    """Owner-local durable runtime and retained snapshot locations."""

    state_path: Path
    snapshot_root: Path
    retention: SnapshotRetention = SnapshotRetention()

    def __post_init__(self) -> None:
        object.__setattr__(self, "state_path", Path(self.state_path).expanduser().resolve())
        object.__setattr__(self, "snapshot_root", Path(self.snapshot_root).expanduser().resolve())
        if not isinstance(self.retention, SnapshotRetention):
            raise TypeError("retention must be SnapshotRetention")

    @classmethod
    def from_connection(cls, con: Any) -> RuntimeOwner:
        """Resolve the durable owner and configured snapshot location of a connection."""
        from terrine.execution._native import raw_connection

        raw = raw_connection(con)
        rows = raw.execute("PRAGMA database_list").fetchall()
        current = raw.execute("SELECT current_database()").fetchone()[0]
        path = next((path for _, name, path in rows if name == current and path), None)
        if path is None:
            raise ValueError("a file-backed DuckDB connection is required")
        database = Path(path).resolve()
        root = os.environ.get("TERRINE_RUNTIME_SNAPSHOT_ROOT")
        return cls(
            database,
            Path(root).expanduser() if root else database.parent / ".terrine" / "snapshots",
        )

    @property
    def reference(self) -> str:
        return f"{self.state_path}\n{self.snapshot_root}"
