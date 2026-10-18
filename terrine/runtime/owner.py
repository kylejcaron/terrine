from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

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

    @property
    def reference(self) -> str:
        return f"{self.state_path}\n{self.snapshot_root}"
