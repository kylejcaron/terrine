"""Exact record bindings for an owner installed by verified restore."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO

from terrine.errors import RefusalSpec

if TYPE_CHECKING:
    from collections.abc import Generator

    from terrine.runtime.owner import RuntimeOwner

_INVALID = RefusalSpec(
    "terrine.runtime.backup_invalid",
    "Owner backup or restore refused ({reason}). Use an offline, checkpointed owner or a "
    "verified bundle and a new destination directory.",
)
_VERSION = 1
_STATE = Path("owner.duckdb")
_ROOT = Path(".terrine/snapshots")
_INVENTORY = Path("inventory.json")
_IMPORT = Path("owner.duckdb.terrine-import.json")


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _key(kind: str, *parts: str) -> str:
    return _json((kind, *parts)).decode()


def _relative(value: str) -> Path:
    path = Path(value)
    if not path.parts or path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise _INVALID.error(reason="unsafe_path")
    return path


@contextmanager
def _open_regular(root: Path, relative: Path) -> Generator[BinaryIO]:
    """Open once through real directories; do not follow links or block on a pipe."""
    if os.name != "posix" or not root.is_absolute():
        raise _INVALID.error(reason="unsupported_platform")
    path = _relative(relative.as_posix())
    options = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    with ExitStack() as stack:
        fd = os.open(root.anchor, options)
        stack.callback(os.close, fd)
        for component in (*root.parts[1:], *path.parts[:-1]):
            fd = os.open(component, options, dir_fd=fd)
            stack.callback(os.close, fd)
        file_fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        stream = stack.enter_context(os.fdopen(file_fd, "rb"))
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise _INVALID.error(reason="not_regular_file")
        yield stream


@dataclass(frozen=True, slots=True)
class _Record:
    origin: str
    digest: str
    path: str | None = None


@dataclass(frozen=True, slots=True)
class _Imports:
    records: dict[str, _Record]

    @classmethod
    def read(cls, owner: RuntimeOwner) -> _Imports | None:
        name = owner.state_path.name + ".terrine-import.json"
        if os.name != "posix" and not (owner.state_path.parent / name).exists():
            return None
        try:
            with _open_regular(owner.state_path.parent, Path(name)) as stream:
                document = json.load(stream)
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            raise _INVALID.error(reason="invalid_import_map") from exc
        try:
            payload = document["payload"]
            if (
                document["digest"] != _hash(_json(payload))
                or payload["version"] != _VERSION
                or payload["destination"] != owner.reference
            ):
                raise _INVALID.error(reason="invalid_import_binding")
            return cls({key: _Record(**record) for key, record in payload["records"].items()})
        except (TypeError, KeyError, AttributeError, ValueError) as exc:
            raise _INVALID.error(reason="invalid_import_map") from exc

    def match(self, key: str, *, origin: str, digest: str) -> _Record | None:
        found = self.records.get(key)
        if found is not None and (found.origin, found.digest) == (origin, digest):
            return found
        return None
