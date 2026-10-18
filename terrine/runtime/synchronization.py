from __future__ import annotations

import threading

__all__ = ["run_publication_lock"]

_GUARD = threading.Lock()
_LOCKS: dict[tuple[str, str], threading.RLock] = {}


def run_publication_lock(namespace: str, dataset: str) -> threading.RLock:
    """Return the owner-process lock serializing cancellation against publication."""
    key = (namespace.casefold(), dataset.casefold())
    with _GUARD:
        return _LOCKS.setdefault(key, threading.RLock())
