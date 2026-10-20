"""Reuse immutable metadata facts during one synchronous read-only operation."""

from __future__ import annotations

from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Any, cast

_FACTS: ContextVar[dict[tuple[Any, ...], Any] | None] = ContextVar("metadata_facts", default=None)


@contextmanager
def metadata_operation() -> Generator[None]:
    """Share facts through nested reads, discarding them before the next operation."""
    if _FACTS.get() is not None:
        yield
        return
    token = _FACTS.set({})
    try:
        yield
    finally:
        _FACTS.reset(token)


def invalidate_metadata() -> None:
    """Discard facts after connection settings or native definitions are mutated."""
    facts = _FACTS.get()
    if facts is not None:
        facts.clear()


def metadata_fact[**P, T](function: Callable[P, T]) -> Callable[P, T]:
    """Cache only successful reads whose arguments identify immutable metadata."""

    @wraps(function)
    def read(*args: P.args, **kwargs: P.kwargs) -> T:
        facts = _FACTS.get()
        if facts is None:
            return function(*args, **kwargs)
        key = (function, args, tuple(sorted(kwargs.items())))
        if key not in facts:
            facts[key] = function(*args, **kwargs)
        return cast(T, facts[key])

    return read
