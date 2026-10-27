"""The SQL dialect a study expression compiles for."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import cache
from typing import ClassVar

import ibis.expr.datatypes as dt
import ibis.expr.types as ir
from ibis.common.exceptions import IbisError

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE
from terrine.backends.registry import get_engine, supported_engine_route
from terrine.errors import TerrineError

__all__ = ["Dialect", "current_dialect", "dialect_for_name", "dialect_scope", "unavailable"]

_DIALECT: ContextVar[str | None] = ContextVar("terrine_compile_dialect", default=None)


@dataclass(frozen=True, slots=True)
class Dialect:
    """Immutable expression policy; unknown engines retain coded refusals."""

    name: str
    operations: ClassVar[frozenset[str]] = frozenset()

    def exact_type(self) -> dt.DataType:
        raise unavailable("exact_state", self.name)


@cache
def _registered_dialect(name: str) -> Dialect:
    engine = get_engine(name)
    assert engine is not None
    return engine.dialect(name)


def dialect_for_name(name: str) -> Dialect:
    """Select expression policy without querying or admitting a connection."""
    name = name.casefold()
    if get_engine(name) is None:
        return Dialect(name)
    return _registered_dialect(name)


@contextmanager
def dialect_scope(dialect: str) -> Generator[None]:
    """Compile every expression inside the block, bound or literal, for ``dialect``."""
    token = _DIALECT.set(dialect.casefold())
    try:
        yield
    finally:
        _DIALECT.reset(token)


def current_dialect(*values: ir.Value) -> Dialect:
    """The enclosing scope's dialect, else the backend of the first bound operand.

    A compile scope is set from the Catalog dialect, which binding matched to
    the execution backend, so operands need no graph traversal there. Ibis
    cannot infer a backend from unbound literals; outside any scope those
    default to DuckDB.
    """
    scoped = _DIALECT.get()
    if scoped is not None:
        return dialect_for_name(scoped)
    for value in values:
        try:
            return dialect_for_name(value._find_backend(use_default=False).name)
        except IbisError:
            continue
    return dialect_for_name("duckdb")


def unavailable(operation: str, dialect: str) -> TerrineError:
    return _CAPABILITY_UNAVAILABLE.error(
        backend=dialect,
        operation=operation,
        status="unsupported",
        route=supported_engine_route(operation),
    )
