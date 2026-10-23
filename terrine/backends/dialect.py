"""The SQL dialect a study expression compiles for."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

import ibis.expr.types as ir
from ibis.common.exceptions import IbisError

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE
from terrine.errors import TerrineError

__all__ = ["dialect_of", "dialect_scope", "unavailable"]

_DIALECT: ContextVar[str | None] = ContextVar("terrine_compile_dialect", default=None)


@contextmanager
def dialect_scope(dialect: str) -> Generator[None]:
    """Compile every expression inside the block, bound or literal, for ``dialect``."""
    token = _DIALECT.set(dialect.casefold())
    try:
        yield
    finally:
        _DIALECT.reset(token)


def dialect_of(*values: ir.Value) -> str:
    """The enclosing scope's dialect, else the backend of the first bound operand.

    A compile scope is set from the Catalog dialect, which binding matched to
    the execution backend, so operands need no graph traversal there. Ibis
    cannot infer a backend from unbound literals; outside any scope those
    default to DuckDB.
    """
    scoped = _DIALECT.get()
    if scoped is not None:
        return scoped
    for value in values:
        try:
            return value._find_backend(use_default=False).name
        except IbisError:
            continue
    return "duckdb"


def unavailable(operation: str, dialect: str) -> TerrineError:
    return _CAPABILITY_UNAVAILABLE.error(
        backend=dialect,
        operation=operation,
        status="unsupported",
        route="Build the study on DuckDB or PostgreSQL.",
    )
