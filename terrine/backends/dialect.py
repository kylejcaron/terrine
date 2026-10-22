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

_DIALECT: ContextVar[str] = ContextVar("terrine_compile_dialect", default="duckdb")


@contextmanager
def dialect_scope(dialect: str) -> Generator[None]:
    """Compile expressions without a bound operand, such as literals, for ``dialect``."""
    token = _DIALECT.set(dialect.casefold())
    try:
        yield
    finally:
        _DIALECT.reset(token)


def dialect_of(*values: ir.Value) -> str:
    """The backend of the first bound operand, else the enclosing scope's dialect.

    Ibis cannot infer a backend from unbound literals, so callers compiling one
    Design set the scope from the Catalog dialect.
    """
    for value in values:
        try:
            return value._find_backend(use_default=False).name
        except IbisError:
            continue
    return _DIALECT.get()


def unavailable(operation: str, dialect: str) -> TerrineError:
    return _CAPABILITY_UNAVAILABLE.error(
        backend=dialect,
        operation=operation,
        status="unsupported",
        route="Build the study on DuckDB or PostgreSQL.",
    )
