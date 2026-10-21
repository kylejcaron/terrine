"""Live PostgreSQL connection fixtures; selected probes fail without settings."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

_SETTINGS = (
    "TERRINE_TEST_POSTGRES_HOST",
    "TERRINE_TEST_POSTGRES_PORT",
    "TERRINE_TEST_POSTGRES_USER",
    "TERRINE_TEST_POSTGRES_PASSWORD",
    "TERRINE_TEST_POSTGRES_DB",
)


@dataclass(frozen=True, slots=True)
class PostgresScratch:
    """An Ibis PostgreSQL connection bound to a schema owned by one test."""

    con: Any
    schema: str


def postgres_settings() -> dict[str, str]:
    missing = [name for name in _SETTINGS if name not in os.environ]
    if missing:
        message = (
            "PostgreSQL probes were selected but settings are missing: "
            f"{', '.join(missing)}. Start a local server with "
            '`eval "$(uv run python -m tests.integration.postgres_scratch start)"`.'
        )
        pytest.fail(message)  # ty: ignore[invalid-argument-type]
    return {name: os.environ[name] for name in _SETTINGS}


def connect_postgres(**overrides: Any) -> Any:
    import ibis

    settings = postgres_settings()
    return ibis.postgres.connect(
        host=settings["TERRINE_TEST_POSTGRES_HOST"],
        port=int(settings["TERRINE_TEST_POSTGRES_PORT"]),
        user=settings["TERRINE_TEST_POSTGRES_USER"],
        password=settings["TERRINE_TEST_POSTGRES_PASSWORD"] or None,
        database=settings["TERRINE_TEST_POSTGRES_DB"],
        **overrides,
    )


@pytest.fixture
def postgres_scratch(worker_id: str) -> Iterator[PostgresScratch]:
    """Own a unique schema so parallel tests never share a namespace."""
    schema = f"terrine_{worker_id}_{uuid.uuid4().hex[:12]}"
    admin = connect_postgres()
    admin.raw_sql(f'CREATE SCHEMA "{schema}"').close()
    con = connect_postgres(schema=schema)
    try:
        yield PostgresScratch(con, schema)
    finally:
        con.disconnect()
        admin.raw_sql(f'DROP SCHEMA "{schema}" CASCADE').close()
        admin.disconnect()
