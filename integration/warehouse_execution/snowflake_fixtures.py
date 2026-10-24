"""Live Snowflake connection fixtures; selected probes fail without settings."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

_SETTINGS = (
    "TERRINE_TEST_SNOWFLAKE_ACCOUNT",
    "TERRINE_TEST_SNOWFLAKE_USER",
    "TERRINE_TEST_SNOWFLAKE_PRIVATE_KEY",
    "TERRINE_TEST_SNOWFLAKE_WAREHOUSE",
    "TERRINE_TEST_SNOWFLAKE_DATABASE",
    "TERRINE_TEST_SNOWFLAKE_SCHEMA",
)


@dataclass(frozen=True, slots=True)
class SnowflakeScratch:
    """An Ibis Snowflake connection whose current schema is owned by one test."""

    con: Any
    database: str
    schema: str

    def sql(self, statement: str) -> list[tuple[Any, ...]]:
        """Run DDL/DML in the scratch schema outside any terrine snapshot."""
        with self.con.con.cursor() as cursor:
            cursor.execute(statement)
            return cursor.fetchall()


def snowflake_settings() -> dict[str, str]:
    missing = [name for name in _SETTINGS if not os.environ.get(name)]
    if missing:
        message = (
            "Snowflake probes were selected but settings are missing: "
            f"{', '.join(missing)}. TERRINE_TEST_SNOWFLAKE_PRIVATE_KEY is a PEM private "
            "key registered on the service user; TERRINE_TEST_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE "
            "is optional."
        )
        pytest.fail(message)  # ty: ignore[invalid-argument-type]
    return {name: os.environ[name] for name in _SETTINGS}


def _private_key(settings: dict[str, str]) -> bytes:
    # A Snowflake service user holds no password: the connector takes a DER private
    # key whose public half is registered on that user.
    from cryptography.hazmat.primitives import serialization

    passphrase = os.environ.get("TERRINE_TEST_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    parsed = serialization.load_pem_private_key(
        settings["TERRINE_TEST_SNOWFLAKE_PRIVATE_KEY"].encode(),
        password=passphrase.encode() if passphrase else None,
    )
    return parsed.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect_snowflake(schema: str | None = None) -> Any:
    import ibis

    settings = snowflake_settings()
    schema = schema or settings["TERRINE_TEST_SNOWFLAKE_SCHEMA"]
    con = ibis.snowflake.connect(
        account=settings["TERRINE_TEST_SNOWFLAKE_ACCOUNT"],
        user=settings["TERRINE_TEST_SNOWFLAKE_USER"],
        private_key=_private_key(settings),
        warehouse=settings["TERRINE_TEST_SNOWFLAKE_WAREHOUSE"],
        database=settings["TERRINE_TEST_SNOWFLAKE_DATABASE"],
        schema=schema,
        create_object_udfs=False,
    )
    # Snowflake silently ignores a connect-time schema the role cannot see, which
    # would surface later as a misleading error; fail at the fixture boundary.
    with con.con.cursor() as cursor:
        database, current = cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").fetchone()
    if current is None:
        con.disconnect()
        message = (
            f"connected to Snowflake, but schema {schema!r} in database {database!r} "
            "is not visible to the connecting role"
        )
        pytest.fail(message)  # ty: ignore[invalid-argument-type]
    return con


@pytest.fixture
def snowflake_scratch(worker_id: str) -> Iterator[SnowflakeScratch]:
    """Own a unique schema so parallel tests never share a namespace."""
    schema = f"TERRINE_CI_{worker_id}_{uuid.uuid4().hex[:12]}".upper()
    admin = connect_snowflake()
    database = admin.current_catalog
    con = None
    try:
        with admin.con.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{database}"."{schema}"')
            # CREATE SCHEMA switches the session to the new schema; the admin
            # session is used only for cleanup, so nothing else depends on it.
        con = connect_snowflake(schema)
        yield SnowflakeScratch(con, database, schema)
    finally:
        # The schema is dropped whatever failed after its creation: the second
        # connection, the test, or closing the test connection.
        try:
            if con is not None:
                con.disconnect()
        finally:
            try:
                with admin.con.cursor() as cursor:
                    cursor.execute(f'DROP SCHEMA IF EXISTS "{database}"."{schema}" CASCADE')
            finally:
                admin.disconnect()
