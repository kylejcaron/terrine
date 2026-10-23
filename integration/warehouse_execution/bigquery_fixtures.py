"""Live BigQuery connection fixtures; selected probes fail without settings."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

_SETTINGS = ("TERRINE_TEST_BIGQUERY_PROJECT", "TERRINE_TEST_BIGQUERY_DATASET")
_LOCATION = "US"


@dataclass(frozen=True, slots=True)
class BigQueryScratch:
    """An Ibis BigQuery connection whose default dataset is owned by one test."""

    con: Any
    project: str
    dataset: str

    def sql(self, statement: str) -> None:
        """Run DDL/DML in the scratch dataset outside any terrine snapshot."""
        from google.cloud import bigquery

        config = bigquery.QueryJobConfig(default_dataset=f"{self.project}.{self.dataset}")
        self.con.client.query(statement, job_config=config).result()


def bigquery_settings() -> dict[str, str]:
    missing = [name for name in _SETTINGS if not os.environ.get(name)]
    if missing:
        message = (
            "BigQuery probes were selected but settings are missing: "
            f"{', '.join(missing)}. Also provide GOOGLE_APPLICATION_CREDENTIALS, "
            "application-default credentials or TERRINE_TEST_BIGQUERY_ACCESS_TOKEN."
        )
        pytest.fail(message)  # ty: ignore[invalid-argument-type]
    return {name: os.environ[name] for name in _SETTINGS}


def _credentials() -> Any:
    token = os.environ.get("TERRINE_TEST_BIGQUERY_ACCESS_TOKEN")
    if token:
        from google.oauth2.credentials import Credentials

        return Credentials(token)
    return None  # GOOGLE_APPLICATION_CREDENTIALS or application-default


def connect_bigquery(dataset: str | None = None) -> Any:
    import ibis

    settings = bigquery_settings()
    return ibis.bigquery.connect(
        project_id=settings["TERRINE_TEST_BIGQUERY_PROJECT"],
        dataset_id=dataset or settings["TERRINE_TEST_BIGQUERY_DATASET"],
        credentials=_credentials(),
        location=_LOCATION,
    )


@pytest.fixture
def bigquery_scratch(worker_id: str) -> Iterator[BigQueryScratch]:
    """Own a unique dataset so parallel tests never share a namespace."""
    settings = bigquery_settings()
    project = settings["TERRINE_TEST_BIGQUERY_PROJECT"]
    dataset = f"terrine_ci_{worker_id}_{uuid.uuid4().hex[:12]}"
    admin = connect_bigquery()
    admin.raw_sql(f"CREATE SCHEMA `{project}.{dataset}` OPTIONS(location='{_LOCATION}')")
    con = connect_bigquery(dataset)
    try:
        yield BigQueryScratch(con, project, dataset)
    finally:
        con.disconnect()
        admin.raw_sql(f"DROP SCHEMA IF EXISTS `{project}.{dataset}` CASCADE")
        admin.disconnect()
