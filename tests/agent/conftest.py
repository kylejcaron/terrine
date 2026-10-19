from pathlib import Path

import ibis
import pytest

from terrine.agent import AgentPolicy, DatasetTools
from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.runtime.recipe import ModelReference


@pytest.fixture
def tools(tmp_path):
    con = ibis.duckdb.connect()
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("CREATE TABLE users (id INTEGER)")
    model = Model.from_catalog(
        Path(__file__).parent / "fixtures/catalog.yaml",
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
        units={"user": UnitSource("users", "id")},
    )
    reference = ModelReference.from_model(model, owner_key="owner")
    policy = AgentPolicy(
        allowed_sources=frozenset({"events"}),
        allowed_catalogs=frozenset({reference.catalog_hash}),
        model_reference=reference,
        design_root=tmp_path / "designs",
    )
    try:
        yield DatasetTools(model, policy, namespace="agent")
    finally:
        con.disconnect()
