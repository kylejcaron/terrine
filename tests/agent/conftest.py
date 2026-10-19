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


@pytest.fixture
def owner_tools(tmp_path):
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.recipe import DestinationRef
    from terrine.runtime.snapshot import SnapshotAuthority

    database = tmp_path / "owner.duckdb"
    con = ibis.duckdb.connect(database=str(database))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("CREATE TABLE users (id INTEGER)")
    con.raw_sql("INSERT INTO users VALUES (1), (2)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01 12:00:00',1,3), ('2025-01-02',2,4)")
    model = Model.from_catalog(
        Path(__file__).parent / "fixtures/catalog.yaml",
        con=con,
        readiness={"events": Coverage("2024-12-01", "2025-02-01")},
        units={"user": UnitSource("users", "id")},
    )
    owner = RuntimeOwner(database, tmp_path / "snapshots")
    model_reference = ModelReference.from_model(model, owner_key=owner.reference)

    class Resolver:
        def resolve(self, reference):
            assert reference == model_reference
            return model

    policy = AgentPolicy(
        prepare=True,
        write=True,
        read=True,
        allowed_sources=frozenset({"events"}),
        allowed_catalogs=frozenset({model_reference.catalog_hash}),
        allowed_destinations=frozenset({DestinationRef(str(database), "agent", "study")}),
        model_reference=model_reference,
        resolver=Resolver(),
        snapshot_authority=SnapshotAuthority(owner),
    )
    try:
        yield DatasetTools(model, policy, namespace="agent")
    finally:
        con.disconnect()
