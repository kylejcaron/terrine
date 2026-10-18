from __future__ import annotations

import duckdb

from terrine.runtime.control import _recover_interrupted_runs
from terrine.runtime.state import RuntimeStore


def test_recovery_fails_runs_with_absent_or_replaced_lease():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    schema = store._schema
    con.execute(
        f"CREATE TABLE {schema}.runtime_runs "
        "(run_id VARCHAR PRIMARY KEY,dataset VARCHAR,fingerprint VARCHAR,state VARCHAR,"
        "published_generation VARCHAR,attempt_fence BIGINT)"
    )
    con.execute(
        f"INSERT INTO {schema}.runtime_runs VALUES "
        "('absent','orders','fp','running',NULL,1),"
        "('replaced','shipments','fp','running',NULL,1)"
    )
    con.execute(
        f"CREATE TABLE {schema}.runtime_leases "
        "(resource VARCHAR PRIMARY KEY,owner VARCHAR,fence BIGINT,expires_at TIMESTAMP)"
    )
    con.execute(
        f"INSERT INTO {schema}.runtime_leases VALUES "
        "('dataset:shipments','new-owner',2,TIMESTAMP '2999-01-01')"
    )
    con.execute(
        f"CREATE TABLE {schema}.runtime_run_publications "
        "(run_id VARCHAR PRIMARY KEY,dataset VARCHAR,generation VARCHAR,version BIGINT)"
    )

    _recover_interrupted_runs(con, namespace="owner")

    assert con.execute(
        f"SELECT run_id,state FROM {schema}.runtime_runs ORDER BY run_id"
    ).fetchall() == [("absent", "failed"), ("replaced", "failed")]
