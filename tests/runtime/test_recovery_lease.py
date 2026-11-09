from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from terrine.runtime.control import _recover_interrupted_runs
from terrine.runtime.state import RuntimeStore


def test_recovery_fails_runs_with_absent_or_replaced_lease():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    schema = store._schema
    con.execute(
        f"INSERT INTO {schema}.runtime_runs (run_id, dataset, fingerprint, state, attempt_fence) "
        "VALUES ('absent','orders','fp','running',1),('replaced','shipments','fp','running',1)"
    )
    con.execute(
        f"INSERT INTO {schema}.runtime_leases VALUES "
        "('dataset:shipments','new-owner',2,TIMESTAMP '2999-01-01')"
    )

    _recover_interrupted_runs(con, namespace="owner")

    assert con.execute(
        f"SELECT run_id,state FROM {schema}.runtime_runs ORDER BY run_id"
    ).fetchall() == [("absent", "failed"), ("replaced", "failed")]


@pytest.mark.parametrize("identity", ["invocation", "dataset_request", "unknown", "reuse_request"])
def test_committed_recovery_uses_stored_plan_identity_or_finishes_failed(identity):
    from terrine.runtime.backends.duckdb import DuckDBBackend, StagedGeneration
    from terrine.runtime.changes import ChangeSet
    from terrine.runtime.graph import dependency_graph
    from terrine.runtime.leases import LeaseStore
    from terrine.runtime.planner import plan_refresh
    from terrine.runtime.planning import PlanInvocation
    from terrine.runtime.saved_plan import (
        DestinationRef,
        ModelReference,
        SavedPlan,
        _bind_saved_plan,
    )
    from terrine.spec import Design, FixedDate
    from tests.runtime.test_duckdb_publication import _lease

    con = duckdb.connect()
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    reuse = identity == "reuse_request"
    if reuse:
        backend.publish_generation(
            StagedGeneration("orders", "base", {"p": pa.table({"id": [1]})}, execution_revision=1),
            expected_manifest=None,
            lease=lease,
        )
    design = Design("user", FixedDate("2025-01-02"))
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash=design.spec_hash,
        snapshot_id="snapshot",
        graph=dependency_graph(sources=("events",)),
        changes=ChangeSet((), "operator") if reuse else None,
        reuse_base_generation="base" if reuse else None,
        reuse_base_revision=1 if reuse else None,
    )
    saved = SavedPlan.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference("model", "catalog", "model", "units", "readiness", "events"),
        destination=DestinationRef("output.duckdb", "owner", "orders"),
        namespace="owner",
        expected_base_manifest="base" if reuse else None,
    )
    bound = _bind_saved_plan(saved, namespace="owner")
    if identity == "invocation":
        invocation = PlanInvocation(
            "invocation",
            plan.serialize().decode(),
            "request",
            saved.destination,
            "snapshot",
            None,
            saved.to_json(),
        )
        import json

        con.execute(
            "INSERT INTO owner.runtime_plan_invocations VALUES (?,?,?,?)",
            ["invocation", "hash", "snapshot", json.dumps(invocation.to_dict())],
        )
    elif identity != "unknown":
        con.execute(
            "INSERT INTO owner.runtime_dataset_requests VALUES (?,?)", ["request", saved.to_json()]
        )
    backend.publish_generation(
        StagedGeneration(
            "orders",
            "committed",
            {"p": pa.table({"id": [2]})},
            run_id="stale",
            execution_revision=1,
        ),
        expected_manifest="base" if reuse else None,
        lease=lease,
    )
    LeaseStore(con, namespace="owner").release(lease)
    con.execute(
        "INSERT INTO owner.runtime_runs "
        "(run_id,dataset,fingerprint,state,attempt_fence,request_key) VALUES "
        "('stale','orders',?,'running',1,'request')",
        [bound.definition.fingerprint],
    )
    _recover_interrupted_runs(con, namespace="owner")
    expected = "failed" if identity in {"unknown", "reuse_request"} else "succeeded"
    assert con.execute("SELECT state FROM owner.runtime_runs WHERE run_id='stale'").fetchone() == (
        expected,
    )


def test_direct_execution_persists_plan_identity_before_committed_crash():
    from terrine.runtime.execute import execute_plan
    from tests.runtime.test_execute import _plan, _publish

    con = duckdb.connect()
    plan = _plan()

    def publish(current, outputs, lease):
        _publish(con, current, outputs, lease, "interrupted")
        raise SystemExit("owner stopped after commit")

    with pytest.raises(SystemExit):
        execute_plan(
            plan,
            con=con,
            namespace="owner",
            run_id="interrupted",
            run_task=lambda *_: pa.table({"id": [1]}),
            publish=publish,
        )
    _recover_interrupted_runs(con, namespace="owner")
    assert con.execute("SELECT state,published_generation FROM owner.runtime_runs").fetchone() == (
        "succeeded",
        "g1",
    )
