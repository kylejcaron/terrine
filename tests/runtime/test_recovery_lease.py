from __future__ import annotations

import hashlib
import json

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


@pytest.mark.parametrize(
    "identity",
    [
        "invocation",
        "dataset_request",
        "unknown",
        "reuse_request",
        "legacy_invocation",
        "legacy_envelope",
    ],
)
def test_committed_recovery_uses_stored_plan_identity_or_finishes_failed(identity):
    from terrine.runtime.backends.duckdb import DuckDBBackend, StagedGeneration
    from terrine.runtime.changes import ChangeSet
    from terrine.runtime.graph import dependency_graph
    from terrine.runtime.leases import LeaseStore
    from terrine.runtime.planner import RefreshPlan, plan_refresh
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
    if identity == "legacy_invocation":
        from pathlib import Path

        saved = SavedPlan.from_json(
            (Path(__file__).parents[1] / "fixtures/retained_plan_v1.json").read_text()
        )
    else:
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
    if identity == "legacy_envelope":
        document = json.loads(saved.to_json())
        document["schema_version"] = 1
        del document["plan_hash"]
        document["plan_hash"] = hashlib.sha256(
            json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        saved = SavedPlan.from_json(json.dumps(document))
    plan = RefreshPlan.deserialize(saved.plan_json)
    bound = _bind_saved_plan(saved, namespace="owner")
    if identity in {"invocation", "legacy_invocation", "legacy_envelope"}:
        invocation = PlanInvocation(
            "invocation",
            plan.serialize().decode(),
            "request",
            saved.destination,
            "snapshot",
            None,
            saved.to_json(),
        )
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
            snapshot_id=bound.snapshot_id,
            execution_revision=None if identity == "legacy_invocation" else 1,
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
    store = RuntimeStore(con, namespace="owner")
    saved.pin_snapshot(store)
    store.pin_snapshot(bound.snapshot_id, kind="active_run", reference='["stale",1]')
    _recover_interrupted_runs(con, namespace="owner")
    from terrine.runtime.status import get_run

    failed = identity in {"unknown", "reuse_request", "legacy_invocation", "legacy_envelope"}
    status = get_run("stale", con=con, namespace="owner")
    assert status.durable_state == ("failed" if failed else "succeeded")
    assert status.error_code == ("terrine.runtime.saved_plan_mismatch" if failed else None)
    assert status.pinned_result_ref == (None if failed else "committed")
    obsolete = identity in {"legacy_invocation", "legacy_envelope"}
    references = store.snapshot_references(bound.snapshot_id)
    assert (("active_run", '["stale",1]') in references) == (failed and not obsolete)
    if obsolete:
        assert not store.mark_snapshot_for_gc(bound.snapshot_id)
        # Reconcile pins left by an earlier owner that already recorded failure.
        store.pin_snapshot(bound.snapshot_id, kind="active_run", reference='["stale",1]')
        store.release_terminal_run_snapshot_references()
        assert ("active_run", '["stale",1]') not in store.snapshot_references(bound.snapshot_id)
        replacement = _lease(con, "owner")
        backend.publish_generation(
            StagedGeneration(
                "orders", "replacement", {"p": pa.table({"id": [3]})}, execution_revision=1
            ),
            expected_manifest="committed",
            lease=replacement,
        )
        LeaseStore(con, namespace="owner").release(replacement)
        saved.release_snapshot(store)
        assert store.snapshot_references(bound.snapshot_id) == ()
        assert store.mark_snapshot_for_gc(bound.snapshot_id)


@pytest.mark.parametrize("upgraded", [False, True])
def test_direct_execution_persists_plan_identity_before_committed_crash(monkeypatch, upgraded):
    import terrine.runtime.planner as planner
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
    if upgraded:
        monkeypatch.setattr(planner, "EXECUTION_REVISION", plan.definition.execution_revision + 1)
        con.execute(
            "INSERT INTO owner.runtime_leases "
            "SELECT 'dataset:orders',run_id,attempt_fence,TIMESTAMP '2999-01-01' "
            "FROM owner.runtime_runs WHERE run_id='interrupted'"
        )
        _recover_interrupted_runs(con, namespace="owner")
        assert con.execute("SELECT state FROM owner.runtime_runs").fetchone() == ("running",)
        assert ("active_run", '["interrupted",1]') in RuntimeStore(
            con, namespace="owner"
        ).snapshot_references(plan.snapshot_id)
        con.execute("UPDATE owner.runtime_leases SET expires_at=TIMESTAMP '2000-01-01'")
    _recover_interrupted_runs(con, namespace="owner")
    from terrine.runtime.status import get_run

    status = get_run("interrupted", con=con, namespace="owner")
    assert status.durable_state == ("failed" if upgraded else "succeeded")
    assert status.pinned_result_ref == (None if upgraded else "g1")
    assert RuntimeStore(con, namespace="owner").snapshot_references(plan.snapshot_id) == (
        ("published_manifest", '["orders","g1"]'),
    )
