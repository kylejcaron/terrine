import json
from typing import cast

import duckdb
import ibis
import pytest

from terrine.data import Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planning import OwnerPlanner, PlanRequest
from terrine.runtime.saved_plan import DestinationRef, ModelReference, SavedPlan
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.spec import agg
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design


def _request():
    return PlanRequest(
        "airflow-run-1",
        ModelReference("owner", "catalog", "model", "units", "readiness", "events"),
        Design("user", FixedDate("2025-01-15")),
        DestinationRef("warehouse.duckdb", "published", "study"),
    )


def test_plan_request_round_trips_as_versioned_owner_payload():
    request = _request()

    restored = PlanRequest.from_dict(request.to_dict())

    assert restored == request
    assert restored.schema_version == 1


def test_plan_request_refuses_unknown_wire_version():
    data = _request().to_dict()
    data["schema_version"] = 99

    with pytest.raises(TerrineError) as error:
        PlanRequest.from_dict(data)

    assert error.value.code == "terrine.runtime.plan_request_invalid"


def test_owner_plan_rolls_back_snapshot_pin_when_invocation_persistence_fails(
    tmp_path, monkeypatch
):
    from tests.runtime.test_saved_plan import _model

    database = tmp_path / "catalog-atomic.duckdb"
    connection = ibis.duckdb.connect(database=str(database))
    connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    connection.raw_sql("CREATE TABLE users (id INTEGER)")
    base_model = _model(connection)
    model = Model.from_catalog(
        base_model.catalog,
        con=connection,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
    )
    request = PlanRequest(
        "materialize-atomic",
        ModelReference.from_model(model, owner_key="owner-model"),
        Design(
            "user",
            FixedDate("2025-01-15"),
            features={"total": agg("total", window=("-1d", "0s"))},
            outcomes={},
        ),
        DestinationRef(str(database), "published", "study"),
    )

    class Resolver:
        def resolve(self, reference):
            return model

    planner = OwnerPlanner(
        resolver=Resolver(),
        snapshot_authority=SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots")),
    )
    original_pin = SavedPlan.pin_snapshot

    def fail_after_pin(saved_plan, store, *, reference=None, _in_transaction=False):
        original_pin(
            saved_plan,
            store,
            reference=reference,
            _in_transaction=_in_transaction,
        )
        raise failure

    monkeypatch.setattr(SavedPlan, "pin_snapshot", fail_after_pin)
    failure = RuntimeError("invocation persistence interrupted")

    try:
        with pytest.raises(RuntimeError) as error:
            planner.plan(request, con=connection.con, namespace="runtime")
        assert error.value is failure
        references = connection.con.execute(
            "SELECT snapshot_id FROM runtime.runtime_snapshot_references "
            "WHERE kind='saved_plan' AND reference=?",
            [request.invocation_id],
        ).fetchall()
        assert references == []
        invocations = connection.con.execute(
            "SELECT invocation_id FROM runtime.runtime_plan_invocations"
        ).fetchall()
        assert invocations == []
    finally:
        connection.disconnect()


def test_owner_plan_refuses_unsupported_runtime_shape_before_resolving_sources():
    from terrine.errors import TerrineError
    from terrine.spec.sequence import EventSequence
    from terrine.windows import Window

    request = PlanRequest(
        "unsupported-shape",
        _request().model,
        Design(
            "user",
            FixedDate("2025-01-15"),
            shape=EventSequence("purchase", Window("0s", "1d"), max_events=3),
        ),
        _request().destination,
    )
    con = duckdb.connect(":memory:")

    class Resolver:
        resolved = False

        def resolve(self, reference):
            self.resolved = True
            raise AssertionError("unsupported shapes must be refused before source resolution")

    class SnapshotSpy:
        captured = False

        def capture(self, *args, **kwargs):
            self.captured = True
            raise AssertionError("unsupported shapes must be refused before snapshot capture")

    snapshot_authority = SnapshotSpy()
    resolver = Resolver()
    planner = OwnerPlanner(
        resolver=resolver,
        snapshot_authority=cast(SnapshotAuthority, snapshot_authority),
    )
    try:
        with pytest.raises(TerrineError) as error:
            planner.plan(request, con=con, namespace="owner")
    finally:
        con.close()

    assert error.value.code == "terrine.compile.runtime_shape_unsupported"
    assert not resolver.resolved
    assert not snapshot_authority.captured


@pytest.mark.parametrize("strict_scan", [False, True])
def test_owner_planning_refuses_before_staging_on_pinned_inputs(tmp_path, monkeypatch, strict_scan):
    from terrine.data import Budget
    from terrine.spec import gt
    from tests.runtime.test_saved_plan import _model

    database = tmp_path / "refused.duckdb"
    connection = ibis.duckdb.connect(database=str(database))
    connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    connection.raw_sql("INSERT INTO events VALUES ('2025-01-14', 1, 4)")
    connection.raw_sql("CREATE TABLE users (id INTEGER)")
    connection.raw_sql("INSERT INTO users VALUES (1)")
    model = Model.from_catalog(
        _model(connection).catalog,
        con=connection,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-14", "2025-02-01")},
    )
    request = PlanRequest(
        "refused-plan",
        ModelReference.from_model(model, owner_key="owner-model"),
        Design(
            "user",
            FixedDate("2025-01-15"),
            eligibility={"history": gt(agg("total", window=("-7d", "0s")), 0)},
        ),
        DestinationRef(str(database), "published", "study"),
        budget=Budget(max_scan_bytes=1) if strict_scan else None,
    )

    class Resolver:
        def resolve(self, reference):
            return model

    snapshots = []
    original_capture = SnapshotAuthority.capture

    def capture(self, *args, **kwargs):
        snapshot = original_capture(self, *args, **kwargs)
        snapshots.append(snapshot)
        connection.raw_sql("DELETE FROM users")
        return snapshot

    monkeypatch.setattr(SnapshotAuthority, "capture", capture)
    authority = SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots"))
    planner = OwnerPlanner(resolver=Resolver(), snapshot_authority=authority)
    try:
        with pytest.raises(TerrineError) as error:
            planner.plan(request, con=connection.con, namespace="owner")

        if strict_scan:
            assert error.value.code == "terrine.budget.scan_unknown"
            assert not snapshots
            assert connection.table("users").count().execute() == 1
        else:
            assert error.value.code == "terrine.study.eligibility_unknown"
            assert dict(error.value.context) == {
                "candidate": 1,
                "excluded_false": 0,
                "unknown": 1,
                "retained": 0,
            }
            assert len(snapshots) == 1
            assert connection.table("users").count().execute() == 0
            assert not RuntimeStore(connection.con, namespace="owner").snapshot_references(
                snapshots[0].snapshot_id
            )
        assert connection.con.execute(
            "SELECT count(*) FROM owner.runtime_plan_invocations"
        ).fetchone() == (0,)
        assert (
            connection.con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='owner' "
                "AND table_name IN ('runtime_runs', 'runtime_partitions', 'runtime_manifests')"
            ).fetchall()
            == []
        )
    finally:
        connection.disconnect()


def test_plan_request_round_trip_preserves_dataset_definition_view():
    from dataclasses import replace

    from terrine.spec.dataset import DatasetDefinition

    request = _request()
    for view in (
        {
            "kind": "training",
            "features": ["x"],
            "labels": ["y"],
            "incomplete_labels": "keep",
            "split": {"kind": "time", "train_until": "2025-01-15"},
        },
        {
            "kind": "scoring",
            "features": ["x"],
            "as_of": "2025-01-15T00:00:00+00:00",
            "eligibility": {},
            "parent_design_hash": request.design.spec_hash,
        },
    ):
        saved_plan = DatasetDefinition(schema_version=1, base_design=request.design, view=view)
        typed = replace(request, dataset_definition=saved_plan)
        restored = PlanRequest.from_dict(json.loads(json.dumps(typed.to_dict())))
        assert restored.dataset_definition is not None
        assert restored.dataset_definition.digest == saved_plan.digest
        assert restored.dataset_definition.base_design.spec_hash == request.design.spec_hash
        assert restored.dataset_definition.view == saved_plan.view
        with pytest.raises(ValueError):
            replace(typed, design=Design("user", FixedDate("2025-01-16")))
