from __future__ import annotations

import json
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest


def test_two_dbt_build_cycles_stop_owner_and_replay_from_separate_processes(tmp_path, monkeypatch):
    pytest.importorskip("dbt")
    import ibis

    from terrine.data import Coverage, UnitSource
    from terrine.model import Model
    from terrine.runtime.api import DesignExecutor, open_published_frame
    from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
    from terrine.runtime.execute import RunReceipt
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner, PlanInvocation, PlanRequest
    from terrine.runtime.recipe import DestinationRef, ModelReference
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.runtime.state import RuntimeStore
    from terrine.spec import agg
    from terrine.spec.anchors import FixedDate
    from terrine.spec.design import Design
    from tests.runtime.test_recipe import _model

    database = tmp_path / "dbt-shared.duckdb"
    project_dir = tmp_path / "dbt_project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)
    (project_dir / "dbt_project.yml").write_text(
        "name: terrine_fixture\nversion: '1.0.0'\nconfig-version: 2\n"
        "profile: terrine_fixture\nmodel-paths: ['models']\n"
    )
    (models_dir / "events.sql").write_text(
        "{{ config(materialized='table') }}\n"
        "select timestamp '2025-01-15' as ts, 1::integer as user_id, "
        "{{ var('value') }}::integer as value\n"
    )
    (models_dir / "users.sql").write_text(
        "{{ config(materialized='table') }}\nselect 1::integer as id\n"
    )
    profile_dir = tmp_path / "profiles"
    profile_dir.mkdir()
    (profile_dir / "profiles.yml").write_text(
        "terrine_fixture:\n  target: local\n  outputs:\n    local:\n"
        "      type: duckdb\n      path: "
        f"{database}\n      schema: main\n      threads: 1\n"
    )
    dbt = shutil.which("dbt")
    assert dbt is not None
    endpoint = ControlEndpoint(Path("/tmp") / f"terrine-dbt-{uuid.uuid4().hex}.sock")
    snapshots = tmp_path / "snapshots"
    executed_task_ids = []
    fail_once = True

    original_run = DesignExecutor.run_snapshot_task

    def run_snapshot_task(self, context, task, dependencies):
        nonlocal fail_once
        if fail_once and len(executed_task_ids) == 1:
            fail_once = False
            raise RuntimeError("transient dbt-triggered task failure")
        result = original_run(self, context, task, dependencies)
        executed_task_ids.append(task.task_id)
        return result

    monkeypatch.setattr(DesignExecutor, "run_snapshot_task", run_snapshot_task)

    def start_owner():
        connection = ibis.duckdb.connect(database=str(database))
        raw_connection = connection.con
        store = RuntimeStore(raw_connection, namespace="owner")
        store.initialize()
        authority = SnapshotAuthority(RuntimeOwner(database, snapshots))

        class Resolver:
            def resolve(self, reference):
                model = Model.from_catalog(
                    _model(connection).catalog,
                    con=connection,
                    units={"user": UnitSource("users", "id")},
                    readiness={"events": Coverage("2025-01-01", "2025-02-01")},
                )
                assert ModelReference.from_model(model, owner_key="dbt-fixture") == reference
                return model

        planner = OwnerPlanner(resolver=Resolver(), snapshot_authority=authority)
        server = ControlServer(
            store,
            namespace="owner",
            endpoint=endpoint,
            plan_planner=planner,
            resolver=Resolver(),
            snapshot_authority=authority,
        )
        server.start()
        return connection, server

    connection, server = start_owner()
    plan_invocations = []
    receipts = []
    try:
        for cycle, value in enumerate((4, 9), start=1):
            server.close()
            connection.disconnect()
            target_path = tmp_path / f"target-{cycle}"
            subprocess.run(
                [
                    dbt,
                    "build",
                    "--project-dir",
                    str(project_dir),
                    "--profiles-dir",
                    str(profile_dir),
                    "--target-path",
                    str(target_path),
                    "--vars",
                    json.dumps({"value": value}),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            connection, server = start_owner()
            if receipts:
                previous = open_published_frame(receipts[0], con=connection.con, namespace="owner")
                assert previous.table["total"].to_pylist() == [4]
            from examples.orchestrators.dbt_core_after_build import (
                reviewed_dbt_relation_names,
            )

            reviewed_relations = reviewed_dbt_relation_names(
                str(target_path / "manifest.json"),
                reviewed_node_ids={
                    "model.terrine_fixture.events",
                    "model.terrine_fixture.users",
                },
            )
            assert reviewed_relations == (
                '"dbt-shared"."main"."events"',
                '"dbt-shared"."main"."users"',
            )
            model = Model.from_catalog(
                _model(connection).catalog,
                con=connection,
                units={"user": UnitSource("users", "id")},
                readiness={"events": Coverage("2025-01-01", "2025-02-01")},
            )
            request = PlanRequest(
                f"dbt-build-{cycle}",
                ModelReference.from_model(model, owner_key="dbt-fixture"),
                Design(
                    "user",
                    FixedDate("2025-01-16"),
                    features={"total": agg("total", window=("-1d", "0s"))},
                    outcomes={},
                ),
                DestinationRef(str(database), "published", "study"),
            )
            request_path = tmp_path / f"request-{cycle}.json"
            request_path.write_text(json.dumps(request.to_dict()))
            client_script = (
                "import json,sys; from pathlib import Path; "
                "from dataclasses import asdict; "
                "from terrine.runtime.control import ControlEndpoint; "
                "from terrine.runtime.planning import PlanRequest; "
                "from examples.orchestrators.dbt_core_after_build import run_after_dbt_build; "
                "request=PlanRequest.from_dict(json.loads(Path(sys.argv[2]).read_text())); "
                "exec('try:\\n receipt=run_after_dbt_build(build_id=request.invocation_id, "
                "plan_request_provider=lambda _: request, "
                'endpoint=ControlEndpoint(Path(sys.argv[1])), namespace="owner")\\n'
                "except Exception as error:\\n "
                'print(getattr(error, "code", None), file=sys.stderr)\\n raise\'); '
                "print(json.dumps(asdict(receipt)))"
            )
            if cycle == 1:
                first_attempt = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        client_script,
                        str(endpoint.path),
                        str(request_path),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                assert first_attempt.returncode != 0
                invocation_row = connection.con.execute(
                    "SELECT invocation_json FROM owner.runtime_plan_invocations "
                    "WHERE invocation_id=?",
                    [request.invocation_id],
                ).fetchone()
                assert invocation_row is not None
                failed_invocation = PlanInvocation.from_dict(json.loads(invocation_row[0]))
                failed_receipt = ControlClient(endpoint, namespace="owner").find_run_by_request_key(
                    failed_invocation.request_key,
                    destination=failed_invocation.destination,
                )
                assert failed_receipt is not None
                assert failed_receipt.state == "failed"
                failed_fence = connection.con.execute(
                    "SELECT attempt_fence FROM owner.runtime_runs WHERE run_id=?",
                    [failed_receipt.run_id],
                ).fetchone()[0]
                completed_before_retry = tuple(executed_task_ids)

            completed = subprocess.run(
                [sys.executable, "-c", client_script, str(endpoint.path), str(request_path)],
                check=True,
                capture_output=True,
                text=True,
            )
            receipt = json.loads(completed.stdout)
            receipts.append(RunReceipt(**receipt))
            published = open_published_frame(receipts[-1], con=connection.con, namespace="owner")
            assert published.table.select(["unit", "total"]).to_pylist() == [
                {"unit": 1, "total": value}
            ]
            assert published.provenance.eligibility_summary.retained == 1
            assert published.provenance.model_hash == model.model_hash
            if cycle == 1:
                assert receipt["run_id"] == failed_receipt.run_id
                advanced_fence = connection.con.execute(
                    "SELECT attempt_fence FROM owner.runtime_runs WHERE run_id=?",
                    [receipt["run_id"]],
                ).fetchone()[0]
                assert advanced_fence > failed_fence
                assert executed_task_ids[: len(completed_before_retry)] == list(
                    completed_before_retry
                )
                assert len(executed_task_ids) == len(set(executed_task_ids))
            replayed = subprocess.run(
                [sys.executable, "-c", client_script, str(endpoint.path), str(request_path)],
                check=True,
                capture_output=True,
                text=True,
            )
            assert json.loads(replayed.stdout)["run_id"] == receipt["run_id"]
            status = connection.con.execute(
                "SELECT state FROM owner.runtime_runs WHERE run_id=?", [receipt["run_id"]]
            ).fetchone()
            assert status == ("succeeded",)
            row = connection.con.execute(
                "SELECT invocation_json FROM owner.runtime_plan_invocations WHERE invocation_id=?",
                [request.invocation_id],
            ).fetchone()
            assert row is not None
            plan_invocations.append(PlanInvocation.from_dict(json.loads(row[0])))
            assert (
                published.provenance.execution_snapshot.snapshot_id
                == plan_invocations[-1].snapshot_id
            )
    finally:
        server.close()
        connection.disconnect()

    assert plan_invocations[0].invocation_id == "dbt-build-1"
    assert plan_invocations[1].invocation_id == "dbt-build-2"
    assert plan_invocations[0].snapshot_id != plan_invocations[1].snapshot_id
    assert plan_invocations[0].request_key != plan_invocations[1].request_key


def test_airflow_and_dagster_share_owner_state_and_failed_run_fence(tmp_path):
    pytest.importorskip("airflow")
    pytest.importorskip("dagster")
    import os
    from pathlib import Path
    from uuid import uuid4

    import ibis
    import pyarrow as pa
    from airflow import DAG
    from dagster import DagsterInstance, materialize

    from terrine.data import Coverage, UnitSource
    from terrine.errors import TerrineError
    from terrine.model import Model
    from terrine.orchestration.airflow import build_materialization_group
    from terrine.orchestration.dagster import build_materialization_asset
    from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner, PlanRequest
    from terrine.runtime.recipe import DestinationRef, ModelReference
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.runtime.state import RuntimeStore
    from terrine.spec import agg
    from terrine.spec.anchors import FixedDate
    from terrine.spec.design import Design
    from tests.runtime.test_recipe import _model

    database = tmp_path / "shared-adapters.duckdb"
    connection = ibis.duckdb.connect(database=str(database))
    connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    connection.raw_sql("INSERT INTO events VALUES ('2025-01-15', 1, 987654321)")
    connection.raw_sql("CREATE TABLE users (id INTEGER)")
    connection.raw_sql("INSERT INTO users VALUES (1)")
    model = Model.from_catalog(
        _model(connection).catalog,
        con=connection,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
    )
    model_reference = ModelReference.from_model(model, owner_key="shared-adapters")
    design = Design(
        "user",
        FixedDate("2025-01-15"),
        features={"total": agg("total", window=("-1d", "0s"))},
        outcomes={},
    )
    destination = DestinationRef(str(database), "published", "study")

    class Resolver:
        def resolve(self, reference):
            assert reference == model_reference
            return model

    class Executor:
        calls = 0

        def run_task(self, task, dependencies):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient owner failure")
            return pa.table({"task_id": [task.task_id]})

        def run_snapshot_task(self, context, task, dependencies):
            return self.run_task(task, dependencies)

        def publish(self, plan, outputs, lease, run_id):
            return f"generation-{run_id}"

    resolver = Resolver()
    executor = Executor()
    authority = SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots"))
    planner = OwnerPlanner(resolver=resolver, snapshot_authority=authority)
    endpoint = ControlEndpoint(Path("/tmp") / f"terrine-composed-{uuid4().hex}.sock")
    server = ControlServer(
        RuntimeStore(connection.con, namespace="owner"),
        namespace="owner",
        endpoint=endpoint,
        plan_planner=planner,
        executor=executor,
        resolver=resolver,
        snapshot_authority=authority,
    )
    airflow_request = PlanRequest(
        "airflow-build",
        model_reference,
        design,
        destination,
    )
    with DAG(dag_id="shared-owner", schedule=None, start_date=None):
        group = build_materialization_group(
            plan_request_provider=lambda context: airflow_request,
            endpoint=endpoint,
            namespace="owner",
            pool="terrine-owner",
        )
    task = next(iter(group.children.values()))
    xcom_payloads = []
    task_instance = type(
        "TaskInstance",
        (),
        {"xcom_push": lambda self, **kwargs: xcom_payloads.append(kwargs)},
    )()
    context = {"ti": task_instance}
    asset = build_materialization_asset(
        plan_request_provider=lambda values: PlanRequest(
            "provider-request",
            model_reference,
            Design(
                "user",
                FixedDate(values["partition_key"]),
                features={"total": agg("total", window=("-1d", "0s"))},
                outcomes={},
            ),
            destination,
        ),
        endpoint=endpoint,
        namespace="owner",
        partitions_start="2025-01-01",
        resource_concurrency_key="terrine-owner",
    )
    server.start()
    try:
        with pytest.raises(TerrineError):
            task.execute(context)
        failed_invocation = task._invocation
        assert failed_invocation is not None
        failed = ControlClient(endpoint, namespace="owner").find_run_by_request_key(
            failed_invocation.request_key,
            destination=failed_invocation.destination,
        )
        assert failed is not None
        first_fence = connection.con.execute(
            "SELECT attempt_fence FROM owner.runtime_runs WHERE run_id=?", [failed.run_id]
        ).fetchone()[0]
        airflow_result = task.execute(context)
        advanced_fence = connection.con.execute(
            "SELECT attempt_fence FROM owner.runtime_runs WHERE run_id=?", [failed.run_id]
        ).fetchone()[0]
        assert airflow_result["run_id"] == failed.run_id
        assert advanced_fence > first_fence

        serialized_xcom = json.dumps(xcom_payloads)
        assert "987654321" not in serialized_xcom
        assert "transient owner failure" not in serialized_xcom
        cli_env = {
            **os.environ,
            "TERRINE_RUNTIME_SOCKET": str(endpoint.path),
            "TERRINE_RUNTIME_NAMESPACE": "owner",
        }
        cli_status = subprocess.run(
            [
                str(Path(sys.executable).with_name("terrine")),
                "status",
                airflow_result["run_id"],
            ],
            check=True,
            capture_output=True,
            text=True,
            env=cli_env,
        )
        cli_run = json.loads(cli_status.stdout)
        assert cli_run["run_id"] == airflow_result["run_id"]
        assert cli_run["durable_state"] == airflow_result["state"]
        assert cli_run["pinned_result_ref"] == airflow_result["published_generation"]

        dagster_result = materialize(
            [asset],
            partition_key="2025-01-15",
            instance=DagsterInstance.ephemeral(),
        )
        assert dagster_result.success
        states = connection.con.execute(
            "SELECT state FROM owner.runtime_runs ORDER BY updated_at, run_id"
        ).fetchall()
        assert [state for (state,) in states].count("succeeded") == 2
        assert (
            connection.con.execute(
                "SELECT count(*) FROM owner.runtime_plan_invocations"
            ).fetchone()[0]
            == 2
        )
    finally:
        server.close()
        connection.disconnect()


def test_dagster_historical_partitions_publish_distinct_owner_outputs(monkeypatch, tmp_path):
    pytest.importorskip("dagster")
    from datetime import UTC, datetime

    import ibis
    from dagster import DagsterInstance, materialize

    from terrine.data import Coverage, UnitSource
    from terrine.model import Model
    from terrine.orchestration.dagster import build_materialization_asset
    from terrine.runtime.api import get_run, open_published_frame, run_serialized_plan
    from terrine.runtime.control import ControlEndpoint
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner, PlanRequest
    from terrine.runtime.recipe import DestinationRef, ModelReference
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.spec import Design, FixedDate, agg
    from tests.runtime.test_recipe import _model

    database = tmp_path / "historical.duckdb"
    connection = ibis.duckdb.connect(database=str(database))
    connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    connection.raw_sql(
        "INSERT INTO events VALUES ('2025-01-13 12:00:00', 1, 3), ('2025-01-14 12:00:00', 1, 4)"
    )
    connection.raw_sql("CREATE TABLE users (id INTEGER)")
    connection.raw_sql("INSERT INTO users VALUES (1)")
    model = Model.from_catalog(
        _model(connection).catalog,
        con=connection,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
    )
    reference_model = ModelReference.from_model(model, owner_key="owner-model")

    class Resolver:
        def resolve(self, reference):
            assert reference == reference_model
            return model

    authority = SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots"))
    planner = OwnerPlanner(resolver=Resolver(), snapshot_authority=authority)
    planned = []
    invocations = []
    receipts = []

    class LocalOwnerClient:
        def __init__(self, endpoint, *, namespace):
            assert namespace == "owner"

        def plan(self, request):
            planned.append(request)
            invocation = planner.plan(request, con=connection.con, namespace="owner")
            invocations.append(invocation)
            return invocation

        def run(self, serialized_plan, *, request_key, destination):
            receipt = run_serialized_plan(
                serialized_plan,
                con=connection.con,
                namespace="owner",
                request_key=request_key,
                destination=destination,
                resolver=Resolver(),
                snapshot_authority=authority,
            )
            receipts.append(receipt)
            return receipt

        def get_run(self, run_id):
            return get_run(run_id, con=connection.con, namespace="owner")

    import terrine.orchestration.dagster as adapter

    monkeypatch.setattr(adapter, "ControlClient", LocalOwnerClient)
    endpoint = ControlEndpoint(tmp_path / "local-owner.sock")
    asset = build_materialization_asset(
        plan_request_provider=lambda context: PlanRequest(
            "provider-id",
            reference_model,
            Design(
                "user",
                FixedDate(context["partition_key"]),
                features={"total": agg("total", window=("-1d", "0s"))},
                outcomes={},
            ),
            DestinationRef(str(database), "published", "study"),
        ),
        endpoint=endpoint,
        namespace="owner",
        partitions_start="2025-01-01",
        resource_concurrency_key="terrine-owner",
    )
    assert asset.node_def.tags["dagster/concurrency_key"] == "terrine-owner"
    from dagster._utils.tags import TagConcurrencyLimitsCounter

    queue_limit = TagConcurrencyLimitsCounter(
        [
            {
                "key": "dagster/concurrency_key",
                "value": "terrine-owner",
                "limit": 1,
            }
        ],
        [cast(Any, SimpleNamespace(tags=dict(asset.node_def.tags)))],
    )
    assert queue_limit.is_blocked(cast(Any, SimpleNamespace(tags=dict(asset.node_def.tags))))
    try:
        instance = DagsterInstance.ephemeral()
        results = [
            materialize([asset], partition_key=partition, instance=instance)
            for partition in ("2025-01-14", "2025-01-15")
        ]
        assert all(result.success for result in results)
        assert len({receipt.published_generation for receipt in receipts}) == 2
        assert [request.requested_scope for request in planned] == [
            "2025-01-14",
            "2025-01-15",
        ]
        assert [request.design.anchor for request in planned] == [
            FixedDate("2025-01-14"),
            FixedDate("2025-01-15"),
        ]
        assert [request.destination.dataset for request in planned] == [
            "study_p2025_01_14",
            "study_p2025_01_15",
        ]
        assert len({invocation.snapshot_id for invocation in invocations}) == 2
        assert len(receipts) == 2
        assert all(
            receipt.state == "succeeded" and receipt.published_generation for receipt in receipts
        )
        published = [
            open_published_frame(receipt, con=connection.con, namespace="owner")
            for receipt in receipts
        ]
        assert [frame.table.select(["unit", "t0", "total"]).to_pylist() for frame in published] == [
            [{"unit": 1, "t0": datetime(2025, 1, 14, tzinfo=UTC), "total": 3}],
            [{"unit": 1, "t0": datetime(2025, 1, 15, tzinfo=UTC), "total": 4}],
        ]
        assert all(frame.provenance.eligibility_summary.retained == 1 for frame in published)
        assert [frame.provenance.execution_snapshot.snapshot_id for frame in published] == [
            invocation.snapshot_id for invocation in invocations
        ]
    finally:
        connection.disconnect()


def test_dagster_same_root_failed_retry_reuses_fence_scoped_outputs(monkeypatch, tmp_path):
    pytest.importorskip("dagster")
    import ibis
    import pyarrow as pa

    from terrine.data import Coverage, UnitSource
    from terrine.model import Model
    from terrine.orchestration.dagster import build_materialization_asset
    from terrine.runtime.api import (
        find_run_by_request_key,
        get_run,
        run_serialized_plan,
    )
    from terrine.runtime.control import ControlEndpoint
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner, PlanRequest
    from terrine.runtime.recipe import DestinationRef, ModelReference
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.spec import Design, FixedDate, agg
    from tests.runtime.test_recipe import _model

    database = tmp_path / "retry.duckdb"
    connection = ibis.duckdb.connect(database=str(database))
    connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    connection.raw_sql("INSERT INTO events VALUES ('2025-01-15', 1, 4)")
    connection.raw_sql("CREATE TABLE users (id INTEGER)")
    connection.raw_sql("INSERT INTO users VALUES (1)")
    model = Model.from_catalog(
        _model(connection).catalog,
        con=connection,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
    )
    reference_model = ModelReference.from_model(model, owner_key="owner-model")

    class Resolver:
        def resolve(self, reference):
            assert reference == reference_model
            return model

    class FailsOnce:
        def __init__(self):
            self.completed = []
            self.failed = False

        def _run(self, task):
            if len(self.completed) == 1 and not self.failed:
                self.failed = True
                raise RuntimeError("transient task failure")
            self.completed.append(task.task_id)
            return pa.table({"task_id": [task.task_id]})

        def run_task(self, task, dependencies):
            return self._run(task)

        def run_snapshot_task(self, context, task, dependencies):
            return self._run(task)

        def publish(self, plan, outputs, lease, run_id):
            return "generation-1"

    executor = FailsOnce()
    authority = SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots"))
    planner = OwnerPlanner(
        resolver=Resolver(),
        snapshot_authority=authority,
    )
    invocations = []
    receipts = []

    class LocalOwnerClient:
        def __init__(self, endpoint, *, namespace):
            assert namespace == "owner"

        def plan(self, request):
            invocation = planner.plan(request, con=connection.con, namespace="owner")
            invocations.append(invocation)
            return invocation

        def run(self, serialized_plan, *, request_key, destination):
            receipt = run_serialized_plan(
                serialized_plan,
                con=connection.con,
                namespace="owner",
                request_key=request_key,
                destination=destination,
                executor=executor,
                resolver=Resolver(),
                snapshot_authority=authority,
            )
            receipts.append(receipt)
            return receipt

        def get_run(self, run_id):
            return get_run(run_id, con=connection.con, namespace="owner")

    import terrine.orchestration.dagster as adapter

    monkeypatch.setattr(adapter, "ControlClient", LocalOwnerClient)
    asset = build_materialization_asset(
        plan_request_provider=lambda context: PlanRequest(
            "provider-id",
            reference_model,
            Design(
                "user",
                FixedDate(context["partition_key"]),
                features={"total": agg("total", window=("-1d", "0s"))},
                outcomes={},
            ),
            DestinationRef(str(database), "published", "study"),
        ),
        endpoint=ControlEndpoint(tmp_path / "local-owner.sock"),
        namespace="owner",
        partitions_start="2025-01-01",
        resource_concurrency_key="terrine-owner",
    )

    class Context:
        partition_key = "2025-01-15"
        run = SimpleNamespace(run_id="retry-run", root_run_id="same-root")

        class Instance:
            def add_run_tags(self, run_id, tags):
                pass

        instance = Instance()

        def add_output_metadata(self, metadata):
            pass

    compute = asset.node_def.compute_fn.decorated_fn
    try:
        with pytest.raises(RuntimeError, match="transient task failure"):
            compute(Context())
        failed = find_run_by_request_key(
            invocations[0].request_key,
            destination=invocations[0].destination,
            con=connection.con,
            namespace="owner",
        )
        assert failed is not None
        first_fence = connection.con.execute(
            "SELECT attempt_fence FROM owner.runtime_runs WHERE run_id=?",
            [failed.run_id],
        ).fetchone()[0]
        completed_before_retry = tuple(executor.completed)

        compute(Context())

        advanced_fence = connection.con.execute(
            "SELECT attempt_fence FROM owner.runtime_runs WHERE run_id=?",
            [failed.run_id],
        ).fetchone()[0]
        assert len(invocations) == 2
        assert invocations[0].invocation_id == invocations[1].invocation_id
        assert invocations[0].request_key == invocations[1].request_key
        assert receipts[0].run_id == failed.run_id
        assert advanced_fence > first_fence
        assert completed_before_retry
        assert executor.completed[: len(completed_before_retry)] == list(completed_before_retry)
        assert len(executor.completed) == len(set(executor.completed))
    finally:
        connection.disconnect()


def test_airflow_operator_runs_and_retries_persisted_owner_invocation(tmp_path):
    pytest.importorskip("airflow")
    from pathlib import Path
    from uuid import uuid4

    import ibis
    import pyarrow as pa
    from airflow import DAG
    from airflow.sdk import BaseOperator

    from terrine.data import Coverage, UnitSource
    from terrine.errors import TerrineError
    from terrine.model import Model
    from terrine.orchestration.airflow import build_materialization_group
    from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner, PlanRequest
    from terrine.runtime.recipe import DestinationRef, ModelReference
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.runtime.state import RuntimeStore
    from terrine.spec import Design, FixedDate, agg
    from tests.runtime.test_recipe import _model

    database = tmp_path / "operator.duckdb"
    connection = ibis.duckdb.connect(database=str(database))
    connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    connection.raw_sql("INSERT INTO events VALUES ('2025-01-10', 1, 3)")
    connection.raw_sql("CREATE TABLE users (id INTEGER)")
    connection.raw_sql("INSERT INTO users VALUES (1)")
    base_model = _model(connection)
    model = Model.from_catalog(
        base_model.catalog,
        con=connection,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
    )
    request = PlanRequest(
        "airflow-stable-invocation",
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

    class Executor:
        calls = 0

        def run_snapshot_task(self, context, task, dependencies):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("planned transient failure")
            return pa.table({"task_id": [task.task_id]})

        def publish(self, plan, outputs, lease, run_id):
            return "generation-1"

    authority = SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots"))
    planner = OwnerPlanner(resolver=Resolver(), snapshot_authority=authority)
    endpoint = ControlEndpoint(Path("/tmp") / f"terrine-airflow-{uuid4().hex}.sock")
    executor = Executor()
    server = ControlServer(
        RuntimeStore(connection.con, namespace="owner"),
        namespace="owner",
        endpoint=endpoint,
        executor=executor,
        plan_planner=planner,
        resolver=Resolver(),
        snapshot_authority=authority,
    )
    with DAG(dag_id="owner-retry", schedule=None, start_date=None):
        group = build_materialization_group(
            plan_request_provider=lambda context: request,
            endpoint=endpoint,
            namespace="owner",
            pool="terrine-owner",
        )
    task = cast(BaseOperator, next(iter(group.children.values())))
    operator = cast(Any, task)
    from types import SimpleNamespace

    context = cast(Any, {"ti": SimpleNamespace(xcom_push=lambda **kwargs: None)})
    server.start()
    try:
        with pytest.raises(TerrineError):
            task.execute(context)
        retried = task.execute(context)
        assert retried["state"] == "succeeded"
        assert retried["snapshot_id"] == operator._invocation.snapshot_id
        receipt = ControlClient(endpoint, namespace="owner").find_run_by_request_key(
            operator._invocation.request_key,
            destination=operator._invocation.destination,
        )
        assert receipt is not None
        assert receipt.state == "succeeded"
        assert executor.calls > 1
    finally:
        server.close()
        connection.disconnect()
