from __future__ import annotations

import json
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

pytest.importorskip("dbt")


def test_two_dbt_build_cycles_stop_owner_and_replay_from_separate_processes(tmp_path):
    import ibis

    from terrine.data import Coverage, UnitSource
    from terrine.model import Model
    from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
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

    class Executor:
        def _run(self, task):
            nonlocal fail_once
            import pyarrow as pa

            if fail_once and len(executed_task_ids) == 1:
                fail_once = False
                raise RuntimeError("transient dbt-triggered task failure")
            executed_task_ids.append(task.task_id)
            return pa.table({"task_id": [task.task_id]})

        def run_task(self, task, dependencies):
            return self._run(task)

        def run_snapshot_task(self, context, task, dependencies):
            return self._run(task)

        def publish(self, plan, outputs, lease, run_id):
            return f"generation-{run_id}"

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
            executor=Executor(),
            resolver=Resolver(),
            snapshot_authority=authority,
        )
        server.start()
        return connection, server

    connection, server = start_owner()
    plan_invocations = []
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
            connection, server = start_owner()
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
                    FixedDate("2025-01-15"),
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
                "receipt=run_after_dbt_build(build_id=request.invocation_id, "
                "plan_request_provider=lambda _: request, "
                "endpoint=ControlEndpoint(Path(sys.argv[1])), namespace='owner'); "
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
    connection.raw_sql("INSERT INTO events VALUES ('2025-01-15', 1, 4)")
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
    context = {"ti": type("TaskInstance", (), {"xcom_push": lambda self, **kwargs: None})()}
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
