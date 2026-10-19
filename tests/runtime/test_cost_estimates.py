import hashlib
import json
from dataclasses import replace
from typing import Literal

import duckdb
import pyarrow as pa
import pytest

from terrine.data import Budget
from terrine.errors import TerrineError
from terrine.runtime.backends.base import BackendCapabilities
from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.cost import ScanAuthorization, ScanEvidence, ScanPhase
from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import RefreshPlan, plan_refresh
from terrine.runtime.status import get_run


class FileOwner:
    """Owner for bounded reads of one immutable local IPC fixture."""

    capabilities = BackendCapabilities(False, frozenset(), True, True, None)

    def __init__(
        self,
        path,
        table,
        *,
        status: Literal[
            "forecast", "verified_upper_bound", "backend_enforced_cap"
        ] = "verified_upper_bound",
        attempts=1,
    ):
        self.path = path
        self.table = table
        self.size = path.stat().st_size
        self.status = status
        self.attempts = attempts
        self.omit = None
        self.calls = []
        self.bytes_read = 0
        self.fail = False

    def estimate_task(self, task, *, definition_hash, snapshot_id):
        scan = ScanEvidence(
            definition_hash,
            snapshot_id,
            self.attempts,
            tuple(
                ScanPhase(
                    phase,
                    self.status,
                    self.size if phase != "retry" else 3 * self.size * (self.attempts - 1),
                    "immutable IPC length; each read is limited to that length",
                )
                for phase in ("capture", "validation", "execution", "retry")
                if phase != self.omit
            ),
        )
        return task.with_estimate(
            scan=scan,
            output_bytes=self.table.nbytes,
            rows=self.table.num_rows,
            basis="immutable Arrow schema and length",
        )

    def authorize_scan(self, plan):
        for task in plan.tasks:
            fresh = self.estimate_task(
                task, definition_hash=plan.definition_hash, snapshot_id=plan.snapshot_id
            )
            if fresh != task or self.path.stat().st_size != self.size:
                raise ValueError("immutable fixture metadata changed")
        return ScanAuthorization(plan.recipe.fingerprint)

    def run_task(self, task, dependencies):
        self.calls.append(task.task_id)
        for _ in ("capture", "validation", "execution"):
            with self.path.open("rb") as stream:
                data = stream.read(self.size)
            self.bytes_read += len(data)
            result = pa.ipc.open_stream(data).read_all()
        if self.fail:
            raise RuntimeError("interrupted after bounded source reads")
        return result


@pytest.fixture
def owner(tmp_path):
    table = pa.table({"value": [1, 2, 3]})
    path = tmp_path / "input.arrow"
    with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return FileOwner(path, table)


def make_plan(owner=None, *, budget=None):
    return plan_refresh(
        namespace="owner",
        dataset="study",
        definition_hash="definition",
        snapshot_id="snapshot",
        graph=dependency_graph(sources=("events",), definition_hash="definition"),
        cost_provider=owner,
        budget=budget,
    )


def run(plan, owner, con, *, run_id="cost-run", authorize=True):
    return execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id=run_id,
        run_task=owner.run_task,
        publish=lambda plan, outputs, lease: "generation",
        authorize_scan=owner.authorize_scan if authorize else None,
    )


def test_metadata_costs_separate_scan_output_rows_and_all_read_phases(owner):
    plan = make_plan(owner)
    assert owner.calls == [] and owner.bytes_read == 0
    assert all(task.estimated_scan_bytes == 3 * owner.size for task in plan.tasks)
    assert all(task.estimated_output_bytes == owner.table.nbytes for task in plan.tasks)
    assert all(task.estimated_rows == len(owner.table) for task in plan.tasks)
    assert plan.scan_bytes == 3 * owner.size * len(plan.tasks)
    assert [phase.phase for phase in plan.tasks[0].scan_evidence.phases] == [
        "capture",
        "validation",
        "execution",
        "retry",
    ]


def test_unknown_duckdb_cost_is_not_output_bytes_and_does_not_query_sources():
    con = duckdb.connect()
    backend = DuckDBBackend(con, namespace="owner")
    con.execute("CREATE TABLE events AS SELECT 42 AS value")
    plan = make_plan(backend, budget=Budget(max_scan_bytes=1, strict_scan=False))
    assert plan.scan_bytes is None and plan.budget_warnings
    assert all(task.estimated_scan_bytes is None for task in plan.tasks)
    assert all(
        phase.scan_bytes is None and phase.basis == "backend_scan_bound_unavailable"
        for task in plan.tasks
        for phase in task.scan_evidence.phases
    )
    with pytest.raises(TerrineError) as error:
        make_plan(backend, budget=Budget(max_scan_bytes=1))
    assert error.value.code == "terrine.budget.scan_unknown"


@pytest.mark.parametrize("status", ["verified_upper_bound", "backend_enforced_cap"])
def test_strict_all_phase_bound_or_cap_admits_and_observations_do_not_rewrite(owner, status):
    owner.status = status
    estimate = make_plan(owner)
    plan = make_plan(owner, budget=Budget(max_scan_bytes=estimate.scan_bytes))
    saved = plan.serialize()
    con = duckdb.connect()
    assert run(plan, owner, con).state == "succeeded"
    assert owner.bytes_read == plan.scan_bytes
    assert plan.serialize() == saved
    cost = get_run("cost-run", con=con, namespace="owner").cost
    frames = sum(task.layer == "frame" for task in plan.tasks)
    assert cost["status"] == "observed"
    assert cost["output_rows"] == frames * len(owner.table)
    assert cost["output_bytes"] == frames * owner.table.nbytes
    assert cost["intermediate_output_bytes"] == (len(plan.tasks) - frames) * owner.table.nbytes
    assert cost["scan_bytes"] is None
    assert cost["scan_reason"] == "executor_scan_observation_unavailable"


@pytest.mark.parametrize("phase", ["capture", "validation", "execution", "retry"])
def test_every_read_phase_is_required_for_strict_authorization(owner, phase):
    owner.omit = phase
    with pytest.raises(TerrineError) as error:
        make_plan(owner, budget=Budget(max_scan_bytes=10 * owner.size))
    assert error.value.code == "terrine.budget.scan_unknown"
    assert owner.calls == []


def test_forecast_is_not_a_strict_bound(owner):
    owner.status = "forecast"
    forecast = make_plan(owner)
    assert forecast.scan_bytes == 3 * owner.size * len(forecast.tasks)
    with pytest.raises(TerrineError) as error:
        make_plan(owner, budget=Budget(max_scan_bytes=forecast.scan_bytes))
    assert error.value.code == "terrine.budget.scan_unknown"
    assert owner.calls == []


def test_known_over_budget_and_unattested_bound_refuse_before_source_callback(owner):
    estimate = make_plan(owner)
    with pytest.raises(TerrineError) as error:
        make_plan(owner, budget=Budget(max_scan_bytes=estimate.scan_bytes - 1))
    assert error.value.code == "terrine.budget.scan_exceeded"
    plan = make_plan(owner, budget=Budget(max_scan_bytes=estimate.scan_bytes))
    with pytest.raises(TerrineError) as error:
        run(plan, owner, duckdb.connect(), authorize=False)
    assert error.value.code == "terrine.budget.scan_authority_unavailable"
    assert owner.calls == []


def test_execution_rechecks_unknown_budget_before_callbacks(owner):
    plan = make_plan()
    unsafe = replace(plan, budget=Budget(max_scan_bytes=owner.size))
    with pytest.raises(TerrineError) as error:
        run(unsafe, owner, duckdb.connect())
    assert error.value.code == "terrine.budget.scan_unknown"
    assert owner.calls == []


def test_retry_allowance_is_aggregate_and_attempts_survive_restart(owner):
    owner.attempts = 2
    estimate = make_plan(owner)
    plan = make_plan(owner, budget=Budget(max_scan_bytes=estimate.scan_bytes))
    con = duckdb.connect()
    owner.fail = True
    for _ in range(owner.attempts):
        with pytest.raises(RuntimeError, match="interrupted"):
            run(plan, owner, con)
    calls = tuple(owner.calls)
    with pytest.raises(TerrineError) as error:
        run(RefreshPlan.deserialize(plan.serialize()), owner, con)
    assert error.value.code == "terrine.budget.scan_attempts_exhausted"
    assert tuple(owner.calls) == calls
    assert owner.bytes_read == 3 * owner.size * owner.attempts
    assert owner.bytes_read <= plan.scan_bytes


def test_partial_retry_can_resume_within_total_bound(owner):
    owner.attempts = 2
    estimate = make_plan(owner)
    plan = make_plan(owner, budget=Budget(max_scan_bytes=estimate.scan_bytes))
    con = duckdb.connect()
    owner.fail = True
    with pytest.raises(RuntimeError, match="interrupted"):
        run(plan, owner, con)
    owner.fail = False
    assert run(RefreshPlan.deserialize(plan.serialize()), owner, con).state == "succeeded"
    assert owner.bytes_read <= plan.scan_bytes
    assert get_run("cost-run", con=con, namespace="owner").cost["attempts"] == len(plan.tasks) + 1


def test_cost_evidence_serializes_and_changes_fingerprint(owner):
    plan = make_plan(owner)
    assert json.loads(plan.serialize())["schema_version"] == 2
    assert RefreshPlan.deserialize(plan.serialize()) == plan
    owner.status = "forecast"
    changed = make_plan(owner)
    assert changed.recipe.fingerprint != plan.recipe.fingerprint
    altered = json.loads(plan.serialize())
    altered["tasks"][0]["scan_evidence"]["phases"][0]["basis"] = "changed"
    with pytest.raises(ValueError, match="malformed|unverifiable"):
        RefreshPlan.deserialize(json.dumps(altered))
    altered = json.loads(plan.serialize())
    altered["schema_version"] = 1
    with pytest.raises(ValueError, match="malformed|unverifiable"):
        RefreshPlan.deserialize(json.dumps(altered))


def test_fingerprint_valid_forecast_cannot_be_promoted_by_budget(owner):
    owner.status = "forecast"
    body = json.loads(make_plan(owner).serialize())
    body["budget"] = {
        "max_rows": None,
        "max_output_bytes": None,
        "max_scan_bytes": 3 * owner.size * len(body["tasks"]),
        "strict_scan": True,
    }
    fingerprint = {key: value for key, value in body.items() if key != "fingerprint"}
    body["fingerprint"] = hashlib.sha256(
        json.dumps(fingerprint, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    with pytest.raises(TerrineError) as error:
        RefreshPlan.deserialize(json.dumps(body))
    assert error.value.code == "terrine.budget.scan_unknown"


@pytest.mark.parametrize("field", ["definition_hash", "snapshot_id"])
def test_cost_evidence_is_bound_to_definition_and_snapshot(owner, field):
    class WrongOwner:
        capabilities = owner.capabilities

        def estimate_task(self, task, **context):
            context[field] = "other"
            return owner.estimate_task(task, **context)

    with pytest.raises(TerrineError) as error:
        make_plan(WrongOwner())
    assert error.value.code == "terrine.runtime.cost_estimate_mismatch"


@pytest.mark.parametrize("value", [-1, True, 1.5])
def test_scan_evidence_rejects_non_integer_or_negative_bytes(value):
    with pytest.raises((TypeError, ValueError)):
        ScanPhase("execution", "verified_upper_bound", value, "fixture metadata")


def test_unbounded_retry_allowance_cannot_authorize_strict_scan(owner):
    class UnboundedOwner:
        capabilities = owner.capabilities

        def estimate_task(self, task, **context):
            estimated = owner.estimate_task(task, **context)
            scan = replace(estimated.scan_evidence, max_attempts=None)
            return estimated.with_estimate(
                scan=scan,
                rows=estimated.estimated_rows,
                output_bytes=estimated.estimated_output_bytes,
                basis=estimated.estimate_basis,
            )

    with pytest.raises(TerrineError) as error:
        make_plan(UnboundedOwner(), budget=Budget(max_scan_bytes=owner.size * 100))
    assert error.value.code == "terrine.budget.scan_unknown"


def test_owner_must_reauthorize_fingerprint_valid_but_untrusted_evidence(owner):
    owner.status = "forecast"
    body = json.loads(make_plan(owner).serialize())
    for task in body["tasks"]:
        for phase in task["scan_evidence"]["phases"]:
            phase["status"] = "verified_upper_bound"
    body["budget"] = {
        "max_rows": None,
        "max_output_bytes": None,
        "max_scan_bytes": sum(task["estimated_scan_bytes"] for task in body["tasks"]),
        "strict_scan": True,
    }
    fingerprint = {key: value for key, value in body.items() if key != "fingerprint"}
    body["fingerprint"] = hashlib.sha256(
        json.dumps(fingerprint, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    plan = RefreshPlan.deserialize(json.dumps(body))
    with pytest.raises(ValueError, match="metadata changed"):
        run(plan, owner, duckdb.connect())
    assert owner.calls == []


def test_scan_grant_must_name_exact_plan(owner):
    estimate = make_plan(owner)
    plan = make_plan(owner, budget=Budget(max_scan_bytes=estimate.scan_bytes))
    with pytest.raises(TerrineError) as error:
        execute_plan(
            plan,
            con=duckdb.connect(),
            namespace="owner",
            run_id="wrong-grant",
            run_task=owner.run_task,
            publish=lambda *args: "g",
            authorize_scan=lambda plan: ScanAuthorization("other"),
        )
    assert error.value.code == "terrine.budget.scan_authority_unavailable"
    assert owner.calls == []


def test_reconstruction_binding_preserves_scan_evidence_and_canonical_fingerprint(owner):
    plan = make_plan(owner)
    identity = "a" * 64
    bound = plan.bind_base_manifest("generation-1", reconstruction_hash=identity)
    assert bound.recipe.reconstruction_hash == identity
    assert bound.scan_bytes == plan.scan_bytes
    assert bound.tasks == plan.tasks
    assert bound.recipe.fingerprint != plan.recipe.fingerprint
    assert RefreshPlan.deserialize(bound.serialize()) == bound
    rebound = bound.bind_base_manifest("generation-2")
    assert rebound.recipe.reconstruction_hash == identity
    assert RefreshPlan.deserialize(rebound.serialize()) == rebound
    assert owner.calls == []
