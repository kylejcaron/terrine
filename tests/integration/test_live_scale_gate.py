from decimal import Decimal
from types import SimpleNamespace

import pytest

from benchmarks.run_live import LiveApproval, run_approved
from terrine.errors import TerrineError
from terrine.runtime.backends.base import BackendCapabilities
from terrine.runtime.cost import ScanAuthorization, ScanEvidence, ScanPhase
from terrine.simulate.scale import ScaleConfig


def approval():
    return {
        "approved": True,
        "backend": "duckdb",
        "account_ref": "owner-account",
        "workspace_ref": "isolated-scale",
        "factory": "approved_owner:open_session",
        "workload": ScaleConfig(units=2, events_per_unit=3, batch_size=2).to_dict(),
        "max_scan_bytes": 1024,
        "max_cost": "1.00",
        "currency": "USD",
        "cases": ["frame"],
    }


@pytest.mark.parametrize(
    "change",
    [
        {"approved": False},
        {"max_scan_bytes": 0},
        {"max_cost": "NaN"},
        {"cases": []},
        {"cases": ["unsupported"]},
    ],
)
def test_live_owner_gate_refuses_before_factory_or_connection(monkeypatch, change):
    calls = []

    def forbidden(*args):
        calls.append(args)
        raise AssertionError("invalid approval opened an owner adapter")

    monkeypatch.setattr("benchmarks.run_live.importlib.import_module", forbidden)
    with pytest.raises(TerrineError) as error:
        run_approved({**approval(), **change})
    assert error.value.code == "terrine.benchmark.live_owner_gate"
    assert calls == []


def test_approval_workload_is_immutable_and_versioned():
    parsed = LiveApproval.model_validate(approval())
    for field in parsed.workload.to_dict():
        with pytest.raises((AttributeError, TypeError)):
            setattr(parsed.workload, field, 99)
    with pytest.raises(TerrineError):
        run_approved({**approval(), "workload": {**approval()["workload"], "version": 1}})


@pytest.mark.parametrize("remote", [False, True])
def test_local_or_unknown_scan_owner_never_enters_a_workspace(monkeypatch, remote):
    class Session:
        backend = "duckdb"
        account_ref = "owner-account"
        workspace_ref = "isolated-scale"
        cost_provider = SimpleNamespace(
            capabilities=BackendCapabilities(False, frozenset(), True, None, None)
        )
        closed = False

        def __init__(self, is_remote):
            self.remote = is_remote

        def isolated_workspace(self):
            raise AssertionError("unbounded or local owner entered a live workspace")

        def close(self):
            self.closed = True

    session = Session(remote)
    monkeypatch.setattr(
        "benchmarks.run_live.importlib.import_module",
        lambda module: SimpleNamespace(open_session=lambda approval: session),
    )
    with pytest.raises(TerrineError) as error:
        run_approved(approval())
    assert error.value.code == (
        "terrine.budget.scan_unknown" if remote else "terrine.benchmark.live_scope_mismatch"
    )
    assert session.closed


@pytest.mark.parametrize("authorized", [False, True])
def test_exact_owner_authorization_precedes_any_workspace_source_work(monkeypatch, authorized):
    class StoppedBeforeSourceWork(RuntimeError):
        pass

    class Provider:
        capabilities = BackendCapabilities(False, frozenset(), True, True, None)

        def estimate_task(self, task, *, definition_hash, snapshot_id):
            return task.with_estimate(
                scan=ScanEvidence(
                    definition_hash,
                    snapshot_id,
                    1,
                    tuple(
                        ScanPhase(phase, "verified_upper_bound", 1, "admission-guard fixture only")
                        for phase in ("capture", "validation", "execution", "retry")
                    ),
                ),
                basis="admission-guard fixture only",
            )

    class Session:
        backend = "duckdb"
        account_ref = "owner-account"
        workspace_ref = "isolated-scale"
        remote = True
        cost_provider = Provider()
        entered = False
        closed = False
        fingerprints = []

        def authorize_workload(self, plan, *, max_cost, currency):
            expected_cost = document["max_cost"]
            assert isinstance(expected_cost, str)
            assert max_cost == Decimal(expected_cost)
            assert currency == document["currency"]
            self.fingerprints.append(plan.definition.fingerprint)
            assert plan.tasks[0].scan_evidence.max_attempts == 1
            assert plan.budget.max_rows == 2 * (2 * 4 + 5)
            return ScanAuthorization(plan.definition.fingerprint if authorized else "wrong-scope")

        def isolated_workspace(self):
            self.entered = True
            raise StoppedBeforeSourceWork

        def observed_cost(self):
            return {"scan_bytes": None, "scan_cost": None}

        def close(self):
            self.closed = True

    session = Session()
    monkeypatch.setattr(
        "benchmarks.run_live.importlib.import_module",
        lambda module: SimpleNamespace(open_session=lambda approval: session),
    )
    for max_cost, currency in (("1.00", "USD"), ("2.00", "USD"), ("1.00", "EUR")):
        document = {
            **approval(),
            "max_cost": max_cost,
            "currency": currency,
            "cases": ["frame", "schedule", "sequential", "event_panel", "sketch"],
        }
        if authorized:
            with pytest.raises(TerrineError) as error:
                run_approved(document)
            assert error.value.code == "terrine.benchmark.live_execution_failed"
            assert error.value.context["report"]["error_type"] == "StoppedBeforeSourceWork"
        else:
            with pytest.raises(TerrineError) as error:
                run_approved(document)
            assert error.value.code == "terrine.budget.scan_authority_unavailable"
        assert session.entered is authorized
        assert session.closed
    assert len(set(session.fingerprints)) == 3
