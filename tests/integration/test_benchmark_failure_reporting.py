"""Failure-lifecycle guards; these fixtures never execute a remote workload."""

import json
from contextlib import contextmanager
from decimal import Decimal
from types import SimpleNamespace

import pytest

from benchmarks.report import summarize
from benchmarks.run_live import failure_report, main, run_approved
from terrine.errors import TerrineError
from terrine.runtime.backends.base import BackendCapabilities
from terrine.runtime.cost import ScanAuthorization, ScanEvidence, ScanPhase
from terrine.simulate.scale import ScaleConfig


@pytest.mark.parametrize("failure", ["query", "cleanup", "observation", "cap_exceeded", "close"])
def test_live_cli_preserves_available_costs_after_cleanup_on_failure(
    monkeypatch, tmp_path, failure
):
    events = []

    class QueryFailed(RuntimeError):
        pass

    class CleanupFailed(RuntimeError):
        pass

    class CloseFailed(RuntimeError):
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
                        ScanPhase(
                            phase, "verified_upper_bound", 1, "failure-lifecycle fixture only"
                        )
                        for phase in ("capture", "validation", "execution", "retry")
                    ),
                ),
                basis="failure-lifecycle fixture only",
            )

    class Session:
        backend = "duckdb"
        account_ref = "owner-account"
        workspace_ref = "isolated-scale"
        remote = True
        cost_provider = Provider()

        def authorize_workload(self, plan, *, max_cost, currency):
            return ScanAuthorization(plan.recipe.fingerprint)

        @contextmanager
        def isolated_workspace(self):
            events.append("open")
            try:
                yield SimpleNamespace(name="duckdb")
            finally:
                events.append("cleanup")
                if failure == "cleanup":
                    raise CleanupFailed("SECRET cleanup SQL")

        def observed_cost(self):
            events.append("observe")
            if failure == "observation":
                raise OSError("SECRET billing response")
            return {
                "scan_bytes": 2048 if failure == "cap_exceeded" else 17,
                "scan_cost": Decimal("2.00" if failure == "cap_exceeded" else "0.25"),
            }

        def close(self):
            events.append("close")
            if failure == "close":
                raise CloseFailed("SECRET connection detail")

    def fail_query(*args, **kwargs):
        events.append("query")
        raise QueryFailed("SECRET customer rows and SQL")

    monkeypatch.setattr(
        "benchmarks.run_live.importlib.import_module",
        lambda module: SimpleNamespace(open_session=lambda approval: Session()),
    )
    monkeypatch.setattr("benchmarks.run_live.load_model", fail_query)
    approval = tmp_path / "approval.json"
    approval.write_text(
        json.dumps(
            {
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
        )
    )
    destination = tmp_path / "failed.json"
    monkeypatch.setattr(
        "sys.argv", ["run_live", "--approval", str(approval), "--output", str(destination)]
    )
    with pytest.raises(SystemExit) as exit_status:
        main()
    assert exit_status.value.code == 2
    encoded = destination.read_text()
    report = json.loads(encoded)
    assert "SECRET" not in encoded
    assert events == ["open", "query", "cleanup", "observe", "close"]
    assert report["status"] == "failed"
    assert report["backend"] == "duckdb"
    assert report["workload"]["version"] == 2
    if failure == "observation":
        assert report["scan_bytes"] is None and report["scan_cost"] is None
        assert report["cost_observation_error_type"] == "OSError"
    else:
        assert report["scan_bytes"] == (2048 if failure == "cap_exceeded" else 17)
        assert Decimal(report["scan_cost"]) == Decimal(
            "2.00" if failure == "cap_exceeded" else "0.25"
        )
    if failure == "cap_exceeded":
        assert report["scan_cost_status"] == "observed_cap_exceeded"
    assert report["error_type"] == ("CleanupFailed" if failure == "cleanup" else "QueryFailed")
    if failure == "close":
        assert report["close_error_type"] == "CloseFailed"


def test_report_aggregates_verified_failed_and_refused_measurements_without_inventing_values():
    records = [
        {"status": "verified", "backend": "duckdb", "workload": {"units": 1}, "scan_bytes": None},
        {"status": "failed", "error_type": "QueryFailed", "scan_bytes": 17, "scan_cost": "0.25"},
        {"status": "refused", "code": "terrine.benchmark.live_owner_gate"},
    ]
    report = summarize(records)
    assert report["all_verified"] is False and report["runs"] == 3
    assert report["measurements"][1]["backend"] is None
    assert report["measurements"][1]["workload"] is None
    assert report["measurements"][1]["scan_bytes"] == 17
    assert report["measurements"][1]["scan_cost"] == "0.25"
    assert report["measurements"][1]["error_type"] == "QueryFailed"
    assert report["measurements"][2]["scan_bytes"] is None
    assert report["measurements"][2]["code"] == "terrine.benchmark.live_owner_gate"


def test_failure_report_is_detached_from_immutable_error_context():
    with pytest.raises(TerrineError) as failure:
        run_approved({})
    frozen = failure.value.context["report"]
    with pytest.raises(TypeError):
        frozen["status"] = "verified"
    recovered = failure_report(failure.value)
    recovered["status"] = "verified"
    assert frozen["status"] == "refused"
    assert frozen["scan_bytes"] is None and frozen["workload"] is None
