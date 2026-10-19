"""Owner-gated live compiler tier. No connection is opened without an approved scope."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import time
import tracemalloc
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator

from benchmarks.workloads import _json, content_digest, designs, load_model, row_bound, run_case
from terrine import Budget
from terrine.errors import RefusalSpec, TerrineError, thaw
from terrine.runtime.cost import ScanAuthorization, authorize_plan_scan
from terrine.runtime.graph import DependencyGraph
from terrine.runtime.planner import RefreshPlan, plan_refresh
from terrine.runtime.state import LayerNode
from terrine.simulate.scale import ScaleConfig

_GATE = RefusalSpec(
    "terrine.benchmark.live_owner_gate",
    "Live execution requires an explicit owner-approved backend, account, isolated workspace, "
    "workload, scan ceiling and spend ceiling; source execution has not been authorized.",
)
_SCOPE = RefusalSpec(
    "terrine.benchmark.live_scope_mismatch",
    "The owner session differs from the approved backend/account/workspace; "
    "configure the matching owner adapter.",
)
_EXECUTION = RefusalSpec(
    "terrine.benchmark.live_execution_failed",
    "Live execution did not verify; inspect the sanitized report and repair the owner adapter "
    "or resource policy before rerunning.",
)


class LiveApproval(BaseModel):
    """Explicit resource policy, not a numerical proof supplied by ordinary Design callers."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    approved: StrictBool
    backend: StrictStr = Field(min_length=1)
    account_ref: StrictStr = Field(min_length=1)
    workspace_ref: StrictStr = Field(min_length=1)
    factory: StrictStr = Field(pattern=r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*$")
    workload: ScaleConfig
    max_scan_bytes: StrictInt = Field(gt=0)
    max_cost: Decimal = Field(gt=0, allow_inf_nan=False)
    currency: StrictStr = Field(pattern=r"^[A-Z]{3}$")
    cases: tuple[StrictStr, ...] = ("frame", "schedule", "sequential", "event_panel", "sketch")

    @field_validator("workload", mode="before")
    @classmethod
    def _workload(cls, value):
        if not isinstance(value, dict):
            raise ValueError("workload must be a complete generator configuration")
        return ScaleConfig(**value)


class LiveSession(Protocol):
    """Owner adapter for one real remote account and an isolated, disposable workspace.

    Opening may read connection metadata only. authorize_workload must install or
    verify scan AND monetary ceilings over uploads, validation, queries, streaming
    reads and cleanup before returning the exact plan authorization. It must refuse
    if either ceiling is not enforceable. The workspace context owns only resources
    created for this invocation; it must not overwrite or delete existing user data.
    Billable cleanup belongs inside isolated_workspace before observed_cost;
    close releases connection resources without starting new data queries.
    """

    backend: str
    account_ref: str
    workspace_ref: str
    remote: bool
    cost_provider: Any

    def authorize_workload(
        self, plan: RefreshPlan, *, max_cost: Decimal, currency: str
    ) -> ScanAuthorization: ...

    def isolated_workspace(self) -> Any: ...

    def observed_cost(self) -> dict[str, Any]: ...

    def probe_cancellation(self) -> bool: ...

    def close(self) -> None: ...


def _empty_report() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "failed",
        "backend": None,
        "workload": None,
        "input_events": None,
        "anchors": None,
        "emitted_rows": None,
        "oracle_match": None,
        "source_digest": None,
        "cases": [],
        "scan_bytes": None,
        "scan_cost": None,
        "scan_cost_status": "not_executed",
        "client_peak_memory_bytes": None,
        "process_peak_memory_bytes": None,
        "process_memory_scope": "owner_process_measurement_unavailable",
        "elapsed_seconds": None,
        "cancellation": {"status": "not_executed", "physically_cancelled": None},
    }


def failure_report(error: Exception, record: dict[str, Any] | None = None) -> dict[str, Any]:
    """Recover sanitized immutable failure evidence, including startup failures."""
    if record is None and isinstance(error, TerrineError) and "report" in error.context:
        return thaw(error.context["report"])
    result = _empty_report() if record is None else record
    result.update(
        {
            "status": "refused" if isinstance(error, TerrineError) else "failed",
            "code": error.code if isinstance(error, TerrineError) else _EXECUTION.code,
            "error_type": type(error).__name__,
        }
    )
    return result


def _observe_cost(session: LiveSession, approval: LiveApproval, record: dict[str, Any]) -> None:
    observed = session.observed_cost()
    raw_scan, raw_cost = observed.get("scan_bytes"), observed.get("scan_cost")
    invalid = False
    if raw_scan is not None:
        if type(raw_scan) is int:
            record["scan_bytes"] = raw_scan
            invalid = raw_scan < 0
        else:
            invalid = True
    if raw_cost is not None:
        try:
            cost = Decimal(str(raw_cost))
        except InvalidOperation:
            invalid = True
        else:
            if cost.is_finite():
                record["scan_cost"] = cost
                invalid = invalid or cost < 0
            else:
                invalid = True
    if invalid:
        record["scan_cost_status"] = "invalid_measurement"
        raise ValueError("owner scan/cost observations are invalid")
    scan, cost = record["scan_bytes"], record["scan_cost"]
    if (scan is not None and scan > approval.max_scan_bytes) or (
        cost is not None and cost > approval.max_cost
    ):
        record["scan_cost_status"] = "observed_cap_exceeded"
        raise ValueError("observed scan or spend exceeded the approved ceiling")
    record["scan_cost_status"] = (
        "observed" if scan is not None and cost is not None else "partly_unavailable"
    )


def run_approved(document: dict[str, Any]) -> dict[str, Any]:
    """Execute authorized Frames; failures carry sanitized evidence in immutable error context."""
    record = _empty_report()
    session: LiveSession | None = None
    approval: LiveApproval | None = None
    authorized = False
    primary: Exception | None = None
    started = time.perf_counter()
    tracemalloc.start()
    try:
        try:
            approval = LiveApproval.model_validate(document)
            config = approval.workload
            if (
                not approval.approved
                or not approval.cases
                or len(set(approval.cases)) != len(approval.cases)
                or not set(approval.cases).issubset(
                    {"frame", "schedule", "sequential", "event_panel", "sketch"}
                )
            ):
                raise ValueError("explicit supported unique cases and approval are required")
        except (TypeError, ValueError) as error:
            raise _GATE.error() from error
        record.update(
            {
                "backend": approval.backend,
                "workload": config.to_dict(),
                "account_ref_digest": hashlib.sha256(approval.account_ref.encode()).hexdigest(),
                "currency": approval.currency,
                "budget": {
                    "max_scan_bytes": approval.max_scan_bytes,
                    "max_cost": approval.max_cost,
                    "currency": approval.currency,
                    "fingerprint": None,
                },
            }
        )
        module, function = approval.factory.split(":")
        factory = getattr(importlib.import_module(module), function)
        session = factory(approval)
        if session.remote is not True or (
            session.backend,
            session.account_ref,
            session.workspace_ref,
        ) != (approval.backend, approval.account_ref, approval.workspace_ref):
            raise _SCOPE.error()
        definition = hashlib.sha256(
            json.dumps(
                {
                    "harness_version": 2,
                    "backend": approval.backend,
                    "account_ref": approval.account_ref,
                    "workspace_ref": approval.workspace_ref,
                    "workload": config.to_dict(),
                    "cases": approval.cases,
                    "max_scan_bytes": approval.max_scan_bytes,
                    "max_cost": str(approval.max_cost),
                    "currency": approval.currency,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        graph = DependencyGraph(
            (LayerNode("frame", "live_workload", "all", definition, "pending"),), ()
        )
        budget = Budget(
            max_scan_bytes=approval.max_scan_bytes,
            max_rows=sum(row_bound(config, name) for name in approval.cases),
        )
        plan = plan_refresh(
            namespace="live_benchmark",
            dataset="synthetic",
            definition_hash=definition,
            snapshot_id=definition,
            graph=graph,
            budget=budget,
            cost_provider=session.cost_provider,
        )
        record["budget"]["fingerprint"] = plan.recipe.fingerprint
        if any(
            task.scan_evidence is None or task.scan_evidence.max_attempts != 1
            for task in plan.tasks
        ):
            raise _GATE.error()
        authorize_plan_scan(
            plan,
            lambda current: session.authorize_workload(
                current, max_cost=approval.max_cost, currency=approval.currency
            ),
        )
        authorized = True
        # The budget scope includes source work, observation and workspace cleanup.
        with session.isolated_workspace() as con:
            if con.name != approval.backend:
                raise _SCOPE.error()
            model, counts = load_model(con, config, dialect=approval.backend)
            record.update(counts)
            cases = designs(config)
            for name in approval.cases:
                result = run_case(model, config, name, cases[name])
                # Sanitize completed cases before any later case can fail.
                physical = result.pop("physical_plan")
                result["physical_plan_digest"] = (
                    hashlib.sha256(json.dumps(physical, sort_keys=True).encode()).hexdigest()
                    if physical is not None
                    else None
                )
                record["cases"].append(result)
            record.update(
                {
                    "anchors": sum(result["anchors"] for result in record["cases"]),
                    "emitted_rows": sum(result["emitted_rows"] for result in record["cases"]),
                    "source_digest": content_digest(
                        con.to_pyarrow_batches(
                            con.table("events").order_by("unit_id", "event_id"),
                            chunk_size=config.batch_size,
                        )
                    ),
                    "oracle_match": True,
                }
            )
            if session.cost_provider.capabilities.cancellation is True:
                if session.probe_cancellation() is not True:
                    raise ValueError("backend did not confirm physical query cancellation")
                record["cancellation"] = {"status": "observed", "physically_cancelled": True}
            else:
                record["cancellation"] = {
                    "status": "unverified_capability",
                    "physically_cancelled": None,
                }
    except Exception as error:
        primary = error
    finally:
        if session is not None:
            if authorized:
                assert approval is not None
                try:
                    _observe_cost(session, approval, record)
                except Exception as error:
                    record["cost_observation_error_type"] = type(error).__name__
                    if record["scan_cost_status"] == "not_executed":
                        record["scan_cost_status"] = "unavailable_observation_error"
                    if primary is None:
                        primary = error
            try:
                session.close()
            except Exception as error:
                record["close_error_type"] = type(error).__name__
                if primary is None:
                    primary = error
        _, record["client_peak_memory_bytes"] = tracemalloc.get_traced_memory()
        record["elapsed_seconds"] = time.perf_counter() - started
        tracemalloc.stop()
    if primary is not None:
        record = failure_report(primary, record)
        portable = json.loads(json.dumps(record, default=_json, allow_nan=False))
        raise TerrineError(
            _EXECUTION.template,
            code=record["code"],
            context={"report": portable},
        ) from primary
    record["status"] = "verified"
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approval", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        record = run_approved(json.loads(args.approval.read_text(encoding="utf-8")))
    except Exception as error:
        record = failure_report(error)
    with args.output.open("x", encoding="utf-8") as stream:
        stream.write(
            json.dumps(record, sort_keys=True, separators=(",", ":"), default=_json) + "\n"
        )
    print(
        json.dumps(
            {key: record[key] for key in ("status", "backend", "code") if key in record},
            sort_keys=True,
        )
    )
    if record["status"] != "verified":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
