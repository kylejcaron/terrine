"""Owner-gated live compiler tier. No connection is opened without an approved scope."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import time
import tracemalloc
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator

from benchmarks.workloads import _json, content_digest, designs, load_model, row_bound, run_case
from terrine import Budget
from terrine.errors import RefusalSpec, TerrineError
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


def run_approved(document: dict[str, Any]) -> dict[str, Any]:
    """Execute actual production Frames only within the owner-authorized session."""
    try:
        approval = LiveApproval.model_validate(document)
        config = approval.workload
        if (
            not approval.approved
            or not approval.cases
            or len(set(approval.cases)) != len(approval.cases)
        ):
            raise ValueError("explicit unique cases and approval are required")
        if not set(approval.cases).issubset(
            {"frame", "schedule", "sequential", "event_panel", "sketch"}
        ):
            raise ValueError("unsupported live test slice")
    except (TypeError, ValueError) as exc:
        raise _GATE.error() from exc
    module, function = approval.factory.split(":")
    factory = getattr(importlib.import_module(module), function)
    session: LiveSession = factory(approval)
    started = time.perf_counter()
    tracemalloc.start()
    try:
        if session.remote is not True or (
            session.backend,
            session.account_ref,
            session.workspace_ref,
        ) != (approval.backend, approval.account_ref, approval.workspace_ref):
            raise _SCOPE.error()
        definition = hashlib.sha256(
            json.dumps(
                {
                    "harness_version": 1,
                    "backend": approval.backend,
                    "account_ref": approval.account_ref,
                    "workspace_ref": approval.workspace_ref,
                    "workload": config.to_dict(),
                    "cases": approval.cases,
                    "max_scan_bytes": approval.max_scan_bytes,
                    "max_cost": format(approval.max_cost, "f"),
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
        # The budget scope is the whole invocation, not merely the final SELECT.
        with session.isolated_workspace() as con:
            if con.name != approval.backend:
                raise _SCOPE.error()
            model, counts = load_model(con, config, dialect=approval.backend)
            cases = designs(config)
            results = [run_case(model, config, name, cases[name]) for name in approval.cases]
            source_digest = content_digest(
                con.to_pyarrow_batches(
                    con.table("events").order_by("unit_id", "event_id"),
                    chunk_size=config.batch_size,
                )
            )
            if session.cost_provider.capabilities.cancellation is True:
                if session.probe_cancellation() is not True:
                    raise ValueError("backend did not confirm physical query cancellation")
                cancellation = {"status": "observed", "physically_cancelled": True}
            else:
                cancellation = {"status": "unverified_capability", "physically_cancelled": None}
        observed = session.observed_cost()
        scan = observed.get("scan_bytes")
        cost = observed.get("scan_cost")
        if scan is not None and (
            type(scan) is not int or scan < 0 or scan > approval.max_scan_bytes
        ):
            raise ValueError("observed scan exceeded the approved cap or is invalid")
        if cost is not None:
            cost = Decimal(str(cost))
            if not cost.is_finite() or not 0 <= cost <= approval.max_cost:
                raise ValueError("observed cost exceeded the approved spend cap or is invalid")
        _, peak = tracemalloc.get_traced_memory()
        # Remote plans can contain account identifiers; retain a digest, not raw SQL or rows.
        for result in results:
            physical = result.pop("physical_plan")
            result["physical_plan_digest"] = (
                hashlib.sha256(json.dumps(physical, sort_keys=True).encode()).hexdigest()
                if physical is not None
                else None
            )
        return {
            "schema_version": 1,
            "status": "verified",
            "backend": approval.backend,
            "account_ref_digest": hashlib.sha256(approval.account_ref.encode()).hexdigest(),
            "source_digest": source_digest,
            "workload": config.to_dict(),
            **counts,
            "oracle_match": True,
            "cases": results,
            "anchors": sum(result["anchors"] for result in results),
            "emitted_rows": sum(result["emitted_rows"] for result in results),
            "scan_bytes": scan,
            "scan_cost": cost,
            "currency": approval.currency,
            "scan_cost_status": "observed"
            if scan is not None and cost is not None
            else "partly_unavailable",
            "cancellation": cancellation,
            "client_peak_memory_bytes": peak,
            "process_peak_memory_bytes": None,
            "process_memory_scope": "owner_process_measurement_unavailable",
            "elapsed_seconds": time.perf_counter() - started,
            "budget": {
                "max_scan_bytes": approval.max_scan_bytes,
                "max_cost": approval.max_cost,
                "currency": approval.currency,
                "fingerprint": plan.recipe.fingerprint,
            },
        }
    finally:
        tracemalloc.stop()
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approval", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        record = run_approved(json.loads(args.approval.read_text(encoding="utf-8")))
    except TerrineError as error:
        record = {"schema_version": 1, "status": "refused", "code": error.code}
    except Exception as error:
        record = {"schema_version": 1, "status": "failed", "error_type": type(error).__name__}
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
