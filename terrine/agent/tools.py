"""Owner-authorized dataset discovery, pinned execution and bounded reads."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import threading
from collections.abc import Callable
from dataclasses import replace
from datetime import date
from pathlib import Path
from time import monotonic
from typing import Any

from pydantic_core import to_jsonable_python

from terrine.agent.definitions import definition_schema, parse_definition
from terrine.agent.design_schema import design_schema
from terrine.agent.policy import _LIMIT, _SCOPE, AgentPolicy, Permission, effective_budget
from terrine.agent.schemas import (
    AgentDatasetReference,
    AgentExportManifest,
    CancelOutput,
    CapabilityOutput,
    CatalogOutput,
    DatasetColumn,
    DefinitionOutput,
    DesignOutput,
    DesignRequest,
    EventOutput,
    EventsOutput,
    MetricMetadata,
    PlanReceipt,
    PropertyMetadata,
    Response,
    RowsOutput,
    RunOutput,
    SchemaOutput,
    SourceMetadata,
    ValidationOutput,
    refusal_response,
)
from terrine.data import _OUTPUT_BYTES_EXCEEDED, _ROWS_EXCEEDED, Budget, Preview
from terrine.dataset.manifest import DatasetManifest
from terrine.dataset.reference import Dataset, dataset_from_run
from terrine.errors import RefusalSpec, TerrineError
from terrine.model import Model
from terrine.runtime.api import Executor, cancel_run, get_run, read_run_events, run_serialized_plan
from terrine.runtime.changes import ChangeSet
from terrine.runtime.planning import OwnerPlanner, PlanInvocation, PlanRequest
from terrine.runtime.saved_plan import (
    DestinationRef,
    ModelReference,
    SavedPlan,
    _bind_saved_plan,
)
from terrine.runtime.state import RuntimeStore
from terrine.spec.anchors import EventAnchor, FixedDate
from terrine.spec.dataset import INVALID_DEFINITION, DatasetDefinition
from terrine.spec.design import Design
from terrine.spec.io import from_json, from_yaml, to_json
from terrine.spec.validate import ensure_runtime_shape_supported, resolve_unit, validate_design

__all__ = ["DatasetTools"]

_INVALID = RefusalSpec("terrine.agent.invalid_request", "Use a valid typed Design payload.")
_STORAGE = RefusalSpec("terrine.agent.storage_denied", "Named Design storage is unavailable.")
_MODEL = RefusalSpec("terrine.agent.model_mismatch", "Owner model configuration has changed.")
_RUN = RefusalSpec(
    "terrine.agent.run_unavailable", "Run has no matching owner-prepared invocation."
)
_OWNER = RefusalSpec(
    "terrine.agent.owner_configuration", "Owner execution configuration is unavailable."
)
_RECEIPT = RefusalSpec(
    "terrine.agent.receipt_unavailable", "Use an unchanged owner-prepared receipt."
)


def _diff(before: Any, after: Any, path: str = "") -> tuple[str, ...]:
    if isinstance(before, dict) and isinstance(after, dict):
        paths = []
        for key in sorted(before.keys() | after.keys()):
            escaped = key.replace("~", "~0").replace("/", "~1")
            child = f"{path}/{escaped}"
            if key not in before or key not in after:
                paths.append(child)
            else:
                paths.extend(_diff(before[key], after[key], child))
        return tuple(paths)
    return () if before == after else (path,)


class DatasetTools:
    """An owner-bound session with a shared request cap across all operations."""

    def __init__(
        self, model: Model, policy: AgentPolicy, *, namespace: str, executor: Executor | None = None
    ) -> None:
        if not isinstance(model, Model) or not isinstance(policy, AgentPolicy):
            raise TypeError("model and policy must be owner-created typed values")
        if not isinstance(namespace, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
            raise ValueError("namespace must be a simple identifier")
        if executor is not None and executor is not policy.executor:
            raise TypeError("executor must be the owner policy executor")
        self.model = model
        self.policy = policy
        self.namespace = namespace
        self._requests = 0
        self._lock = threading.RLock()

    def _call[T](self, permission: Permission, operation: Callable[[], T]) -> Response[T]:
        try:
            with self._lock:
                if self._requests >= self.policy.max_requests:
                    raise _LIMIT.error()
                self._requests += 1
                self.policy.authorize(permission)
                reference = self.policy.model_reference
                if (
                    reference is not None
                    and ModelReference.from_model(self.model, owner_key=reference.owner_key)
                    != reference
                ):
                    raise _MODEL.error()
                catalog = (
                    reference.catalog_hash
                    if reference
                    else ModelReference.from_model(self.model, owner_key="inspection").catalog_hash
                )
                self.policy.authorize(permission, catalog=catalog)
            return Response[T](status="ok", output=operation())
        except TerrineError as error:
            return refusal_response(error)

    def _parse(self, serialized: str) -> Design:
        try:
            DesignRequest(serialized=serialized)
            design = (
                from_json(serialized)
                if serialized.lstrip().startswith("{")
                else from_yaml(serialized)
            )
        except TerrineError:
            raise
        except (TypeError, ValueError) as error:
            raise _INVALID.error(
                route="Use get_design_schema and design_example to construct a Design."
            ) from error
        return resolve_unit(design, self.model)

    def _validated(self, design: Design) -> ValidationOutput:
        design = resolve_unit(design, self.model)
        report = validate_design(design, self.model)
        self.policy.authorize("validate", sources=report.required_sources)
        return ValidationOutput(
            spec_hash=design.spec_hash,
            required_sources=report.required_sources,
            runtime_obligations=report.runtime_obligations,
        )

    def _design_output(self, serialized: str, base: str | None = None) -> DesignOutput:
        design = self._parse(serialized)
        valid = self._validated(design)
        canonical = to_json(design)
        paths = (
            ()
            if base is None
            else _diff(json.loads(to_json(self._parse(base))), json.loads(canonical))
        )
        return DesignOutput(**valid.model_dump(), serialized=canonical, changed_paths=paths)

    def inspect_catalog(self) -> Response[CatalogOutput]:
        def inspect():
            allowed = self.policy.allowed_sources
            return CatalogOutput(
                sources=tuple(
                    SourceMetadata(
                        name=name,
                        units=tuple(sorted(source.units)),
                        events=tuple(sorted(source.events)),
                        properties=tuple(sorted(source.properties)),
                    )
                    for name, source in sorted(self.model.sources.items())
                    if name in allowed
                ),
                metrics=tuple(
                    MetricMetadata(
                        name=name,
                        unit=metric.unit,
                        kind=metric.kind,
                        sources=metric.source_names,
                    )
                    for name, metric in sorted(self.model.metrics.items())
                    if set(metric.source_names) <= allowed
                ),
                properties=tuple(
                    PropertyMetadata(
                        name=name, source=prop.source, dtype=prop.dtype, as_of=prop.as_of
                    )
                    for name, prop in sorted(self.model.catalog.properties.items())
                    if prop.source in allowed
                ),
                units=tuple(
                    sorted(
                        {
                            unit
                            for name, source in self.model.sources.items()
                            if name in allowed
                            for unit in source.units
                        }
                    )
                ),
            )

        return self._call("inspect", inspect)

    def list_capabilities(self) -> Response[CapabilityOutput]:
        def capabilities():
            shapes = []
            for shape in ("cross_section", "panel", "sequence", "trajectory"):
                try:
                    ensure_runtime_shape_supported(shape)
                except TerrineError:
                    continue
                shapes.append(shape)
            from terrine.spec.state_panel import StateObservations, StatePanel

            ensure_runtime_shape_supported(
                StatePanel(StateObservations("state", ("a", "b")), max_periods=2)
            )
            shapes.append("StatePanel")
            return CapabilityOutput(
                operations=(
                    "inspect_catalog",
                    "list_capabilities",
                    "get_design_schema",
                    "get_dataset_definition_schema",
                    "design_example",
                    "propose_design",
                    "validate_design",
                    "propose_definition",
                    "validate_definition",
                    "save_design",
                    "load_design",
                    "preview_dataset",
                    "prepare_dataset",
                    "execute_plan",
                    "stage",
                    "prepare_refresh",
                    "refresh",
                    "export_parquet",
                    "preview_rows",
                    "get_run_status",
                    "get_run_events",
                    "request_cancel",
                ),
                runtime_shapes=tuple(shapes),
            )

        return self._call("inspect", capabilities)

    def get_design_schema(self) -> Response[SchemaOutput]:
        return self._call("inspect", lambda: SchemaOutput(document=design_schema()))

    def design_example(self) -> Response[DesignOutput]:
        def example():
            sources = [
                (name, source)
                for name, source in sorted(self.model.sources.items())
                if name in self.policy.allowed_sources and source.units
            ]
            for name, source in sources:
                for unit in sorted(source.units):
                    if unit in self.model.units:
                        coverage = self.model.readiness.sources.get(name)
                        anchor = coverage.start.date() if coverage else date(2000, 1, 1)
                        return self._design_output(to_json(Design(unit, FixedDate(anchor))))
            for name, source in sources:
                if source.events and name in self.model.readiness.sources:
                    design = Design(sorted(source.units)[0], EventAnchor(sorted(source.events)[0]))
                    return self._design_output(to_json(design))
            raise _INVALID.error(
                route="Configure a unit roster or allow an event with declared coverage."
            )

        return self._call("validate", example)

    def propose_design(self, serialized: str, *, base: str | None = None) -> Response[DesignOutput]:
        return self._call("validate", lambda: self._design_output(serialized, base))

    def validate_design(self, serialized: str) -> Response[ValidationOutput]:
        return self._call("validate", lambda: self._validated(self._parse(serialized)))

    def _storage(self, name: str) -> tuple[int, str]:
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_-]*", name)
            or self.policy.design_root is None
        ):
            raise _STORAGE.error(route="Use a simple name under the owner-configured design root.")
        root = self.policy.design_root
        try:
            root.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            return descriptor, f"{name}.json"
        except OSError as error:
            raise _STORAGE.error() from error

    def save_design(self, name: str, serialized: str) -> Response[DesignOutput | DefinitionOutput]:
        def save():
            output = self._definition_output(serialized)
            root, filename = self._storage(name)
            temporary = f".design-{secrets.token_hex(12)}"
            try:
                try:
                    existing = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=root)
                except FileNotFoundError:
                    pass
                else:
                    os.close(existing)
                descriptor = os.open(
                    temporary,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=root,
                )
                with os.fdopen(descriptor, "w") as stream:
                    stream.write(output.serialized)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, filename, src_dir_fd=root, dst_dir_fd=root)
            except OSError as error:
                raise _STORAGE.error() from error
            finally:
                try:
                    os.unlink(temporary, dir_fd=root)
                except FileNotFoundError:
                    pass
                os.close(root)
            return output

        return self._call("validate", save)

    def load_design(self, name: str) -> Response[DesignOutput | DefinitionOutput]:
        def load():
            root, filename = self._storage(name)
            try:
                descriptor = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=root)
                with os.fdopen(descriptor) as stream:
                    serialized = stream.read()
            except OSError as error:
                raise _STORAGE.error() from error
            finally:
                os.close(root)
            return self._definition_output(serialized)

        return self._call("validate", load)

    def _authorize_run(self, run_id: str, permission: Permission) -> None:
        if not isinstance(run_id, str) or not run_id:
            raise _INVALID.error()
        con = self.model.con.con
        store = RuntimeStore(con, namespace=self.namespace)
        tables = {
            item[0]
            for item in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE lower(table_schema)=lower(?)",
                [self.namespace],
            ).fetchall()
        }
        if not {"runtime_runs", "runtime_plan_invocations"} <= tables:
            raise _RUN.error()
        columns = {
            item[0]
            for item in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE lower(table_schema)=lower(?) AND table_name='runtime_runs'",
                [self.namespace],
            ).fetchall()
        }
        if "destination_json" not in columns:
            raise _RUN.error()
        row = con.execute(
            f"SELECT destination_json, fingerprint FROM {store._schema}.runtime_runs "
            "WHERE run_id=?",
            [run_id],
        ).fetchone()
        if row is None or row[0] is None:
            raise _RUN.error()
        destination = DestinationRef(**json.loads(row[0]))
        self.policy.authorize(permission, destination=destination)
        invocations = con.execute(
            f"SELECT invocation_json FROM {store._schema}.runtime_plan_invocations"
        ).fetchall()
        for record in invocations:
            invocation = PlanInvocation.from_dict(json.loads(record[0]))
            if invocation.destination != destination or not invocation.saved_plan_json:
                continue
            definition = SavedPlan.from_json(invocation.saved_plan_json)
            if (
                definition.model != self.policy.model_reference
                or definition.namespace != self.namespace
            ):
                continue
            if (
                _bind_saved_plan(definition, namespace=self.namespace).definition.fingerprint
                != row[1]
            ):
                continue
            report = validate_design(from_json(definition.design_json), self.model)
            self.policy.authorize(permission, sources=report.required_sources)
            return
        raise _RUN.error(route="Use a run from an owner-prepared invocation for this Model.")

    def get_run_status(self, run_id: str) -> Response[RunOutput]:
        def status():
            self._authorize_run(run_id, "read")
            result = get_run(run_id, con=self.model.con.con, namespace=self.namespace)
            return RunOutput(
                run_id=result.run_id,
                durable_state=result.durable_state,
                phase=result.phase,
                error_code=result.error_code,
                pinned_result_ref=result.pinned_result_ref,
                latest_event_cursor=result.latest_event_cursor,
            )

        return self._call("read", status)

    def get_run_events(
        self, run_id: str, *, after: str | None = None, limit: int = 100
    ) -> Response[EventsOutput]:
        def events():
            if (
                isinstance(limit, bool)
                or not isinstance(limit, int)
                or not 0 < limit <= self.policy.batch_cap
            ):
                raise _INVALID.error(
                    route="Choose a positive event limit no larger than the owner batch cap."
                )
            self._authorize_run(run_id, "read")
            page = read_run_events(
                run_id, con=self.model.con.con, namespace=self.namespace, after=after, limit=limit
            )
            return EventsOutput(
                events=tuple(
                    EventOutput(
                        sequence=event.sequence,
                        kind=event.kind,
                        state=event.payload.get("state"),
                        phase=event.payload.get("phase"),
                    )
                    for event in page.events
                ),
                next_cursor=page.next_cursor,
                has_more=page.has_more,
                cursor_gap=page.cursor_gap,
            )

        return self._call("read", events)

    def request_cancel(self, run_id: str) -> Response[CancelOutput]:
        def cancel():
            self._authorize_run(run_id, "cancel")
            receipt = cancel_run(run_id, con=self.model.con.con, namespace=self.namespace)
            return CancelOutput(
                run_id=run_id,
                disposition=receipt.disposition,
                durable_state=receipt.status.durable_state,
            )

        return self._call("cancel", cancel)

    def get_dataset_definition_schema(self) -> Response[SchemaOutput]:
        return self._call("inspect", lambda: SchemaOutput(document=definition_schema()))

    def _dataset_definition_output(
        self, serialized: str, base: str | None = None
    ) -> DefinitionOutput:
        definition = parse_definition(serialized, self.model)
        valid = self._validated(definition.base_design)
        canonical = definition.canonical_json().decode()
        paths = (
            ()
            if base is None
            else _diff(
                json.loads(parse_definition(base, self.model).canonical_json()),
                json.loads(canonical),
            )
        )
        return DefinitionOutput(
            **valid.model_dump(),
            serialized=canonical,
            changed_paths=paths,
            definition_digest=definition.digest,
        )

    def propose_definition(
        self, serialized: str, *, base: str | None = None
    ) -> Response[DefinitionOutput]:
        return self._call("validate", lambda: self._dataset_definition_output(serialized, base))

    def validate_definition(self, serialized: str) -> Response[DefinitionOutput]:
        def validate():
            output = self._dataset_definition_output(serialized)
            try:
                parse_definition(serialized, self.model).build(self.model)
            except (KeyError, TypeError, ValueError) as error:
                if isinstance(error, TerrineError):
                    raise
                raise _INVALID.error(
                    route="Use the recorded training or scoring view fields."
                ) from error
            return output

        return self._call("validate", validate)

    def _definition_output(self, serialized: str) -> DesignOutput | DefinitionOutput:
        try:
            payload = json.loads(serialized)
        except (TypeError, ValueError):
            return self._design_output(serialized)
        if isinstance(payload, dict) and "base_design" in payload:
            return self._dataset_definition_output(serialized)
        return self._design_output(serialized)

    def _dataset_definition(self, definition: DatasetDefinition | str) -> DatasetDefinition:
        if not isinstance(definition, (DatasetDefinition, str)):
            raise _INVALID.error(route="Use the discovered DatasetDefinition schema.")
        return parse_definition(
            definition.canonical_json().decode()
            if isinstance(definition, DatasetDefinition)
            else definition,
            self.model,
        )

    def _resolved_definition_design(self, definition: DatasetDefinition) -> Design:
        try:
            return definition.build(self.model)._design
        except TerrineError:
            raise
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            raise INVALID_DEFINITION.error(reason="dataset view cannot be constructed") from error

    def preview_dataset(
        self,
        definition: DatasetDefinition | str,
        *,
        budget: Budget | None = None,
        bounded_count: int = 0,
    ) -> Response[Preview]:
        def preview():
            from terrine.compile import cost

            if (
                isinstance(bounded_count, bool)
                or not isinstance(bounded_count, int)
                or not 0 <= bounded_count <= self.policy.max_bounded_count
            ):
                raise _INVALID.error(route="Use a bounded count within the owner cap.")
            parsed = self._dataset_definition(definition)
            design = self._resolved_definition_design(parsed)
            valid = self._validated(design)
            if bounded_count:
                self.policy.authorize("preview", sources=valid.required_sources)
            effective = effective_budget(self.policy.budget, budget)
            cost._scan_policy(effective)
            result = cost.preview(design, self.model, effective, bounded_count=bounded_count)
            return replace(
                result,
                coverage={
                    name: value
                    for name, value in result.coverage.items()
                    if name in valid.required_sources
                },
            )

        return self._call("validate", preview)

    def _owner_planner(self, destination: DestinationRef) -> OwnerPlanner:
        authority = self.policy.snapshot_authority
        if authority is None or self.policy.resolver is None or self.policy.model_reference is None:
            raise _OWNER.error(route="Configure an owner resolver and retained snapshot authority.")
        from terrine.runtime.owner import RuntimeOwner

        try:
            connected_owner = RuntimeOwner.from_connection(self.model.con)
        except (TypeError, ValueError) as error:
            raise _OWNER.error(
                route="Use an owner-configured file-backed DuckDB connection."
            ) from error
        if connected_owner.reference != authority.owner.reference:
            raise _OWNER.error(
                route="Align the owner's configured snapshot root with the connected dataset owner."
            )
        if (
            destination.schema != self.namespace
            or Path(destination.database).resolve() != authority.owner.state_path.resolve()
        ):
            raise _SCOPE.error(route="Use this owner's database and runtime namespace.")
        return OwnerPlanner(resolver=self.policy.resolver, snapshot_authority=authority)

    @staticmethod
    def _receipt(invocation: PlanInvocation) -> PlanReceipt:
        saved = SavedPlan.from_json(invocation.saved_plan_json)
        identity = json.dumps(invocation.to_dict(), sort_keys=True, separators=(",", ":"))
        return PlanReceipt(
            receipt_id=hashlib.sha256(identity.encode()).hexdigest(),
            design_hash=saved.design_hash,
            plan_hash=saved.plan_hash,
        )

    def _prepare(
        self,
        definition: DatasetDefinition,
        *,
        destination: DestinationRef,
        invocation_id: str,
        budget: Budget | None,
        changes: ChangeSet | None = None,
        expected_base_manifest: str | None = None,
        bind_base_manifest: bool = False,
    ) -> PlanReceipt:
        valid = self._validated(definition.base_design)
        self.policy.authorize("prepare", sources=valid.required_sources, destination=destination)
        planner = self._owner_planner(destination)
        reference = self.policy.model_reference
        if reference is None:
            raise _OWNER.error()
        try:
            request = PlanRequest(
                invocation_id,
                reference,
                definition.base_design,
                destination,
                changes=changes,
                budget=effective_budget(self.policy.budget, budget),
                dataset_definition=definition,
                expected_base_manifest=expected_base_manifest,
                bind_base_manifest=bind_base_manifest,
            )
        except (TypeError, ValueError) as error:
            raise _INVALID.error(
                route="Use a safe invocation ID and typed change evidence."
            ) from error
        return self._receipt(
            planner.plan(request, con=self.model.con.con, namespace=self.namespace)
        )

    def prepare_dataset(
        self,
        definition: DatasetDefinition | str,
        *,
        destination: DestinationRef,
        invocation_id: str,
        budget: Budget | None = None,
    ) -> Response[PlanReceipt]:
        return self._call(
            "prepare",
            lambda: self._prepare(
                self._dataset_definition(definition),
                destination=destination,
                invocation_id=invocation_id,
                budget=budget,
            ),
        )

    def _resolve_receipt(
        self, receipt: PlanReceipt, permission: Permission
    ) -> tuple[PlanInvocation, SavedPlan]:
        if not isinstance(receipt, PlanReceipt):
            raise _RECEIPT.error()
        con = self.model.con.con
        exists = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE lower(table_schema)=lower(?) "
            "AND table_name='runtime_plan_invocations'",
            [self.namespace],
        ).fetchone()
        if exists is None:
            raise _RECEIPT.error()
        for (payload,) in con.execute(
            f'SELECT invocation_json FROM "{self.namespace}".runtime_plan_invocations'
        ).fetchall():
            document = json.loads(payload)
            identity = json.dumps(document, sort_keys=True, separators=(",", ":"))
            if hashlib.sha256(identity.encode()).hexdigest() != receipt.receipt_id:
                continue
            invocation = PlanInvocation.from_dict(document)
            if self._receipt(invocation) != receipt:
                continue
            saved = SavedPlan.from_json(invocation.saved_plan_json)
            if saved.namespace != self.namespace or saved.model != self.policy.model_reference:
                raise _SCOPE.error()
            valid = validate_design(from_json(saved.design_json), self.model)
            self.policy.authorize(
                permission, sources=valid.required_sources, destination=saved.destination
            )
            self._owner_planner(saved.destination)
            planned_budget = _bind_saved_plan(saved, namespace=self.namespace).budget
            if effective_budget(self.policy.budget, planned_budget) != planned_budget:
                raise _SCOPE.error(route="Prepare a new invocation under the current owner budget.")
            return invocation, saved
        raise _RECEIPT.error()

    def _execute(self, receipt: PlanReceipt, *, request_key: str):
        if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 256:
            raise _INVALID.error(route="Use a nonempty request key of at most 256 characters.")
        invocation, saved = self._resolve_receipt(receipt, "write")
        deadline = monotonic() + self.policy.max_run_seconds
        return run_serialized_plan(
            invocation.saved_plan_json,
            con=self.model.con.con,
            namespace=self.namespace,
            request_key=request_key,
            destination=saved.destination,
            executor=self.policy.executor,
            resolver=self.policy.resolver,
            snapshot_authority=self.policy.snapshot_authority,
            max_active_runs=self.policy.max_concurrency,
            cancelled=lambda: monotonic() >= deadline,
        )

    def execute_plan(self, receipt: PlanReceipt, *, request_key: str) -> Response[RunOutput]:
        def execute():
            result = self._execute(receipt, request_key=request_key)
            status = get_run(result.run_id, con=self.model.con.con, namespace=self.namespace)
            return RunOutput(
                run_id=status.run_id,
                durable_state=status.durable_state,
                phase=status.phase,
                error_code=status.error_code,
                pinned_result_ref=status.pinned_result_ref,
                latest_event_cursor=status.latest_event_cursor,
            )

        return self._call("write", execute)

    @staticmethod
    def _dataset_reference(dataset: Dataset) -> AgentDatasetReference:
        manifest = dataset.manifest
        return AgentDatasetReference(
            dataset=manifest.generation.dataset_id,
            generation=manifest.generation.generation_id,
            definition_hash=manifest.design_hash,
            model_hash=manifest.model_hash,
            manifest_hash=hashlib.sha256(manifest.to_json()).hexdigest(),
            definition_reference=manifest.definition.digest,
            row_count=manifest.generation.row_count,
            columns=tuple(
                DatasetColumn(
                    name=column.name,
                    data_type=column.data_type,
                    role=column.role,
                    complete_column=column.metadata.get("complete_column"),
                    reason_column=column.metadata.get("reason_column"),
                )
                for column in manifest.columns
            ),
        )

    def _resolve_dataset(
        self, reference: AgentDatasetReference, permission: Permission
    ) -> tuple[Dataset, SavedPlan]:
        if not isinstance(reference, AgentDatasetReference):
            raise _RECEIPT.error(route="Use the unchanged reference returned by stage or refresh.")
        con = self.model.con.con
        exists = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE lower(table_schema)=lower(?) "
            "AND table_name='runtime_dataset_recipes'",
            [self.namespace],
        ).fetchone()
        if exists is None:
            raise _RECEIPT.error()
        record = con.execute(
            f'SELECT recipe_json,manifest_json FROM "{self.namespace}".runtime_dataset_recipes '
            "WHERE dataset=? AND generation=?",
            [reference.dataset, reference.generation],
        ).fetchone()
        if record is None:
            raise _RECEIPT.error()
        saved = SavedPlan.from_json(record[0])
        if saved.namespace != self.namespace or saved.model != self.policy.model_reference:
            raise _SCOPE.error()
        valid = validate_design(from_json(saved.design_json), self.model)
        self.policy.authorize(
            permission, sources=valid.required_sources, destination=saved.destination
        )
        manifest = DatasetManifest.from_json(record[1])
        if manifest.definition.digest != reference.definition_reference:
            raise _RECEIPT.error()
        dataset = Dataset.open_generation(
            reference.dataset, reference.generation, con=self.model.con, namespace=self.namespace
        )
        if self._dataset_reference(dataset) != reference:
            raise _RECEIPT.error()
        return dataset, saved

    def _stage(self, receipt: PlanReceipt, *, request_key: str) -> AgentDatasetReference:
        _, saved = self._resolve_receipt(receipt, "write")
        result = self._execute(receipt, request_key=request_key)
        authority = self.policy.snapshot_authority
        if result.state != "succeeded" or authority is None:
            raise _RUN.error(route="Inspect the run state before requesting a committed dataset.")
        return self._dataset_reference(
            dataset_from_run(
                saved,
                result,
                model=self.model,
                namespace=self.namespace,
                request_key=request_key,
                snapshot_authority=authority,
            )
        )

    def stage(self, receipt: PlanReceipt, *, request_key: str) -> Response[AgentDatasetReference]:
        return self._call("write", lambda: self._stage(receipt, request_key=request_key))

    def prepare_refresh(
        self,
        reference: AgentDatasetReference,
        *,
        invocation_id: str,
        changes: ChangeSet | None = None,
        budget: Budget | None = None,
    ) -> Response[PlanReceipt]:
        def prepare():
            dataset, saved = self._resolve_dataset(reference, "prepare")
            return self._prepare(
                dataset.manifest.definition,
                destination=saved.destination,
                invocation_id=invocation_id,
                budget=budget,
                changes=changes,
                expected_base_manifest=reference.generation,
                bind_base_manifest=True,
            )

        return self._call("prepare", prepare)

    def refresh(
        self, reference: AgentDatasetReference, receipt: PlanReceipt, *, request_key: str
    ) -> Response[AgentDatasetReference]:
        def refresh():
            self._resolve_dataset(reference, "write")
            _, saved = self._resolve_receipt(receipt, "write")
            if (
                saved.expected_base_manifest != reference.generation
                or saved.destination.dataset != reference.dataset
            ):
                raise _RECEIPT.error(
                    route="Use a refresh receipt prepared from this exact generation."
                )
            return self._stage(receipt, request_key=request_key)

        return self._call("write", refresh)

    def export_parquet(
        self,
        reference: AgentDatasetReference,
        *,
        destination: str,
        partition_by: tuple[str, ...] = (),
        budget: Budget | None = None,
    ) -> Response[AgentExportManifest]:
        def export():
            from terrine.compile.cost import _scan_policy

            if not isinstance(destination, str) or not destination:
                raise _INVALID.error()
            path = Path(destination).expanduser().resolve()
            if path not in self.policy.allowed_exports:
                raise _SCOPE.error(route="Use an exact owner-approved export destination.")
            effective = effective_budget(self.policy.budget, budget)
            _scan_policy(effective)
            dataset, _ = self._resolve_dataset(reference, "export")
            manifest = dataset.export_parquet(
                destination=str(path), partition_by=partition_by, budget=effective
            )
            return AgentExportManifest(
                dataset=manifest.dataset_id,
                generation=manifest.generation_id,
                row_count=manifest.row_count,
                byte_count=manifest.byte_count,
                shard_count=len(manifest.shards),
            )

        return self._call("export", export)

    def preview_rows(
        self, reference: AgentDatasetReference, *, limit: int, columns: tuple[str, ...]
    ) -> Response[RowsOutput]:
        def preview():
            from terrine.compile.cost import _scan_policy

            if (
                isinstance(limit, bool)
                or not isinstance(limit, int)
                or not 0 < limit <= self.policy.max_preview_rows
            ):
                raise _INVALID.error(route="Use a positive row limit within the owner preview cap.")
            if (
                not isinstance(columns, tuple)
                or not columns
                or any(not isinstance(name, str) for name in columns)
                or len(set(columns)) != len(columns)
                or not set(columns) <= self.policy.preview_columns
            ):
                raise _SCOPE.error(route="Request only unique owner-approved preview columns.")
            budget = self.policy.budget
            _scan_policy(budget)
            dataset, _ = self._resolve_dataset(reference, "preview")
            if not set(columns) <= {column.name for column in dataset.manifest.columns}:
                raise _SCOPE.error()
            if (
                budget.max_rows is not None
                and min(limit, dataset.manifest.generation.row_count) > budget.max_rows
            ):
                raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
            iterator = dataset.to_arrow_batches(batch_size=min(limit, self.policy.batch_cap))
            rows = []
            byte_count = 0
            try:
                for batch in iterator:
                    projected = batch.select(columns).slice(0, limit - len(rows))
                    if (
                        budget.max_rows is not None
                        and len(rows) + projected.num_rows > budget.max_rows
                    ):
                        raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
                    byte_count += projected.nbytes
                    if budget.max_output_bytes is not None and byte_count > budget.max_output_bytes:
                        raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
                    for row in projected.to_pylist():
                        rows.append(
                            tuple(
                                None
                                if name in self.policy.redacted_columns
                                else to_jsonable_python(
                                    row[name], bytes_mode="base64", inf_nan_mode="strings"
                                )
                                for name in columns
                            )
                        )
                    if len(rows) >= limit:
                        break
            finally:
                iterator.close()
            return RowsOutput(columns=columns, rows=tuple(rows))

        return self._call("preview", preview)
