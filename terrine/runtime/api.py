from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from typing import Any, Protocol

import ibis

from terrine.errors import RefusalSpec
from terrine.execution._metadata import metadata_operation
from terrine.runtime.backends.base import CostProvider, PublicationReceipt
from terrine.runtime.cost import _AUTHORITY, ScanAuthorization, ScanEvidence, ScanPhase
from terrine.runtime.events import read_run_events
from terrine.runtime.execute import RunReceipt, execute_plan
from terrine.runtime.planner import RefreshPlan, plan_refresh
from terrine.runtime.saved_plan import (
    _INVALID as _SAVED_PLAN_MISMATCH,
)
from terrine.runtime.saved_plan import (
    DestinationRef,
    ModelResolver,
    SavedPlan,
    _bind_saved_plan,
    resolve_plan,
)
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.runtime.status import _recover_interrupted_runs, cancel_run, get_run

__all__ = [
    "CostProvider",
    "DesignExecutor",
    "Executor",
    "RunReceipt",
    "ScanAuthorization",
    "ScanAuthorizingExecutor",
    "ScanEvidence",
    "ScanPhase",
    "cancel_run",
    "find_run_by_request_key",
    "get_run",
    "open_published_frame",
    "open_published_generation",
    "plan_refresh",
    "read_run_events",
    "run_plan",
    "run_serialized_plan",
]

_EXECUTOR_UNAVAILABLE = RefusalSpec(
    "terrine.runtime.executor_unavailable",
    "No owner execution callbacks are configured for this runtime.",
)


class Executor(Protocol):
    """Owner-provided callbacks that execute bounded tasks and publish a generation."""

    def run_task(self, task: Any, dependencies: Any) -> Any: ...

    def publish(self, plan: RefreshPlan, outputs: Any, lease: Any, run_id: str) -> Any: ...


class ScanAuthorizingExecutor(Executor, Protocol):
    """Owner executor that attests all reads, including snapshot reopening and retries."""

    def authorize_scan(self, plan: RefreshPlan) -> ScanAuthorization: ...


def run_plan(
    plan: RefreshPlan,
    *,
    con: Any,
    namespace: str,
    request_key: str | None = None,
    destination: DestinationRef | None = None,
    executor: Executor | None = None,
    max_active_runs: int | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> RunReceipt:
    """Run a plan under owner execution callbacks, replaying destination-scoped keys."""
    if request_key is not None:
        if destination is None:
            raise ValueError("destination is required with a request_key")
        existing = find_run_by_request_key(
            request_key, destination=destination, con=con, namespace=namespace
        )
        if existing is not None:
            store = RuntimeStore(con, namespace=namespace)
            store.initialize()
            fingerprint = con.execute(
                f"SELECT fingerprint FROM {store._schema}.runtime_runs WHERE run_id=?",
                [existing.run_id],
            ).fetchone()
            if fingerprint is None or fingerprint[0] != plan.definition.fingerprint:
                from terrine.errors import RefusalSpec

                raise RefusalSpec(
                    "terrine.runtime.request_key_conflict",
                    "Request key is already bound to a different plan.",
                ).error()
            if existing.state in {"running", "cancel_requested", "succeeded", "cancelled"}:
                if existing.state in {"succeeded", "cancelled"}:
                    from terrine.runtime.execute import _release_run_snapshot_refs

                    _release_run_snapshot_refs(store, plan.snapshot_id, existing.run_id)
                _record_result_partitions(plan, existing, con=con, namespace=namespace)
                return existing
            run_id = existing.run_id
        else:
            run_id = str(uuid.uuid4())
    else:
        if destination is not None:
            raise ValueError("destination is only used with a request_key")
        run_id = str(uuid.uuid4())
    if executor is None:
        raise _EXECUTOR_UNAVAILABLE.error()
    receipt = execute_plan(
        plan,
        con=con,
        namespace=namespace,
        run_id=run_id,
        request_key=request_key,
        destination=destination,
        max_active_runs=max_active_runs,
        cancelled=cancelled,
        run_task=executor.run_task,
        authorize_scan=getattr(executor, "authorize_scan", None),
        publish=lambda current_plan, outputs, lease: executor.publish(
            current_plan, outputs, lease, run_id
        ),
    )
    _record_result_partitions(plan, receipt, con=con, namespace=namespace)
    return receipt


def run_serialized_plan(
    serialized: bytes | str,
    *,
    con: Any,
    namespace: str,
    request_key: str | None = None,
    destination: DestinationRef | None = None,
    executor: Any | None = None,
    resolver: ModelResolver | None = None,
    snapshot_authority: SnapshotAuthority | None = None,
    max_active_runs: int | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> RunReceipt:
    """Verify and execute a plan, reopening owner-retained inputs for saved plans."""
    document = json.loads(serialized)
    if isinstance(document, dict) and "plan_json" in document:
        saved_plan = SavedPlan.from_json(serialized)
        embedded_plan = _bind_saved_plan(saved_plan, namespace=namespace)
        saved_plan_destination = saved_plan.destination
        if destination is not None and destination != saved_plan_destination:
            raise _SAVED_PLAN_MISMATCH.error(reason="supplied destination differs from saved plan")
        if request_key is not None:
            destination = saved_plan_destination
            existing = find_run_by_request_key(
                request_key, destination=destination, con=con, namespace=namespace
            )
            if existing is not None:
                store = RuntimeStore(con, namespace=namespace)
                store.initialize()
                fingerprint = con.execute(
                    f"SELECT fingerprint FROM {store._schema}.runtime_runs WHERE run_id=?",
                    [existing.run_id],
                ).fetchone()
                if fingerprint is None or fingerprint[0] != embedded_plan.definition.fingerprint:
                    raise RefusalSpec(
                        "terrine.runtime.request_key_conflict",
                        "Request key is already bound to a different plan.",
                    ).error()
            if existing is not None and existing.state in {
                "running",
                "cancel_requested",
                "succeeded",
                "cancelled",
            }:
                return run_plan(
                    embedded_plan,
                    con=con,
                    namespace=namespace,
                    request_key=request_key,
                    destination=destination,
                )
        elif destination is not None:
            raise ValueError("destination is only used with a request_key")
        if resolver is None or snapshot_authority is None:
            raise ValueError("saved plans require an owner resolver and snapshot authority")
        plan, model, construction, _saved_plan_destination = resolve_plan(
            saved_plan, resolver=resolver, namespace=namespace
        )
        from terrine.spec.dataset import DatasetDefinition

        design = (
            construction.base_design
            if isinstance(construction, DatasetDefinition)
            else construction
        )
        receipt = snapshot_authority.find_retained(plan.snapshot_id, namespace=namespace)
        if receipt.model_hash != model.model_hash:
            raise RefusalSpec(
                "terrine.runtime.saved_plan_mismatch",
                "Saved plan reconstruction failed verification "
                "(retained snapshot Model declaration differs).",
            ).error()
        from terrine.execution.gates import enforce_shape_budget

        enforce_shape_budget(design.shape, plan.budget)
        if executor is None:
            _preflight_design_partitions(plan)
            preflight_runtime_shape(design, plan.budget)
            executor = DesignExecutor(
                model,
                design,
                plan,
                con,
                namespace=namespace,
                dataset_definition=saved_plan.restore_dataset_definition(),
                output_root=snapshot_authority.owner.snapshot_root / namespace / "outputs",
            )
        if not callable(getattr(executor, "run_snapshot_task", None)):
            raise ValueError("saved plans require an executor with run_snapshot_task")
        executor = _SnapshotBoundExecutor(executor, snapshot_authority, receipt, con, namespace)
    else:
        plan = RefreshPlan.deserialize(serialized)
    return run_plan(
        plan,
        con=con,
        namespace=namespace,
        request_key=request_key,
        destination=destination,
        executor=executor,
        max_active_runs=max_active_runs,
        cancelled=cancelled,
    )


class _SnapshotBoundExecutor:
    def __init__(
        self, executor: Any, authority: SnapshotAuthority, receipt: Any, con: Any, namespace: str
    ) -> None:
        self.executor = executor
        self.authority = authority
        self.receipt = receipt
        self.con = con
        self.namespace = namespace

    def authorize_scan(self, plan: RefreshPlan) -> ScanAuthorization:
        authorize = getattr(self.executor, "authorize_scan", None)
        if not callable(authorize):
            raise _AUTHORITY.error()
        return authorize(plan)

    def run_task(self, task: Any, dependencies: Any) -> Any:
        context = self.authority.reopen(self.receipt, con=self.con, namespace=self.namespace)
        try:
            return self.executor.run_snapshot_task(context, task, dependencies)
        finally:
            context.close()

    def publish(self, plan: RefreshPlan, outputs: Any, lease: Any, run_id: str) -> Any:
        return self.executor.publish(plan, outputs, lease, run_id)


def find_run_by_request_key(
    request_key: str,
    *,
    destination: DestinationRef,
    con: Any,
    namespace: str,
) -> RunReceipt | None:
    if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 256:
        raise ValueError("request_key must be a non-empty string of at most 256 characters")
    if not isinstance(destination, DestinationRef):
        raise TypeError("destination must be DestinationRef")
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    table = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE lower(table_schema)=lower(?) AND table_name='runtime_runs'",
        [namespace],
    ).fetchone()
    if table is None:
        return None
    con.execute(f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS request_key VARCHAR")
    con.execute(
        f"ALTER TABLE {schema}.runtime_runs ADD COLUMN IF NOT EXISTS destination_json VARCHAR"
    )
    _recover_interrupted_runs(con, namespace=namespace)
    destination_json = json.dumps(destination.to_dict(), sort_keys=True, separators=(",", ":"))
    row = con.execute(
        f"SELECT run_id,state,published_generation FROM {schema}.runtime_runs "
        "WHERE request_key=? AND destination_json=?",
        [request_key, destination_json],
    ).fetchone()
    return RunReceipt(*row) if row is not None else None


def _record_result_partitions(
    plan: RefreshPlan, receipt: RunReceipt, *, con: Any, namespace: str
) -> None:
    if receipt.state != "succeeded":
        return
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.runtime_run_results "
        "(run_id VARCHAR NOT NULL, partition VARCHAR NOT NULL, PRIMARY KEY(run_id,partition))"
    )
    dependencies = {dependency for task in plan.tasks for dependency in task.dependencies}
    results = sorted(task.task_id for task in plan.tasks if task.task_id not in dependencies)
    if not results:
        return
    for partition in results:
        con.execute(
            f"INSERT INTO {schema}.runtime_run_results VALUES (?, ?) ON CONFLICT DO NOTHING",
            [receipt.run_id, partition],
        )


def open_published_generation(receipt: RunReceipt, *, con: Any, namespace: str) -> ibis.Table:
    """Open only partitions recorded for the exact run publication receipt."""
    if not isinstance(receipt, RunReceipt) or receipt.state != "succeeded":
        raise ValueError("a succeeded run receipt is required")
    if receipt.published_generation is None:
        raise ValueError("run receipt has no published generation")
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    row = con.execute(
        f"SELECT dataset,state,published_generation FROM {schema}.runtime_runs WHERE run_id=?",
        [receipt.run_id],
    ).fetchone()
    if row is None or row[1] != "succeeded" or row[2] != receipt.published_generation:
        raise ValueError("run receipt does not match authoritative runtime state")
    from terrine.runtime.backends.duckdb import DuckDBBackend

    backend = DuckDBBackend(con, namespace=namespace)
    publication = con.execute(
        f"SELECT dataset,generation,version FROM {schema}.runtime_run_publications WHERE run_id=?",
        [receipt.run_id],
    ).fetchone()
    if (
        publication is None
        or publication[0] != row[0]
        or publication[1] != receipt.published_generation
    ):
        raise ValueError("run has no matching durable publication receipt")
    partitions = con.execute(
        f"SELECT p.partition FROM {schema}.runtime_run_results p "
        f"JOIN {schema}.runtime_partitions r ON r.partition=p.partition "
        "WHERE p.run_id=? AND r.dataset=? AND r.generation=? ORDER BY p.partition",
        [receipt.run_id, publication[0], publication[1]],
    ).fetchall()
    if not partitions:
        raise ValueError("published generation has no retained partitions")
    published = PublicationReceipt(
        publication[0], publication[1], publication[2], tuple(item[0] for item in partitions)
    )
    relations = [
        backend.read_relation(published, partition, ibis_backend=None)
        for (partition,) in partitions
    ]
    return relations[0] if len(relations) == 1 else ibis.union(*relations, distinct=False)


def preflight_runtime_shape(design: Any, budget: Any = None) -> None:
    from terrine.compile.materialize import preflight_materialization_shape
    from terrine.execution.gates import enforce_shape_budget
    from terrine.runtime.materialize import ensure_runtime_expressions_supported

    enforce_shape_budget(design.shape, budget)
    ensure_runtime_expressions_supported(design)
    preflight_materialization_shape(design, budget)


def _preflight_design_partitions(plan: RefreshPlan) -> None:
    partitions = {task.partition for task in plan.tasks if task.layer == "frame"}
    if len(partitions) > 1:
        raise RefusalSpec(
            "terrine.runtime.design_partition_unsupported",
            "The built-in Design executor supports one output partition; use a "
            "single-partition plan or a custom executor with per-partition bounds.",
        ).error()


_EXACT_LAYER_COLUMNS_KEY = b"terrine.runtime.exact_layer_columns"


def _persistable_layer(relation: Any, execute: Any):

    exact_columns = tuple(
        name
        for name, dtype in relation.schema().items()
        if dtype.is_unknown() and str(dtype.raw_type).upper() == "BIGNUM"
    )
    serializable = (
        relation.mutate(**{name: relation[name].cast("string") for name in exact_columns})
        if exact_columns
        else relation
    )
    result = execute(serializable)
    if exact_columns:
        import pyarrow as pa

        metadata = {_EXACT_LAYER_COLUMNS_KEY: json.dumps(exact_columns).encode()}
        if isinstance(result, pa.RecordBatchReader):
            result = pa.RecordBatchReader.from_batches(
                result.schema.with_metadata(metadata), result
            )
        else:
            result = result.replace_schema_metadata(metadata)
    return result


def _restore_persisted_layer(context: Any, arrow: Any, name: str):
    from ibis.expr import datatypes as dt
    from sqlglot import exp as sge

    exact_dtype = dt.Unknown(raw_type=sge.DataType.build("BIGNUM", dialect="duckdb"))

    from terrine.runtime.arrow import ArrowOutput

    metadata = arrow.schema.metadata or {}
    if isinstance(arrow, ArrowOutput):
        with arrow.open() as reader:
            raw = context.con.con
            raw.register(name + "_input", reader)
            try:
                raw.execute(f'CREATE TEMP TABLE "{name}" AS SELECT * FROM "{name}_input"')
            finally:
                raw.unregister(name + "_input")
        relation = context.con.table(name)
    else:
        relation = context.con.create_table(name, obj=arrow, temp=True)
    exact_columns = json.loads(metadata.get(_EXACT_LAYER_COLUMNS_KEY, b"[]"))
    return (
        relation.mutate(**{column: relation[column].cast(exact_dtype) for column in exact_columns})
        if exact_columns
        else relation
    )


class DesignExecutor:
    """Execute a Design through snapshot-bound compiler layers and atomic publication."""

    def __init__(
        self,
        model: Any,
        design: Any,
        plan: Any,
        con: Any,
        *,
        namespace: str,
        dataset_definition: Any = None,
        output_root: Any = None,
    ) -> None:
        from threading import RLock

        from terrine.runtime.backends.duckdb import DuckDBBackend

        self._model = model
        self.design = design
        self._plan = plan
        self._keys: tuple[Any, ...] | None = None
        self._backend = DuckDBBackend(con, namespace=namespace)
        self._layers: dict[str, dict[Any, Any]] = {}
        self._lock = RLock()
        self._frame = (
            dataset_definition.build(model)
            if dataset_definition is not None
            else design.build(model)
        )
        if output_root is None:
            from terrine.runtime.owner import RuntimeOwner

            output_root = RuntimeOwner.from_connection(con).snapshot_root / namespace / "outputs"
        self._output_root = output_root

    def run_snapshot_task(self, context: Any, task: Any, dependencies: Any):
        import uuid

        import pyarrow as pa

        from terrine.compile.layers import build_layer, plan_layers
        from terrine.data import MaterializedFrame, Provenance

        if task.layer in {
            "daily",
            "roster",
            "selector",
            "state_point",
            "state_period",
            "baseline",
        }:
            return pa.table({"_terrine_empty": pa.array([], type=pa.int8())})
        execution_model = self._execution_model(context)
        with self._lock:
            if self._keys is None:
                self._keys = plan_layers(self.design, execution_model).keys
            layer_keys = self._keys
        if task.layer == "prefix":
            keys = [
                self._with_snapshot(key, self._plan.snapshot_id)
                for key in layer_keys
                if key.layer == "prefix" and key.source == task.source
            ]
            results = {
                key: self._stream_layer(
                    build_layer(
                        self.design, execution_model, key, relation_resolver=context.resolve
                    ),
                    context,
                )
                for key in keys
            }
            with self._lock:
                self._layers.setdefault(task.partition, {}).update(results)
            return (
                next(iter(results.values()))
                if results
                else pa.table({"_terrine_empty": pa.array([], type=pa.int8())})
            )
        if task.layer == "anchor":
            key = self._with_snapshot(layer_keys[0], self._plan.snapshot_id)
            result = self._stream_layer(
                build_layer(self.design, execution_model, key, relation_resolver=context.resolve),
                context,
            )
            with self._lock:
                self._layers.setdefault(task.partition, {})[key] = result
            return result
        if task.layer != "frame":
            raise ValueError(f"unsupported runtime task layer: {task.layer}")

        from terrine.runtime.materialize import compose_runtime_frame

        persisted = {}
        registered = []
        with self._lock:
            layer_outputs = dict(self._layers.get(task.partition, {}))
        for key, arrow in layer_outputs.items():
            name = f"_terrine_runtime_layer_{uuid.uuid4().hex}"
            relation = _restore_persisted_layer(context, arrow, name)
            registered.append(name)
            persisted[key] = relation
        spooled = None
        try:
            compiled = compose_runtime_frame(
                self.design, execution_model, persisted=persisted, budget=self._plan.budget
            )
            import terrine.compile.provenance as _provenance
            from terrine.data import SamplingMeta
            from terrine.execution.gates import _eligibility_summary
            from terrine.spec.anchors import FixedDate
            from terrine.spec.sequence import EventSequence
            from terrine.spec.state_panel import StatePanel
            from terrine.spec.trajectory import CohortSpec, LifeTable, TimeToEvent

            excluded_counts = {}
            if self._frame._training_view is not None:
                from terrine.frame import _excluded_label_counts

                if self._frame._training_view.incomplete_labels == "drop":
                    excluded_counts = _excluded_label_counts(
                        compiled, self._frame._training_view, context.execute
                    )
            compiled = self._frame._active_compiled(compiled)
            summary = _eligibility_summary(compiled.audit_table, context.execute)
            reader = context.to_arrow_batches(compiled.table, chunk_size=65_536)
            from terrine.runtime.arrow import ArrowOutput

            table = self._frame._normalize_output(pa.Table.from_batches([], schema=reader.schema))
            table, _ = self._frame._split_output(table, compiled)
            view = self._frame._training_view
            if view is not None and view.split is not None:
                source_reader = reader

                def split_batches():
                    try:
                        for batch in source_reader:
                            normalized = self._frame._normalize_output(
                                pa.Table.from_batches([batch])
                            )
                            normalized, counts = self._frame._split_output(normalized, compiled)
                            for reason, count in counts.items():
                                excluded_counts[reason] = excluded_counts.get(reason, 0) + count
                            yield from normalized.cast(table.schema).to_batches()
                    finally:
                        source_reader.close()

                spooled = ArrowOutput.write(
                    pa.RecordBatchReader.from_batches(table.schema, split_batches()),
                    root=self._output_root,
                    budget=self._plan.budget,
                )
                reader = spooled.open()
            panel = _provenance.panel_metadata(self.design)
            if isinstance(self.design.shape, StatePanel):
                shape = "state_panel"
            elif isinstance(self.design.shape, EventSequence):
                shape = "event_sequence"
            elif _provenance.calendar_comparison_metadata(self.design) is not None:
                shape = "calendar_comparison"
            elif panel is not None:
                shape = panel.shape
            elif isinstance(self.design.shape, CohortSpec):
                shape = "ltv"
            elif isinstance(self.design.shape, LifeTable):
                shape = "life_table"
            elif isinstance(self.design.shape, TimeToEvent):
                shape = "time_to_event"
            else:
                shape = self.design.shape if isinstance(self.design.shape, str) else "cross_section"

            treatment = self.design.treatment
            treatment_column = compiled.roles.treatment
            baseline_binary = (
                treatment is not None
                and treatment_column is not None
                and isinstance(self.design.anchor, FixedDate)
                and self.design.shape == "cross_section"
                and treatment.at == 0
                and treatment.known_at_t0
                and pa.types.is_boolean(table.schema.field(treatment_column).type)
            )
            table = self._frame._winsorize_output(table)
            metadata = MaterializedFrame(
                table,
                compiled.roles,
                compiled.columns,
                Provenance(
                    feature_cutoffs=compiled.feature_cutoffs,
                    eligibility_cutoffs=compiled.eligibility_cutoffs,
                    eligibility_summary=summary,
                    design_hash=self.design.spec_hash,
                    model_hash=self._model.model_hash,
                    source_generations=context.snapshot.source_generations,
                    execution_snapshot=context.snapshot,
                    state_panel=_provenance.state_panel_metadata(self.design, self._model),
                    shape=shape,
                    treatment_design="baseline_binary" if baseline_binary else None,
                    treatment_knowledge=(
                        "retrospective_effective_time" if treatment is not None else None
                    ),
                    treatment_known_at_t0=treatment.known_at_t0 if treatment is not None else False,
                    expression_timings=compiled.expression_timings,
                    sequence_meta=compiled.sequence_meta,
                    panel=panel,
                    sampling=(
                        SamplingMeta(
                            self.design.sample.strata,
                            self.design.sample.controls_per_treated,
                            self.design.sample.seed,
                        )
                        if self.design.sample is not None
                        else None
                    ),
                    calendar_comparison=_provenance.calendar_comparison_metadata(self.design),
                    administrative_cutoff=self.design.administrative_cutoff,
                ),
            )
            metadata = (
                MaterializedFrame(
                    metadata.table,
                    metadata.roles,
                    metadata.columns,
                    self._frame._view_provenance(metadata.provenance, excluded_counts),
                )
                .to_arrow()
                .schema
            )

            def batches():
                try:
                    for batch in reader:
                        normalized = self._frame._normalize_output(pa.Table.from_batches([batch]))
                        normalized = self._frame._winsorize_output(normalized)
                        yield from normalized.cast(metadata).to_batches()
                finally:
                    reader.close()

            return ArrowOutput.write(
                pa.RecordBatchReader.from_batches(metadata, batches()),
                root=self._output_root,
                budget=self._plan.budget,
            )
        finally:
            if spooled is not None:
                spooled.path.unlink(missing_ok=True)
            for name in registered:
                context.con.drop_table(name, force=True)

    def _stream_layer(self, relation: Any, context: Any):
        from terrine.runtime.arrow import ArrowOutput

        def read_layer(table):
            return context.to_arrow_batches(table, chunk_size=65_536)

        reader = _persistable_layer(relation, read_layer)
        return ArrowOutput.write(reader, root=self._output_root)

    @metadata_operation()
    def _execution_model(self, context: Any):
        from terrine.spec.validate import validate_design

        required_sources = set(validate_design(self.design, self._model).required_sources)
        return self._model._for_execution(
            context, source_names=required_sources, unit_name=self.design.unit
        )

    @staticmethod
    def _with_snapshot(key: Any, snapshot_id: str):
        from dataclasses import replace

        return replace(key, snapshot_id=snapshot_id)

    def publish(self, plan: Any, outputs: Any, lease: Any, run_id: str):
        from terrine.runtime.arrow import ArrowOutput
        from terrine.runtime.backends.base import StagedGeneration

        if not plan.tasks:
            return self._backend.reuse_generation(
                plan.dataset,
                expected_manifest=plan.expected_base_manifest,
                lease=lease,
                run_id=run_id,
            )

        stage = StagedGeneration(
            plan.dataset,
            f"generation-{run_id}",
            {
                key: output.open() if isinstance(output, ArrowOutput) else output
                for key, output in outputs.items()
            },
            run_id=run_id,
            snapshot_id=plan.snapshot_id,
        )
        return self._backend.publish_generation(
            stage, expected_manifest=plan.expected_base_manifest, lease=lease
        )


def open_published_frame(receipt: RunReceipt, *, con: Any, namespace: str):
    """Read metadata-preserving Arrow output from the exact published run generation."""
    import pyarrow as pa

    if not isinstance(receipt, RunReceipt) or receipt.state != "succeeded":
        raise ValueError("a succeeded run receipt is required")
    if receipt.published_generation is None:
        raise ValueError("run receipt has no published generation")
    store = RuntimeStore(con, namespace=namespace)
    store.initialize()
    schema = store._schema
    run = con.execute(
        f"SELECT dataset,state,published_generation FROM {schema}.runtime_runs WHERE run_id=?",
        [receipt.run_id],
    ).fetchone()
    if run is None or run[1] != "succeeded" or run[2] != receipt.published_generation:
        raise ValueError("run receipt does not match authoritative runtime state")
    publication = con.execute(
        f"SELECT dataset,generation,version FROM {schema}.runtime_run_publications WHERE run_id=?",
        [receipt.run_id],
    ).fetchone()
    if publication is None or (publication[0], publication[1]) != (run[0], run[2]):
        raise ValueError("run has no matching durable publication receipt")
    partitions = con.execute(
        f"SELECT p.partition FROM {schema}.runtime_run_results p "
        f"JOIN {schema}.runtime_partitions r ON r.partition=p.partition "
        "WHERE p.run_id=? AND r.dataset=? AND r.generation=? ORDER BY p.partition",
        [receipt.run_id, run[0], run[2]],
    ).fetchall()
    if not partitions:
        raise ValueError("published generation has no retained partitions")
    from terrine.data import MaterializedFrame
    from terrine.runtime.backends.base import PublicationReceipt
    from terrine.runtime.backends.duckdb import DuckDBBackend

    publication_receipt = PublicationReceipt(
        run[0], run[2], publication[2], tuple(item[0] for item in partitions)
    )
    backend = DuckDBBackend(con, namespace=namespace)
    frames = [
        MaterializedFrame.from_arrow(backend.read_partition(publication_receipt, partition[0]))
        for partition in partitions
    ]
    first = frames[0]
    if any(
        frame.roles != first.roles
        or frame.columns != first.columns
        or frame.provenance != first.provenance
        for frame in frames[1:]
    ):
        raise ValueError("published Arrow partitions do not share one Frame metadata contract")
    table = pa.concat_tables([frame.table for frame in frames])
    return MaterializedFrame(table, first.roles, first.columns, first.provenance)
