"""Staging DuckDB datasets and reading their pinned generations."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from terrine import Budget, agg
from terrine.data import MaterializedFrame
from terrine.dataset.reference import Dataset
from terrine.dataset.splits import TimeSplit
from terrine.errors import TerrineError, thaw
from terrine.frame import Frame
from terrine.spec import BinnedMetricState, Design, FixedDate, Schedule, StatePanel
from terrine.spec.expressions import since
from tests import _worlds
from tests._timing import HANG_GUARD_SECONDS
from tests.conftest import events_world as events_world  # noqa: F401
from tests.conftest import interop_world as interop_world  # noqa: F401
from tests.dataset.test_staging import _durable_frame
from tests.fixtures.interop import interop_design


def test_state_panel_stage_matches_ordinary_normalization_and_support(events_world, tmp_path):
    base, con = _durable_frame(events_world, tmp_path)
    frame = (
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-04"),
            shape=StatePanel(
                BinnedMetricState("sessions", (0,), ("none", "some")),
                periods=2,
                baseline_features={"baseline": agg("sessions", window=("-1d", "0s"))},
            ),
        )
        .build(base._model)
        .training_set(
            label="to_state", features=("baseline", "from_state"), incomplete_labels="keep"
        )
    )
    budget = Budget(max_rows=100, max_output_bytes=100_000, strict_scan=False)
    ordinary = frame.materialize(budget=budget)
    staged = frame.stage("periods", budget=budget)
    batches = list(staged.to_arrow_batches(batch_size=1))
    actual = MaterializedFrame.from_arrow(pa.Table.from_batches(batches))
    assert actual.table.schema.field("gap").type == pa.duration("us")
    order = [("unit", "ascending"), ("period_index", "ascending")]
    assert actual.table.sort_by(order).equals(ordinary.table.sort_by(order))
    assert actual.provenance.state_panel == ordinary.provenance.state_panel
    assert actual.provenance.dataset_view == ordinary.provenance.dataset_view
    assert actual.provenance.state_panel is not None
    assert actual.provenance.state_panel.period_column in actual.table.column_names
    con.disconnect()


def test_stage_never_collects_full_output_through_snapshot_execute(
    events_world, tmp_path, monkeypatch
):
    from terrine.execution.inputs import ExecutionContext

    original = ExecutionContext.execute

    def execute(context, relation):
        if {"unit", "t0", "sessions", "churn"}.issubset(relation.columns):
            raise AssertionError("full output must cross the bounded Arrow boundary")
        return original(context, relation)

    monkeypatch.setattr(ExecutionContext, "execute", execute)
    frame, con = _durable_frame(events_world, tmp_path)
    staged = frame.stage("bounded", budget=Budget(strict_scan=False))
    iterator = staged.to_arrow_batches(batch_size=1)
    assert next(iterator).num_rows == 1
    iterator.close()
    assert con.con.execute("SELECT 1").fetchone() == (1,)
    con.disconnect()


def test_staging_runs_the_check_schedule_once(events_world, tmp_path, monkeypatch):
    """Composition validates the frame; staging then reads only its eligibility
    summary, so no gate or anchor statement runs twice in one task."""
    import ibis
    from ibis.backends.duckdb import Backend

    from terrine.execution.inputs import ExecutionContext

    frame, con = _durable_frame(events_world, tmp_path)
    executed = []
    context_execute, backend_to_pyarrow = ExecutionContext.execute, Backend.to_pyarrow

    def execute(self, query):
        executed.append(ibis.to_sql(query, dialect="duckdb"))
        return context_execute(self, query)

    def to_pyarrow(self, expr, **kwargs):
        executed.append(ibis.to_sql(expr, dialect="duckdb"))
        return backend_to_pyarrow(self, expr, **kwargs)

    monkeypatch.setattr(ExecutionContext, "execute", execute)
    monkeypatch.setattr(Backend, "to_pyarrow", to_pyarrow)
    try:
        frame.stage("dataset")
    finally:
        con.disconnect()
    gates = [sql for sql in executed if "__terrine_gate_check_" in sql]
    anchors = [sql for sql in executed if "invalid_count" in sql]
    assert len(gates) == 1
    assert anchors and len(anchors) == len(set(anchors))


def test_staged_batches_preserve_frame_metadata_and_verify_definition_ownership(
    events_world, tmp_path
):
    frame, con = _durable_frame(events_world, tmp_path)
    training = frame.training_set(label="churn", incomplete_labels="keep")
    expected = training.materialize(budget=Budget(strict_scan=False))
    dataset = training.stage("training", budget=Budget(strict_scan=False))
    batches = list(dataset.to_arrow_batches(batch_size=1))
    actual = MaterializedFrame.from_arrow(pa.Table.from_batches(batches))
    assert actual.roles == expected.roles
    assert actual.columns == expected.columns
    assert actual.provenance.dataset_view == expected.provenance.dataset_view
    assert all(batch.num_rows <= 1 for batch in batches)
    wrong = replace(dataset.manifest, definition=frame_definition(frame))
    with pytest.raises(TerrineError) as error:
        Dataset.from_manifest(wrong, con=con)
    assert error.value.code == "terrine.dataset.invalid_generation"
    con.disconnect()


def frame_definition(frame):
    from terrine.spec.dataset import DatasetDefinition

    return DatasetDefinition(base_design=frame._design)


def test_stage_renews_lease_during_snapshot_tasks(events_world, tmp_path, monkeypatch):
    import threading

    from terrine.runtime.api import DesignExecutor
    from terrine.runtime.leases import LeaseStore

    renewed = threading.Event()
    original_renew = LeaseStore.renew
    original_task = DesignExecutor.run_snapshot_task

    def renew(store, lease, *, ttl, now):
        result = original_renew(store, lease, ttl=ttl, now=now)
        renewed.set()
        return result

    def run_task(executor, context, task, dependencies):
        assert renewed.wait(timeout=HANG_GUARD_SECONDS)
        return original_task(executor, context, task, dependencies)

    monkeypatch.setattr(LeaseStore, "renew", renew)
    monkeypatch.setattr(DesignExecutor, "run_snapshot_task", run_task)
    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("long_stage", budget=Budget(strict_scan=False))
    assert renewed.is_set()
    assert dataset.manifest.generation.row_count > 0
    con.disconnect()


def test_frame_stage_publishes_and_reads_its_pinned_generation(events_world, tmp_path, monkeypatch):
    frame, con = _durable_frame(events_world, tmp_path)

    def collect_forbidden(*args, **kwargs):
        raise AssertionError("staging must stream the compiled output")

    monkeypatch.setattr(type(frame), "materialize", collect_forbidden)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    pinned = Dataset.from_manifest(dataset.manifest, con=con)
    original_generation = dataset.manifest.generation.generation_id

    assert dataset.manifest.generation.dataset_id == "training"
    assert sum(batch.num_rows for batch in dataset.to_arrow_batches(batch_size=1)) == (
        dataset.manifest.generation.row_count
    )
    assert all(batch.num_rows <= 1 for batch in pinned.to_arrow_batches(batch_size=1))

    newer = frame.stage("training", budget=Budget(strict_scan=False))
    assert newer.manifest.generation.generation_id != original_generation
    assert sum(batch.num_rows for batch in pinned.to_arrow_batches(batch_size=2)) == (
        dataset.manifest.generation.row_count
    )
    con.disconnect()


def test_manifest_reopen_rejects_tampered_generation_row_count(events_world, tmp_path):
    from dataclasses import replace

    from terrine.errors import TerrineError

    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    altered_generation = replace(
        dataset.manifest.generation,
        row_count=dataset.manifest.generation.row_count + 1,
    )
    altered_manifest = replace(dataset.manifest, generation=altered_generation)
    with pytest.raises(TerrineError) as error:
        Dataset.from_manifest(altered_manifest, con=con)
    assert error.value.code == "terrine.dataset.invalid_generation"
    con.disconnect()


def test_roster_only_stage_uses_executor_default_durable_outputs(
    events_world, tmp_path, monkeypatch
):
    from terrine.runtime.api import DesignExecutor

    frame, con = _durable_frame(events_world, tmp_path)
    original = DesignExecutor.__init__

    def initialize(self, *args, **kwargs):
        kwargs.pop("output_root", None)
        original(self, *args, **kwargs)

    monkeypatch.setattr(DesignExecutor, "__init__", initialize)
    con.drop_table("labels")
    roster = Design("user", FixedDate("2025-01-05")).build(frame._model)
    dataset = roster.stage("roster", budget=Budget(strict_scan=False))
    assert dataset.manifest.generation.row_count == 2
    assert set(dataset.manifest.source_coverage) == set()
    assert sorted(
        key for batch in dataset.to_arrow_batches() for key in batch["unit"].to_pylist()
    ) == ["a", "b"]
    con.disconnect()


def test_reopen_accepts_resolved_owner_scoped_definition_reference(events_world, tmp_path):
    from dataclasses import replace

    from terrine.dataset.manifest import DatasetManifest
    from terrine.spec.dataset import DatasetDefinitionStore

    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    definitions = DatasetDefinitionStore(tmp_path / "definitions", owner="customer")
    reference = definitions.put(dataset.manifest.definition)
    manifest = DatasetManifest.from_json(
        replace(dataset.manifest, definition_reference=reference).to_json(),
        definition_store=definitions,
    )
    reopened = Dataset.from_manifest(manifest, con=con)
    assert reopened.manifest.definition_reference == reference
    assert sum(batch.num_rows for batch in reopened.to_arrow_batches()) == 2
    con.disconnect()


def test_reopen_preserves_version_one_manifest_without_frame_metadata(
    events_world, tmp_path, monkeypatch
):
    import json
    from dataclasses import replace

    from terrine.dataset.manifest import DatasetManifest
    from terrine.runtime.backends.duckdb import _digest_relation

    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    checksums = []
    for partition in dataset.manifest.generation.output_refs:
        physical = con.con.execute(
            "SELECT table_name FROM terrine_runtime.runtime_partitions WHERE partition=?",
            [partition],
        ).fetchone()
        assert physical is not None
        checksum = _digest_relation(con.con, "terrine_runtime", physical[0], version=1)
        con.con.execute(
            "UPDATE terrine_runtime.runtime_partitions SET digest=? WHERE partition=?",
            [checksum, partition],
        )
        checksums.append(checksum)
    old = replace(
        dataset.manifest,
        frame_metadata=None,
        generation=replace(dataset.manifest.generation, checksums=tuple(checksums)),
    ).to_json()
    assert "frame_metadata" not in json.loads(old)
    con.con.execute(
        "UPDATE terrine_runtime.runtime_dataset_recipes SET manifest_json=? "
        "WHERE dataset=? AND generation=?",
        [old, "training", dataset.manifest.generation.generation_id],
    )
    restored = DatasetManifest.from_json(old)
    assert restored.to_json() == old
    reopened = Dataset.from_manifest(restored, con=con)
    pandas_values = reopened.to_pandas(values_only=True)
    polars_values = reopened.to_polars(values_only=True)
    assert not any(name.endswith(("__complete", "__reason")) for name in pandas_values.columns)
    assert not any(name.endswith(("__complete", "__reason")) for name in polars_values.columns)
    arrow_without_metadata = reopened.to_pyarrow().replace_schema_metadata(None)
    monkeypatch.setattr(
        Dataset,
        "to_pyarrow",
        lambda self, *, budget=None: arrow_without_metadata,
    )
    with pytest.raises(TerrineError) as error:
        reopened.to_pandas(values_only=True)
    assert error.value.code == "terrine.dataset.invalid_view"
    assert error.value.context["reason"] == "observation_metadata_unavailable"
    assert sum(batch.num_rows for batch in reopened.to_arrow_batches()) == 2
    con.disconnect()


@dataclass(frozen=True)
class CommonScenario:
    frame: Frame
    dataset: Dataset
    expected: pa.Table


@pytest.fixture(scope="module", params=[False, True], ids=["typed_rows", "empty"])
def common_scenario(request, tmp_path_factory, interop_world):
    model = _worlds.bind_model(interop_world, tmp_path_factory.mktemp("common-scenario"))
    try:
        if request.param:
            model.con.raw_sql("DELETE FROM users")
        frame = interop_design(full=False).build(model)
        dataset = frame.stage(
            "common_stack",
            budget=Budget(max_rows=3, max_output_bytes=1_000_000, strict_scan=False),
        )
        expected = frame.to_pyarrow(budget=Budget(max_rows=3))
        yield CommonScenario(frame, dataset, expected)
    finally:
        model.con.disconnect()


def test_local_arrow_preserves_exact_temporal_values_and_semantic_metadata(common_scenario):
    materialized = common_scenario.frame.materialize(budget=Budget(max_rows=3))
    arrow = materialized.to_pyarrow().sort_by("unit")
    assert arrow.schema.field("amount").type == pa.decimal128(38, 2)
    assert arrow.schema.field("t0").type == pa.timestamp("us", tz="UTC")
    restored = MaterializedFrame.from_arrow(arrow)
    assert restored.roles == materialized.roles
    assert restored.columns == materialized.columns
    assert restored.provenance == materialized.provenance
    if arrow.num_rows:
        rows = arrow.to_pylist()
        assert [row["amount"] for row in rows] == [
            Decimal("999999999999999999999999999999999999.98"),
            None,
            None,
        ]
        assert [row["t0"] for row in rows] == [datetime(2025, 1, 2, 12, tzinfo=UTC)] * 3
        assert arrow["note"].null_count == 3
        amount_meta = restored.columns["amount"]
        assert rows[1][amount_meta.reason_column] == "all_values_null"
    else:
        assert arrow.num_rows == 0
        assert arrow.schema.field("note").type == pa.string()
    decorated = replace(
        materialized,
        table=materialized.table.replace_schema_metadata(
            {b"consumer": b"kept", b"terrine.materialized_frame.v2": b"stale"}
        ),
    )
    transported = decorated.to_pyarrow()
    assert transported.schema.metadata[b"consumer"] == b"kept"
    assert MaterializedFrame.from_arrow(transported).to_metadata() == decorated.to_metadata()


@pytest.mark.parametrize("extra", ["pandas", "polars"])
def test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow(common_scenario, extra):
    module = pytest.importorskip(extra)
    expected = common_scenario.expected.sort_by("unit")
    expected = expected.select(
        [
            "unit",
            "anchor_id",
            "t0",
            "amount",
            "amount__complete",
            "amount__reason",
            "note",
            "note__complete",
            "note__reason",
        ]
    )
    converted = getattr(common_scenario.frame, f"to_{extra}")(budget=Budget(max_rows=3))
    if extra == "pandas":
        assert all(isinstance(dtype, module.ArrowDtype) for dtype in converted.dtypes)
        actual = pa.Table.from_pandas(converted, preserve_index=False)
    else:
        actual = converted.to_arrow()
    actual = actual.sort_by("unit")
    assert actual.schema.field("amount").type == expected.schema.field("amount").type
    assert actual.schema.field("t0").type == expected.schema.field("t0").type
    assert actual.to_pylist() == expected.to_pylist()
    assert actual["note"].null_count == expected["note"].null_count


def test_dataset_batches_and_parquet_preserve_typed_metadata(common_scenario, tmp_path):
    budget = Budget(max_rows=3, max_output_bytes=1_000_000)
    expected = common_scenario.expected.sort_by("unit")
    expected_metadata = MaterializedFrame.from_arrow(expected)
    dataset = common_scenario.dataset
    batches = list(dataset.to_arrow_batches(batch_size=1))
    assert all(batch.num_rows <= 1 for batch in batches)
    if batches:
        streamed = pa.Table.from_batches(batches).sort_by("unit")
        assert streamed.to_pylist() == expected.to_pylist()
        assert MaterializedFrame.from_arrow(streamed).columns == expected_metadata.columns
    destination = tmp_path / "parquet"
    exported = dataset.export_parquet(destination=str(destination), budget=budget)
    assert exported.generation_id == dataset.manifest.generation.generation_id
    assert exported.schema == tuple((field.name, str(field.type)) for field in expected.schema)
    readback = pa.concat_tables(
        [pq.ParquetFile(destination / shard.path).read() for shard in exported.shards]
    ).sort_by("unit")
    assert readback.schema.equals(expected.schema, check_metadata=False)
    assert readback.to_pylist() == expected.to_pylist()
    restored = MaterializedFrame.from_arrow(readback)
    assert restored.roles == expected_metadata.roles
    assert restored.columns == expected_metadata.columns
    assert restored.provenance.design_hash == expected_metadata.provenance.design_hash
    assert restored.provenance.model_hash == expected_metadata.provenance.model_hash
    assert thaw(dataset.manifest.frame_metadata) == restored.to_metadata()


def test_dataset_reads_match_materialized_rows(common_scenario, monkeypatch):
    dataset = common_scenario.dataset

    expected = common_scenario.expected
    expected_schema = dataset._arrow_schema()
    original = Dataset._iter_arrow_batches

    def reversed_batches(self, batch_size):
        table = pa.Table.from_batches(list(original(self, batch_size)), schema=self._arrow_schema())
        table = table.take(pa.array(range(table.num_rows - 1, -1, -1), type=pa.int64()))
        yield from table.to_batches(max_chunksize=1)

    monkeypatch.setattr(Dataset, "_iter_arrow_batches", reversed_batches)
    actual = dataset.to_pyarrow()
    assert actual.to_pylist() == expected.to_pylist()
    assert actual.schema.equals(expected_schema, check_metadata=True)
    assert dataset.to_pandas().equals(common_scenario.frame.to_pandas())
    pytest.importorskip("polars")
    assert (
        dataset.to_polars(values_only=True).to_dicts()
        == common_scenario.frame.to_polars(values_only=True).to_dicts()
    )


@pytest.mark.parametrize("common_scenario", [False], ids=["typed_rows"], indirect=True)
def test_dataset_read_budget_refuses(common_scenario):
    dataset = common_scenario.dataset
    expected = dataset.to_pyarrow()
    bounded = dataset.to_pyarrow(budget=Budget(max_output_bytes=expected.nbytes, strict_scan=False))
    assert bounded.equals(expected, check_metadata=True)
    with pytest.raises(TerrineError) as rows_error:
        dataset.to_pyarrow(budget=Budget(max_rows=1, strict_scan=False))
    assert rows_error.value.code == "terrine.budget.rows_exceeded"
    with pytest.raises(TerrineError) as bytes_error:
        dataset.to_pyarrow(budget=Budget(max_output_bytes=expected.nbytes - 1, strict_scan=False))
    assert bytes_error.value.code == "terrine.budget.output_bytes_exceeded"


def test_dataset_read_strict_scan_matches_export(common_scenario, tmp_path):
    dataset = common_scenario.dataset
    budget = Budget(max_scan_bytes=1, strict_scan=True)
    codes = []
    for read in (
        lambda: dataset.to_pyarrow(budget=budget),
        lambda: dataset.export_parquet(destination=str(tmp_path / "strict"), budget=budget),
    ):
        with pytest.raises(TerrineError) as error:
            read()
        codes.append(error.value.code)
    assert codes == ["terrine.budget.scan_unknown", "terrine.budget.scan_unknown"]


def test_conflicting_dataset_view_flags_refuse_before_read(common_scenario, monkeypatch):
    dataset = common_scenario.dataset

    def fail_read(*args, **kwargs):
        raise AssertionError("dataset read should not run")

    monkeypatch.setattr(Dataset, "to_pyarrow", fail_read)
    with pytest.raises(TerrineError) as error:
        dataset.to_pandas(values_only=True, include_observation_metadata=True)
    assert error.value.code == "terrine.dataset.invalid_view"
    assert error.value.context["reason"] == "values_only_excludes_observation_metadata"


@pytest.mark.parametrize("training", [False, True])
def test_since_staged_read_preserves_elapsed_values_and_selected_instants(
    events_world, tmp_path, training
):
    initial, con = _durable_frame(events_world, tmp_path)
    try:
        frame = Design(
            "user",
            FixedDate("2025-01-05"),
            features={"recency": since("session", window=("-3d", 0))},
            outcomes={"churn": agg("churned", window=(0, "1d"))},
        ).build(initial._model)
        if training:
            frame = frame.training_set(
                label="churn",
                incomplete_labels="keep",
                split=TimeSplit(train_until="2025-01-07T00:00:00Z"),
            )
        expected = frame.materialize(budget=Budget(strict_scan=False))
        dataset = frame.stage("recency", budget=Budget(strict_scan=False))
        actual = MaterializedFrame.from_arrow(dataset.to_pyarrow())
        assert pa.types.is_duration(actual.table["recency"].type)
        assert actual.table["recency"].to_pylist() == [timedelta(days=2)] * 2
        assert actual.table.sort_by("unit").equals(expected.table.sort_by("unit"))
        assert actual.columns["recency"].measure == "elapsed"
        timing = actual.provenance.expression_timings["recency"]
        assert timing == expected.provenance.expression_timings["recency"]
        assert pa.types.is_timestamp(actual.table[timing.point_time_column].type)
        assert (
            actual.table[timing.point_time_column].to_pylist()
            == [datetime(2025, 1, 3, tzinfo=UTC)] * 2
        )
        assert timing.point_time_column not in dataset.to_pandas().columns
    finally:
        con.disconnect()
