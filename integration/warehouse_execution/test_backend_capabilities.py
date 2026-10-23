"""Executed local capability evidence; no external warehouse connections."""

import sys
from _thread import LockType, RLock
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from threading import Event

import ibis
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from terrine import retention
from terrine.data import Budget, Coverage, MaterializedFrame, UnitSource
from terrine.errors import TerrineError, thaw
from terrine.execution.backend import backend_capabilities
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import (
    CalendarComparisonPanel,
    CohortSpec,
    Design,
    EventAnchor,
    EventPanel,
    FixedDate,
    Initiation,
    Schedule,
    Sequential,
    agg,
    event_time,
    from_json,
    quantile,
    to_json,
)
from terrine.spec.expressions import RetentionMetric
from terrine.spec.io import from_yaml, to_yaml
from terrine.spec.trajectory import CohortGrouping, Trajectory
from tests._timing import HANG_GUARD_SECONDS
from tests.compile.test_ltv import _trajectory_model
from tests.compile.test_panels import _event_model
from tests.compile.test_retention import retention_model as retention_model
from tests.conftest import events_world as events_world
from tests.conftest import native_inputs as native_inputs
from tests.fixtures.interop import interop_frame
from tests.reference.retention import retention_rows
from tests.test_frame_runtime import _design, _model


def test_duckdb_typed_dataset_arrow_parquet_and_generation_parity(tmp_path):
    con = ibis.duckdb.connect(str(tmp_path / "owner.duckdb"))
    try:
        frame = interop_frame(con)
        budget = Budget(max_rows=3, max_output_bytes=1_000_000)
        expected = frame.to_pyarrow(budget=budget).sort_by("unit")
        metadata = MaterializedFrame.from_arrow(expected)
        rows = expected.to_pylist()
        assert [row["amount"] for row in rows] == [
            Decimal("999999999999999999999999999999999999.98"),
            None,
            Decimal("0.00"),
        ]
        assert [row["tier"] for row in rows] == ["old", None, "empty"]
        assert rows[1][metadata.columns["amount"].reason_column] == "all_values_null"
        assert all(
            row[metadata.columns["future"].reason_column] == "incomplete_coverage" for row in rows
        )
        score = metadata.columns["score"]
        assert score.numeric_policy is None
        assert expected.schema.field("amount").type == pa.decimal128(38, 2)
        assert expected.schema.field("t0").type == pa.timestamp("us", tz="UTC")

        dataset = frame.stage("interop", budget=budget)
        batches = list(dataset.to_arrow_batches(batch_size=1))
        assert all(batch.num_rows <= 1 for batch in batches)
        streamed = pa.Table.from_batches(batches).sort_by("unit")
        assert streamed.equals(expected)
        streamed_metadata = MaterializedFrame.from_arrow(streamed)
        assert streamed_metadata.columns == metadata.columns
        assert streamed_metadata.roles == metadata.roles
        assert streamed_metadata.provenance.design_hash == metadata.provenance.design_hash
        assert streamed_metadata.provenance.model_hash == metadata.provenance.model_hash
        assert dataset.manifest.source_snapshots["snapshot_id"]
        assert dataset.manifest.source_snapshots["receipt_hash"]
        snapshot = streamed_metadata.provenance.execution_snapshot
        assert snapshot.input_binding_id is not None
        assert snapshot.snapshot_id == dataset.manifest.source_snapshots["snapshot_id"]
        assert snapshot.input_binding_id == snapshot.snapshot_id
        assert snapshot.replay_equivalence == "verified"
        assert snapshot.source_generations
        assert all(
            isinstance(value, str) and value for value in snapshot.source_generations.values()
        )
        assert metadata.provenance.execution_snapshot.input_binding_id is None
        assert set(dataset.manifest.source_snapshots["revisions"].values()) == {None}

        destination = tmp_path / "export"
        exported = dataset.export_parquet(destination=str(destination), budget=budget)
        assert backend_capabilities(con).native_parquet_unload is None
        assert exported.generation_id == dataset.manifest.generation.generation_id
        assert exported.schema == tuple((field.name, str(field.type)) for field in streamed.schema)
        readback = pa.concat_tables(
            [pq.ParquetFile(destination / shard.path).read() for shard in exported.shards]
        ).sort_by("unit")
        assert readback.equals(streamed, check_metadata=True)
        assert (
            MaterializedFrame.from_arrow(readback).to_metadata() == streamed_metadata.to_metadata()
        )
        assert thaw(dataset.manifest.frame_metadata) == streamed_metadata.to_metadata()
        for extra in ("pandas", "polars"):
            try:
                module = __import__(extra)
            except ImportError:
                continue
            converted = (
                pa.Table.from_pandas(
                    readback.to_pandas(types_mapper=module.ArrowDtype), preserve_index=False
                )
                if extra == "pandas"
                else module.from_arrow(readback).to_arrow()
            )
            assert converted.to_pylist() == readback.to_pylist()
            assert converted.schema.field("amount").type == pa.decimal128(38, 2)

        con.con.execute("""
            INSERT INTO events VALUES
            ('a', '2025-01-02 11:59:59+00', 0.02, 0.0, NULL)
        """)
        with pytest.raises(TerrineError) as error:
            frame.materialize(budget)
        assert error.value.code == "terrine.temporal.exact_overflow"
        pinned = pa.Table.from_batches(list(dataset.to_arrow_batches(batch_size=2))).sort_by("unit")
        assert pinned.equals(streamed, check_metadata=True)
    finally:
        con.disconnect()


def test_shared_model_concurrent_exports_keep_independent_results(native_inputs):
    frame = _design().build(_model(native_inputs))
    origin = native_inputs[1].con
    execute, fetchall = origin.execute.__func__, origin.fetchall.__func__
    executed, release, fetched = Event(), Event(), Event()

    def export(first):
        paused = False

        def interleave(_frame, event, operation):
            nonlocal paused
            if first:
                if event == "c_return" and operation is execute and not paused:
                    paused = True
                    executed.set()
                    assert release.wait(timeout=HANG_GUARD_SECONDS)
                elif event == "c_return" and operation is fetchall:
                    fetched.set()
            elif not release.is_set() and (
                (event == "c_return" and operation is fetchall)
                or (event == "return" and isinstance(operation, (LockType, RLock)))
            ):
                # Allow serialization, or force the competing result to be consumed
                # before the first export fetches. No replacement database is used.
                release.set()
                assert fetched.wait(timeout=HANG_GUARD_SECONDS)

        sys.setprofile(interleave)
        try:
            return frame.materialize().table.select(["unit", "t0"]).sort_by("unit")
        finally:
            sys.setprofile(None)
            release.set()
            fetched.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(export, True)
        assert executed.wait(timeout=HANG_GUARD_SECONDS)
        second = pool.submit(export, False)
        results = [future.result(timeout=HANG_GUARD_SECONDS) for future in (first, second)]
    expected = [{"unit": unit, "t0": datetime(2025, 1, 3, tzinfo=UTC)} for unit in ("a", "b")]
    assert all(result.to_pylist() == expected for result in results)


def test_cohort_collection_observes_fresh_snapshot_and_budget():
    import pytest

    from terrine.errors import TerrineError

    con, model = _trajectory_model()
    try:
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=3, cumulative=True),
            ),
        ).build(model)
        cohorts = frame.cohorts(
            grouping=CohortGrouping(
                bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"
            ),
            value_column="cumulative_value",
        )
        con.raw_sql(
            "INSERT INTO events (user_id, ts, kind, amount) VALUES "
            "('u2', TIMESTAMPTZ '2025-01-03 00:00:00+00', 'revenue', 14)"
        )
        result = cohorts.materialize(budget=Budget(max_rows=20, max_output_bytes=100_000))
        row = next(
            row for row in result.table.to_pylist() if row["bucket_index"] == 0 and row["age"] == 0
        )
        assert row["value"] == 12.0
        operation = result.provenance.cohort_operation
        assert operation is not None
        assert operation.parent_design_hash == frame._design.spec_hash
        assert operation.value_column == "cumulative_value"
        restored = MaterializedFrame.from_arrow(result.to_arrow())
        assert restored.provenance.cohort_operation == operation
        with pytest.raises(TerrineError) as error:
            cohorts.materialize(budget=Budget(max_rows=1, max_output_bytes=100_000))
        assert error.value.code == "terrine.budget.rows_exceeded"
    finally:
        con.disconnect()


def test_cohort_table_reuses_parent_ltv_relation_and_provenance():
    con, model = _trajectory_model()
    try:
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=1, cumulative=False),
            ),
        ).build(model)
        cohorts = frame.cohorts(
            grouping=CohortGrouping(bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC)),
            value_column="period_value",
        )
        assert cohorts.parent_frame is frame
        result = cohorts.materialize()
        operation = result.provenance.cohort_operation
        assert operation is not None
        assert operation.parent_design_hash == frame._design.spec_hash
        assert operation.derived_operation_hash
    finally:
        con.disconnect()


def test_repeated_first_entry_events_count_one_cohort_member():
    con, model = _trajectory_model()
    try:
        con.raw_sql(
            "INSERT INTO events (user_id, ts, kind, amount) VALUES "
            "('u1', TIMESTAMPTZ '2025-01-02 00:00:00+00', 'purchase', 0)"
        )
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=1, cumulative=False),
            ),
        ).build(model)
        rows = (
            frame.cohorts(
                grouping=CohortGrouping(
                    bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC)
                ),
                value_column="period_value",
            )
            .to_pyarrow()
            .to_pylist()
        )
        cohort = next(row for row in rows if row["bucket_index"] == 0)
        assert cohort["at_risk"] == 2
    finally:
        con.disconnect()


def test_cohort_summary_surrogate_export_retains_age_and_denominators():
    from terrine.export import to_surrogate_arrow

    con, model = _trajectory_model()
    try:
        cohort = (
            Design(
                "user",
                EventAnchor("purchase", which="first"),
                shape=CohortSpec(
                    origin="purchase",
                    trajectory=Trajectory("net_revenue", "7d", periods=2, cumulative=False),
                ),
            )
            .build(model)
            .cohorts(
                grouping=CohortGrouping(
                    bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"
                ),
                value_column="period_value",
            )
        )
        materialized = cohort.materialize()
        exported = to_surrogate_arrow(materialized, features=(), outcomes=("value",))
        assert {"age", "at_risk", "observable"} <= set(exported.data.column_names)
        assert sorted(set(exported.data["age"].to_pylist())) == [0, 1]
    finally:
        con.disconnect()


def test_trajectory_preserves_event_feature_timing_through_arrow_round_trip():
    con, model = _trajectory_model()
    try:
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            features={
                "prior_revenue": agg("net_revenue", window=("-1d", "0s")),
                "purchase_time": event_time("purchase", window=("-1d", "0s")),
            },
            shape=CohortSpec(
                "purchase", Trajectory("net_revenue", "7d", periods=1, cumulative=False)
            ),
        ).build(model)

        arrow = frame.to_pyarrow()
        restored = MaterializedFrame.from_arrow(arrow)
        prior = restored.provenance.expression_timings["prior_revenue"]
        assert prior.point_time_column == "cohort_time"
        assert prior.point_time_column in arrow.column_names
        if prior.start_bound_column is not None:
            assert prior.start_bound_column in arrow.column_names
        if prior.end_bound_column is not None:
            assert prior.end_bound_column in arrow.column_names
        timing = restored.provenance.expression_timings["purchase_time"]
        assert timing.point_time_column in arrow.column_names
        assert timing.start_bound_column in arrow.column_names
        assert timing.end_bound_column in arrow.column_names
    finally:
        con.disconnect()


def test_sequential_anticipation_extends_control_risk_horizon(native_inputs):
    from terrine.errors import TerrineError

    model = _event_model(native_inputs)
    model.con.raw_sql(
        "UPDATE events SET ts = TIMESTAMPTZ '2025-01-11 00:00:00+00' WHERE user_id = 'b'"
    )
    design = Sequential(
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-04"),
            outcomes={"sessions_after_t0": agg("sessions", window=("0d", "7d"))},
        ),
        treatment=Initiation("events", "session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
        anticipation="2d",
    )

    with pytest.raises(TerrineError) as error:
        design.build(model).materialize()
    assert error.value.code == "terrine.compile.later_control_initiation"

    model.con.raw_sql(
        "UPDATE events SET ts = TIMESTAMPTZ '2025-01-12 00:00:00+00' WHERE user_id = 'b'"
    )
    frame = design.build(model).materialize()
    panel = frame.provenance.panel
    assert panel is not None
    assert panel.anticipation_seconds == 2 * 86_400
    restored_panel = type(frame).from_arrow(frame.to_arrow()).provenance.panel
    assert restored_panel is not None
    assert restored_panel.anticipation_seconds == 2 * 86_400


def test_calendar_comparison_compiles_quantile_outcome_horizon(native_inputs):
    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE events ADD COLUMN metric_value INTEGER")
    con.raw_sql("UPDATE events SET metric_value = 1")
    definition = catalog.model_dump(mode="python")
    definition["sources"]["events"]["facts"] += ("session_value",)
    definition["facts"]["session_value"] = {
        "source": "events",
        "column": "metric_value",
    }
    definition["measures"]["session_value_sum"] = {
        "fact": "session_value",
        "aggregation": "sum",
    }
    definition["metrics"]["session_value"] = {
        "kind": "mean",
        "entity": "user",
        "measure": "session_value_sum",
    }
    catalog = Catalog.model_validate(definition)
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-12")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    base = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-12"),
        outcomes={"p50": quantile("session_value", "0.5", window=("0d", "1d"))},
    )
    frame = CalendarComparisonPanel(
        base,
        model,
        cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
        control="never_treated",
        reference_period=-1,
        comparison_periods=(0,),
    ).materialize(Budget(max_rows=200, max_output_bytes=1_000_000))

    assert frame.table.num_rows > 0
    assert "p50" in frame.roles.outcomes


def test_all_shapes_build_materialize_and_arrow_round_trip(native_inputs):
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    treatment = Initiation("events", "session", history_start=datetime(2025, 1, 1, tzinfo=UTC))
    sequential = Sequential(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-05")),
        treatment=treatment,
    )
    event_panel = EventPanel(
        Design("user", EventAnchor("session", "first")),
        periods=(-1, 0),
        period_seconds=86_400,
    )

    assert from_json(to_json(sequential)) == sequential
    for design in (sequential, event_panel):
        materialized = design.build(model).materialize(
            Budget(max_rows=20, max_output_bytes=100_000)
        )
        restored = MaterializedFrame.from_arrow(materialized.to_arrow())
        assert restored.table.equals(materialized.table)
        assert restored.provenance.shape == (
            "sequential" if design is sequential else "event_panel"
        )
        assert restored.provenance.panel is not None
        if design is sequential:
            assert restored.provenance.panel.trials == ("schedule:0", "schedule:1")


def test_winsorization_uses_frozen_explicit_bounds_at_declared_stage():
    from terrine.spec.transforms import Winsorization
    from tests.fixtures.winsorization import create_model, design

    con = ibis.duckdb.connect()
    try:
        model = create_model(con)
        con.raw_sql(
            "UPDATE events SET amount=9 WHERE ts::DATE='2025-01-02'; "
            "INSERT INTO events VALUES ('a','2025-01-02 13:00:00+00',9,'known')"
        )
        frame = design().build(model)
        original = frame.materialize()
        assert original.table["value"][0].as_py() == Decimal(18)
        transformed = frame.winsorize(Winsorization("value", lower="0", upper="10"))
        result = transformed.materialize()
        expected = original.table["value"].to_pandas().clip(Decimal(0), Decimal(10)).tolist()
        assert result.table["value"].to_pylist() == expected == [Decimal(10)] * 5
        assert result.table["label"].equals(original.table["label"])
        metadata = result.columns["value"]
        assert metadata.window == original.columns["value"].window
        assert metadata.reason_column == original.columns["value"].reason_column
        assert metadata.complete_column == original.columns["value"].complete_column
        assert metadata.transformation["stage"] == "post_aggregation"
        assert metadata.transformation["fit"] is None
        with pytest.raises(TerrineError) as escape:
            _ = transformed.expr
        assert escape.value.code == "terrine.transform.winsorization_expr_unsupported"
    finally:
        con.disconnect()


@pytest.mark.parametrize("horizon", ["2d", "open_ended"])
def test_named_retention_roundtrip_frame_matches_independent_occurrences(retention_model, horizon):
    expression = retention(
        "occurrence",
        label_property="event_name",
        target_event="target",
        thresholds=(1, 3),
        horizon=horizon,
    )
    assert isinstance(expression, RetentionMetric)
    design = Design("user", FixedDate("2025-01-02"), outcomes={"retained": expression})
    assert from_json(to_json(design)) == from_yaml(to_yaml(design)) == design
    assert from_json(to_json(design)).spec_hash == design.spec_hash
    result = design.build(retention_model).materialize()
    table = result.to_arrow()
    meta = result.columns["retained"]
    rows = {row["unit"]: row for row in table.to_pylist()}
    events = (
        retention_model.con.table("events")
        .select(unit="user_id", event_id="id", name="name", timestamp="ts")
        .to_pandas()
    )
    anchors = (
        table.select(["unit", "anchor_id", "t0", "_observed_from", "_observed_until"])
        .to_pandas()
        .rename(columns={"_observed_from": "observed_from", "_observed_until": "observed_until"})
    )
    expected = retention_rows(
        events,
        anchors,
        thresholds=(1, 3),
        horizon=2 * 86400 if horizon == "2d" else None,
        coverage_start=datetime(2025, 1, 1, tzinfo=UTC),
        coverage_end=datetime(2025, 1, 5, tzinfo=UTC),
        cutoff=datetime(2025, 1, 5, tzinfo=UTC),
    )
    for oracle in expected:
        row = rows[oracle["unit"]]
        assert row["retained"] == oracle["retention_band"]
        assert row[meta.complete_column] == oracle["complete"]
        assert row[meta.reason_column] == oracle["reason"]
    assert rows["a"]["retained"] == (1 if horizon == "2d" else 2)
    assert rows["empty"]["retained"] == 0
    assert rows["unknown"][meta.reason_column] == "incomplete_label_history"
    assert meta.numeric_policy == "exact"
    restored = type(result).from_arrow(table)
    assert restored.columns == result.columns
    assert restored.provenance.expression_timings == result.provenance.expression_timings
