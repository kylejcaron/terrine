import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa
import pytest

from terrine.data import (
    BoundCoverage,
    Budget,
    ColumnMeta,
    Coverage,
    CoverageSummary,
    EligibilitySummary,
    ExecutionSnapshot,
    ExpressionTiming,
    FeatureCutoff,
    MaterializedFrame,
    Preview,
    Provenance,
    Roles,
    StatePanelMeta,
    UnitSource,
)
from terrine.errors import TerrineError
from terrine.results import Diagnostic, DiagnosticPolicy, Estimate


def _provenance():
    return Provenance(
        feature_cutoffs={"revenue": FeatureCutoff(-1, True, "recorded_at")},
        eligibility_cutoffs={},
        eligibility_summary=None,
        treatment_knowledge="recorded_at",
        weight_kind=None,
        weight_cutoff=None,
        weight_depends_on_treatment=None,
        design_hash="design",
        model_hash="model",
        source_generations={"events": None},
        execution_snapshot=ExecutionSnapshot(
            snapshot_id=None,
            backend="duckdb",
            source_generations={"events": None},
            input_binding_id=None,
            replay_equivalence="unverified",
        ),
        shape="cross_section",
        treatment_design="baseline_binary",
        treatment_known_at_t0=True,
        expression_timings={
            "first_purchase": ExpressionTiming(
                "event_time",
                "signup",
                (-86400, 0),
                (-86400, 0),
                ("signup",),
                {"signup": (-86400, 0)},
                "selected_time",
                "start_bound",
                "end_bound",
            )
        },
    )


def test_sequence_metadata_round_trips_arrow_as_typed_provenance():
    from terrine.data import SequenceMeta
    from terrine.windows import Window

    provenance = replace(
        _provenance(),
        shape="event_sequence",
        sequence_meta=SequenceMeta(
            fact="signup",
            window=Window(0, 3600),
            max_events=10,
            representation="positions",
            event_fields=("kind", "amount"),
            event_id_column="event_id",
            event_time_column="event_time",
            position_column="sequence_position",
            event_count_column="event_count",
            complete_column="sequence_complete",
            reason_column="sequence_reason",
            property_reason_columns={"kind": "kind_reason", "amount": "amount_reason"},
        ),
    )
    frame = MaterializedFrame(
        pa.table({"unit": ["a"], "nested": [[{"event_id": "e1", "amount": 3}]]}),
        Roles("unit", "anchor_id", "t0", None, (), ()),
        {},
        provenance,
    )
    restored = MaterializedFrame.from_arrow(frame.to_arrow())
    assert restored.table.to_pylist() == frame.table.to_pylist()
    assert restored.provenance.sequence_meta == provenance.sequence_meta


def test_data_exports_feature_cutoff():
    import terrine.data as data

    assert {"FeatureCutoff", "EligibilitySummary"} <= set(data.__all__)


def test_coverage_requires_ordered_dates_and_binds_to_utc_instants():
    coverage = Coverage(start=date(2025, 1, 1), through=date(2025, 1, 3))
    bound = coverage.bind(lambda day: datetime.combine(day, datetime.min.time(), UTC))
    assert bound == BoundCoverage(
        start=datetime(2025, 1, 1, tzinfo=UTC),
        end_exclusive=datetime(2025, 1, 4, tzinfo=UTC),
    )
    with pytest.raises((TypeError, ValueError)):
        Coverage(start=date(2025, 1, 3), through=date(2025, 1, 1))
    assert Coverage("2025-01-01", "2025-01-03") == coverage
    with pytest.raises(TerrineError) as error:
        Coverage(date.max, date.max)
    assert error.value.code == "terrine.metadata.unrepresentable_coverage"
    with pytest.raises(TerrineError) as error:
        BoundCoverage(
            datetime.min.replace(tzinfo=timezone(timedelta(hours=14))),
            datetime(2025, 1, 1, tzinfo=UTC),
        )
    assert error.value.code == "terrine.metadata.unrepresentable_coverage"


def test_eligibility_summary_rejects_inconsistent_counts_with_stable_code():
    with pytest.raises(TerrineError) as error:
        EligibilitySummary(candidate=4, excluded_false=1, unknown=1, retained=1)
    assert error.value.code == "terrine.metadata.invalid_eligibility_summary"


def test_metadata_mappings_are_deeply_immutable_and_detached():
    generations = {"events": None}
    feature_cutoffs = {"revenue": FeatureCutoff(-1, True, "recorded_at")}
    snapshot = ExecutionSnapshot(None, "duckdb", generations, None, "unverified")
    provenance = Provenance(
        feature_cutoffs=feature_cutoffs,
        eligibility_cutoffs={},
        design_hash="d",
        model_hash="m",
        source_generations=generations,
        execution_snapshot=snapshot,
        shape="cross_section",
        treatment_design=None,
    )
    generations["events"] = "later"
    feature_cutoffs.clear()
    assert provenance.source_generations["events"] is None
    assert "revenue" in provenance.feature_cutoffs
    with pytest.raises(TypeError):
        provenance.source_generations["events"] = "changed"  # ty: ignore[invalid-assignment] intentional mutation


def test_materialized_frame_metadata_json_roundtrip_preserves_arrow_values():
    table = pa.table(
        {
            "unit": pa.array(["a", "b"], type=pa.string()),
            "t0": pa.array(
                [datetime(2025, 1, 1, tzinfo=UTC)] * 2, type=pa.timestamp("us", tz="UTC")
            ),
            "treated": pa.array([True, None], type=pa.bool_()),
            "amount": pa.array([Decimal("12.3400"), None], type=pa.decimal128(12, 4)),
        }
    )
    frame = MaterializedFrame(
        table=table,
        roles=Roles("unit", "anchor", "t0", "treated", (), ("amount",)),
        columns={
            "amount": ColumnMeta(None, "outcome", None, None, "amount_complete", "amount_reason"),
        },
        provenance=_provenance(),
    )
    restored = MaterializedFrame.from_json(frame.to_json())
    assert restored.roles == frame.roles
    assert restored.columns == frame.columns
    assert restored.provenance == frame.provenance
    assert restored.table.schema == table.schema
    assert restored.table.column("amount").to_pylist() == [Decimal("12.3400"), None]
    assert restored.table.column("treated").to_pylist() == [True, None]
    assert json.loads(frame.to_json())["roles"]["unit"] == "unit"
    payload = json.loads(frame.to_json())
    payload["provenance"]["unknown_policy"] = True
    with pytest.raises(ValueError):
        MaterializedFrame.from_json(json.dumps(payload))


def test_materialized_frame_arrow_schema_metadata_can_be_recovered():
    frame = MaterializedFrame(
        table=pa.table(
            {
                "unit": ["u"],
                "anchor": ["a"],
                "t0": pa.array(
                    [datetime(2025, 1, 1, tzinfo=UTC)], type=pa.timestamp("us", tz="UTC")
                ),
            }
        ),
        roles=Roles("unit", "anchor", "t0", None, (), ()),
        columns={},
        provenance=_provenance(),
    )
    recovered = MaterializedFrame.from_arrow(frame.to_arrow())
    assert recovered.roles == frame.roles
    assert recovered.provenance == frame.provenance
    assert recovered.table.schema.metadata == frame.table.schema.metadata


def test_budget_and_preview_validate_counts_and_coverage():
    assert Budget(max_rows=0, max_output_bytes=1, max_scan_bytes=0).max_rows == 0
    preview = Preview(
        None,
        None,
        None,
        "unknown",
        {
            "events": CoverageSummary(
                datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)
            )
        },
        {},
        (),
    )
    assert preview.planned_rows is None
    for kwargs in ({"max_rows": -1}, {"max_output_bytes": -1}, {"max_scan_bytes": -1}):
        with pytest.raises((TypeError, ValueError)):
            Budget(**kwargs)  # ty: ignore[invalid-argument-type] intentional invalid inputs
    with pytest.raises((TypeError, ValueError)):
        Preview(-1, None, None, None, {}, {}, ())


def test_unit_source_requires_named_table_and_key():
    with pytest.raises((TypeError, ValueError)):
        UnitSource(table="users", key="")


def test_estimation_result_models_reject_nonfinite_values_and_preserve_status():
    diagnostic = Diagnostic(
        "balance",
        "unavailable",
        {"smd": None},
        {"warn": 0.1},
        {"reason": "feature has no observed values"},
    )
    assert diagnostic.status == "unavailable"
    with pytest.raises(ValueError):
        Diagnostic("overlap", "pass", {"ess": float("inf")}, {}, {})
    with pytest.raises(ValueError):
        DiagnosticPolicy(overlap_fail=0.1, overlap_warn=0.05)
    with pytest.raises(ValueError):
        Estimate(
            "ipw",
            "ATE",
            "outcome",
            float("nan"),
            0.2,
            (-1, 1),
            0.05,
            10,
            10,
            "unit",
            "crossfit_parametric_sandwich",
            (diagnostic,),
            {},
        )


def test_state_panel_roles_and_metadata_survive_arrow_json_roundtrip():
    from dataclasses import replace

    meta = StatePanelMeta(
        state_source={"kind": "observations", "property": "status"},
        levels=("new", "active", "closed"),
        ordered=True,
        absorbing=("closed",),
        panel_start=datetime(2025, 1, 1, tzinfo=UTC),
        administrative_end=datetime(2025, 2, 1, tzinfo=UTC),
        period_seconds=604800,
        baseline_features=("plan",),
        time_varying_features=("sessions",),
        prior_state_timing="at_origin",
        period_column="period_index",
        from_time_column="from_time",
        to_time_column="to_time",
        scheduled_to_column="scheduled_to",
        gap_column="gap",
        terminal_column="terminal",
    )
    frame = MaterializedFrame(
        table=pa.table(
            {
                "unit": ["u"],
                "anchor_id": ["a"],
                "t0": pa.array(
                    [datetime(2025, 1, 1, tzinfo=UTC)], type=pa.timestamp("us", tz="UTC")
                ),
            }
        ),
        roles=Roles(
            "unit",
            "anchor_id",
            "t0",
            None,
            (),
            ("to_state",),
            cluster_unit="unit",
            prior_state="from_state",
        ),
        columns={},
        provenance=replace(_provenance(), state_panel=meta),
    )
    recovered = MaterializedFrame.from_arrow(frame.to_arrow())
    assert recovered.roles == frame.roles
    assert recovered.provenance.state_panel == meta
    assert recovered.provenance.state_panel.levels == ("new", "active", "closed")
