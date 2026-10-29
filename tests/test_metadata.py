import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Literal

import pyarrow as pa
import pytest

from terrine.data import (
    ApproximationMeta,
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

_Stamp = tuple[str, str, Literal["verified", "unverified"]]
_VERIFIED_STAMP: _Stamp = ("duckdb", "duckdb", "verified")
_UNVERIFIED_STAMP: _Stamp = ("sqlite", "generic", "unverified")
_STAMPS = pytest.mark.parametrize(
    "stamp", [_VERIFIED_STAMP, _UNVERIFIED_STAMP], ids=["verified", "unverified"]
)


def _provenance(stamp: _Stamp = _VERIFIED_STAMP):
    engine_name, dialect, engine_status = stamp
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
            backend=engine_name,
            source_generations={"events": None},
            input_binding_id=None,
            replay_equivalence="unverified",
        ),
        shape="cross_section",
        treatment_design="baseline_binary",
        engine_name=engine_name,
        dialect=dialect,
        engine_status=engine_status,
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


def _frame(provenance):
    return MaterializedFrame(
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
        provenance=provenance,
    )


def test_sequence_metadata_round_trips_arrow_as_typed_provenance():
    from terrine.data import SequenceMeta
    from terrine.windows import Window

    provenance = replace(
        _provenance(),
        shape="event_sequence",
        sequence_meta=SequenceMeta(
            event="signup",
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
            label_property="kind",
            session_gap_seconds=1800,
            session_fields=("session_index", "sessions"),
            sessions_column="sessions",
            sessions_complete_column="sessions_complete",
            sessions_reason_column="sessions_reason",
            transition_from_column="transition_from",
            transition_to_column="transition_to",
            funnel_event_property="kind",
            funnel_steps=("view", "purchase"),
            funnel_within_seconds=7200,
            funnel_attempts_column="funnel_attempts",
            funnel_attempts_complete_column="funnel_attempts_complete",
            funnel_attempts_reason_column="funnel_attempts_reason",
            funnel_fields=("start_time", "matched_event_ids", "status"),
            identity_kind="configured",
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


def test_array_sequence_transition_references_survive_arrow_roundtrip():
    from terrine.data import SequenceMeta
    from terrine.windows import Window

    sequence_meta = SequenceMeta(
        event="signup",
        window=Window(0, 3600),
        max_events=10,
        representation="arrays",
        event_fields=("event_values",),
        event_id_column=None,
        event_time_column=None,
        position_column=None,
        event_count_column="event_count",
        complete_column="sequence_complete",
        reason_column="sequence_reason",
        property_reason_columns={},
        label_property="kind",
        session_gap_seconds=1800,
        session_fields=("sessions",),
        sessions_column="sessions",
        sessions_complete_column="sessions_complete",
        sessions_reason_column="sessions_reason",
        transition_from_column="event_values.transition_from",
        transition_to_column="event_values.transition_to",
    )
    provenance = replace(
        _provenance(),
        shape="event_sequence",
        sequence_meta=sequence_meta,
    )
    frame = MaterializedFrame(
        pa.table(
            {
                "unit": ["a"],
                "event_values": [[{"transition_from": None, "transition_to": "signup"}]],
            }
        ),
        Roles("unit", "anchor_id", "t0", None, (), ()),
        {},
        provenance,
    )
    restored = MaterializedFrame.from_arrow(frame.to_arrow())
    restored_meta = restored.provenance.sequence_meta
    assert restored_meta is not None
    assert restored_meta.transition_from_column == "event_values.transition_from"
    assert restored_meta.transition_to_column == "event_values.transition_to"


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
        engine_name="duckdb",
        dialect="duckdb",
        engine_status="verified",
    )
    generations["events"] = "later"
    feature_cutoffs.clear()
    assert provenance.source_generations["events"] is None
    assert "revenue" in provenance.feature_cutoffs
    with pytest.raises(TypeError):
        provenance.source_generations["events"] = "changed"  # ty: ignore[invalid-assignment] intentional mutation


def test_dataset_view_resolved_instant_round_trips_json_and_arrow():
    from terrine.data import DatasetViewMeta

    provenance = replace(
        _provenance(),
        dataset_view=DatasetViewMeta(
            "scoring",
            ("revenue",),
            (),
            "view",
            "features",
            resolved_as_of=datetime(2025, 1, 1, tzinfo=UTC),
        ),
    )
    frame = _frame(provenance)
    assert (
        MaterializedFrame.from_json(frame.to_json()).provenance.dataset_view
        == provenance.dataset_view
    )
    assert (
        MaterializedFrame.from_arrow(frame.to_arrow()).provenance.dataset_view
        == provenance.dataset_view
    )


@_STAMPS
def test_materialized_frame_metadata_json_roundtrip_preserves_arrow_values(stamp):
    from terrine.windows import Window

    table = pa.table(
        {
            "unit": pa.array(["a", "b"], type=pa.string()),
            "t0": pa.array(
                [datetime(2025, 1, 1, tzinfo=UTC)] * 2, type=pa.timestamp("us", tz="UTC")
            ),
            "treated": pa.array([True, None], type=pa.bool_()),
            "amount": pa.array([Decimal("12.3400"), None], type=pa.decimal128(12, 4)),
            "spend": pa.array([0.30000000000000004, None], type=pa.float64()),
        }
    )
    frame = MaterializedFrame(
        table=table,
        roles=Roles("unit", "anchor", "t0", "treated", ("spend",), ("amount",)),
        columns={
            "amount": ColumnMeta(
                None,
                "outcome",
                None,
                None,
                "amount_complete",
                "amount_reason",
                approximation=ApproximationMeta(
                    algorithm="hll",
                    implementation="duckdb",
                    version="1.2.0",
                    parameters={"precision": 12},
                    error_description="HLL relative standard error ≈ 1.04/√m",
                ),
            ),
            "spend": ColumnMeta(
                Window(-86400, 0),
                "feature",
                "spend_observed_from",
                "spend_observed_until",
                "spend_complete",
                "spend_reason",
            ),
        },
        provenance=_provenance(stamp),
    )
    restored = MaterializedFrame.from_json(frame.to_json())
    assert restored.roles == frame.roles
    assert restored.columns == frame.columns
    assert MaterializedFrame.from_arrow(frame.to_arrow()).columns == frame.columns
    assert restored.provenance == frame.provenance
    assert MaterializedFrame.from_arrow(frame.to_arrow()).provenance == frame.provenance
    assert (
        restored.provenance.engine_name,
        restored.provenance.dialect,
        restored.provenance.engine_status,
    ) == stamp
    assert restored.table.schema == table.schema
    assert restored.table.column("amount").to_pylist() == [Decimal("12.3400"), None]
    assert restored.table.column("treated").to_pylist() == [True, None]
    assert json.loads(frame.to_json())["roles"]["unit"] == "unit"
    payload = json.loads(frame.to_json())
    # A float window column is published rounded once, with no precision policy
    # and no companion error column in the saved metadata.
    assert restored.columns["spend"].numeric_policy is None
    assert set(payload["columns"]["spend"]) == {
        "window",
        "role",
        "observed_from_column",
        "observed_until_column",
        "complete_column",
        "reason_column",
        "numeric_policy",
        "approximation",
        "transformation",
    }
    payload["provenance"]["unknown_policy"] = True
    with pytest.raises(ValueError):
        MaterializedFrame.from_json(json.dumps(payload))


def test_materialized_frame_arrow_schema_metadata_can_be_recovered():
    frame = _frame(_provenance())
    recovered = MaterializedFrame.from_arrow(frame.to_arrow())
    assert recovered.columns == frame.columns
    assert recovered.roles == frame.roles
    assert recovered.provenance == frame.provenance
    assert recovered.table.schema.metadata == frame.table.schema.metadata
    encoded = frame.to_arrow().schema.metadata[b"terrine.materialized_frame.v2"]
    assert json.loads(encoded)["version"] == 2


def test_pre_change_arrow_metadata_refuses_with_portable_version_error():
    import copy
    import pickle
    from importlib.resources import files

    raw = files("tests.fixtures").joinpath("materialized_frame_v1.json").read_bytes()
    table = pa.table({"unit": ["u"], "anchor": ["a"]}).replace_schema_metadata(
        {b"terrine.materialized_frame.v1": raw}
    )
    with pytest.raises(TerrineError) as error:
        MaterializedFrame.from_arrow(table)
    for restored in (
        error.value,
        copy.deepcopy(error.value),
        pickle.loads(pickle.dumps(error.value)),
    ):
        assert restored.code == "terrine.metadata.unsupported_version"
        assert restored.context["version"] == 1
        with pytest.raises(TypeError):
            restored.context["version"] = 2  # ty: ignore[invalid-assignment] intentional mutation


def test_engine_stamp_round_trips_and_historic_verified_metadata():
    from importlib.resources import files

    frame = _frame(_provenance(_UNVERIFIED_STAMP))
    json_payload = json.loads(frame.to_json())
    arrow_payload = json.loads(
        frame.to_arrow().schema.metadata[b"terrine.materialized_frame.v2"].decode("utf-8")
    )
    assert json_payload["version"] == 1
    assert arrow_payload["version"] == 2
    for restored in (
        MaterializedFrame.from_json(frame.to_json()),
        MaterializedFrame.from_arrow(frame.to_arrow()),
    ):
        assert restored.provenance.engine_name == "sqlite"
        assert restored.provenance.dialect == "generic"
        assert restored.provenance.engine_status == "unverified"
        assert restored.provenance == frame.provenance
        assert restored.table.equals(frame.table)

    # Saved before result stamps existed: its provenance records only the verified
    # execution dialect, which decodes as that engine without re-materialization.
    saved = json.loads(files("tests.fixtures").joinpath("materialized_frame_v1.json").read_bytes())
    assert not {"engine_name", "dialect", "engine_status"} & set(saved["provenance"])
    arrow_envelope = json.dumps({**saved, "version": 2, "bookkeeping_columns": []}).encode("utf-8")
    for historical in (
        MaterializedFrame.from_json(json.dumps({**saved, "arrow_ipc": json_payload["arrow_ipc"]})),
        MaterializedFrame.from_arrow(
            frame.table.replace_schema_metadata({b"terrine.materialized_frame.v2": arrow_envelope})
        ),
    ):
        assert historical.provenance.engine_name == "duckdb"
        assert historical.provenance.dialect == "duckdb"
        assert historical.provenance.engine_status == "verified"
        assert historical.provenance.execution_snapshot.backend == "duckdb"
        assert historical.roles == frame.roles
        assert historical.table.equals(frame.table)


_ALL_STAMP_FIELDS = ("dialect", "engine_name", "engine_status")


@pytest.mark.parametrize(
    ("stamp", "backend", "reason", "fields"),
    [
        (
            {"dialect": "generic", "engine_status": "unverified"},
            "sqlite",
            "partial",
            ("dialect", "engine_status"),
        ),
        (
            {"engine_name": "sqlite", "engine_status": "unverified"},
            "sqlite",
            "partial",
            ("engine_name", "engine_status"),
        ),
        (
            {"engine_name": "sqlite", "dialect": "generic"},
            "sqlite",
            "partial",
            ("dialect", "engine_name"),
        ),
        ({"engine_name": "duckdb"}, "duckdb", "partial", ("engine_name",)),
        (
            {"engine_name": "sqlite", "dialect": "generic", "engine_status": "verified"},
            "sqlite",
            "inconsistent",
            _ALL_STAMP_FIELDS,
        ),
        (
            {"engine_name": "duckdb", "dialect": "postgres", "engine_status": "verified"},
            "duckdb",
            "inconsistent",
            _ALL_STAMP_FIELDS,
        ),
        (
            {"engine_name": "duckdb", "dialect": "duckdb", "engine_status": "trusted"},
            "duckdb",
            "inconsistent",
            _ALL_STAMP_FIELDS,
        ),
        (
            {"engine_name": "duckdb", "dialect": "duckdb", "engine_status": "verified"},
            "sqlite",
            "inconsistent",
            _ALL_STAMP_FIELDS,
        ),
        (
            {"engine_name": "sqlite", "dialect": "generic", "engine_status": "unverified"},
            "duckdb",
            "inconsistent",
            _ALL_STAMP_FIELDS,
        ),
        ({}, "arrow", "unknown_legacy_engine", ()),
    ],
    ids=[
        "unverified_without_engine_name",
        "unverified_without_dialect",
        "unverified_without_status",
        "verified_name_only",
        "generic_claims_verified",
        "registered_name_dialect_mismatch",
        "invalid_status",
        "verified_stamp_contradicts_execution_backend",
        "generic_stamp_contradicts_execution_backend",
        "absent_unknown_legacy_engine",
    ],
)
def test_partial_or_inconsistent_engine_stamp_refuses(stamp, backend, reason, fields):
    import copy
    import pickle

    frame = _frame(_provenance())
    payload = frame.to_metadata()
    for name in _ALL_STAMP_FIELDS:
        del payload["provenance"][name]
    payload["provenance"].update(stamp)
    payload["provenance"]["execution_snapshot"]["backend"] = backend
    serialized = json.dumps({**payload, "arrow_ipc": json.loads(frame.to_json())["arrow_ipc"]})
    arrow = frame.table.replace_schema_metadata(
        {
            b"terrine.materialized_frame.v2": json.dumps(
                {**payload, "version": 2, "bookkeeping_columns": []}
            ).encode("utf-8")
        }
    )
    for decode in (
        lambda: MaterializedFrame.from_json(serialized),
        lambda: MaterializedFrame.from_arrow(arrow),
    ):
        with pytest.raises(TerrineError) as error:
            decode()
        for restored in (
            error.value,
            copy.deepcopy(error.value),
            pickle.loads(pickle.dumps(error.value)),
        ):
            assert restored.code == "terrine.metadata.invalid_engine_stamp"
            assert restored.context["reason"] == reason
            assert restored.context["fields"] == fields
            assert "re-materialize" in str(restored)
            with pytest.raises(TypeError):
                restored.context["reason"] = "partial"  # ty: ignore[invalid-assignment] intentional mutation
    if len(stamp) == len(_ALL_STAMP_FIELDS):
        with pytest.raises(TerrineError) as constructed:
            replace(
                _provenance(),
                execution_snapshot=ExecutionSnapshot(None, backend, {}, None, "unverified"),
                engine_name=stamp["engine_name"],
                dialect=stamp["dialect"],
                engine_status=stamp["engine_status"],
            )
        assert constructed.value.code == "terrine.metadata.invalid_engine_stamp"
        assert constructed.value.context["reason"] == "inconsistent"
        assert constructed.value.context["fields"] == _ALL_STAMP_FIELDS


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
    for field in ("max_rows", "max_output_bytes", "max_scan_bytes"):
        for value in (-1, True, 1.0):
            with pytest.raises(TerrineError) as error:
                Budget(**{field: value})  # ty: ignore[invalid-argument-type] intentional invalid inputs
            assert error.value.code == "terrine.budget.invalid_limit"
            assert error.value.context == {"reason": "not_nonnegative_integer", "field": field}
    with pytest.raises((TypeError, ValueError)):
        Preview(-1, None, None, None, {}, {}, ())


def test_unit_source_requires_named_table_and_key():
    for field, arguments in (("key", {"key": ""}), ("entered_at", {"entered_at": ""})):
        with pytest.raises(TerrineError) as error:
            UnitSource(**{"table": "users", "key": "user_id", **arguments})
        assert error.value.code == "terrine.model.invalid_binding"
        assert error.value.context == {"reason": "empty_name", "field": field}


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
