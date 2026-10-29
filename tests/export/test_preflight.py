from dataclasses import replace

import pyarrow as pa
import pytest

from terrine.data import EligibilitySummary
from terrine.errors import TerrineError
from terrine.export.preflight import preflight_export


@pytest.mark.parametrize(
    ("data", "consumer", "context"),
    [
        (None, None, {"reason": "wrong_type", "field": "data"}),
        ("frame", "pandas", {"reason": "unsupported_value", "field": "consumer"}),
    ],
)
def test_preflight_inputs_refuse_with_codes(frame, data, consumer, context):
    with pytest.raises(TerrineError) as error:
        preflight_export(frame if data == "frame" else data, consumer=consumer)
    assert error.value.code == "terrine.export.invalid_input"
    assert {key: error.value.context[key] for key in context} == context


def test_incomplete_outcomes_refuse_by_default(frame):
    table = frame.table.set_column(
        frame.table.schema.get_field_index("complete"), "complete", pa.array([False, True])
    ).set_column(
        frame.table.schema.get_field_index("reason"),
        "reason",
        pa.array(["incomplete_coverage", None], type=pa.string()),
    )
    with pytest.raises(TerrineError) as error:
        preflight_export(replace(frame, table=table))
    assert error.value.code == "terrine.export.incomplete_outcome"


def test_explicit_drop_reports_exact_null_reasons(frame):
    table = frame.table.set_column(
        frame.table.schema.get_field_index("complete"), "complete", pa.array([False, False])
    ).set_column(
        frame.table.schema.get_field_index("reason"),
        "reason",
        pa.array(["incomplete_coverage", "incomplete_observation"]),
    )
    result = preflight_export(replace(frame, table=table), drop_incomplete=True)
    assert result.table.num_rows == 0
    assert result.excluded_counts == {"incomplete_coverage": 1, "incomplete_observation": 1}


def test_unknown_eligibility_has_shared_refusal(frame):
    candidate = replace(
        frame,
        provenance=replace(
            frame.provenance,
            eligibility_cutoffs={"eligible": frame.provenance.feature_cutoffs["x"]},
            eligibility_summary=EligibilitySummary(2, 0, 1, 1),
        ),
    )
    with pytest.raises(TerrineError) as error:
        preflight_export(candidate)
    assert error.value.code == "terrine.export.unknown_eligibility"


def test_repeated_independent_units_require_cluster_column(frame):
    table = frame.table.take(pa.array([0, 0]))
    with pytest.raises(TerrineError) as error:
        preflight_export(replace(frame, table=table))
    assert error.value.code == "terrine.export.cluster_required"


def test_repeated_units_reject_missing_declared_cluster_column(frame):
    table = frame.table.take(pa.array([0, 0]))
    roles = replace(frame.roles, cluster_unit="cluster_id")
    with pytest.raises(TerrineError) as error:
        preflight_export(replace(frame, table=table, roles=roles))
    assert error.value.code == "terrine.export.cluster_required"


def test_grace_defined_baseline_treatment_has_shared_refusal(frame):
    candidate = replace(
        frame, provenance=replace(frame.provenance, treatment_design="grace_initiation")
    )
    with pytest.raises(TerrineError) as error:
        preflight_export(candidate)
    assert error.value.code == "terrine.export.grace_baseline_treatment"


@pytest.mark.parametrize("weight_role", ["weight", "target_weight", "trial_weight"])
def test_dowhy_refuses_declared_weights_even_if_column_missing(frame, weight_role):
    candidate = replace(frame, roles=replace(frame.roles, **{weight_role: "w"}))
    with pytest.raises(TerrineError) as error:
        preflight_export(candidate, consumer="dowhy")
    assert error.value.code == "terrine.export.unsupported_weights"
    assert error.value.context["consumer"] == "dowhy"


def test_dowhy_refuses_repeated_units_even_with_clusters(frame):
    candidate = replace(
        frame, table=frame.table.take([0, 0]), roles=replace(frame.roles, cluster_unit="id")
    )
    with pytest.raises(TerrineError) as error:
        preflight_export(candidate, consumer="dowhy")
    assert error.value.code == "terrine.export.repeated_units_unsupported"


def test_shared_preflight_precedes_consumer_limitations(frame):
    candidate = replace(
        frame,
        roles=replace(frame.roles, weight="missing"),
        provenance=replace(frame.provenance, treatment_design="grace_initiation"),
    )
    with pytest.raises(TerrineError) as error:
        preflight_export(candidate, consumer="dowhy")
    assert error.value.code == "terrine.export.grace_baseline_treatment"


def test_exclusions_count_rows_per_reason_not_duplicate_outcomes(frame):
    candidate = replace(
        frame,
        table=frame.table.set_column(6, "complete", pa.array([False, True])).set_column(
            7, "reason", pa.array(["incomplete_coverage", None])
        ),
        roles=replace(frame.roles, outcomes=("y", "x")),
        columns={**frame.columns, "x": frame.columns["y"]},
    )
    result = preflight_export(candidate, drop_incomplete=True)
    assert result.table["id"].to_pylist() == [2]
    assert result.excluded_counts == {"incomplete_coverage": 1}
