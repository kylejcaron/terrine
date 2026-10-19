from dataclasses import replace

import pyarrow as pa
import pytest

from terrine.data import EligibilitySummary
from terrine.errors import TerrineError
from terrine.export.preflight import preflight_export


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


def test_grace_defined_baseline_treatment_has_shared_refusal(frame):
    candidate = replace(frame, provenance=replace(frame.provenance, treatment_design="grace_initiation"))
    with pytest.raises(TerrineError) as error:
        preflight_export(candidate)
    assert error.value.code == "terrine.export.grace_baseline_treatment"
