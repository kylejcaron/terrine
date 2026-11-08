import inspect

import pytest

from terrine.data import (
    ApproximationMeta,
    CalendarComparisonMeta,
    CohortOperationMeta,
    ColumnMeta,
    DatasetViewMeta,
    ExecutionSnapshot,
    ExpressionTiming,
    FeatureCutoff,
    MaterializedFrame,
    PanelMeta,
    Preview,
    Provenance,
    Roles,
    SamplingMeta,
    SequenceMeta,
    StatePanelMeta,
    TreatmentHistoryMeta,
    TrialWeightMeta,
)

_RESULT_RECORDS = (
    Provenance,
    ColumnMeta,
    Roles,
    Preview,
    ExpressionTiming,
    SamplingMeta,
    SequenceMeta,
    StatePanelMeta,
    CalendarComparisonMeta,
    CohortOperationMeta,
    DatasetViewMeta,
    ExecutionSnapshot,
    PanelMeta,
    FeatureCutoff,
    ApproximationMeta,
    TreatmentHistoryMeta,
    TrialWeightMeta,
    MaterializedFrame,
)


@pytest.mark.parametrize("record", _RESULT_RECORDS)
def test_result_records_are_keyword_only(record: type) -> None:
    parameters = inspect.signature(record).parameters.values()

    assert parameters
    assert all(parameter.kind is inspect.Parameter.KEYWORD_ONLY for parameter in parameters)
