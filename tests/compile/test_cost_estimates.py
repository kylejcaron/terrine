# ruff: noqa: F811
import pytest

from terrine import Budget, Design, FixedDate, agg, preview
from terrine.errors import TerrineError
from tests.compile.test_columns import column_model  # noqa: F401


def test_compiler_preview_preserves_unknowns_without_source_execution(column_model, monkeypatch):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1s", "0s"))},
    )
    reads = []

    def forbidden(*args, **kwargs):
        reads.append(True)
        raise AssertionError("metadata preview must not execute source rows")

    monkeypatch.setattr(column_model.con, "to_pyarrow", forbidden)
    report = preview(design, column_model, budget=Budget(max_scan_bytes=1))
    assert report.scan_bytes is None and report.output_bytes is None and report.planned_rows is None
    assert report.scan_reason == "backend_scan_bound_unavailable"
    assert report.budget_warnings
    with pytest.raises(TerrineError) as error:
        preview(design, column_model, budget=Budget(max_scan_bytes=1), bounded_count=1)
    assert error.value.code == "terrine.budget.scan_unknown"
    assert reads == []
