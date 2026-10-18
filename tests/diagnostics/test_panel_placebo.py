from datetime import UTC, datetime

import pyarrow as pa
import pytest

from terrine.data import ExecutionSnapshot, MaterializedFrame, Provenance, Roles
from terrine.diagnostics import negative_control_test, placebo_test


def _frame(*, include_negative=True, planted=False):
    rows = []
    for unit in range(40):
        treated = unit < 20
        for period in (-2, -1, 0):
            rows.append(
                {
                    "unit": unit,
                    "anchor_id": (unit, period),
                    "t0": datetime(2025, 1, 1, tzinfo=UTC),
                    "treatment": treated,
                    "event_period": period,
                    "y": float(treated and planted and period == -2),
                    **({"negative": float(treated)} if include_negative else {}),
                }
            )
    return MaterializedFrame(
        pa.Table.from_pylist(rows),
        Roles("unit", "anchor_id", "t0", "treatment", (), ("y",), cluster_unit="unit"),
        {},
        Provenance(
            {},
            {},
            "design",
            "model",
            {},
            ExecutionSnapshot(None, "arrow", {}, None, "unverified"),
            "calendar_comparison",
            None,
        ),
    )


def test_placebo_effect_is_detected_when_planted():
    result = placebo_test(_frame(planted=True), outcome="y", placebo_periods=(-2,))
    assert result.status == "warn"
    assert result.metrics["n_periods"] == 1
    assert result.metrics["max_abs_effect"] == pytest.approx(1.0)
    assert result.metrics["any_reject"] is True
    assert (
        result.context["interpretation"]
        == "placebo_evidence_does_not_authorize_causal_interpretation"
    )


def test_missing_negative_control_is_unavailable_not_pass():
    result = negative_control_test(
        _frame(include_negative=False), outcome="y", negative_control="negative"
    )
    assert result.status == "unavailable"
    assert result.context["reason"] == "missing_negative_control"
