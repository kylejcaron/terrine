from __future__ import annotations

import pytest

from terrine.data import Budget
from terrine.dataset.reference import Dataset
from terrine.errors import TerrineError
from terrine.spec import Design, FixedDate, agg
from tests import _worlds
from tests.fixtures.readme import readme_design, readme_model


def _durable_frame(events_world, tmp_path):
    model = _worlds.bind_model(events_world, tmp_path)
    con = model.con
    frame = Design(
        "user",
        FixedDate("2025-01-05"),
        features={"sessions": agg("sessions", window=("-7d", "0s"))},
        outcomes={"churn": agg("churned", window=("0s", "7d"))},
    ).build(model)
    return frame, con


def test_invalid_arrow_batch_size_refuses_before_read():
    dataset = Dataset.__new__(Dataset)
    with pytest.raises(TerrineError) as error:
        dataset.to_arrow_batches(batch_size=0)
    assert error.value.code == "terrine.dataset.invalid_batch_size"


def test_in_memory_model_refuses_to_stage_before_any_capture(monkeypatch):
    from terrine.runtime.snapshot import SnapshotAuthority

    def captured(*args, **kwargs):
        raise AssertionError("snapshot captured")

    monkeypatch.setattr(SnapshotAuthority, "capture", captured)
    frame = readme_design().build(readme_model())
    with pytest.raises(TerrineError) as error:
        frame.stage("spend_forecast", budget=Budget(strict_scan=False))
    assert error.value.code == "terrine.dataset.invalid_destination"
    assert error.value.context["reason"] == "file_backed_connection_required"
    assert "route" in error.value.context
