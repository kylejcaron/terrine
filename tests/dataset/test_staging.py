from __future__ import annotations

import pytest

from terrine.dataset.reference import Dataset
from terrine.spec import Design, FixedDate, agg
from tests import _worlds


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
    from terrine.errors import TerrineError

    dataset = Dataset.__new__(Dataset)
    with pytest.raises(TerrineError) as error:
        dataset.to_arrow_batches(batch_size=0)
    assert error.value.code == "terrine.dataset.invalid_batch_size"
