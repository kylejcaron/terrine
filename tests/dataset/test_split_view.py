from __future__ import annotations

import pyarrow as pa

from terrine.data import Budget, MaterializedFrame
from terrine.dataset.splits import GroupSplit, TimeSplit, group_bucket
from terrine.spec import Design, EventAnchor, agg
from tests.dataset.test_staging import _durable_frame


def test_training_split_assigns_support_without_reassigning_overlap(native_inputs, tmp_path):
    initial, con = _durable_frame(native_inputs, tmp_path)
    base = Design(
        "user",
        EventAnchor("session"),
        features={"sessions": agg("sessions", window=("-1d", "0s"))},
        outcomes={"churn": agg("churned", window=("0s", "1d"))},
    ).build(initial._model)
    training = base.training_set(
        label="churn",
        incomplete_labels="keep",
        split=TimeSplit(train_until="2025-01-03T12:00:00Z"),
    )
    ordinary = training.materialize(Budget(strict_scan=False))
    assert ordinary.table["__terrine_split"].to_pylist() == ["unassigned"] * 2
    assert ordinary.table["__terrine_split_reason"].to_pylist() == ["label_overlap"] * 2
    assert ordinary.provenance.dataset_view is not None
    assert ordinary.provenance.dataset_view.counts_by_reason == {"label_overlap": 2}
    staged = training.stage("training", budget=Budget(strict_scan=False))
    actual = MaterializedFrame.from_arrow(
        pa.Table.from_batches(list(staged.to_arrow_batches(batch_size=1)))
    )
    assert actual.provenance.dataset_view == ordinary.provenance.dataset_view
    assert actual.table.sort_by("unit").equals(ordinary.table.sort_by("unit"))
    assert staged.manifest.frame_metadata["provenance"]["dataset_view"]["counts_by_reason"] == {
        "label_overlap": 2
    }
    con.disconnect()


def test_group_split_view_matches_typed_reference_and_reconstruction(native_inputs, tmp_path):
    initial, con = _durable_frame(native_inputs, tmp_path)
    policy = GroupSplit(seed=17)
    training = initial.training_set(label="churn", incomplete_labels="keep", split=policy)
    result = training.materialize(Budget(strict_scan=False))
    assert result.table["__terrine_split"].to_pylist() == [
        group_bucket(key, policy) for key in result.table["unit"].to_pylist()
    ]
    staged = training.stage("groups", budget=Budget(strict_scan=False))
    rebuilt = staged.manifest.recipe.build(initial._model).materialize(Budget(strict_scan=False))
    assert rebuilt.table.sort_by("unit").equals(result.table.sort_by("unit"))
    con.disconnect()
