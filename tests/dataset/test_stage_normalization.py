from __future__ import annotations

import pyarrow as pa

from terrine.data import Budget, MaterializedFrame
from terrine.spec import BinnedMetricState, Design, Schedule, StatePanel, agg
from tests.dataset.test_staging import _durable_frame


def test_state_panel_stage_matches_ordinary_normalization_and_support(events_world, tmp_path):
    base, con = _durable_frame(events_world, tmp_path)
    frame = (
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-05"),
            shape=StatePanel(
                BinnedMetricState("sessions", (0,), ("none", "some")),
                max_periods=2,
                baseline_features={"baseline": agg("sessions", window=("-1d", "0s"))},
            ),
        )
        .build(base._model)
        .training_set(
            label="to_state", features=("baseline", "from_state"), incomplete_labels="keep"
        )
    )
    budget = Budget(max_rows=100, max_output_bytes=100_000, strict_scan=False)
    ordinary = frame.materialize(budget)
    staged = frame.stage("periods", budget=budget)
    batches = list(staged.to_arrow_batches(batch_size=1))
    actual = MaterializedFrame.from_arrow(pa.Table.from_batches(batches))
    assert actual.table.schema.field("gap").type == pa.duration("us")
    order = [("unit", "ascending"), ("period_index", "ascending")]
    assert actual.table.sort_by(order).equals(ordinary.table.sort_by(order))
    assert actual.provenance.state_panel == ordinary.provenance.state_panel
    assert actual.provenance.dataset_view == ordinary.provenance.dataset_view
    assert actual.provenance.state_panel is not None
    assert actual.provenance.state_panel.period_column in actual.table.column_names
    con.disconnect()


def test_stage_never_collects_full_output_through_snapshot_execute(
    events_world, tmp_path, monkeypatch
):
    from terrine.execution.inputs import ExecutionContext

    original = ExecutionContext.execute

    def execute(context, relation):
        if {"unit", "t0", "sessions", "churn"}.issubset(relation.columns):
            raise AssertionError("full output must cross the bounded Arrow boundary")
        return original(context, relation)

    monkeypatch.setattr(ExecutionContext, "execute", execute)
    frame, con = _durable_frame(events_world, tmp_path)
    staged = frame.stage("bounded", budget=Budget(strict_scan=False))
    iterator = staged.to_arrow_batches(batch_size=1)
    assert next(iterator).num_rows == 1
    iterator.close()
    assert con.con.execute("SELECT 1").fetchone() == (1,)
    con.disconnect()
