from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pytest

from terrine.data import ColumnMeta, ExpressionTiming, Roles
from terrine.dataset.splits import GroupSplit, TimeSplit, group_bucket
from terrine.dataset.training import assign_training_splits
from terrine.windows import Window


def _compiled(label, window, timing):
    return SimpleNamespace(
        roles=Roles("unit", "anchor_id", "t0", None, (), (label,)),
        columns={label: ColumnMeta(window, "outcome", None, None, "complete", "reason")},
        expression_timings={label: timing},
    )


def test_relative_label_support_includes_selector_origin_and_search_interval():
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    table = pa.table(
        {
            "unit": ["a"],
            "t0": [origin],
            "label": [1],
            "complete": [True],
            "reason": [None],
            "label_end": [origin + timedelta(days=5)],
            "selector_time": [origin + timedelta(days=6)],
        }
    )
    timing = ExpressionTiming(
        "event_time",
        "selector",
        (-86400, -86400),
        (0, 864000),
        ("selector",),
        {"selector": (0, 864000)},
        "selector_time",
        None,
        "label_end",
    )
    result, counts = assign_training_splits(
        table,
        compiled=_compiled("label", Window("-2d", "-1d"), timing),
        labels=("label",),
        policy=TimeSplit(train_until=origin + timedelta(days=5, hours=12)),
        binned_state=False,
    )
    assert result["__terrine_split"].to_pylist() == ["unassigned"]
    assert counts == {"label_overlap": 1}


def test_cross_section_names_do_not_select_state_panel_endpoint():
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    table = pa.table(
        {
            "unit": ["a"],
            "t0": [origin],
            "to_state": [1],
            "complete": [True],
            "reason": [None],
            "to_time": [origin - timedelta(days=1)],
        }
    )
    timing = ExpressionTiming("fixed_t0", None, None, (0, 604800), (), {}, "t0", None, None)
    result, counts = assign_training_splits(
        table,
        compiled=_compiled("to_state", Window("0s", "7d"), timing),
        labels=("to_state",),
        policy=TimeSplit(train_until=origin + timedelta(days=4)),
        binned_state=False,
    )
    assert result["__terrine_split"].to_pylist() == ["unassigned"]
    assert counts == {"label_overlap": 1}


@pytest.mark.parametrize("temporal", [False, True])
def test_complete_fixed_asof_label_is_available_for_split_assignment(temporal):
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    table = pa.table(
        {"unit": ["a"], "t0": [origin], "label": ["gold"], "complete": [True], "reason": [None]}
    )
    timing = ExpressionTiming("fixed_t0", None, None, (0, 0), (), {}, "t0", None, None)
    policy = TimeSplit(train_until=origin + timedelta(days=1)) if temporal else GroupSplit(seed=17)
    result, counts = assign_training_splits(
        table,
        compiled=_compiled("label", None, timing),
        labels=("label",),
        policy=policy,
        binned_state=False,
    )
    expected = group_bucket("a", policy) if isinstance(policy, GroupSplit) else "train"
    assert result["__terrine_split"].to_pylist() == [expected]
    assert counts == {}
