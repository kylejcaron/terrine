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
        roles=Roles(
            unit="unit", anchor="anchor_id", t0="t0", treatment=None, features=(), outcomes=(label,)
        ),
        columns={
            label: ColumnMeta(
                window=window,
                role="outcome",
                observed_from_column=None,
                observed_until_column=None,
                complete_column="complete",
                reason_column="reason",
            )
        },
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
        kind="event_time",
        selector_ref="selector",
        relative_offsets=(-86400, -86400),
        support_seconds=(0, 864000),
        selector_dependencies=("selector",),
        selector_support_seconds={"selector": (0, 864000)},
        point_time_column="selector_time",
        start_bound_column=None,
        end_bound_column="label_end",
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
    timing = ExpressionTiming(
        kind="fixed_t0",
        selector_ref=None,
        relative_offsets=None,
        support_seconds=(0, 604800),
        selector_dependencies=(),
        selector_support_seconds={},
        point_time_column="t0",
        start_bound_column=None,
        end_bound_column=None,
    )
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
    timing = ExpressionTiming(
        kind="fixed_t0",
        selector_ref=None,
        relative_offsets=None,
        support_seconds=(0, 0),
        selector_dependencies=(),
        selector_support_seconds={},
        point_time_column="t0",
        start_bound_column=None,
        end_bound_column=None,
    )
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
