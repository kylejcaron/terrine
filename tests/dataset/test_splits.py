from datetime import UTC, date, datetime
from typing import Any, cast
from zoneinfo import ZoneInfo

import pytest

from terrine.dataset.splits import (
    GroupSplit,
    TimeSplit,
    assign_group_splits,
    assign_splits,
    group_bucket,
    reason_counts,
)


def test_group_split_typed_keys_are_order_independent_and_never_alias() -> None:
    policy = GroupSplit(seed=3)
    assert group_bucket(7, policy) == group_bucket(7, policy)
    assert group_bucket(7, policy) != group_bucket("7", policy)
    assert group_bucket("unit-1", policy) == group_bucket("unit-1", policy)


def test_repeated_unit_keys_keep_one_group_bucket_when_row_order_changes() -> None:
    policy = GroupSplit(seed=17)
    rows = [{"id": "a", "unit": "same"}, {"id": "b", "unit": "same"}]
    first = assign_group_splits(rows, policy)
    assert first == assign_group_splits(list(reversed(rows)), policy)
    assert first["a"].split == first["b"].split


def test_time_split_purges_overlapping_labels_and_respects_point_vs_interval_equality() -> None:
    policy = TimeSplit(train_until=date(2025, 2, 1), validation_until=date(2025, 3, 1))
    rows = [
        {
            "id": "interval",
            "origin": datetime(2025, 1, 1, tzinfo=UTC),
            "label_end": datetime(2025, 2, 1, tzinfo=UTC),
        },
        {
            "id": "point",
            "origin": datetime(2025, 1, 2, tzinfo=UTC),
            "label_end": datetime(2025, 2, 1, tzinfo=UTC),
            "point_label": True,
        },
    ]
    result = assign_splits(rows, policy, catalog_timezone=UTC)
    assert result["interval"].split == "train"
    assert result["point"].split == "unassigned"
    assert result["point"].reason == "label_overlap"


def test_time_split_purges_fifty_six_day_label_horizon_without_reassignment() -> None:
    policy = TimeSplit(train_until="2025-03-01", validation_until="2025-05-01")
    row = {
        "id": "overlap",
        "origin": datetime(2025, 1, 4, tzinfo=UTC),
        "label_end": datetime(2025, 3, 1, tzinfo=UTC),
    }
    assert assign_splits([row], policy, catalog_timezone=UTC)["overlap"].split == "train"
    overlap = dict(row, label_end=datetime(2025, 3, 2, tzinfo=UTC))
    assignment = assign_splits([overlap], policy, catalog_timezone=UTC)["overlap"]
    assert assignment.split == "unassigned"
    assert assignment.reason == "label_overlap"


def test_time_split_embargo_exclusions_have_reason_counts() -> None:
    policy = TimeSplit(train_until="2025-02-01", validation_until="2025-03-01", embargo="2d")
    rows = [{"id": "gap", "origin": datetime(2025, 2, 2, tzinfo=UTC), "label_end": None}]
    result = assign_splits(rows, policy, catalog_timezone=UTC)
    assert result["gap"].split == "unassigned"
    assert reason_counts(result) == {"embargo": 1}


def test_group_split_rejects_ambiguous_keys_and_invalid_fractions() -> None:
    policy = GroupSplit()
    for key in (True, None, 1.0):
        with pytest.raises(TypeError):
            group_bucket(cast(Any, key), policy)
    with pytest.raises(ValueError):
        GroupSplit(train_fraction=0.0)
    with pytest.raises(ValueError):
        GroupSplit(seed=True)


def test_combined_split_keeps_group_mismatches_unassigned() -> None:
    policy = TimeSplit(
        train_until="2025-02-01",
        groups=GroupSplit(train_fraction=0.5, validation_fraction=0, seed=4),
    )
    assert policy.groups is not None
    key = next(value for value in range(100) if group_bucket(value, policy.groups) != "train")
    row = {
        "id": "x",
        "origin": datetime(2025, 1, 1, tzinfo=UTC),
        "label_end": datetime(2025, 1, 2, tzinfo=UTC),
        "unit": key,
    }
    assignment = assign_splits([row], policy, catalog_timezone=UTC)["x"]
    assert assignment.split == "unassigned"
    assert assignment.reason == "split_mismatch"


def test_time_split_retains_unavailable_and_known_null_label_reasons() -> None:
    policy = TimeSplit(train_until="2025-02-01")
    rows = [
        {
            "id": "missing",
            "origin": datetime(2025, 1, 1, tzinfo=UTC),
            "label_end": None,
            "label_reason": "missing_coverage",
        },
        {
            "id": "known_null",
            "origin": datetime(2025, 1, 2, tzinfo=UTC),
            "label_end": datetime(2025, 1, 2, tzinfo=UTC),
            "label_available": False,
            "label_reason": "known_null",
        },
    ]
    result = assign_splits(rows, policy, catalog_timezone=UTC)
    assert result["missing"].split == "unassigned"
    assert result["missing"].reason == "missing_coverage"
    assert result["known_null"].split == "unassigned"
    assert result["known_null"].reason == "known_null"


def test_calendar_boundary_uses_catalog_local_day_and_preserves_date_until_binding() -> None:
    policy = TimeSplit(train_until="2025-02-01")
    bound = policy.bind(ZoneInfo("America/New_York"))
    assert bound.train_until == datetime(2025, 2, 1, 5, tzinfo=UTC)
    row = {
        "id": "local",
        "origin": datetime(2025, 2, 1, 2, tzinfo=UTC),
        "label_end": datetime(2025, 2, 1, 4, tzinfo=UTC),
    }
    assert assign_splits([row], bound)["local"].split == "train"


def test_submicrosecond_split_timestamps_are_rejected() -> None:
    with pytest.raises(ValueError):
        TimeSplit(train_until="2025-02-01T00:00:00.000000001Z")
    policy = TimeSplit(train_until="2025-02-01T00:00:00Z")
    row = {
        "id": "precision",
        "origin": datetime(2025, 1, 1, tzinfo=UTC),
        "label_end": "2025-02-01T00:00:00.000000001Z",
    }
    with pytest.raises(ValueError):
        assign_splits([row], policy)


def test_mixed_calendar_and_instant_boundaries_order_after_catalog_binding() -> None:
    policy = TimeSplit(train_until="2025-02-01T02:00:00Z", validation_until="2025-02-01")
    bound = policy.bind(ZoneInfo("America/New_York"))
    assert bound.validation_until == datetime(2025, 2, 1, 5, tzinfo=UTC)
    row = {
        "id": "mixed",
        "origin": datetime(2025, 2, 1, 3, tzinfo=UTC),
        "label_end": datetime(2025, 2, 1, 4, tzinfo=UTC),
    }
    assert assign_splits([row], bound)["mixed"].split == "validation"
