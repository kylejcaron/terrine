from datetime import UTC, datetime, timedelta

from terrine.runtime.changes import Change, ChangeSet, affected_by_change, window_intersects


def test_watermark_advance_is_not_change_detection():
    before = datetime(2025, 1, 1, tzinfo=UTC)
    after = datetime(2025, 1, 2, tzinfo=UTC)
    change_set = ChangeSet.from_readiness_advance("events", before, after)
    assert len(change_set.changes) == 1
    change = change_set.changes[0]
    assert change.kind == "readiness"
    assert change.source == "events"
    assert change.before_time_range is None
    assert change.after_time_range == (before, after)


def test_readiness_advance_emits_completeness_change_without_row_support():
    before = datetime(2025, 1, 2, tzinfo=UTC)
    after = datetime(2025, 1, 3, tzinfo=UTC)
    change_set = ChangeSet.from_readiness_advance("events", before, after)
    assert len(change_set.changes) == 1
    assert change_set.changes[0].kind == "readiness"


def test_window_boundary_equality_and_event_anchor_identity_invalidation():
    start = datetime(2025, 1, 1, tzinfo=UTC)
    edge = start + timedelta(days=3)
    assert not window_intersects((edge, edge + timedelta(days=1)), (start, edge))
    assert window_intersects((edge - timedelta(seconds=1), edge), (start, edge))
    change = Change("insert", "events", after_time_range=(start, start + timedelta(days=1)))
    assert affected_by_change(change, (start, start + timedelta(days=2)))


def test_update_timestamp_move_invalidates_old_and_new_support():
    t0 = datetime(2025, 1, 1, tzinfo=UTC)
    old, new = (t0, t0 + timedelta(days=1)), (t0 + timedelta(days=5), t0 + timedelta(days=6))
    change = Change("update", "events", before_time_range=old, after_time_range=new)
    assert affected_by_change(change, old)
    assert affected_by_change(change, new)


def test_delete_uses_previous_support():
    t0 = datetime(2025, 1, 1, tzinfo=UTC)
    change = Change("delete", "events", before_time_range=(t0, t0 + timedelta(days=1)))
    assert affected_by_change(change, (t0, t0 + timedelta(days=1)))


def test_key_only_delete_conservatively_invalidates_unknown_time_support():
    change = Change("delete", "events", before_key_range=("unit-a", "unit-a"))
    assert affected_by_change(
        change, (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC))
    )


def test_update_unit_key_move_invalidates_old_and_new_units_conservatively():
    change = Change(
        "update",
        "events",
        before_key_range=("unit-a", "unit-a"),
        after_key_range=("unit-b", "unit-b"),
    )
    window = (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC))
    assert affected_by_change(change, window)


def test_missing_change_support_forces_conservative_rebuild():
    change = Change("restatement", "events")
    window = (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC))
    assert affected_by_change(change, window)


def test_incomplete_update_support_forces_conservative_rebuild():
    start = datetime(2025, 1, 1, tzinfo=UTC)
    change = Change(
        "update",
        "events",
        before_time_range=(start, start + timedelta(days=1)),
    )
    assert affected_by_change(change, (start + timedelta(days=10), start + timedelta(days=11)))


def test_dimension_and_strict_asof_corrections_invalidate_lookup_intervals():
    start = datetime(2025, 1, 1, tzinfo=UTC)
    effective = (start + timedelta(days=2), start + timedelta(days=4))
    lookup = (start + timedelta(days=3), start + timedelta(days=5))
    dimension = Change(
        "update", "dimensions", before_time_range=effective, after_time_range=effective
    )
    strict_asof = Change(
        "update", "prices", before_time_range=effective, after_time_range=effective
    )
    assert affected_by_change(dimension, lookup)
    assert affected_by_change(strict_asof, lookup)


def test_roster_exit_and_readiness_retraction_invalidate_candidates():
    start = datetime(2025, 1, 1, tzinfo=UTC)
    window = (start, start + timedelta(days=3))
    exit_change = Change("roster", "roster", before_time_range=(start, start + timedelta(days=1)))
    readiness = ChangeSet.from_readiness_advance(
        "events", start + timedelta(days=4), start + timedelta(days=2)
    )
    assert affected_by_change(exit_change, window)
    assert affected_by_change(readiness.changes[0], window)


def test_ratio_sources_change_independently():
    start = datetime(2025, 1, 1, tzinfo=UTC)
    window = (start, start + timedelta(days=2))
    left = Change("insert", "numerator", after_time_range=(start, start + timedelta(days=1)))
    right = Change(
        "insert",
        "denominator",
        after_time_range=(start + timedelta(days=4), start + timedelta(days=5)),
    )
    assert affected_by_change(left, window)
    assert not affected_by_change(right, window)
