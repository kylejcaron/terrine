from datetime import UTC, datetime, timedelta

from terrine.runtime.changes import Change, ChangeSet, affected_by_change, window_intersects


def test_watermark_advance_is_not_change_detection():
    before = datetime(2025, 1, 1, tzinfo=UTC)
    after = datetime(2025, 2, 1, tzinfo=UTC)
    assert ChangeSet.from_readiness_advance("events", before, after).changes == ()


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
