from datetime import UTC, datetime, timedelta, timezone

from terrine.compile.support import WindowUse, _shared_input_span


def test_monthly_shared_read_hull_keeps_both_half_open_boundaries():
    first = datetime(2025, 2, 1, tzinfo=UTC)
    last = datetime(2025, 3, 1, tzinfo=UTC)
    trailing = WindowUse("orders", "customer", -30 * 86400, 0)
    following = WindowUse("orders", "customer", 0, 30 * 86400)

    start = datetime(2025, 1, 2, tzinfo=UTC)
    end = datetime(2025, 3, 31, tzinfo=UTC)
    assert trailing.input_span(first, last) == (start, last)
    assert _shared_input_span((trailing, following), (first, last)) == (start, end)
    assert following.input_span(first, last) == (first, end)
    assert trailing.contains_change(first, instant=start)
    assert not following.contains_change(last, instant=end)


def test_reverse_point_and_interval_support_match_enumerated_membership():
    origin = datetime(2025, 2, 1, tzinfo=UTC)
    instants = [origin + timedelta(days=i) for i in range(34)]
    for use in (
        WindowUse("orders", "customer", -30 * 86400, 0),
        WindowUse("visits", "site", 0, 86400),
    ):
        for a, anchor in enumerate(instants):
            # A 30-day trailing use consumes the preceding 30 slots; a one-day
            # forward use consumes the slot starting at the anchor.
            selected = set(instants[max(0, a - 30) : a]) if use.source == "orders" else {anchor}
            for i, instant in enumerate(instants):
                assert use.contains_change(anchor, instant=instant) == (instant in selected)
                for j in range(i, len(instants)):
                    assert use.intersects_change(anchor, start=instant, end=instants[j]) == bool(
                        selected.intersection(instants[i:j])
                    )


def test_unknown_or_unrepresentable_support_stays_conservative():
    use = WindowUse("orders", "customer", -86400, 0)
    ordinary = datetime(2025, 1, 2, tzinfo=timezone(timedelta(hours=-5)))
    early = datetime.min.replace(tzinfo=UTC)
    assert use.input_span(ordinary, ordinary) == (
        datetime(2025, 1, 1, 5, tzinfo=UTC),
        datetime(2025, 1, 2, 5, tzinfo=UTC),
    )
    assert use.input_span(early, early) is None
    assert use.input_span(None, ordinary) is None
    assert use.contains_change(early, instant=ordinary)
    assert use.contains_change(ordinary, instant=ordinary.replace(tzinfo=None))
    assert use.intersects_change(ordinary, start=None, end=ordinary)
    assert use.intersects_change(early, start=ordinary, end=ordinary + timedelta(seconds=1))
    assert not use.intersects_change(early, start=ordinary, end=ordinary)
