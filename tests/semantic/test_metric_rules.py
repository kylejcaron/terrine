from datetime import UTC, datetime

import pytest

from terrine.semantic.rules import calendar_average_alignment, supports_event_resolution
from terrine.windows import Window


def test_calendar_average_uses_exact_non_utc_local_midnight():
    boundary = "UTC-05:30"
    aligned = datetime(2025, 3, 1, 5, 30, tzinfo=UTC)
    assert (
        calendar_average_alignment(Window("-2d", "1d"), anchor=aligned, day_boundary=boundary)
        == "aligned"
    )
    assert (
        calendar_average_alignment(
            Window("-2d", "1d"), anchor=aligned.replace(microsecond=1), day_boundary=boundary
        )
        == "unaligned"
    )
    assert (
        calendar_average_alignment(Window("-2d", "1d"), anchor=None, day_boundary=boundary)
        == "runtime_check"
    )


def test_calendar_average_rejects_non_whole_day_domains():
    anchor = datetime(2025, 3, 1, tzinfo=UTC)
    assert (
        calendar_average_alignment(Window("-1d", "1d"), anchor=anchor, day_boundary="UTC")
        == "aligned"
    )
    assert (
        calendar_average_alignment(Window("-1h", "1d"), anchor=anchor, day_boundary="UTC")
        == "unaligned"
    )
    assert (
        calendar_average_alignment(
            Window("-1d", "1d"), anchor=anchor.replace(hour=1), day_boundary="UTC"
        )
        == "unaligned"
    )


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("DUCKDB", True),
        ("POSTGRES", True),
        ("BIGQUERY", True),
        ("SNOWFLAKE", True),
        ("unregistered", False),
    ],
)
def test_event_resolution_retains_registered_feature_boundary(name, expected):
    assert supports_event_resolution(name) is expected
