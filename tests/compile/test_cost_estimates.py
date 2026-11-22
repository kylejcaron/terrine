# ruff: noqa: F811
from collections import Counter
from datetime import UTC, datetime

import pytest

from terrine import Budget, Design, EventAnchor, FixedDate, agg, preview
from terrine.data import CoverageSummary
from terrine.errors import TerrineError
from terrine.spec import (
    BinnedMetricState,
    CalendarComparisonSpec,
    CohortSpec,
    EventSequence,
    Initiation,
    Schedule,
    StateObservations,
    StatePanel,
    Trajectory,
)
from terrine.windows import Window
from tests.compile.test_columns import column_model  # noqa: F401


def test_compiler_preview_preserves_unknowns_without_source_execution(column_model, monkeypatch):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1s", "0s"))},
    )
    reads = []

    def forbidden(*args, **kwargs):
        reads.append(True)
        raise AssertionError("metadata preview must not execute source rows")

    monkeypatch.setattr(column_model.con, "to_pyarrow", forbidden)
    report = preview(design, column_model, budget=Budget(max_scan_bytes=1))
    assert report.scan_bytes is None and report.output_bytes is None and report.planned_rows is None
    assert report.scan_reason == "backend_scan_bound_unavailable"
    assert report.budget_warnings
    (use,) = report.source_reads
    assert (use.family, use.source) == ("local_event", "events")
    assert use.event_span is not None
    assert use.event_span.start == datetime(2025, 1, 2, 11, 59, 59, tzinfo=UTC)
    assert use.event_span.end_exclusive == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert use.physical_key is None and use.reason == "layout_unverified"
    with pytest.raises(TerrineError) as error:
        preview(design, column_model, budget=Budget(max_scan_bytes=1), bounded_count=1)
    assert error.value.code == "terrine.budget.scan_unknown"
    assert reads == []


def test_event_derived_origin_has_no_literal_scan_span(column_model):
    design = Design(
        "user",
        EventAnchor("amount", "first"),
        features={"total": agg("total", window=("-1s", "0s"))},
    )
    (use,) = preview(design, column_model).source_reads
    assert use.source == "events" and use.family == "local_event"
    assert use.event_span is None
    assert use.physical_key is None and use.reason == "event_bounds_unknown"


def _span(start, end):
    return CoverageSummary(
        datetime.fromisoformat(start).replace(tzinfo=UTC),
        datetime.fromisoformat(end).replace(tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("step", "last_day", "last_t0", "state_reads"),
    [
        (
            "1d",
            "2025-01-03",
            "2025-01-03T00:00",
            [("local_day", "2025-01-01", "2025-01-04")],
        ),
        (
            "12h",
            "2025-01-02",
            "2025-01-02T12:00",
            [
                ("local_event", "2025-01-01T12:00", "2025-01-02T12:00"),
                ("local_event", "2025-01-02T12:00", "2025-01-03"),
            ],
        ),
    ],
    ids=["compact", "separate-endpoint"],
)
def test_binned_panel_preview_reports_each_result_read(
    column_model, step, last_day, last_t0, state_reads
):
    design = Design(
        "user",
        Schedule(step, "2025-01-02", last_day),
        features={"trailing_hour": agg("total", window=("-1h", "0s"))},
        shape=StatePanel(
            BinnedMetricState("total", (0,), ("none", "some")),
            periods=2,
            baseline_features={"baseline": agg("total", window=("-2d", "0s"))},
        ),
    )
    uses = preview(design, column_model).source_reads
    assert Counter((use.family, use.event_span) for use in uses) == Counter(
        [
            ("local_event", _span("2025-01-01T23:00", last_t0)),
            *((family, _span(start, end)) for family, start, end in state_reads),
            ("local_event", _span("2024-12-31", "2025-01-02")),
        ]
    )
    assert {(use.source, use.physical_key, use.reason) for use in uses} == {
        ("events", None, "layout_unverified")
    }


@pytest.mark.parametrize(
    "shape",
    [
        CohortSpec("amount", Trajectory("total", "1d", periods=2)),
        EventSequence("amount", Window("0s", "1h"), max_events=5),
    ],
    ids=["ltv", "event-sequence"],
)
def test_event_anchored_wrapper_preview_keeps_unknown_additive_bounds(column_model, shape):
    design = Design(
        "user",
        EventAnchor("amount", "first"),
        features={"total": agg("total", window=("-1d", "0s"))},
        shape=shape,
    )
    (use,) = preview(design, column_model).source_reads
    assert (use.source, use.family) == ("events", "local_event")
    assert (use.event_span, use.physical_key, use.reason) == (None, None, "event_bounds_unknown")


@pytest.mark.parametrize(
    ("shape", "baseline"),
    [
        (
            CalendarComparisonSpec(
                Initiation("amount", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
                "never_treated",
                -1,
                (0,),
            ),
            [],
        ),
        (
            StatePanel(
                StateObservations("tier", ("old", "new")),
                periods=2,
                baseline_features={"baseline": agg("total", window=("-2d", "0s"))},
            ),
            [_span("2024-12-30", "2025-01-01")],
        ),
    ],
    ids=["calendar-comparison", "observed-panel"],
)
def test_scheduled_wrapper_preview_reports_additives_not_direct_reads(
    column_model, shape, baseline
):
    design = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-02"),
        features={"total": agg("total", window=("-1d", "0s"))},
        shape=shape,
    )
    uses = preview(design, column_model).source_reads
    assert Counter(use.event_span for use in uses) == Counter(
        [_span("2024-12-31", "2025-01-02"), *baseline]
    )
    assert all(use.source == "events" and use.physical_key is None for use in uses)
