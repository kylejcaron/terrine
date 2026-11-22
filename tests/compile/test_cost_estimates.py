# ruff: noqa: F811
from collections import Counter
from datetime import UTC, datetime

import pytest

from terrine import Budget, Design, EventAnchor, FixedDate, agg, asof, eq, preview
from terrine.data import Coverage, CoverageSummary, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
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
from tests.fixtures.readme import readme_model


def test_preview_distinguishes_roster_values_from_membership_and_observation(
    column_model, monkeypatch
):
    anchor = FixedDate("2025-02-01")
    features = {"previous": agg("total", window=("-1d", "0s"))}
    con = column_model.con
    con.raw_sql(
        "CREATE TABLE bounded_users AS SELECT id, NULL::TIMESTAMPTZ AS joined, "
        "NULL::TIMESTAMPTZ AS followed_until, 'active' AS status, 'private' AS unused FROM users"
    )
    bounded = Model.from_catalog(
        column_model.catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
        units={
            "user": UnitSource(
                "bounded_users",
                "id",
                entered_at="joined",
                observed_until="followed_until",
                attributes=("status", "unused"),
            )
        },
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("metadata preview must not execute source rows")

    monkeypatch.setattr(con, "to_pyarrow", forbidden)
    design = Design(
        "user", anchor, features=features, eligibility={"active": eq(asof("status"), "active")}
    )
    report = preview(design, bounded)
    assert report.coverage["events"].end_exclusive.date().isoformat() == "2025-02-02"
    (use,) = report.assumptions.sources
    assert (use.source, use.role, use.output, use.time_axis) == (
        "events",
        "feature",
        "previous",
        "event_time",
    )
    assert use.knowledge == use.historical_versions == "unknown"
    assert report.assumptions.roster.scope == "bounded"
    assert report.assumptions.roster.entered_at
    assert not report.assumptions.roster.exited_at
    assert not report.assumptions.roster.observed_from
    assert report.assumptions.roster.observed_until
    assert [
        (use.attribute, use.role, use.output, use.time_axis, use.knowledge, use.historical_versions)
        for use in report.assumptions.roster_attributes
    ] == [("status", "eligibility", "active", "static", "unknown", "unknown")]
    unbounded = preview(Design("user", anchor, features=features), column_model)
    assert unbounded.assumptions.roster.scope == "current_unbounded"


def test_event_anchor_without_roster_reports_no_historical_membership(readme_con):
    from terrine import EventAnchor

    roster_model = readme_model(readme_con)
    model = Model.from_catalog(
        roster_model.catalog, con=readme_con, readiness=roster_model.readiness, units={}
    )
    event = next(iter(model.sources["orders"].events))
    report = preview(Design("customer", EventAnchor(event)), model)
    assert report.assumptions.roster.scope == "event_anchored_no_roster"
    assert [(use.source, use.role) for use in report.assumptions.sources] == [("orders", "anchor")]


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
