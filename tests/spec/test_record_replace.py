import dataclasses
from datetime import UTC, date, datetime

import pytest

from terrine.errors import TerrineError
from terrine.spec import (
    CohortGrouping,
    Design,
    EventAnchor,
    EventPanelSpec,
    FunnelSpec,
    Initiation,
    LifeTable,
    Schedule,
    SequentialSpec,
    SessionSpec,
    TimeToEvent,
    Trajectory,
    Treatment,
    agg,
    asof,
    from_yaml,
    retention,
    to_yaml,
)
from terrine.spec.expressions import AttributeAsOf, RetentionMetric
from terrine.windows import Duration, Window

_HISTORY = datetime(2024, 1, 1, tzinfo=UTC)

RECORDS = (
    Window("-30d", "-1h"),
    EventAnchor("orders", offset="1d"),
    Treatment("plan", at="1d"),
    Design("customer", EventAnchor("orders", offset="2h"), gap="1w"),
    SequentialSpec(Initiation("orders", history_start=_HISTORY), None, "1d", "-2d"),
    EventPanelSpec((-1, 0, 1), "1d", -1),
    SessionSpec("30m"),
    FunnelSpec("kind", ("view", "buy"), within="2h"),
    Trajectory("spend", "30d", 2),
    CohortGrouping("30d", datetime(2025, 1, 1, tzinfo=UTC)),
    LifeTable(TimeToEvent("churn"), "7d"),
    asof("plan", before="-1d"),
    retention("orders", label_property="kind", target_event="paid", thresholds=(1,), horizon="30d"),
    Schedule("1mo", "2025-01-31", "2025-06-30"),
)
OTHER_FIELD = {
    Window: ("end", Duration(3600)),
    EventAnchor: ("event", "signup"),
    Treatment: ("known_at_t0", True),
    Design: ("unit", "account"),
    SequentialSpec: ("treatment", Initiation("visits", history_start=_HISTORY)),
    EventPanelSpec: ("reference_period", 0),
    SessionSpec: ("gap", Duration(60)),
    FunnelSpec: ("steps", ("visit", "buy")),
    Trajectory: ("periods", 3),
    CohortGrouping: ("statistic", "total"),
    LifeTable: ("group_by", ("segment",)),
    AttributeAsOf: ("attribute", "tier"),
    RetentionMetric: ("target_event", "refund"),
    Schedule: ("end", date(2025, 7, 31)),
}


@pytest.mark.parametrize("record", RECORDS, ids=lambda r: type(r).__name__)
def test_replace_round_trips_and_accepts_other_fields(record):
    assert dataclasses.replace(record) == record
    name, value = OTHER_FIELD[type(record)]
    assert getattr(dataclasses.replace(record, **{name: value}), name) == value


def test_design_document_round_trip_preserves_durations():
    design = Design(
        "customer",
        Schedule("1mo", "2025-01-01", "2025-06-01"),
        gap="1w",
        features={"spend_30d": agg("spend", window=("-30d", "0s"))},
    )
    assert from_yaml(to_yaml(design)) == design
    lines = {line.strip() for line in to_yaml(design).splitlines()}
    assert {"gap: 1w", "every: 1mo", "start: -30d", "end: 0s"} <= lines


# Saved by to_yaml before durations were written as text.
VERSION_3_DOCUMENT = """\
design:
  administrative_cutoff: null
  anchor:
    end:
      type: date
      value: '2025-02-03'
    every: 604800
    months: null
    start:
      type: date
      value: '2025-01-06'
    type: Schedule
  anchor_range: null
  eligibility: {}
  features:
    spend_30d:
      metric: spend
      type: MetricAgg
      window:
        end: 0
        start: -2592000
        type: Window
  gap: 86400
  outcomes: {}
  sample: null
  shape: cross_section
  treatment:
    at: 86400
    attribute: plan
    known_at_t0: false
    type: Treatment
  unit: customer
schema_version: 3
"""


def test_version_3_design_document_refuses():
    with pytest.raises(TerrineError) as error:
        from_yaml(VERSION_3_DOCUMENT)
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == "unsupported_schema_version"
    assert error.value.context["schema_version"] == 3
